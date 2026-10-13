package reconciler

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/contract"
	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/jenkins"
)

// Collecting and Uploading, in the enhancement's Reconciler State Machine:
//
//	Collecting --> Uploading: artifacts fetched
//	Collecting --> Error: fetch failed
//	Uploading --> Complete: S3 write ok
//	Uploading --> Error: S3 write failed
//
// A failed Jenkins read or S3 write is first treated as transient: the job stays
// in flight and collection is retried on the next tick, up to
// maxCollectAttempts. Once retries are used up, the result is written with status
// error and the message names what is missing and the build's Jenkins result. An
// archived artifact that is deliberately not copied (too large, unsafe path, or
// named like the console log) also makes the result error, without retries: the
// contract's artifacts would be incomplete, and a pass is never reported for a
// run whose outputs did not all reach S3.

// maxCollectAttempts is how many ticks a finished build's outputs are
// re-collected after a transient Jenkins or S3 failure before an error result
// is written.
const maxCollectAttempts = 3

// maxListedSkips bounds how many artifacts are named per problem in the message.
const maxListedSkips = 5

// missingKind is why an archived artifact did not reach S3.
type missingKind int

const (
	missingFetch  missingKind = iota // download from Jenkins failed (transient)
	missingUpload                    // S3 upload failed (transient)
	missingSkip                      // not copied by policy (size cap, unsafe or reserved name)
)

type missing struct {
	path, why string
	kind      missingKind
}

// collected is what was copied from a Jenkins build to S3, and what was not.
type collected struct {
	names      []string  // artifact names actually uploaded, for result.json
	missing    []missing // archived artifacts not uploaded
	listFailed bool      // the artifact list could not be read from Jenkins
	logMissing string    // why build-log.txt was not uploaded; "" if it was
	notes      []string  // informational (e.g. log truncation); do not affect status
}

func (c *collected) miss(path, why string, kind missingKind) {
	c.missing = append(c.missing, missing{contract.Truncate(path, 100), why, kind})
}

// transient reports whether some failure may succeed on a retry.
func (c collected) transient() bool {
	if c.listFailed || c.logMissing != "" {
		return true
	}
	for _, m := range c.missing {
		if m.kind != missingSkip {
			return true
		}
	}
	return false
}

// complete reports whether every archived artifact and the console log reached S3.
func (c collected) complete() bool {
	return !c.listFailed && c.logMissing == "" && len(c.missing) == 0
}

// problems describes what is missing, one entry per kind of problem.
func (c collected) problems() []string {
	var out []string
	if c.listFailed {
		out = append(out, "the artifact list could not be fetched from Jenkins")
	}
	for _, g := range []struct {
		kind    missingKind
		what    string
		withWhy bool
	}{
		{missingFetch, "could not be fetched from Jenkins", false},
		{missingUpload, "could not be uploaded to S3", false},
		{missingSkip, "skipped", true},
	} {
		var items []string
		n := 0
		for _, m := range c.missing {
			if m.kind != g.kind {
				continue
			}
			n++
			if len(items) < maxListedSkips {
				item := fmt.Sprintf("%q", m.path)
				if g.withWhy {
					item += " (" + m.why + ")"
				}
				items = append(items, item)
			}
		}
		if n == 0 {
			continue
		}
		noun := "artifacts"
		if n == 1 {
			noun = "artifact"
		}
		s := fmt.Sprintf("%d %s %s: %s", n, noun, g.what, strings.Join(items, ", "))
		if n > maxListedSkips {
			s += fmt.Sprintf(", and %d more", n-maxListedSkips)
		}
		out = append(out, s)
	}
	if c.logMissing != "" {
		out = append(out, contract.BuildLogArtifact+" "+c.logMissing)
	}
	return out
}

// message returns base followed by what is missing (introduced by sep) and any
// informational notes.
func (c collected) message(base, sep string) string {
	msg := base
	if p := c.problems(); len(p) > 0 {
		msg += sep + strings.Join(p, "; ")
	}
	for _, n := range c.notes {
		msg += "; " + n
	}
	return msg
}

// collectOutputs copies a build's archived artifacts (if withArtifacts) and its
// console log to S3. Failures are recorded for the caller, which decides the status.
// t.sent (name -> size) holds what earlier attempts already put in S3; those are
// skipped, and every upload is added to it. t.logNote keeps the log's truncation
// note across retries, when the log is already in S3.
func (r *Reconciler) collectOutputs(ctx context.Context, id string, t *tracked, withArtifacts bool) collected {
	var c collected
	if t.sent == nil {
		t.sent = map[string]int64{}
	}
	if withArtifacts {
		r.uploadArtifacts(ctx, id, t.buildURL, &c, t.sent)
	}
	if slices.ContainsFunc(c.missing, func(m missing) bool { return m.kind == missingUpload }) {
		c.logMissing = "could not be uploaded to S3" // S3 just failed; do not wait on it again
		return c
	}
	r.uploadBuildLog(ctx, id, &c, t)
	return c
}

// uploadArtifacts copies every Jenkins artifact to S3. Each is read
// with a limit (MaxArtifactBytes, or what is left of MaxTotalArtifactBytes), so
// an oversized artifact is skipped without being held in memory.
func (r *Reconciler) uploadArtifacts(ctx context.Context, id, buildURL string, c *collected, sent map[string]int64) {
	paths, err := r.jenkins.ListArtifacts(ctx, buildURL)
	if err != nil {
		r.log.Error("list artifacts", "prowJobID", id, "err", err)
		c.listFailed = true
		return
	}
	if n := r.cfg.MaxArtifacts; len(paths) > n {
		r.log.Warn("too many Jenkins artifacts; skipping the extras", "prowJobID", id, "count", len(paths), "max", n)
		c.miss(fmt.Sprintf("%d artifacts beyond the first %d", len(paths)-n, n), fmt.Sprintf("MAX_ARTIFACTS (%d) reached", n), missingSkip)
		paths = paths[:n]
	}
	budget := r.cfg.MaxTotalArtifactBytes
	s3Failed := false
	for _, p := range paths {
		// Jenkins' relative path is kept under artifacts/; the shim copies
		// the prefix recursively, recreating the directories.
		if err := contract.ValidateArtifactName(p); err != nil {
			r.log.Warn("skipping Jenkins artifact with an unsafe path", "prowJobID", id, "err", err)
			c.miss(p, "unsafe path", missingSkip)
			continue
		}
		if p == contract.BuildLogArtifact {
			// Reserved for the console log, which the contract puts there.
			r.log.Warn("skipping Jenkins artifact that collides with the console log", "prowJobID", id, "path", p)
			c.miss(p, "name reserved for the console log", missingSkip)
			continue
		}
		if size, ok := sent[p]; ok { // uploaded by an earlier attempt
			budget -= size
			c.names = append(c.names, p)
			continue
		}
		if s3Failed {
			// S3 is likely down or hanging; each further upload would wait on it too.
			c.miss(p, "", missingUpload)
			continue
		}
		limit := min(r.cfg.MaxArtifactBytes, budget)
		if limit <= 0 {
			c.miss(p, fmt.Sprintf("MAX_TOTAL_ARTIFACT_BYTES (%d) reached", r.cfg.MaxTotalArtifactBytes), missingSkip)
			continue
		}
		data, err := r.jenkins.DownloadArtifact(ctx, buildURL, p, limit)
		if errors.Is(err, jenkins.ErrArtifactTooLarge) {
			r.log.Warn("skipping oversized Jenkins artifact", "prowJobID", id, "path", p, "limit", limit, "err", err)
			what := fmt.Sprintf("MAX_ARTIFACT_BYTES (%d)", r.cfg.MaxArtifactBytes)
			if limit < r.cfg.MaxArtifactBytes {
				what = fmt.Sprintf("the %d bytes left of MAX_TOTAL_ARTIFACT_BYTES (%d)", limit, r.cfg.MaxTotalArtifactBytes)
			}
			c.miss(p, "larger than "+what, missingSkip)
			continue
		}
		if err != nil {
			r.log.Error("download artifact", "prowJobID", id, "path", p, "err", err)
			c.miss(p, "", missingFetch)
			continue
		}
		budget -= int64(len(data))
		if err := r.s3.PutArtifact(ctx, id, p, data); err != nil {
			r.log.Error("upload artifact", "prowJobID", id, "name", p, "err", err)
			c.miss(p, "", missingUpload)
			s3Failed = true
			continue
		}
		sent[p] = int64(len(data))
		c.names = append(c.names, p)
	}
}

// uploadBuildLog copies the build's console log to artifacts/build-log.txt. A log
// longer than MaxLogBytes is cut to its tail, behind a notice saying so.
func (r *Reconciler) uploadBuildLog(ctx context.Context, id string, c *collected, t *tracked) {
	if _, ok := t.sent[contract.BuildLogArtifact]; ok {
		if t.logNote != "" {
			c.notes = append(c.notes, t.logNote)
		}
		c.names = append(c.names, contract.BuildLogArtifact)
		return
	}
	tail, total, err := r.jenkins.ConsoleText(ctx, t.buildURL, r.cfg.MaxLogBytes)
	if err != nil {
		r.log.Error("download console log", "prowJobID", id, "err", err)
		c.logMissing = "could not be fetched from Jenkins"
		return
	}
	data := tail
	if total > int64(len(tail)) {
		notice := fmt.Sprintf("[build log truncated by the reconciler: showing the last %d of %d bytes]\n", len(tail), total)
		data = append([]byte(notice), tail...)
		t.logNote = fmt.Sprintf("build log truncated to its last %d bytes", len(tail))
		c.notes = append(c.notes, t.logNote)
	}
	if err := r.s3.PutArtifact(ctx, id, contract.BuildLogArtifact, data); err != nil {
		r.log.Error("upload console log", "prowJobID", id, "err", err)
		c.logMissing = "could not be uploaded to S3"
		return
	}
	t.sent[contract.BuildLogArtifact] = int64(len(data))
	c.names = append(c.names, contract.BuildLogArtifact)
}
