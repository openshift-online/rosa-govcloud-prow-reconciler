// Package reconciler contains the control loop that bridges Prow and Jenkins,
// following the Reconciler State Machine in the enhancement
// (https://github.com/openshift-online/rosa-enhancements/blob/main/enhancements/sre-tooling/rosa-govcloud-prow-jenkins-bridge.md).
//
// Before the first tick, in-flight state is rebuilt from Jenkins (recover.go).
// Then each tick it:
//  1. Polls the Prow feed and, for every active matching ProwJob it has not seen,
//     adopts an existing Jenkins run, rejects the ProwJob with an error result
//     (allowlist, age), or triggers a Jenkins build (Discovered,
//     Rejected, Triggered).
//  2. Advances every in-flight run: queue -> running -> finished (Monitoring),
//     then copies the console log and artifacts to S3 and writes result.json
//     last (Collecting, Uploading, Complete or Error; artifacts.go). A run that
//     times out, or whose ProwJob is no longer active, is stopped in Jenkins and
//     gets an error result (Timeout; stop.go).
//  3. Retries stop requests until Jenkins confirms the run is no longer active.
//
// A ProwJob stays active in the feed until the CI shim reads result.json and Prow
// updates it, so the reconciler keeps a "done" record for every ProwJob it has
// resolved and never triggers or writes for it again while Prow still lists it.
//
// Run shuts down gracefully: see its doc comment. The loop is single-goroutine,
// so its maps need no locking.
package reconciler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"time"

	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/config"
	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/contract"
	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/jenkins"
	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/prow"
	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/s3"
)

// tracked is the reconciler's memory of one job it has acted on.
type tracked struct {
	jenkinsJob string // for queue-item lookups; set at trigger or adopt
	queueURL   string
	buildURL   string // "" while the run is queued
	// deadline is when the reconciler gives up on the build: MaxBuildTime after
	// the earlier of the trigger (or recovered run's start) and the ProwJob's start.
	// It also bounds retryResult; after a timeout, abandon moves it MaxBuildTime
	// past the timeout so the error result keeps being retried.
	deadline time.Time
	// absentPolls counts consecutive successful feed polls that did not list
	// this ProwJob.
	absentPolls int
	// queueMisses counts consecutive lookups that found no run for a queue item
	// that 404s.
	queueMisses int
	// collectAttempts counts attempts at copying a finished build's outputs.
	collectAttempts int
	// sent maps each artifact name already in S3 to its size, so retries skip it.
	sent map[string]int64
	// logNote is the build log's truncation note, kept for retries that skip the upload.
	logNote string
	// result is the built result.json once collection is complete; from then on
	// only its PutResult is retried.
	result *contract.Result
	// abandon, if set, is why the run must be stopped: the ProwJob is no longer
	// active in Prow, so nothing is waiting for its result.
	abandon string
}

// Reconciler wires the three clients together and holds in-flight state.
type Reconciler struct {
	cfg     *config.Config
	prow    *prow.Client
	jenkins *jenkins.Client
	s3      s3.Writer
	log     *slog.Logger
	now     func() time.Time // injectable clock for tests

	// inflight is keyed by PROW_JOB_ID: every job triggered or adopted whose
	// result.json is not yet written or given up on. State is in memory only;
	// after a restart it is rebuilt from Jenkins (see recover.go), since the
	// reconciler's S3 identity is write-only.
	inflight map[string]*tracked

	// done is keyed by PROW_JOB_ID (or, for ignored jobs, the raw ProwJob name) and
	// records ProwJobs that reached a terminal state here: result written (or given
	// up on), rejected, or ignored. A done job is never re-triggered or re-written. Records are pruned
	// once the feed reports the job non-active, or after it has been absent from the
	// feed for MaxBuildTime, which bounds memory to roughly the active job set.
	done map[string]*doneRecord

	// recovered holds Jenkins runs found at startup (and by uncertain-trigger
	// checks), keyed by PROW_JOB_ID. An active ProwJob with an entry here is adopted
	// instead of triggered. Pruned like done.
	recovered map[string]recoveredRun

	// failedTriggers holds ProwJobs whose last trigger attempt failed but will be
	// retried. Pruned like recovered.
	failedTriggers map[string]*failedTrigger

	// stopping holds Jenkins runs the reconciler has given up on and asked Jenkins
	// to stop, until Jenkins confirms they are no longer running (see stop.go).
	stopping map[string]*stopTask

	// stalled is set when a Jenkins call times out during a discover, advance or
	// stop pass. The pass stops there and the rest waits for the next tick: a slow
	// Jenkins would otherwise cost one timeout per job and starve the loop.
	stalled bool

	// quit is Run's context. Once it is done the reconciler is shutting down: the
	// current step finishes (on a separate context, see Run) but nothing new is
	// started. Nil outside Run.
	quit context.Context
}

// failedTrigger is the memory of a ProwJob's failed trigger attempts.
type failedTrigger struct {
	at time.Time // last failure
	// unsure means the last attempt may still have queued a build, so Jenkins is
	// checked before triggering again.
	unsure bool
	// authFails counts consecutive 401/403 answers.
	authFails int
}

// doneRecord is the memory of a ProwJob the reconciler has finished with.
type doneRecord struct {
	outcome  string    // human-readable, for logs
	lastSeen time.Time // last time the feed listed the job as active
}

// New constructs a Reconciler.
func New(cfg *config.Config, p *prow.Client, j *jenkins.Client, w s3.Writer, log *slog.Logger) *Reconciler {
	return &Reconciler{
		cfg:            cfg,
		prow:           p,
		jenkins:        j,
		s3:             w,
		log:            log,
		now:            time.Now,
		inflight:       make(map[string]*tracked),
		done:           make(map[string]*doneRecord),
		recovered:      make(map[string]recoveredRun),
		failedTriggers: make(map[string]*failedTrigger),
		stopping:       make(map[string]*stopTask),
	}
}

// ErrShutdownTimeout is returned by Run when the step in progress at shutdown
// did not finish within ShutdownTimeout and was cut off.
var ErrShutdownTimeout = errors.New("shutdown timeout expired before the in-progress step finished")

// Run polls on cfg.PollInterval until ctx is cancelled, then shuts down
// gracefully: no new trigger, write, or job is started, while the step in
// progress (a Jenkins call or S3 upload, a result.json write) runs on a
// detached context for up to cfg.ShutdownTimeout. Jenkins builds are not
// stopped: restart recovery (recover.go) adopts them. Since result.json is always
// written after the artifacts, a step cut off by the timeout can leave artifacts
// without result.json, which the next run completes, but never the reverse. Run
// returns nil on a clean shutdown and ErrShutdownTimeout if the step had to be
// cut off.
func (r *Reconciler) Run(ctx context.Context) error {
	r.log.Info("reconciler starting",
		"prowFeed", r.cfg.ProwJobsURL,
		"jenkins", r.cfg.JenkinsURL,
		"bucket", r.cfg.S3Bucket,
		"pollInterval", r.cfg.PollInterval,
		"allowlist", r.cfg.JobAllowlist,
	)
	// Never trigger before in-flight state has been rebuilt from Jenkins: doing so
	// blind would start a second build for every ProwJob already running.
	if !r.recoverUntilDone(ctx) {
		r.log.Info("reconciler stopped", "reason", ctx.Err())
		return nil
	}

	work, cancelWork := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelWork()
	cutOff := make(chan struct{})
	go func() {
		select {
		case <-work.Done():
			return
		case <-ctx.Done():
		}
		t := time.NewTimer(r.cfg.ShutdownTimeout)
		defer t.Stop()
		select {
		case <-work.Done():
		case <-t.C:
			close(cutOff)
			cancelWork()
		}
	}()
	r.quit = ctx
	defer func() { r.quit = nil }()

	t := time.NewTicker(r.cfg.PollInterval)
	defer t.Stop()

	// A tick that overran the interval leaves a tick queued; drop it so the next
	// feed poll waits a full interval instead of following at once.
	tickOnce := func() {
		start := time.Now()
		r.tick(work)
		if time.Since(start) > r.cfg.PollInterval {
			t.Reset(r.cfg.PollInterval)
		}
	}
	tickOnce() // run once immediately
	for {
		select {
		case <-ctx.Done():
			select {
			case <-cutOff:
				r.log.Error("reconciler stopped; the in-progress step was cut off", "shutdownTimeout", r.cfg.ShutdownTimeout)
				return ErrShutdownTimeout
			default:
			}
			r.log.Info("reconciler stopped", "reason", ctx.Err())
			return nil
		case <-t.C:
			tickOnce()
		}
	}
}

// quitting reports whether Run is shutting down.
func (r *Reconciler) quitting() bool {
	return r.quit != nil && r.quit.Err() != nil
}

// tick runs one full discover + advance pass. Errors are logged, not fatal —
// a transient Prow/Jenkins hiccup shouldn't kill the loop. Each phase stops
// between jobs once Run is shutting down.
func (r *Reconciler) tick(ctx context.Context) {
	r.discover(ctx)
	r.advance(ctx)
	r.advanceStops(ctx)
}

// discover polls Prow and triggers Jenkins for newly-seen active jobs.
func (r *Reconciler) discover(ctx context.Context) {
	if r.quitting() {
		return
	}
	// The feed poll only reads, so it is abandoned as soon as shutdown starts.
	pollCtx := ctx
	if r.quit != nil {
		pollCtx = r.quit
	}
	feed, err := r.prow.ListFeed(pollCtx, r.cfg.ExecutorLabel)
	if r.quitting() {
		return
	}
	if err != nil {
		// Nothing is inferred from a failed poll: in particular, no run is
		// treated as gone from the feed.
		r.log.Error("poll prow", "err", err)
		return
	}
	jobs := feed.Jobs
	r.pruneDone(jobs)
	r.checkInflight(feed)
	r.stalled = false
	for _, j := range jobs {
		if r.quitting() {
			r.log.Info("shutting down; not starting new Jenkins builds")
			return
		}
		if r.stalled {
			r.log.Warn("Jenkins timed out; remaining ProwJobs wait for the next tick")
			return
		}
		if !j.Active() {
			continue
		}
		if d, ok := r.done[j.ID]; ok {
			d.lastSeen = r.now()
			continue // already resolved; Prow just hasn't caught up yet
		}
		if !prow.ValidProwJobID(j.ID) {
			// Without a trustworthy ID there is no safe S3 key to write a result to.
			r.log.Warn("ProwJob name is not a canonical UUID; ignoring", "prowJobID", contract.Truncate(j.ID, 80))
			r.markDone(j.ID, "ignored: invalid ProwJob name")
			continue
		}
		if _, seen := r.inflight[j.ID]; seen {
			continue
		}
		if r.adopt(j) {
			continue
		}
		if !slices.Contains(r.cfg.JobAllowlist, j.JenkinsJob) {
			// Security: a crafted annotation must not run an arbitrary job. Surface
			// the refusal to Prow so the run fails fast instead of timing out.
			r.log.Warn("jenkins job not in allowlist; rejecting", "prowJobID", j.ID, "jenkinsJob", contract.Truncate(j.JenkinsJob, 80))
			r.finishWithError(ctx, j.ID, fmt.Sprintf("jenkins job %q not in allowlist", contract.Truncate(j.JenkinsJob, 80)))
			continue
		}
		if f := r.failedTriggers[j.ID]; f != nil && f.unsure {
			// The previous trigger may have reached Jenkins. Only retry once
			// Jenkins confirms there is no run for this ProwJob.
			if !r.checkJenkins(ctx, j) || r.adopt(j) {
				continue
			}
			delete(r.failedTriggers, j.ID)
		}
		if s := j.StartTime; !s.IsZero() && r.now().Sub(s) > r.cfg.MaxBuildTime {
			// The CI shim started with the ProwJob and is about to give up (or
			// already has); a build started now could not report in time.
			r.log.Warn("ProwJob older than MaxBuildTime; not triggering", "prowJobID", j.ID, "age", r.now().Sub(s))
			r.finishWithError(ctx, j.ID, fmt.Sprintf("ProwJob started %s ago, more than MAX_BUILD_TIME (%s); Jenkins not triggered",
				r.now().Sub(s).Round(time.Second), r.cfg.MaxBuildTime))
			continue
		}
		r.trigger(ctx, j)
	}
}

// deadline returns when to give up on a run of j that started at startedAt. The
// clock starts at the ProwJob's start when that is earlier, because that is (at
// the latest) when the CI shim's own timeout starts: a trigger delayed by feed lag
// or a reconciler outage must not push the deadline past the shim's.
func (r *Reconciler) deadline(j prow.Job, startedAt time.Time) time.Time {
	base := startedAt
	if s := j.StartTime; !s.IsZero() && s.Before(base) {
		base = s
	}
	return base.Add(r.cfg.MaxBuildTime)
}

// checkInflight marks in-flight runs whose ProwJob is no longer waiting for them:
// the feed reports it in a terminal state (aborted, or failed because the shim
// gave up), or it has been missing from FeedAbsentPolls consecutive successful
// polls. Only a non-empty feed counts as evidence of absence; an empty one is
// more likely a Deck problem than every ProwJob vanishing, and breaks the streak.
func (r *Reconciler) checkInflight(feed *prow.Feed) {
	byID := make(map[string]prow.Job, len(feed.Jobs))
	for _, j := range feed.Jobs {
		byID[j.ID] = j
	}
	for id, t := range r.inflight {
		if t.abandon != "" {
			continue
		}
		j, present := byID[id]
		switch {
		case present && !j.Active():
			t.abandon = fmt.Sprintf("ProwJob is %s in Prow; stopping the Jenkins run", contract.Truncate(j.State, 32))
		case present:
			t.absentPolls = 0
		case feed.Items > 0:
			t.absentPolls++
			if t.absentPolls >= r.cfg.FeedAbsentPolls {
				t.abandon = fmt.Sprintf("ProwJob missing from the Prow feed for %d consecutive polls; stopping the Jenkins run", t.absentPolls)
			}
		default:
			t.absentPolls = 0 // empty feed: no evidence either way, restart the streak
		}
		if t.abandon != "" {
			r.log.Warn("ProwJob no longer active; stopping its Jenkins run", "prowJobID", id, "state", contract.Truncate(j.State, 32), "absentPolls", t.absentPolls)
		}
	}
}

// markDone records that the reconciler is finished with a ProwJob.
func (r *Reconciler) markDone(id, outcome string) {
	delete(r.inflight, id)
	delete(r.recovered, id)
	delete(r.failedTriggers, id)
	r.done[id] = &doneRecord{outcome: outcome, lastSeen: r.now()}
}

// pruneDone forgets done records the feed no longer needs us to remember: jobs
// it reports as non-active (terminal ProwJob states never become active again),
// and jobs absent from the feed for longer than MaxBuildTime. The grace period
// keeps a briefly incomplete feed from causing a re-trigger.
func (r *Reconciler) pruneDone(jobs []prow.Job) {
	inactive := make(map[string]bool, len(jobs))
	present := make(map[string]bool, len(jobs))
	for _, j := range jobs {
		present[j.ID] = true
		if !j.Active() {
			inactive[j.ID] = true
		}
	}
	// expired: finished in Prow, or absent from the feed and last seen more
	// than MaxBuildTime ago.
	expired := func(id string, at time.Time) bool {
		return inactive[id] || (!present[id] && r.now().Sub(at) > r.cfg.MaxBuildTime)
	}
	for id, d := range r.done {
		if expired(id, d.lastSeen) {
			delete(r.done, id)
		}
	}
	for id, rr := range r.recovered {
		if inactive[id] && (rr.run.Building || rr.run.BuildURL == "") {
			// Found at startup, still queued or building, but its ProwJob has
			// finished: an orphan (e.g. a stop that was pending at shutdown).
			r.log.Warn("recovered Jenkins run belongs to a finished ProwJob; stopping it", "prowJobID", id)
			r.stopping[id] = &stopTask{tracked: &tracked{queueURL: rr.run.QueueURL, buildURL: rr.run.BuildURL, jenkinsJob: rr.jenkinsJob}, reason: "ProwJob finished in Prow"}
		}
		if expired(id, rr.foundAt) {
			delete(r.recovered, id)
		}
	}
	for id, f := range r.failedTriggers {
		if expired(id, f.at) {
			delete(r.failedTriggers, id)
		}
	}
}

// maxAuthFails is how many consecutive 401/403 answers to a trigger fail the ProwJob.
const maxAuthFails = 3

func (r *Reconciler) trigger(ctx context.Context, j prow.Job) {
	queueURL, err := r.jenkins.TriggerBuild(ctx, j.JenkinsJob, map[string]string{config.ReservedParam: j.ID})
	if err != nil {
		r.log.Error("trigger jenkins", "prowJobID", j.ID, "jenkinsJob", j.JenkinsJob, "err", err)
		f := r.failedTriggers[j.ID]
		if f == nil {
			f = &failedTrigger{}
			r.failedTriggers[j.ID] = f
		}
		f.at = r.now()
		// A 4xx means Jenkins refused the request, so nothing was queued and a retry
		// would be refused again. Only the code goes into the (public) result.
		var se *jenkins.StatusError
		var msg string
		switch {
		case errors.As(err, &se) && se.Terminal():
			msg = fmt.Sprintf("Jenkins rejected the trigger with HTTP %d", se.Code)
		case se != nil && (se.Code == http.StatusUnauthorized || se.Code == http.StatusForbidden):
			// A rotated or briefly rejected token: retry, but not forever. Nothing
			// was queued, so the retry needs no Jenkins check (which would also be
			// refused).
			if f.authFails++; f.authFails < maxAuthFails {
				return
			}
			msg = fmt.Sprintf("Jenkins rejected the trigger with HTTP %d after %d attempts", se.Code, maxAuthFails)
		default:
			f.unsure = true // retried next tick, after checking Jenkins for it
			r.noteTimeout(err)
			return
		}
		t := &tracked{jenkinsJob: j.JenkinsJob, deadline: r.deadline(j, r.now())}
		r.inflight[j.ID] = t // so a failed write is retried, not the trigger
		r.failTracked(ctx, j.ID, t, msg)
		return
	}
	r.log.Info("triggered jenkins build", "prowJobID", j.ID, "jenkinsJob", j.JenkinsJob, "queue", queueURL)
	now := r.now()
	r.inflight[j.ID] = &tracked{jenkinsJob: j.JenkinsJob, queueURL: queueURL, deadline: r.deadline(j, now)}
}

// advance moves every in-flight job forward one step. A job whose ProwJob is no
// longer active (see checkInflight) is stopped. A job that is still queued
// or building (or could not be polled) past its deadline gets a timeout error;
// the check comes after the poll so a build that has already finished, such as
// one recovered after a restart, reports its real result. A timed-out run is
// stopped in Jenkins. MaxBuildTime is not checked against the CI shim's wait:
// if the shim gives up first, this result arrives after its own failure.
func (r *Reconciler) advance(ctx context.Context) {
	r.stalled = false
	for id, t := range r.inflight {
		if r.quitting() || r.stalled {
			return
		}
		if t.result != nil {
			r.retryResult(ctx, id, t)
			continue
		}
		if !r.step(ctx, id, t) {
			continue
		}
		if t.abandon != "" {
			r.abandon(ctx, id, t, t.abandon)
			continue
		}
		if r.now().After(t.deadline) {
			r.log.Warn("build exceeded MaxBuildTime; failing", "prowJobID", id, "deadline", t.deadline)
			r.abandon(ctx, id, t, fmt.Sprintf("timed out: no Jenkins result within MAX_BUILD_TIME (%s); stopping the Jenkins run", r.cfg.MaxBuildTime))
		}
	}
}

// step advances one job and reports whether it is still waiting on Jenkins.
func (r *Reconciler) step(ctx context.Context, id string, t *tracked) (waiting bool) {
	if t.buildURL == "" {
		buildURL, err := r.resolveQueue(ctx, id, t.jenkinsJob, &t.queueURL)
		switch {
		case errors.Is(err, jenkins.ErrQueueItemCancelled):
			r.log.Warn("jenkins queue item cancelled; failing", "prowJobID", id, "queue", t.queueURL)
			r.failTracked(ctx, id, t, "Jenkins queue item was cancelled before the build started")
			return false
		case errors.Is(err, errNoRun):
			t.queueMisses++
			r.log.Warn("queue item vanished and no run found", "prowJobID", id, "misses", t.queueMisses)
			if t.queueMisses >= maxQueueMisses {
				r.failTracked(ctx, id, t, "queue item disappeared and no build was found")
				return false
			}
			return true
		case err != nil:
			r.log.Error("resolve queue item", "prowJobID", id, "err", err)
			r.noteTimeout(err)
			return true // a failed lookup is not a miss
		}
		t.queueMisses = 0
		if buildURL == "" {
			return true // still queued
		}
		t.buildURL = buildURL
		r.log.Info("build started", "prowJobID", id, "build", buildURL)
		return true
	}
	b, err := r.jenkins.GetBuild(ctx, t.buildURL)
	if err != nil {
		r.log.Error("get build", "prowJobID", id, "err", err)
		r.noteTimeout(err)
		return true
	}
	if b.Building {
		return true // keep waiting
	}
	r.finish(ctx, id, t, b) // on S3 failure it stays tracked and retries
	return false
}

// noteTimeout stops the current pass if err is a Jenkins request timeout.
func (r *Reconciler) noteTimeout(err error) {
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		r.stalled = true
	}
}

// maxQueueMisses is how many consecutive lookups may find no run for a queue
// item Jenkins has forgotten (404) before the run is failed.
const maxQueueMisses = 3

// errNoRun is returned by resolveQueue when Jenkins has forgotten the queue
// item and no run carries the PROW_JOB_ID.
var errNoRun = errors.New("queue item vanished and no run found")

// resolveQueue resolves the queue item *queueURL to its build URL ("" while
// queued). Jenkins expires finished items (404), so the build may still exist:
// it is then looked up by PROW_JOB_ID like restart recovery does, and a run still
// queued under another URL updates *queueURL.
func (r *Reconciler) resolveQueue(ctx context.Context, id, jenkinsJob string, queueURL *string) (string, error) {
	buildURL, err := r.jenkins.ResolveQueueItem(ctx, *queueURL)
	var se *jenkins.StatusError
	if !errors.As(err, &se) || se.Code != http.StatusNotFound {
		return buildURL, err
	}
	runs, err := r.jenkins.ListRuns(ctx, jenkinsJob, config.ReservedParam, recoveryBuildDepth)
	if err != nil {
		return "", fmt.Errorf("look up run of vanished queue item: %w", err)
	}
	found := map[string]recoveredRun{}
	r.collect(found, jenkinsJob, runs, id)
	rr, ok := found[id]
	switch {
	case !ok:
		return "", errNoRun
	case rr.run.BuildURL != "":
		r.log.Info("build found for vanished queue item", "prowJobID", id, "build", rr.run.BuildURL)
	default:
		r.log.Info("vanished queue item still queued under another URL", "prowJobID", id, "queue", rr.run.QueueURL)
		*queueURL = rr.run.QueueURL
	}
	return rr.run.BuildURL, nil
}

// finish collects artifacts and the console log (Collecting/Uploading in the
// enhancement's state machine) and writes result.json. The status follows the
// Jenkins result only if every output reached S3; otherwise it is error, and the
// message gives the Jenkins result and what is missing (see artifacts.go).
// result.json's artifacts list names only what was uploaded. Artifacts are
// uploaded first: result.json is the shim's completion signal, so it must be the
// last object written.
func (r *Reconciler) finish(ctx context.Context, id string, t *tracked, b *jenkins.Build) {
	t.collectAttempts++
	out := r.collectOutputs(ctx, id, t, true)
	if ctx.Err() != nil {
		// Cut off by the shutdown timeout. Never write result.json for outputs
		// that only failed because of that; the next run collects them again.
		r.log.Warn("collection interrupted by shutdown; result not written", "prowJobID", id)
		return
	}
	if out.transient() && t.collectAttempts < maxCollectAttempts {
		// Transient Jenkins/S3 errors keep the job in flight (enhancement:
		// Reconciler State Machine).
		r.log.Warn("some outputs failed to copy; retrying next tick", "prowJobID", id, "attempt", t.collectAttempts)
		return
	}
	status := contract.StatusForJenkinsResult(b.Result)
	if !out.complete() {
		status = contract.StatusError
		r.log.Error("build outputs incomplete; writing an error result", "prowJobID", id, "jenkinsResult", contract.Truncate(b.Result, 32), "problems", out.problems())
	}
	result := contract.Result{
		Status:       status,
		JenkinsBuild: t.buildURL,
		Artifacts:    out.names,
		Message:      out.message(finishedMessage(b), ", but "),
	}
	if b.DurationMS > 0 {
		result.Duration = (time.Duration(b.DurationMS) * time.Millisecond).String()
	}
	t.result = &result
	r.putResult(ctx, id, t)
}

// putResult writes the held result.json and reports whether it succeeded; on
// failure the job stays tracked and advance retries just this write (see retryResult).
func (r *Reconciler) putResult(ctx context.Context, id string, t *tracked) bool {
	if err := r.s3.PutResult(ctx, id, *t.result); err != nil {
		r.log.Error("write result.json", "prowJobID", id, "err", err)
		return false
	}
	r.log.Info("wrote result", "prowJobID", id, "status", t.result.Status, "artifacts", len(t.result.Artifacts))
	r.markDone(id, "result written: "+t.result.Status)
	return true
}

// retryResult retries only the result.json write. If S3 keeps failing until the
// ProwJob is no longer active or the deadline passes (for a timeout, a fresh
// MaxBuildTime from the timeout), nothing is waiting for the result any more:
// log it and stop.
func (r *Reconciler) retryResult(ctx context.Context, id string, t *tracked) {
	if r.putResult(ctx, id, t) || ctx.Err() != nil {
		return
	}
	if t.abandon != "" || r.now().After(t.deadline) {
		r.log.Error("giving up on result.json: S3 kept failing past the deadline or the ProwJob is no longer active", "prowJobID", id, "abandon", t.abandon)
		r.markDone(id, "result.json not written")
	}
}

// failTracked gives a tracked job an error result. Like any result, a failed
// write is retried by retryResult.
func (r *Reconciler) failTracked(ctx context.Context, id string, t *tracked, msg string) {
	t.result = &contract.Result{Status: contract.StatusError, JenkinsBuild: t.buildURL, Message: msg}
	r.putResult(ctx, id, t)
}

// finishWithError writes an error Result (so the shim exits 1 fast) for a
// ProwJob rejected before it was tracked, and marks it done so it is neither
// re-triggered nor re-written. A failed write is retried by the next discover.
func (r *Reconciler) finishWithError(ctx context.Context, id, msg string) {
	result := contract.Result{Status: contract.StatusError, Message: msg}
	if err := r.s3.PutResult(ctx, id, result); err != nil {
		r.log.Error("write error result.json", "prowJobID", id, "err", err)
		return // retry next tick
	}
	r.markDone(id, "error result written")
}

// finishedMessage describes a finished build's result for result.json. The
// result string comes from Jenkins, so it is quoted and truncated if unexpected.
func finishedMessage(b *jenkins.Build) string {
	switch b.Result {
	case "":
		return fmt.Sprintf("Jenkins build %d finished without a result", b.Number)
	case "SUCCESS", "FAILURE", "UNSTABLE", "ABORTED", "NOT_BUILT":
		return fmt.Sprintf("Jenkins build %d finished with %s", b.Number, b.Result)
	default:
		return fmt.Sprintf("Jenkins build %d finished with unrecognized result %q", b.Number, contract.Truncate(b.Result, 32))
	}
}
