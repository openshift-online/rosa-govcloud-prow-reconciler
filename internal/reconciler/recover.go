package reconciler

import (
	"context"
	"time"

	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/config"
	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/jenkins"
	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/prow"
)

// Crash recovery. The reconciler's S3 identity is write-only, so it cannot ask S3
// what it already did. Jenkins is the record instead: every build the reconciler
// starts carries a PROW_JOB_ID parameter. On startup, before anything is
// triggered, the queue and recent builds of each allowlisted job are listed, and
// an active ProwJob with a matching run is adopted rather than triggered again: a
// queued or running build is monitored, and a finished build has its result
// collected and written.

// recoveryBuildDepth is how many recent builds per allowlisted job are scanned.
const recoveryBuildDepth = 100

// maxRecoveryBackoff caps the wait between failed startup recovery attempts.
const maxRecoveryBackoff = 5 * time.Minute

// recoveredRun is a Jenkins run found for a PROW_JOB_ID that is not yet tracked.
type recoveredRun struct {
	run        jenkins.Run
	jenkinsJob string
	foundAt    time.Time
}

// recoverUntilDone retries recoverState with capped exponential backoff until it
// succeeds (true) or ctx is cancelled (false). If Jenkins cannot be read, nothing
// is triggered: triggering blind could duplicate every in-flight build.
func (r *Reconciler) recoverUntilDone(ctx context.Context) bool {
	backoff := r.cfg.PollInterval
	for {
		err := r.recoverState(ctx)
		if err == nil {
			return true
		}
		r.log.Error("rebuild state from Jenkins failed; not triggering until it succeeds", "err", err, "retryIn", backoff)
		select {
		case <-ctx.Done():
			return false
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, maxRecoveryBackoff)
	}
}

// recoverState lists the queue and recent builds of every allowlisted job and
// remembers the runs that carry a PROW_JOB_ID. All jobs must be listed
// successfully; a partial view could miss a running build.
func (r *Reconciler) recoverState(ctx context.Context) error {
	found := make(map[string]recoveredRun)
	for _, job := range r.cfg.JobAllowlist {
		runs, err := r.jenkins.ListRuns(ctx, job, config.ReservedParam, recoveryBuildDepth)
		if err != nil {
			return err
		}
		r.collect(found, job, runs, "")
	}
	for id, rr := range found {
		r.recovered[id] = rr
	}
	r.log.Info("rebuilt state from Jenkins", "runs", len(found), "jobs", len(r.cfg.JobAllowlist))
	return nil
}

// collect records runs into found, keeping the most relevant run per ProwJob:
// runs arrive queue items first, then builds newest first, and a build wins over
// a queue item (it is usually the same run, seen after it left the queue). If
// onlyID is set, other ProwJobs are ignored.
func (r *Reconciler) collect(found map[string]recoveredRun, job string, runs []jenkins.Run, onlyID string) {
	for _, run := range runs {
		if !prow.ValidProwJobID(run.ProwJobID) || (onlyID != "" && run.ProwJobID != onlyID) {
			continue
		}
		prev, ok := found[run.ProwJobID]
		if !ok || (prev.run.BuildURL == "" && run.BuildURL != "") {
			found[run.ProwJobID] = recoveredRun{run: run, jenkinsJob: job, foundAt: r.now()}
		}
	}
}

// checkJenkins looks for an existing run of j after an uncertain trigger failure.
// It returns false if Jenkins could not be read, in which case the caller must
// not trigger.
func (r *Reconciler) checkJenkins(ctx context.Context, j prow.Job) bool {
	runs, err := r.jenkins.ListRuns(ctx, j.JenkinsJob, config.ReservedParam, recoveryBuildDepth)
	if err != nil {
		r.log.Error("check Jenkins before re-trigger", "prowJobID", j.ID, "err", err)
		r.noteTimeout(err)
		return false
	}
	r.collect(r.recovered, j.JenkinsJob, runs, j.ID)
	return true
}

// adopt starts tracking a recovered run for j instead of triggering a new build.
func (r *Reconciler) adopt(j prow.Job) bool {
	rr, ok := r.recovered[j.ID]
	if !ok {
		return false
	}
	delete(r.recovered, j.ID)
	started := rr.run.StartedAt
	if started.Unix() <= 0 { // Jenkins reported no time
		started = r.now()
	}
	r.inflight[j.ID] = &tracked{jenkinsJob: rr.jenkinsJob, queueURL: rr.run.QueueURL, buildURL: rr.run.BuildURL, deadline: r.deadline(j, started)}
	r.log.Info("adopted existing Jenkins run", "prowJobID", j.ID, "jenkinsJob", rr.jenkinsJob,
		"build", rr.run.BuildURL, "queue", rr.run.QueueURL, "building", rr.run.Building)
	return true
}
