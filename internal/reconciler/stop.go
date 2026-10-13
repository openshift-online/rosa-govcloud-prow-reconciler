package reconciler

import (
	"context"
	"errors"

	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/contract"
	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/jenkins"
)

// Stopping Jenkins runs. When the reconciler gives up on a run (timeout, or its
// ProwJob ended or vanished from the feed) or finds an orphan after a restart
// (a recovered run whose ProwJob has finished), the build is stopped rather than
// left consuming an executor. A stop request is only a request: a queue item can start a build just as it is cancelled, and a
// build can ignore or delay an abort. So every attempt first re-reads the run's
// state and only then acts, and attempts repeat on later ticks until Jenkins
// shows the run is no longer queued or building.

// maxStopAttempts bounds how many ticks the reconciler keeps trying to stop one
// run before giving up and logging it.
const maxStopAttempts = 10

// stopTask is a Jenkins run the reconciler is trying to stop. It shares the
// tracked job's queue and build URLs, so what the stop learns updates the job.
type stopTask struct {
	*tracked
	reason   string // for logs
	attempts int
}

// stopRun makes one attempt at stopping s and reports whether the run is
// confirmed to be finished (or was never started).
func (r *Reconciler) stopRun(ctx context.Context, id string, s *stopTask) (stopped bool) {
	s.attempts++
	if s.buildURL == "" {
		if s.queueURL == "" {
			return true
		}
		buildURL, err := r.resolveQueue(ctx, id, s.jenkinsJob, &s.queueURL)
		if errors.Is(err, jenkins.ErrQueueItemCancelled) || errors.Is(err, errNoRun) {
			r.log.Info("jenkins queue item is gone; nothing to stop", "prowJobID", id, "queue", s.queueURL, "reason", s.reason, "err", err)
			return true
		}
		if err != nil {
			r.log.Error("stop: resolve queue item", "prowJobID", id, "err", err)
			r.noteTimeout(err)
			return false
		}
		if buildURL == "" {
			if err := r.jenkins.CancelQueueItem(ctx, s.queueURL); err != nil {
				r.log.Error("cancel jenkins queue item", "prowJobID", id, "queue", s.queueURL, "err", err)
				r.noteTimeout(err)
			} else {
				r.log.Info("requested cancel of jenkins queue item", "prowJobID", id, "queue", s.queueURL, "reason", s.reason)
			}
			return false // confirmed on the next attempt
		}
		s.buildURL = buildURL // started after all; stop the build instead
	}
	b, err := r.jenkins.GetBuild(ctx, s.buildURL)
	if err != nil {
		r.log.Error("stop: get build", "prowJobID", id, "err", err)
		r.noteTimeout(err)
		return false
	}
	if !b.Building {
		r.log.Info("jenkins build is not running", "prowJobID", id, "build", s.buildURL, "result", b.Result, "reason", s.reason)
		return true
	}
	if err := r.jenkins.StopBuild(ctx, s.buildURL); err != nil {
		r.log.Error("stop jenkins build", "prowJobID", id, "build", s.buildURL, "err", err)
		r.noteTimeout(err)
	} else {
		r.log.Info("requested stop of jenkins build", "prowJobID", id, "build", s.buildURL, "reason", s.reason)
	}
	return false // confirmed on the next attempt
}

// advanceStops retries every pending stop once, until a Jenkins timeout.
func (r *Reconciler) advanceStops(ctx context.Context) {
	r.stalled = false
	for id, s := range r.stopping {
		if r.quitting() || r.stalled {
			return
		}
		if r.stopRun(ctx, id, s) {
			delete(r.stopping, id)
			continue
		}
		if s.attempts >= maxStopAttempts {
			r.log.Error("giving up stopping jenkins run", "prowJobID", id, "build", s.buildURL, "queue", s.queueURL, "attempts", s.attempts)
			delete(r.stopping, id)
		}
	}
}

// abandon gives up on an in-flight run: it asks Jenkins to stop it (confirmed
// in advanceStops), uploads the log so far, and writes an error result.json.
// Like any result, a failed write is retried by retryResult, so this runs once.
func (r *Reconciler) abandon(ctx context.Context, id string, t *tracked, msg string) {
	if s := (&stopTask{tracked: t, reason: msg}); !r.stopRun(ctx, id, s) {
		r.stopping[id] = s
	}
	var out collected
	if t.buildURL != "" {
		out = r.collectOutputs(ctx, id, t, false) // the log so far; once, even if the result write is retried
	}
	if ctx.Err() != nil {
		return // cut off by the shutdown timeout; retried after restart
	}
	if t.abandon == "" {
		// Timed out: the deadline has passed by definition, but this result is the
		// shim's only answer. Keep retrying until the ProwJob is no longer active
		// or another MaxBuildTime passes.
		t.deadline = r.now().Add(r.cfg.MaxBuildTime)
	}
	t.result = &contract.Result{Status: contract.StatusError, JenkinsBuild: t.buildURL, Artifacts: out.names, Message: out.message(msg, "; ")}
	r.putResult(ctx, id, t)
}
