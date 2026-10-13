// Command conncheck validates the reconciler's two connections independently and
// READ-ONLY: it polls the Prow feed and probes the Jenkins job(s), but never triggers
// a build or writes to S3. Run it once you have the Jenkins values to confirm wiring
// before starting the reconciler proper.
//
//	JENKINS_URL=https://jenkins.example.internal \
//	JENKINS_USER=<JENKINS_USER> JENKINS_API_TOKEN=<JENKINS_API_TOKEN> \
//	JENKINS_JOB_ALLOWLIST=example-job \
//	  go run ./cmd/conncheck
//
// The Prow feed is always checked (PROW_JOBS_URL, EXECUTOR_LABEL, and PROW_FEED_*
// apply, validated as the reconciler validates them). Jenkins is checked only if
// JENKINS_URL is set, and then JENKINS_USER, JENKINS_API_TOKEN, and
// JENKINS_JOB_ALLOWLIST are required. It exits 0 if every check passes, else 1.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"slices"
	"syscall"

	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/config"
	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/contract"
	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/jenkins"
	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/prow"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	err := run(ctx, os.Args[1:], os.Getenv, os.Stdout)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, "conncheck:", err)
		os.Exit(1)
	}
}

// errFailed reports that at least one check failed; the details are in the output.
var errFailed = errors.New("one or more checks failed; see above")

// run performs the checks, writing a report to out.
func run(ctx context.Context, args []string, getenv func(string) string, out io.Writer) error {
	if len(args) > 0 {
		return fmt.Errorf("unexpected arguments %q: configuration is read from environment variables only", args)
	}
	ok := checkProw(ctx, getenv, out)
	fmt.Fprintln(out)
	ok = checkJenkins(ctx, getenv, out) && ok

	fmt.Fprintln(out)
	if !ok {
		fmt.Fprintln(out, "RESULT: FAIL")
		return errFailed
	}
	fmt.Fprintln(out, "RESULT: PASS")
	return nil
}

func checkProw(ctx context.Context, getenv func(string) string, out io.Writer) bool {
	cfg, err := config.LoadFeed(getenv)
	if err != nil {
		fmt.Fprintf(out, "[prow] FAIL: %v\n", err)
		return false
	}
	fmt.Fprintf(out, "[prow] GET %s (filter executor=%s)\n", cfg.ProwJobsURL, cfg.ExecutorLabel)

	c := prow.New(cfg.ProwJobsURL)
	c.Timeout, c.MaxBytes = cfg.FeedTimeout, cfg.FeedMaxBytes
	feed, err := c.ListFeed(ctx, cfg.ExecutorLabel)
	if err != nil {
		fmt.Fprintf(out, "[prow] FAIL: %v\n", err)
		return false
	}
	active := 0
	for _, j := range feed.Jobs {
		if j.Active() {
			active++
		}
	}
	fmt.Fprintf(out, "[prow] OK: feed reachable; %d ProwJob(s), %d matching, %d active\n", feed.Items, len(feed.Jobs), active)
	for _, j := range feed.Jobs {
		fmt.Fprintf(out, "       - %q  state=%q jenkinsJob=%q\n", contract.Truncate(j.ID, 100), contract.Truncate(j.State, 100), contract.Truncate(j.JenkinsJob, 100))
	}
	if len(feed.Jobs) == 0 {
		fmt.Fprintln(out, "[prow] NOTE: no jobs matched; expected unless a bridge periodic is currently active.")
	}
	return true
}

func checkJenkins(ctx context.Context, getenv func(string) string, out io.Writer) bool {
	if getenv("JENKINS_URL") == "" {
		fmt.Fprintln(out, "[jenkins] SKIP: JENKINS_URL not set")
		return true
	}
	cfg, err := config.LoadJenkins(getenv)
	if err != nil {
		fmt.Fprintf(out, "[jenkins] FAIL: %v\n", err)
		return false
	}
	c := jenkins.New(cfg.JenkinsURL, cfg.JenkinsUser, cfg.JenkinsAPIToken)
	fmt.Fprintf(out, "[jenkins] probing %d job(s) at %s\n", len(cfg.JobAllowlist), c.BaseURL)

	ok := true
	for _, job := range cfg.JobAllowlist {
		info, err := c.JobInfo(ctx, job)
		if err != nil {
			fmt.Fprintf(out, "[jenkins] FAIL %q: %v\n", job, err)
			fmt.Fprintln(out, "          (check JENKINS_URL, the job name, and JENKINS_USER/JENKINS_API_TOKEN)")
			ok = false
			continue
		}
		fmt.Fprintf(out, "[jenkins] OK %q: buildable=%t params=%v\n", job, info.Buildable, info.Parameters)
		if !info.Buildable {
			fmt.Fprintf(out, "[jenkins] FAIL %q: job is not buildable (disabled?)\n", job)
			ok = false
		}
		if !slices.Contains(info.Parameters, config.ReservedParam) {
			fmt.Fprintf(out, "[jenkins] FAIL %q: no %s parameter declared; add a string parameter %[2]s to the job.\n", job, config.ReservedParam)
			ok = false
		}
	}
	return ok
}
