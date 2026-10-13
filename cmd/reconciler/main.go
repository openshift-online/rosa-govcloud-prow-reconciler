// Command reconciler bridges Prow ProwJobs to an in-boundary Jenkins instance and
// writes results back to S3 for the CI shim to consume.
//
// Config is entirely via environment (see internal/config and the README); the
// only argument is -version. Minimal run:
//
//	JENKINS_URL=https://jenkins.example.internal \
//	JENKINS_USER=<JENKINS_USER> JENKINS_API_TOKEN=<JENKINS_API_TOKEN> \
//	JENKINS_JOB_ALLOWLIST=example-job \
//	S3_BUCKET=<S3_BUCKET> \
//	AWS_ACCESS_KEY_ID=<AWS_ACCESS_KEY_ID> AWS_SECRET_ACCESS_KEY=<AWS_SECRET_ACCESS_KEY> \
//	  reconciler
//
// It exits 0 after a clean shutdown on SIGTERM or SIGINT, and 1 on a
// configuration error or if the shutdown timeout cut off the step in progress.
// Only a single replica may run (see the README).
package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/config"
	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/jenkins"
	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/prow"
	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/reconciler"
	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/s3"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	err := run(ctx, os.Args[1:], os.Getenv, os.Stderr)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, "reconciler:", err)
		os.Exit(1)
	}
}

// run loads config from getenv, wires the clients, and runs the loop until ctx
// is cancelled. Logs go to logw. AWS credentials are resolved by the SDK's
// default chain, which reads the process environment, not getenv.
func run(ctx context.Context, args []string, getenv func(string) string, logw io.Writer) error {
	if len(args) == 1 && args[0] == "-version" {
		_, err := fmt.Fprintln(logw, "reconciler", version)
		return err
	}
	if len(args) > 0 {
		return fmt.Errorf("unexpected arguments %q: configuration is read from environment variables only", args)
	}
	log := slog.New(slog.NewTextHandler(logw, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg, err := config.Load(getenv)
	if err != nil {
		return err
	}

	w, err := s3.New(ctx, s3.Options{
		Bucket:       cfg.S3Bucket,
		Region:       cfg.S3Region,
		Endpoint:     cfg.S3Endpoint,
		UsePathStyle: cfg.S3UsePathStyle,
	})
	if err != nil {
		return fmt.Errorf("s3 client: %w", err)
	}
	log.Info("s3 writer configured",
		"region", cfg.S3Region,
		"endpoint", cfg.S3Endpoint,
		"pathStyle", cfg.S3UsePathStyle,
	)

	feed := prow.New(cfg.ProwJobsURL)
	feed.Timeout, feed.MaxBytes = cfg.FeedTimeout, cfg.FeedMaxBytes
	r := reconciler.New(
		cfg,
		feed,
		jenkins.New(cfg.JenkinsURL, cfg.JenkinsUser, cfg.JenkinsAPIToken),
		w,
		log,
	)
	return r.Run(ctx)
}
