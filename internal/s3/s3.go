// Package s3 writes the reconciler's output to the GovCloud S3 bucket under the
// ProwJob UUID prefix, honoring the shim contract:
//
//	s3://{bucket}/{PROW_JOB_ID}/result.json
//	s3://{bucket}/{PROW_JOB_ID}/artifacts/{name}   (name may be a nested relative path)
//
// The implementation uses aws-sdk-go-v2 (vendored, so builds need no module
// proxy). It only ever calls PutObject: the reconciler's IAM identity is
// write-only and must not need GetObject, HeadObject, or ListBucket.
package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"

	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/contract"
	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/prow"
)

// Writer persists reconciler output for a ProwJob. Callers must write every
// artifact before result.json: the shim treats result.json as the completion
// signal and stops polling as soon as it appears.
type Writer interface {
	// PutResult writes result.json under the ProwJob prefix.
	PutResult(ctx context.Context, prowJobID string, r contract.Result) error
	// PutArtifact writes one artifact under {prowJobID}/artifacts/{name}, where
	// name is a relative path accepted by contract.ValidateArtifactName.
	PutArtifact(ctx context.Context, prowJobID, name string, data []byte) error
}

// Options configures SDKWriter.
type Options struct {
	Bucket string
	Region string
	// Endpoint overrides the resolved S3 endpoint; empty uses the region's FIPS endpoint.
	Endpoint string
	// UsePathStyle addresses objects as {endpoint}/{bucket}/{key}.
	UsePathStyle bool
}

// putTimeout bounds a single PutObject of n bytes so a stalled connection cannot
// block the single-goroutine reconcile loop indefinitely, while leaving large
// artifacts time to upload at >= 1 MiB/s. SDK retries share this one budget.
func putTimeout(n int) time.Duration {
	return 2*time.Minute + time.Duration(n>>20)*time.Second
}

// SDKWriter uploads with aws-sdk-go-v2. Credentials come from the SDK's default
// chain (env vars, shared config/profile, web identity/IRSA, IMDS) — a
// write-scoped identity distinct from the shim's read-only one. Credentials are
// never logged or included in errors.
type SDKWriter struct {
	client *s3.Client
	bucket string
}

// New builds an SDKWriter. It does not contact AWS; credentials are resolved
// lazily on the first write.
func New(ctx context.Context, o Options) (*SDKWriter, error) {
	return newWriter(ctx, o, nil)
}

// newWriter is New with extra SDK options, used by tests to inject credentials,
// an HTTP client, or a faster retry backoff.
func newWriter(ctx context.Context, o Options, loadOpts []func(*awsconfig.LoadOptions) error, s3Opts ...func(*s3.Options)) (*SDKWriter, error) {
	if o.Bucket == "" {
		return nil, errors.New("s3: bucket is required")
	}
	if o.Region == "" {
		return nil, errors.New("s3: region is required")
	}
	opts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(o.Region)}
	// FIPS is mandatory; the SDK rejects FIPS with a custom endpoint, so an override opts out.
	if o.Endpoint == "" {
		opts = append(opts, awsconfig.WithUseFIPSEndpoint(aws.FIPSEndpointStateEnabled))
	}
	opts = append(opts, loadOpts...)
	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}
	client := s3.NewFromConfig(cfg, append([]func(*s3.Options){func(so *s3.Options) {
		if o.Endpoint != "" {
			so.BaseEndpoint = aws.String(o.Endpoint)
		}
		so.UsePathStyle = o.UsePathStyle
	}}, s3Opts...)...)
	return &SDKWriter{client: client, bucket: o.Bucket}, nil
}

func (w *SDKWriter) PutResult(ctx context.Context, prowJobID string, r contract.Result) error {
	if !prow.ValidProwJobID(prowJobID) {
		return fmt.Errorf("refusing to write result.json for invalid ProwJob ID %q", prowJobID)
	}
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal result: %w", err)
	}
	return w.put(ctx, prowJobID+"/result.json", "application/json", data)
}

func (w *SDKWriter) PutArtifact(ctx context.Context, prowJobID, name string, data []byte) error {
	if !prow.ValidProwJobID(prowJobID) {
		return fmt.Errorf("refusing to write artifact for invalid ProwJob ID %q", prowJobID)
	}
	if err := contract.ValidateArtifactName(name); err != nil {
		return err
	}
	return w.put(ctx, prowJobID+"/artifacts/"+name, contentType(name), data)
}

// put uploads data to s3://{bucket}/{key}. No server-side-encryption headers are
// sent; the bucket's default encryption applies.
func (w *SDKWriter) put(ctx context.Context, key, ctype string, data []byte) error {
	ctx, cancel := context.WithTimeout(ctx, putTimeout(len(data)))
	defer cancel()
	_, err := w.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(w.bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
		ContentType:   aws.String(ctype),
	})
	if err != nil {
		return &putError{dest: "s3://" + w.bucket + "/" + key, err: err}
	}
	return nil
}

// contentType picks the object Content-Type from the artifact name, so Spyglass
// and browsers render JUnit and logs correctly.
func contentType(name string) string {
	switch strings.ToLower(path.Ext(name)) {
	case ".json":
		return "application/json"
	case ".xml":
		return "application/xml"
	case ".txt", ".log":
		return "text/plain; charset=utf-8"
	default:
		return "application/octet-stream"
	}
}

// putError reports a failed upload. Its message is built from known-safe parts
// (destination, AWS error code/message, status, request ID) rather than the raw
// SDK error, so request URLs and headers never reach the logs. Unwrap still
// exposes the SDK error for errors.Is/As (e.g. context.Canceled).
type putError struct {
	dest string
	err  error
}

func (e *putError) Unwrap() error { return e.err }

func (e *putError) Error() string {
	msg := "S3 PutObject " + e.dest + ": "
	var respErr *awshttp.ResponseError
	hasResp := errors.As(e.err, &respErr)
	var apiErr smithy.APIError
	if errors.As(e.err, &apiErr) {
		msg += apiErr.ErrorCode() + ": " + apiErr.ErrorMessage()
		if hasResp {
			msg += fmt.Sprintf(" (status %d, request id %s)", respErr.HTTPStatusCode(), respErr.ServiceRequestID())
		}
		return msg
	}
	switch {
	case errors.Is(e.err, context.Canceled):
		return msg + context.Canceled.Error()
	case errors.Is(e.err, context.DeadlineExceeded):
		return msg + context.DeadlineExceeded.Error()
	}
	var urlErr *url.Error
	if errors.As(e.err, &urlErr) {
		return msg + "transport error: " + urlErr.Err.Error()
	}
	if hasResp {
		return msg + fmt.Sprintf("status %d (request id %s)", respErr.HTTPStatusCode(), respErr.ServiceRequestID())
	}
	var opErr *smithy.OperationError
	if errors.As(e.err, &opErr) {
		// Typically credential resolution or signing; these carry no secrets.
		return msg + opErr.Err.Error()
	}
	return msg + "upload failed"
}
