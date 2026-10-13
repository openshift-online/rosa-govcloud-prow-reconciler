// Package prow reads the public Prow ProwJob feed and extracts the jobs this
// reconciler is responsible for. It is READ-ONLY: the reconciler never writes back
// to Prow. Prow's own controllers advance ProwJob status once the CI shim reads the
// result.json we write to S3.
package prow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// Annotation keys the reconciler reads off the ProwJob.
const (
	AnnotationJenkinsJob = "govcloud.ci/jenkins-job" // required: Jenkins job name to run
	LabelExecutor        = "executor"                // matched against Config.ExecutorLabel
)

// Job is the reconciler-facing view of a matched ProwJob.
type Job struct {
	// ID is the canonical PROW_JOB_ID (== ProwJob metadata.name, the UUID). This is
	// the S3 prefix the shim polls, and the value the reconciler passes to Jenkins.
	ID         string
	JenkinsJob string // from AnnotationJenkinsJob
	State      string // status.state: triggered|pending|success|failure|aborted|error
	// StartTime is status.startTime, when Prow created the ProwJob; zero if absent
	// or unparseable. The CI shim's timeout starts after this.
	StartTime time.Time
}

// prowJobIDPattern is the canonical (lowercase, hyphenated) UUID form Prow uses
// for ProwJob names.
var prowJobIDPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// ValidProwJobID reports whether id is a canonical UUID. The feed is untrusted and
// the ID becomes an S3 key prefix and a Jenkins parameter, so anything else is
// refused.
func ValidProwJobID(id string) bool {
	return prowJobIDPattern.MatchString(id)
}

// Active reports whether the ProwJob is still awaiting a result (i.e. we should
// act on it). Completed/aborted jobs are ignored.
func (j Job) Active() bool {
	return j.State == "triggered" || j.State == "pending"
}

// Client polls the Prow feed.
type Client struct {
	URL  string
	HTTP *http.Client
	// Omit is sent as Deck's omit query parameter, which drops the named fields
	// from every ProwJob in the response (see DefaultOmit). Empty sends none.
	Omit []string
	// Timeout bounds one whole feed request, including reading the body.
	Timeout time.Duration
	// MaxBytes caps the (decompressed) bytes read from one response; a larger
	// feed is a failed poll.
	MaxBytes int64
}

// Feed request defaults. prowjobs.js is the whole-instance firehose (about
// 186 MB per the enhancement), so the timeout is generous and the size cap far
// above the expected size.
const (
	DefaultTimeout  = 5 * time.Minute
	DefaultMaxBytes = 1 << 30 // 1 GiB
)

// DefaultOmit is the omit list sent to Deck. Deck's prowjobs.js handler accepts
// omit=annotations,labels,decoration_config,pod_spec and clears those fields;
// the reconciler needs labels and annotations, but not the pod spec or
// decoration config, which make up most of each ProwJob. A Deck that does not
// know the parameter ignores it.
var DefaultOmit = []string{"pod_spec", "decoration_config"}

// New returns a Client with the default omit list, timeout, and size cap. The
// timeout is applied per request through its context.
func New(url string) *Client {
	return &Client{
		URL:      url,
		HTTP:     &http.Client{},
		Omit:     DefaultOmit,
		Timeout:  DefaultTimeout,
		MaxBytes: DefaultMaxBytes,
	}
}

// item is the subset of one ProwJob in prowjobs.js that the reconciler reads.
type item struct {
	Metadata struct {
		Name        string            `json:"name"`
		Labels      map[string]string `json:"labels"`
		Annotations map[string]string `json:"annotations"`
	} `json:"metadata"`
	Status struct {
		State     string `json:"state"`
		StartTime string `json:"startTime"`
	} `json:"status"`
}

// Feed is one successfully fetched and decoded snapshot of the ProwJob feed.
type Feed struct {
	Jobs  []Job // jobs matching the executor label with a Jenkins-job annotation
	Items int   // total ProwJobs in the feed, matching or not
}

// ErrFeedTooLarge is returned when a response exceeds Client.MaxBytes.
var ErrFeedTooLarge = errors.New("prow feed exceeds the size limit")

// ListFeed fetches the feed and returns the jobs whose executor label matches
// executorLabel and that carry a Jenkins-job annotation. Both active and completed
// jobs are returned; callers filter with Job.Active as needed. It also counts
// every ProwJob in the feed (Feed.Items), so callers can tell an empty feed from
// one that merely has no matching jobs.
//
// The response is decoded as a stream: ProwJobs are read one at a time from the
// "items" array and only matching ones are kept, so memory use is bounded by the
// matches plus one ProwJob rather than by the feed size. It returns an error
// unless the whole response was fetched and parsed as one complete JSON object
// with an "items" array: a truncated, oversized, or malformed feed is a failed
// poll, never a partial result.
func (c *Client) ListFeed(ctx context.Context, executorLabel string) (*Feed, error) {
	u, err := c.requestURL()
	if err != nil {
		return nil, err
	}
	if c.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.Timeout)
		defer cancel()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch prow feed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("prow feed returned %s: %s", resp.Status, body)
	}
	var body io.Reader = resp.Body
	if c.MaxBytes > 0 {
		body = &capReader{r: resp.Body, left: c.MaxBytes}
	}
	f, err := decodeFeed(body, executorLabel)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			err = ctxErr // e.g. the read was cut off by the timeout
		}
		return nil, fmt.Errorf("decode prow feed: %w", err)
	}
	return f, nil
}

// requestURL adds the omit parameter to URL.
func (c *Client) requestURL() (string, error) {
	u, err := url.Parse(c.URL)
	if err != nil {
		return "", fmt.Errorf("invalid prow feed URL")
	}
	if len(c.Omit) > 0 {
		q := u.Query()
		q.Set("omit", strings.Join(c.Omit, ","))
		u.RawQuery = q.Encode()
	}
	return u.String(), nil
}

// decodeFeed parses a ProwJobList from r, keeping only the jobs that match
// executorLabel. The document must be a single JSON object with an "items"
// array (or null), and nothing may follow it.
func decodeFeed(r io.Reader, executorLabel string) (*Feed, error) {
	dec := json.NewDecoder(r)
	if err := expectDelim(dec, '{'); err != nil {
		return nil, err
	}
	f := &Feed{}
	sawItems := false
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return nil, err
		}
		key, ok := tok.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected %v in feed object", tok)
		}
		if key != "items" {
			if err := skipValue(dec); err != nil {
				return nil, err
			}
			continue
		}
		if sawItems {
			return nil, errors.New(`duplicate "items" in feed`)
		}
		sawItems = true
		if err := decodeItems(dec, executorLabel, f); err != nil {
			return nil, err
		}
	}
	if err := expectDelim(dec, '}'); err != nil {
		return nil, err
	}
	switch _, err := dec.Token(); {
	case err == io.EOF:
	case err != nil:
		return nil, err
	default:
		return nil, errors.New("unexpected data after the feed object")
	}
	if !sawItems {
		return nil, errors.New(`feed has no "items"`)
	}
	return f, nil
}

// decodeItems reads the "items" value one ProwJob at a time.
func decodeItems(dec *json.Decoder, executorLabel string, f *Feed) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	if tok == nil {
		return nil // "items": null, as Go encodes an empty list
	}
	if tok != json.Delim('[') {
		return fmt.Errorf(`"items" is not an array`)
	}
	for dec.More() {
		var it item
		if err := dec.Decode(&it); err != nil {
			return err
		}
		f.Items++
		if j, ok := match(it, executorLabel); ok {
			f.Jobs = append(f.Jobs, j)
		}
	}
	return expectDelim(dec, ']')
}

// match converts a ProwJob with the executor label and a Jenkins-job annotation
// into a Job.
func match(it item, executorLabel string) (Job, bool) {
	if it.Metadata.Labels[LabelExecutor] != executorLabel {
		return Job{}, false
	}
	jenkinsJob := it.Metadata.Annotations[AnnotationJenkinsJob]
	if jenkinsJob == "" {
		return Job{}, false // matched executor but no target job — skip, don't guess
	}
	return Job{
		ID:         it.Metadata.Name,
		JenkinsJob: jenkinsJob,
		State:      it.Status.State,
		StartTime:  parseTime(it.Status.StartTime),
	}, true
}

func expectDelim(dec *json.Decoder, want json.Delim) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	if tok != want {
		return fmt.Errorf("expected %q in feed, got %v", want, tok)
	}
	return nil
}

// skipValue discards the next JSON value token by token, without buffering it.
func skipValue(dec *json.Decoder) error {
	depth := 0
	for {
		tok, err := dec.Token()
		if err != nil {
			return err
		}
		switch tok {
		case json.Delim('{'), json.Delim('['):
			depth++
		case json.Delim('}'), json.Delim(']'):
			depth--
		}
		if depth == 0 {
			return nil
		}
	}
}

// capReader fails once more than left bytes have been read.
type capReader struct {
	r    io.Reader
	left int64
}

func (c *capReader) Read(p []byte) (int, error) {
	if c.left < 0 {
		return 0, ErrFeedTooLarge
	}
	if int64(len(p))-1 > c.left { // not c.left+1, which overflows at MaxInt64
		p = p[:c.left+1]
	}
	n, err := c.r.Read(p)
	c.left -= int64(n)
	if c.left < 0 {
		return n, ErrFeedTooLarge
	}
	return n, err
}

// parseTime parses an RFC 3339 timestamp, returning the zero time if it is absent
// or malformed.
func parseTime(raw string) time.Time {
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}
	}
	return t
}
