// Package jenkins is a thin client over the in-boundary Jenkins REST API: trigger a
// parameterized build, resolve the queue item to a build, poll it to completion,
// list runs for restart recovery, stop builds and cancel queue items, and pull
// artifacts and the console log. Every request uses basic auth and is only ever
// sent to the configured base URL's origin.
package jenkins

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/contract"
)

// Client talks to one Jenkins instance.
type Client struct {
	BaseURL  string // e.g. https://jenkins.example.internal (no trailing slash)
	User     string // basic-auth user (a dedicated service account)
	APIToken string // basic-auth API token for User
	HTTP     *http.Client

	// APITimeout bounds each API request, including reading its response.
	APITimeout time.Duration
	// DownloadTimeout bounds each artifact or console-log download, which can be
	// far larger than an API response.
	DownloadTimeout time.Duration
}

// Default per-request timeouts.
const (
	DefaultAPITimeout      = 30 * time.Second
	DefaultDownloadTimeout = 15 * time.Minute
)

// New returns a Client with default per-request timeouts. The timeouts are
// applied per request through its context rather than as an http.Client.Timeout,
// so a large download is not cut off at the API timeout. The HTTP client refuses
// redirects that leave the BaseURL origin, so credentials only ever go to the
// configured host. Every request uses basic auth (user:api-token), which is the
// trigger path the enhancement specifies.
func New(baseURL, user, apiToken string) *Client {
	c := &Client{
		BaseURL:         strings.TrimRight(baseURL, "/"),
		User:            user,
		APIToken:        apiToken,
		APITimeout:      DefaultAPITimeout,
		DownloadTimeout: DefaultDownloadTimeout,
	}
	c.HTTP = &http.Client{CheckRedirect: c.checkRedirect}
	return c
}

// StatusError is a non-success HTTP answer from Jenkins. Its text can include a
// slice of the response body, so it is for logs, never for result.json.
type StatusError struct {
	Code int
	msg  string
}

func (e *StatusError) Error() string { return e.msg }

// Terminal reports whether repeating the request cannot help: every 4xx except
// 408 (request timeout), 429 (rate limited) and 401/403 (a rotated token may be
// accepted shortly; the caller bounds those retries).
func (e *StatusError) Terminal() bool {
	switch e.Code {
	case http.StatusRequestTimeout, http.StatusTooManyRequests, http.StatusUnauthorized, http.StatusForbidden:
		return false
	}
	return e.Code >= 400 && e.Code < 500
}

// ErrQueueItemCancelled is returned by ResolveQueueItem when the queue item was
// cancelled before it got an executor. It is terminal: retrying cannot succeed.
var ErrQueueItemCancelled = errors.New("jenkins queue item was cancelled")

// Build is the subset of Jenkins' build JSON the reconciler needs.
type Build struct {
	Number     int    `json:"number"`
	Building   bool   `json:"building"`
	Result     string `json:"result"`   // SUCCESS|FAILURE|UNSTABLE|ABORTED|null(while building)
	DurationMS int64  `json:"duration"` // milliseconds
}

// Artifact is one archived artifact on a build.
type Artifact struct {
	RelativePath string `json:"relativePath"`
}

// JobInfo is a read-only snapshot used to validate a job is triggerable before the
// reconciler relies on it.
type JobInfo struct {
	Buildable  bool
	Parameters []string // declared build parameter names
}

// JobInfo fetches whether a job is buildable and its declared parameters. Used by
// the conncheck command; does not trigger anything.
func (c *Client) JobInfo(ctx context.Context, job string) (*JobInfo, error) {
	var raw struct {
		Buildable bool `json:"buildable"`
		Property  []struct {
			ParameterDefinitions []struct {
				Name string `json:"name"`
			} `json:"parameterDefinitions"`
		} `json:"property"`
	}
	u := fmt.Sprintf("%s/job/%s/api/json?tree=buildable,property[parameterDefinitions[name]]", c.BaseURL, url.PathEscape(job))
	if err := c.getJSON(ctx, u, &raw); err != nil {
		return nil, err
	}
	info := &JobInfo{Buildable: raw.Buildable}
	for _, p := range raw.Property {
		for _, d := range p.ParameterDefinitions {
			info.Parameters = append(info.Parameters, d.Name)
		}
	}
	return info, nil
}

// TriggerBuild starts a parameterized build and returns the queue-item URL from the
// Location response header. Resolve it with ResolveQueueItem to get the build.
//
// The trigger is authenticated with basic auth (user:api-token), as the
// enhancement specifies: an unauthenticated ?token= POST is rejected (403) by
// typical fronting proxies. Without credentials it fails without sending
// anything. Parameters are sent as an application/x-www-form-urlencoded POST
// body, never in the URL: Jenkins reads buildWithParameters parameters from the
// form body too, and URLs end up in error strings, proxy access logs, and
// Jenkins' own logs.
func (c *Client) TriggerBuild(ctx context.Context, job string, params map[string]string) (queueURL string, err error) {
	if !c.hasBasicAuth() {
		return "", fmt.Errorf("trigger %s: Jenkins basic-auth credentials are not configured", job)
	}
	form := url.Values{}
	for k, v := range params {
		form.Set(k, v)
	}
	endpoint := fmt.Sprintf("%s/job/%s/buildWithParameters", c.BaseURL, url.PathEscape(job))

	req, err := c.newRequest(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := c.do(req)
	if err != nil {
		return "", fmt.Errorf("trigger %s: %w", job, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated { // Jenkins returns 201 with Location
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return "", &StatusError{resp.StatusCode, fmt.Sprintf("trigger %s returned %s: %s", job, resp.Status, body)}
	}
	loc := resp.Header.Get("Location")
	if loc == "" {
		return "", fmt.Errorf("trigger %s: no Location header (queue URL) in response", job)
	}
	queueURL, err = c.Rebase(loc)
	if err != nil {
		return "", fmt.Errorf("trigger %s: queue URL: %w", job, err)
	}
	return queueURL, nil
}

// ResolveQueueItem checks a queue item. It returns the build URL (rebased onto
// BaseURL) once Jenkins has assigned an executor; before that it returns ("", nil)
// — call again later.
func (c *Client) ResolveQueueItem(ctx context.Context, queueURL string) (buildURL string, err error) {
	var q struct {
		Cancelled  bool `json:"cancelled"`
		Executable *struct {
			URL string `json:"url"`
		} `json:"executable"`
	}
	if err := c.getJSON(ctx, strings.TrimRight(queueURL, "/")+"/api/json", &q); err != nil {
		return "", err
	}
	if q.Cancelled {
		return "", fmt.Errorf("%w: %s", ErrQueueItemCancelled, redactURL(queueURL))
	}
	if q.Executable == nil {
		return "", nil // still queued
	}
	return c.Rebase(q.Executable.URL)
}

// Run is a queue item or build of a job that carries a PROW_JOB_ID parameter.
// The reconciler uses these to rebuild its state after a restart.
type Run struct {
	ProwJobID string
	QueueURL  string    // set while the run is still waiting in the queue
	BuildURL  string    // set once Jenkins has started a build
	Building  bool      // build still running
	StartedAt time.Time // build start, or time it entered the queue
}

type paramActions []struct {
	Parameters []struct {
		Name  string `json:"name"`
		Value any    `json:"value"` // not always a string (e.g. boolean params)
	} `json:"parameters"`
}

func (a paramActions) get(name string) string {
	for _, act := range a {
		for _, p := range act.Parameters {
			if p.Name == name {
				if v, ok := p.Value.(string); ok {
					return v
				}
			}
		}
	}
	return ""
}

// ListRuns returns the queued items and the most recent depth builds of job that
// carry the parameter param, newest first. The queue is read before the builds so
// that an item leaving the queue between the two reads is still seen (as a build).
func (c *Client) ListRuns(ctx context.Context, job, param string, depth int) ([]Run, error) {
	var queue struct {
		Items []struct {
			URL          string       `json:"url"`
			InQueueSince int64        `json:"inQueueSince"`
			Actions      paramActions `json:"actions"`
			Task         struct {
				Name string `json:"name"`
			} `json:"task"`
		} `json:"items"`
	}
	qTree := url.Values{"tree": {"items[url,inQueueSince,task[name],actions[parameters[name,value]]]"}}
	if err := c.getJSON(ctx, c.BaseURL+"/queue/api/json?"+qTree.Encode(), &queue); err != nil {
		return nil, fmt.Errorf("list queue: %w", err)
	}
	var builds struct {
		Builds []struct {
			URL       string       `json:"url"`
			Building  bool         `json:"building"`
			Timestamp int64        `json:"timestamp"`
			Actions   paramActions `json:"actions"`
		} `json:"builds"`
	}
	bTree := url.Values{"tree": {fmt.Sprintf("builds[url,building,timestamp,actions[parameters[name,value]]]{0,%d}", depth)}}
	if err := c.getJSON(ctx, fmt.Sprintf("%s/job/%s/api/json?%s", c.BaseURL, url.PathEscape(job), bTree.Encode()), &builds); err != nil {
		return nil, fmt.Errorf("list builds of %s: %w", job, err)
	}

	var runs []Run
	for _, it := range queue.Items {
		id := it.Actions.get(param)
		if it.Task.Name != job || id == "" {
			continue
		}
		q, err := c.Rebase(it.URL)
		if err != nil {
			return nil, err
		}
		runs = append(runs, Run{ProwJobID: id, QueueURL: q, StartedAt: time.UnixMilli(it.InQueueSince)})
	}
	for _, b := range builds.Builds {
		id := b.Actions.get(param)
		if id == "" {
			continue
		}
		u, err := c.Rebase(b.URL)
		if err != nil {
			return nil, err
		}
		runs = append(runs, Run{ProwJobID: id, BuildURL: u, Building: b.Building, StartedAt: time.UnixMilli(b.Timestamp)})
	}
	return runs, nil
}

// StopBuild asks Jenkins to abort a running build (POST {build}/stop). Jenkins
// answers with a redirect to the build page, which is not followed. A successful
// response only means the request was accepted: callers confirm with GetBuild.
func (c *Client) StopBuild(ctx context.Context, buildURL string) error {
	return c.post(ctx, strings.TrimRight(buildURL, "/")+"/stop")
}

// CancelQueueItem removes a waiting item from the build queue
// (POST {jenkins}/queue/cancelItem?id=N). queueURL must be a queue item URL under
// BaseURL. As with StopBuild, callers confirm with ResolveQueueItem: the item may
// have started a build just before the cancel arrived.
func (c *Client) CancelQueueItem(ctx context.Context, queueURL string) error {
	id, err := c.queueItemID(queueURL)
	if err != nil {
		return err
	}
	return c.post(ctx, c.BaseURL+"/queue/cancelItem?"+url.Values{"id": {id}}.Encode())
}

// queueItemID extracts N from a queue item URL {BaseURL}/queue/item/N/.
func (c *Client) queueItemID(queueURL string) (string, error) {
	rebased, err := c.Rebase(queueURL)
	if err != nil {
		return "", err
	}
	u, err := url.Parse(rebased)
	if err != nil {
		return "", err
	}
	base, err := url.Parse(c.BaseURL)
	if err != nil {
		return "", err
	}
	rest := strings.TrimPrefix(u.Path, strings.TrimRight(base.Path, "/")+"/queue/item/")
	id := strings.TrimSuffix(rest, "/")
	if rest == u.Path || id == "" || strings.Trim(id, "0123456789") != "" || len(id) > 19 {
		return "", fmt.Errorf("not a Jenkins queue item URL: %q", redactURL(queueURL))
	}
	return id, nil
}

// post sends an empty-bodied POST and accepts any 2xx or 3xx response.
func (c *Client) post(ctx context.Context, u string) error {
	req, err := c.newRequest(ctx, http.MethodPost, u, nil)
	if err != nil {
		return err
	}
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 400 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("POST %s returned %s: %s", redactURL(req.URL.String()), resp.Status, body)
	}
	return nil
}

// GetBuild fetches current build status.
func (c *Client) GetBuild(ctx context.Context, buildURL string) (*Build, error) {
	var b Build
	if err := c.getJSON(ctx, strings.TrimRight(buildURL, "/")+"/api/json", &b); err != nil {
		return nil, err
	}
	return &b, nil
}

// ListArtifacts returns the relative paths of a build's archived artifacts.
func (c *Client) ListArtifacts(ctx context.Context, buildURL string) ([]string, error) {
	var resp struct {
		Artifacts []Artifact `json:"artifacts"`
	}
	if err := c.getJSON(ctx, strings.TrimRight(buildURL, "/")+"/api/json?tree=artifacts[relativePath]", &resp); err != nil {
		return nil, err
	}
	paths := make([]string, 0, len(resp.Artifacts))
	for _, a := range resp.Artifacts {
		paths = append(paths, a.RelativePath)
	}
	return paths, nil
}

// ErrArtifactTooLarge is returned by DownloadArtifact when an artifact is larger
// than the caller's limit.
var ErrArtifactTooLarge = errors.New("artifact exceeds size limit")

// DownloadArtifact fetches one artifact's bytes, reading at most maxBytes. A
// larger artifact (by Content-Length, or once more than maxBytes have been read)
// returns ErrArtifactTooLarge. relPath comes from Jenkins' artifact listing and is
// validated so it cannot escape the build's artifact/ tree.
func (c *Client) DownloadArtifact(ctx context.Context, buildURL, relPath string, maxBytes int64) ([]byte, error) {
	escaped, err := escapeArtifactPath(relPath)
	if err != nil {
		return nil, err
	}
	if maxBytes <= 0 {
		return nil, ErrArtifactTooLarge
	}
	maxBytes = min(maxBytes, math.MaxInt64-1) // so maxBytes+1 below cannot overflow
	u := strings.TrimRight(buildURL, "/") + "/artifact/" + escaped
	req, err := c.newRequest(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.doWithin(req, c.DownloadTimeout)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("download %s returned %s", escaped, resp.Status)
	}
	if resp.ContentLength > maxBytes {
		return nil, fmt.Errorf("%w: %d bytes", ErrArtifactTooLarge, resp.ContentLength)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxBytes {
		return nil, fmt.Errorf("%w: more than %d bytes", ErrArtifactTooLarge, maxBytes)
	}
	return data, nil
}

// ConsoleText streams a build's console log ({build}/consoleText) and returns at
// most its last maxBytes bytes, plus the log's total size. Only the tail is kept
// in memory: for a failing build the end of the log is what matters.
func (c *Client) ConsoleText(ctx context.Context, buildURL string, maxBytes int64) (tail []byte, total int64, err error) {
	if maxBytes <= 0 {
		return nil, 0, fmt.Errorf("console log size cap must be positive")
	}
	req, err := c.newRequest(ctx, http.MethodGet, strings.TrimRight(buildURL, "/")+"/consoleText", nil)
	if err != nil {
		return nil, 0, err
	}
	resp, err := c.doWithin(req, c.DownloadTimeout)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, 0, fmt.Errorf("GET %s returned %s", redactURL(req.URL.String()), resp.Status)
	}
	return readTail(resp.Body, maxBytes)
}

// readTail reads r to EOF and returns its last max bytes and total length,
// using at most about max bytes of memory however long r is.
func readTail(r io.Reader, max int64) ([]byte, int64, error) {
	var (
		buf   []byte // the whole input, while it fits in max
		ring  []byte // the last max bytes, once it does not; oldest at pos
		pos   int
		total int64
		chunk = make([]byte, 32<<10)
	)
	for {
		n, err := r.Read(chunk)
		total += int64(n)
		p := chunk[:n]
		switch {
		case ring == nil && int64(len(buf)+n) <= max:
			buf = append(buf, p...)
		case ring == nil:
			all := append(buf, p...)
			ring = make([]byte, max)
			copy(ring, all[int64(len(all))-max:])
			buf = nil
		default:
			for len(p) > 0 {
				k := copy(ring[pos:], p)
				p = p[k:]
				pos = (pos + k) % len(ring)
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, total, err
		}
	}
	if ring == nil {
		return buf, total, nil
	}
	// Rotate the ring left by pos so the oldest byte comes first.
	reverse(ring[:pos])
	reverse(ring[pos:])
	reverse(ring)
	return ring, total, nil
}

func reverse(b []byte) {
	for i, j := 0, len(b)-1; i < j; i, j = i+1, j-1 {
		b[i], b[j] = b[j], b[i]
	}
}

// escapeArtifactPath validates a Jenkins artifact relativePath (with the same
// rules as the S3 key it becomes) and returns it with each segment path-escaped, so
// "#", "?", "%", ";" and spaces in a name cannot change the request.
func escapeArtifactPath(relPath string) (string, error) {
	if err := contract.ValidateArtifactName(relPath); err != nil {
		return "", err
	}
	segs := strings.Split(relPath, "/")
	for i, seg := range segs {
		segs[i] = url.PathEscape(seg)
	}
	return strings.Join(segs, "/"), nil
}

func (c *Client) getJSON(ctx context.Context, u string, out any) error {
	req, err := c.newRequest(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return &StatusError{resp.StatusCode, fmt.Sprintf("GET %s returned %s: %s", redactURL(req.URL.String()), resp.Status, body)}
	}
	err = json.NewDecoder(resp.Body).Decode(out)
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10)) // drain so the connection is reused
	return err
}

// newRequest is the only way this client builds a request. It rebases raw onto
// BaseURL (see Rebase) and only then attaches credentials, so basic auth can never
// be sent to a host other than the configured Jenkins.
func (c *Client) newRequest(ctx context.Context, method, raw string, body io.Reader) (*http.Request, error) {
	u, err := c.Rebase(raw)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return nil, err
	}
	if c.hasBasicAuth() {
		req.SetBasicAuth(c.User, c.APIToken)
	}
	return req, nil
}

// Rebase maps a URL that Jenkins handed back (queue Location, executable.url, …)
// onto the configured BaseURL. Jenkins builds those from its own "root URL"
// setting, which can name a different host or scheme than the one we reach it on
// (or, if misconfigured or compromised, a foreign host). The path must sit under
// BaseURL's path; the scheme and host are always taken from BaseURL. Relative
// URLs are resolved against BaseURL. Anything else is rejected.
func (c *Client) Rebase(raw string) (string, error) {
	base, err := url.Parse(c.BaseURL)
	if err != nil || base.Scheme == "" || base.Host == "" {
		return "", fmt.Errorf("invalid Jenkins base URL %q", redactURL(c.BaseURL))
	}
	root := *base
	root.Path = strings.TrimRight(base.Path, "/") + "/"
	root.RawPath = ""
	root.RawQuery, root.Fragment, root.User = "", "", nil

	ref, err := url.Parse(raw)
	if err != nil || ref.Opaque != "" {
		return "", fmt.Errorf("invalid Jenkins URL %q", redactURL(raw))
	}
	u := root.ResolveReference(ref) // also removes dot segments
	if !strings.HasPrefix(u.Path+"/", root.Path) {
		return "", fmt.Errorf("URL %q is outside the Jenkins root %q", redactURL(raw), redactURL(root.String()))
	}
	for _, seg := range strings.Split(u.Path, "/") {
		if seg == "." || seg == ".." { // e.g. smuggled in as %2e%2e
			return "", fmt.Errorf("URL %q contains a dot segment", redactURL(raw))
		}
	}
	u.Scheme, u.Host, u.User = base.Scheme, base.Host, nil
	u.Fragment, u.RawFragment = "", ""
	return u.String(), nil
}

// checkRedirect refuses any redirect that leaves the BaseURL origin. Redirects in
// answer to a POST (stop, cancel) are not followed at all: they only point back at
// a UI page, and following would turn the POST into a GET.
func (c *Client) checkRedirect(req *http.Request, via []*http.Request) error {
	if len(via) > 0 && via[0].Method == http.MethodPost {
		return http.ErrUseLastResponse
	}
	if len(via) >= 10 {
		return errors.New("stopped after 10 redirects")
	}
	base, err := url.Parse(c.BaseURL)
	if err != nil || !sameOrigin(base, req.URL) {
		return fmt.Errorf("refusing redirect to %q: not the configured Jenkins origin", redactURL(req.URL.String()))
	}
	return nil
}

func sameOrigin(a, b *url.URL) bool {
	return strings.EqualFold(a.Scheme, b.Scheme) &&
		strings.EqualFold(a.Hostname(), b.Hostname()) &&
		effectivePort(a) == effectivePort(b)
}

func effectivePort(u *url.URL) string {
	if p := u.Port(); p != "" {
		return p
	}
	if strings.EqualFold(u.Scheme, "https") {
		return "443"
	}
	return "80"
}

// do sends an API request within APITimeout. See doWithin.
func (c *Client) do(req *http.Request) (*http.Response, error) {
	return c.doWithin(req, c.APITimeout)
}

// doWithin sends req with a deadline of timeout (none if <= 0) covering the
// whole exchange, including reading the body; the deadline is released when the
// body is closed. It scrubs the request URL out of any transport error: the
// *url.Error returned by http.Client embeds the full URL, and callers wrap and log
// these errors, so anything sensitive in a URL would otherwise leak to logs.
func (c *Client) doWithin(req *http.Request, timeout time.Duration) (*http.Response, error) {
	cancel := context.CancelFunc(func() {})
	if timeout > 0 {
		var ctx context.Context
		ctx, cancel = context.WithTimeout(req.Context(), timeout)
		req = req.WithContext(ctx)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		cancel()
		var ue *url.Error
		if errors.As(err, &ue) {
			ue.URL = redactURL(ue.URL)
		}
		return nil, err
	}
	resp.Body = &cancelOnClose{ReadCloser: resp.Body, cancel: cancel}
	return resp, nil
}

// cancelOnClose releases a request's deadline when its body is closed.
type cancelOnClose struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b *cancelOnClose) Close() error {
	err := b.ReadCloser.Close()
	b.cancel()
	return err
}

// redactURL drops userinfo, query, and fragment so a URL is safe to log.
func redactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "<unparseable URL>"
	}
	u.User = nil
	u.RawQuery = ""
	u.ForceQuery = false
	u.Fragment = ""
	u.RawFragment = ""
	return u.String()
}

func (c *Client) hasBasicAuth() bool {
	return c.User != "" && c.APIToken != ""
}
