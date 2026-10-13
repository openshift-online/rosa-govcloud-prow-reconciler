package reconciler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"sync"
	"testing"

	"github.com/openshift-online/rosa-govcloud-prow-reconciler/internal/jenkins"
)

type fjQueueItem struct {
	id        int
	job       string
	params    url.Values
	build     int // 0 until an executor picks it up
	cancelled bool
	since     int64
}

type fjBuild struct {
	number    int
	building  bool
	result    string
	params    url.Values
	artifacts map[string]string
	console   string // consoleText; "" serves defaultConsole
	timestamp int64
}

const defaultConsole = "Started by remote host\nFinished\n"

// fakeJenkins is a minimal in-memory Jenkins: buildWithParameters, queue items,
// the queue listing, job build listings, build status, and artifacts.
type fakeJenkins struct {
	mu            sync.Mutex
	srv           *httptest.Server
	triggers      []url.Values
	queue         map[int]*fjQueueItem
	nextQueue     int
	builds        map[string][]*fjBuild // per job, oldest first
	autoStart     bool                  // queue items get a running build immediately
	failTrigger   bool
	triggerStatus int            // if set, buildWithParameters answers this status
	lostTrigger   bool           // queue the build, then answer 500 (response lost)
	failReads     bool           // every GET returns 500
	readStatus    int            // if set, every GET answers this status
	stops         []string       // "job/n" per POST .../stop
	cancels       []int          // queue ids per POST /queue/cancelItem
	ignoreStop    bool           // accept stop requests but keep building
	startOnCancel bool           // a cancelled queue item starts its build instead (race)
	failConsole   bool           // consoleText returns 500
	zeroDuration  bool           // builds report duration 0 instead of 1500 ms
	consoleReads  int            // consoleText requests served
	failArtifacts map[string]int // artifact path -> remaining downloads that return 500 (<0: always)
	// beforeTrigger, if set, runs (without the lock) at the start of every
	// buildWithParameters request, e.g. to block it.
	beforeTrigger func()
	// beforeBuild, if set, runs (without the lock) at the start of every build
	// status request.
	beforeBuild func()
}

func (fj *fakeJenkins) set(f func(fj *fakeJenkins)) {
	fj.mu.Lock()
	defer fj.mu.Unlock()
	f(fj)
}

func newFakeJenkins(t *testing.T) *fakeJenkins {
	fj := &fakeJenkins{queue: map[int]*fjQueueItem{}, builds: map[string][]*fjBuild{}, autoStart: true}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /job/{job}/buildWithParameters", fj.handleTrigger)
	mux.HandleFunc("GET /queue/item/{id}/api/json", fj.handleQueueItem)
	mux.HandleFunc("GET /queue/api/json", fj.handleQueue)
	mux.HandleFunc("GET /job/{job}/api/json", fj.handleJob)
	mux.HandleFunc("GET /job/{job}/{n}/api/json", fj.handleBuild)
	mux.HandleFunc("GET /job/{job}/{n}/artifact/{path...}", fj.handleArtifact)
	mux.HandleFunc("GET /job/{job}/{n}/consoleText", fj.handleConsole)
	mux.HandleFunc("POST /job/{job}/{n}/stop", fj.handleStop)
	mux.HandleFunc("POST /queue/cancelItem", fj.handleCancel)
	fj.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fj.mu.Lock()
		fail, code := fj.failReads && r.Method == http.MethodGet, fj.readStatus
		fj.mu.Unlock()
		if fail {
			code = http.StatusInternalServerError
		}
		if code != 0 && r.Method == http.MethodGet {
			http.Error(w, "injected failure", code)
			return
		}
		mux.ServeHTTP(w, r)
	}))
	t.Cleanup(fj.srv.Close)
	return fj
}

func (fj *fakeJenkins) client() *jenkins.Client {
	return jenkins.New(fj.srv.URL, "svc", "api-token")
}

func (fj *fakeJenkins) triggerCount() int {
	fj.mu.Lock()
	defer fj.mu.Unlock()
	return len(fj.triggers)
}

func (fj *fakeJenkins) lastTrigger() url.Values {
	fj.mu.Lock()
	defer fj.mu.Unlock()
	if len(fj.triggers) == 0 {
		return nil
	}
	return fj.triggers[len(fj.triggers)-1]
}

// addBuild records a build directly (e.g. one started before a restart).
func (fj *fakeJenkins) addBuild(job, prowJobID string, building bool, result string) int {
	fj.mu.Lock()
	defer fj.mu.Unlock()
	n := len(fj.builds[job]) + 1
	fj.builds[job] = append(fj.builds[job], &fjBuild{
		number: n, building: building, result: result,
		params:    url.Values{"PROW_JOB_ID": {prowJobID}},
		timestamp: 1767225600000, // 2026-01-01T00:00:00Z
	})
	return n
}

// addQueued records a queue item waiting for an executor.
func (fj *fakeJenkins) addQueued(job, prowJobID string) int {
	fj.mu.Lock()
	defer fj.mu.Unlock()
	fj.nextQueue++
	fj.queue[fj.nextQueue] = &fjQueueItem{id: fj.nextQueue, job: job, params: url.Values{"PROW_JOB_ID": {prowJobID}}, since: 1767225600000}
	return fj.nextQueue
}

func (fj *fakeJenkins) finish(job string, n int, result string) {
	fj.mu.Lock()
	defer fj.mu.Unlock()
	b := fj.builds[job][n-1]
	b.building, b.result = false, result
}

func (fj *fakeJenkins) cancelQueued(id int) {
	fj.mu.Lock()
	defer fj.mu.Unlock()
	fj.queue[id].cancelled = true
}

func (fj *fakeJenkins) startLocked(q *fjQueueItem) {
	n := len(fj.builds[q.job]) + 1
	fj.builds[q.job] = append(fj.builds[q.job], &fjBuild{number: n, building: true, params: q.params, timestamp: q.since})
	q.build = n
}

func (fj *fakeJenkins) handleTrigger(w http.ResponseWriter, r *http.Request) {
	fj.mu.Lock()
	hook := fj.beforeTrigger
	fj.mu.Unlock()
	if hook != nil {
		hook()
	}
	fj.mu.Lock()
	defer fj.mu.Unlock()
	if fj.triggerStatus != 0 {
		http.Error(w, "injected failure", fj.triggerStatus)
		return
	}
	if fj.failTrigger {
		http.Error(w, "injected failure", http.StatusInternalServerError)
		return
	}
	_ = r.ParseForm()
	fj.triggers = append(fj.triggers, r.PostForm)
	fj.nextQueue++
	q := &fjQueueItem{id: fj.nextQueue, job: r.PathValue("job"), params: r.PostForm, since: 1767225600000}
	fj.queue[q.id] = q
	if fj.autoStart {
		fj.startLocked(q)
	}
	if fj.lostTrigger {
		http.Error(w, "upstream timeout", http.StatusGatewayTimeout)
		return
	}
	w.Header().Set("Location", fmt.Sprintf("%s/queue/item/%d/", fj.srv.URL, q.id))
	w.WriteHeader(http.StatusCreated)
}

func (fj *fakeJenkins) handleQueueItem(w http.ResponseWriter, r *http.Request) {
	fj.mu.Lock()
	defer fj.mu.Unlock()
	id, _ := strconv.Atoi(r.PathValue("id"))
	q, ok := fj.queue[id]
	if !ok {
		http.NotFound(w, r)
		return
	}
	out := map[string]any{"cancelled": q.cancelled}
	if q.build != 0 {
		out["executable"] = map[string]any{"url": fmt.Sprintf("%s/job/%s/%d/", fj.srv.URL, q.job, q.build)}
	}
	_ = json.NewEncoder(w).Encode(out)
}

func paramsAction(v url.Values) []map[string]any {
	var ps []map[string]any
	keys := make([]string, 0, len(v))
	for k := range v {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		ps = append(ps, map[string]any{"name": k, "value": v.Get(k)})
	}
	return []map[string]any{{"_class": "hudson.model.CauseAction"}, {"parameters": ps}}
}

func (fj *fakeJenkins) handleQueue(w http.ResponseWriter, _ *http.Request) {
	fj.mu.Lock()
	defer fj.mu.Unlock()
	var items []map[string]any
	for _, q := range fj.queue {
		if q.build != 0 || q.cancelled {
			continue
		}
		items = append(items, map[string]any{
			"id": q.id, "url": fmt.Sprintf("queue/item/%d/", q.id), "inQueueSince": q.since,
			"task":    map[string]any{"name": q.job},
			"actions": paramsAction(q.params),
		})
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"items": items})
}

func (fj *fakeJenkins) buildJSON(job string, b *fjBuild) map[string]any {
	var arts []map[string]any
	paths := make([]string, 0, len(b.artifacts))
	for p := range b.artifacts {
		paths = append(paths, p)
	}
	sort.Strings(paths) // a stable order, like Jenkins
	for _, p := range paths {
		arts = append(arts, map[string]any{"relativePath": p})
	}
	var result any // JSON null while building, or for a finished build with no result
	if !b.building && b.result != "" {
		result = b.result
	}
	duration := 1500
	if fj.zeroDuration {
		duration = 0
	}
	return map[string]any{
		"number": b.number, "url": fmt.Sprintf("%s/job/%s/%d/", fj.srv.URL, job, b.number),
		"building": b.building, "result": result, "duration": duration, "timestamp": b.timestamp,
		"actions": paramsAction(b.params), "artifacts": arts,
	}
}

func (fj *fakeJenkins) handleJob(w http.ResponseWriter, r *http.Request) {
	fj.mu.Lock()
	defer fj.mu.Unlock()
	job := r.PathValue("job")
	var builds []map[string]any
	for i := len(fj.builds[job]) - 1; i >= 0; i-- { // newest first, like Jenkins
		builds = append(builds, fj.buildJSON(job, fj.builds[job][i]))
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"builds": builds})
}

func (fj *fakeJenkins) lookup(r *http.Request) (string, *fjBuild) {
	job := r.PathValue("job")
	n, _ := strconv.Atoi(r.PathValue("n"))
	if n < 1 || n > len(fj.builds[job]) {
		return job, nil
	}
	return job, fj.builds[job][n-1]
}

func (fj *fakeJenkins) handleBuild(w http.ResponseWriter, r *http.Request) {
	fj.mu.Lock()
	hook := fj.beforeBuild
	fj.mu.Unlock()
	if hook != nil {
		hook()
	}
	fj.mu.Lock()
	defer fj.mu.Unlock()
	job, b := fj.lookup(r)
	if b == nil {
		http.NotFound(w, r)
		return
	}
	_ = json.NewEncoder(w).Encode(fj.buildJSON(job, b))
}

func (fj *fakeJenkins) handleArtifact(w http.ResponseWriter, r *http.Request) {
	fj.mu.Lock()
	defer fj.mu.Unlock()
	_, b := fj.lookup(r)
	if b == nil {
		http.NotFound(w, r)
		return
	}
	p := r.PathValue("path")
	data, ok := b.artifacts[p]
	if !ok {
		http.NotFound(w, r)
		return
	}
	if n := fj.failArtifacts[p]; n != 0 {
		fj.failArtifacts[p] = n - 1
		http.Error(w, "injected failure", http.StatusInternalServerError)
		return
	}
	_, _ = w.Write([]byte(data))
}

func (fj *fakeJenkins) handleStop(w http.ResponseWriter, r *http.Request) {
	fj.mu.Lock()
	defer fj.mu.Unlock()
	job, b := fj.lookup(r)
	if b == nil {
		http.NotFound(w, r)
		return
	}
	fj.stops = append(fj.stops, fmt.Sprintf("%s/%d", job, b.number))
	if b.building && !fj.ignoreStop {
		b.building, b.result = false, "ABORTED"
	}
	http.Redirect(w, r, fmt.Sprintf("/job/%s/%d/", job, b.number), http.StatusFound)
}

func (fj *fakeJenkins) handleCancel(w http.ResponseWriter, r *http.Request) {
	fj.mu.Lock()
	defer fj.mu.Unlock()
	id, _ := strconv.Atoi(r.URL.Query().Get("id"))
	q, ok := fj.queue[id]
	if !ok {
		http.NotFound(w, r)
		return
	}
	fj.cancels = append(fj.cancels, id)
	switch {
	case q.build != 0 || q.cancelled:
	case fj.startOnCancel:
		fj.startLocked(q)
	default:
		q.cancelled = true
	}
	w.WriteHeader(http.StatusNoContent)
}

func (fj *fakeJenkins) stopCalls() []string {
	fj.mu.Lock()
	defer fj.mu.Unlock()
	return append([]string(nil), fj.stops...)
}

func (fj *fakeJenkins) cancelCalls() []int {
	fj.mu.Lock()
	defer fj.mu.Unlock()
	return append([]int(nil), fj.cancels...)
}

func (fj *fakeJenkins) building(job string, n int) bool {
	fj.mu.Lock()
	defer fj.mu.Unlock()
	return fj.builds[job][n-1].building
}

func (fj *fakeJenkins) handleConsole(w http.ResponseWriter, r *http.Request) {
	fj.mu.Lock()
	defer fj.mu.Unlock()
	_, b := fj.lookup(r)
	if b == nil {
		http.NotFound(w, r)
		return
	}
	fj.consoleReads++
	if fj.failConsole {
		http.Error(w, "injected failure", http.StatusInternalServerError)
		return
	}
	c := b.console
	if c == "" {
		c = defaultConsole
	}
	_, _ = w.Write([]byte(c))
}
