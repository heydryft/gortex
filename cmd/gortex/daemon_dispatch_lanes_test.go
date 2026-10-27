package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/zzet/gortex/internal/daemon"
	"github.com/zzet/gortex/internal/graph"
	gortexmcp "github.com/zzet/gortex/internal/mcp"
	"github.com/zzet/gortex/internal/mcp/streamable"
	"github.com/zzet/gortex/internal/query"
	"github.com/zzet/gortex/internal/testenv"
)

// Only the controlled holders are synthetic; all other calls use the real
// dispatcher, facade classifier, HTTP mount, Unix server and remote router.
// No source file is modified by the simulated mutation holders.
type transportLaneDispatcher struct {
	*mcpDispatcher
	started chan struct{}
	release <-chan struct{}
}

func (d *transportLaneDispatcher) Dispatch(ctx context.Context, sess *daemon.Session, frame []byte) ([]byte, error) {
	var envelope struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
		Params struct {
			Name      string `json:"name"`
			Arguments struct {
				Hold bool `json:"lane_test_hold"`
			} `json:"arguments"`
		} `json:"params"`
	}
	if err := json.Unmarshal(frame, &envelope); err != nil {
		return nil, err
	}
	if envelope.Method == "tools/call" && envelope.Params.Name == "edit_file" && envelope.Params.Arguments.Hold {
		d.started <- struct{}{}
		select {
		case <-d.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return []byte(`{"jsonrpc":"2.0","id":` + string(envelope.ID) + `,"result":{"held":true}}`), nil
	}
	return d.mcpDispatcher.Dispatch(ctx, sess, frame)
}

func TestDaemonHTTPAndUnixShareLanesBeforeRemoteRouting(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, "config"))
	t.Setenv("XDG_DATA_HOME", filepath.Join(home, "data"))
	t.Setenv("XDG_CACHE_HOME", filepath.Join(home, "cache"))
	t.Setenv("GORTEX_QUERY_LOG_DISABLE", "1")
	t.Setenv("GORTEX_TOOLS", "full")
	t.Setenv("GORTEX_TOOLS_MODE", "hide")
	dir := testenv.ShortTempDir(t)
	socket := filepath.Join(dir, "s")
	t.Setenv("GORTEX_DAEMON_SOCKET", socket)
	t.Setenv("GORTEX_DAEMON_PIDFILE", filepath.Join(dir, "p"))
	t.Setenv("GORTEX_DAEMON_STATEFILE", filepath.Join(dir, "state.json"))

	var remoteCalls atomic.Int32
	remoteIdentity := make(chan string, 2)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remoteCalls.Add(1)
		select {
		case remoteIdentity <- r.Header.Get(streamable.HeaderSessionID):
		default:
		}
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodPost || r.URL.Path != "/v1/tools/explore" {
			http.Error(w, "unexpected remote endpoint", http.StatusBadRequest)
			return
		}
		_, _ = io.WriteString(w, `{"remote":true}`)
	}))
	t.Cleanup(upstream.Close)
	router := daemon.NewRouter(daemon.RouterConfig{Servers: &daemon.ServersConfig{Server: []daemon.ServerEntry{{Slug: "remote", URL: upstream.URL, Workspaces: []string{"lane-remote"}}}}, LocalSlug: daemon.LocalServerSentinel, Logger: zap.NewNop()})
	g := graph.New()
	policy := gortexmcp.ToolPolicyConfig{Preset: "full"}
	srv := gortexmcp.NewServer(query.NewEngine(g), g, nil, nil, zap.NewNop(), nil, gortexmcp.MultiRepoOptions{ToolPolicy: &policy})
	inner := newMCPDispatcher(srv, nil, zap.NewNop())
	inner.SetRouter(router)
	limit := daemon.MCPDispatchSnapshot().Limit
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	dispatcher := &transportLaneDispatcher{mcpDispatcher: inner, started: make(chan struct{}, limit), release: release}
	unix, unixDone := startProxyRestartDaemon(t, socket, dispatcher)
	t.Cleanup(func() { unblock(); stopProxyRestartDaemon(t, unix, unixDone) })
	mount := buildDaemonStreamableHandler(dispatcher, daemon.NewSessionRegistry(), router, zap.NewNop(), nil)
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// A server-side request deadline (not the HTTP client's timeout) exercises
		// the required deadline-minus100ms queue budget through the real mount.
		ctx, cancel := context.WithTimeout(r.Context(), 500*time.Millisecond)
		defer cancel()
		mount.ServeHTTP(w, r.WithContext(ctx))
	}))
	t.Cleanup(httpServer.Close)
	httpClient := &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{Proxy: nil}}
	t.Cleanup(httpClient.CloseIdleConnections)
	post := func(session string, frame string) ([]byte, string) {
		t.Helper()
		req, err := http.NewRequest(http.MethodPost, httpServer.URL+"/mcp", bytes.NewBufferString(frame))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		if session != "" {
			req.Header.Set(streamable.HeaderSessionID, session)
		}
		response, err := httpClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		payload, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != http.StatusOK {
			t.Fatalf("HTTP status%d: %s", response.StatusCode, payload)
		}
		return payload, response.Header.Get(streamable.HeaderSessionID)
	}
	_, sid := post("", `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"lane-test","version":"1"}}}`)
	if sid == "" {
		t.Fatal("HTTP initialization minted no session")
	}
	client, err := daemon.DialTo(socket, daemon.Handshake{Mode: daemon.ModeMCP, PID: os.Getpid(), Tools: "full", ToolsMode: "hide"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := client.WriteMCPFrame([]byte(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"lane-test","version":"1"}}}`)); err != nil {
		t.Fatal(err)
	}
	if reply, err := client.ReadMCPFrame(); err != nil || !bytes.Contains(reply, []byte(`"result"`)) {
		t.Fatalf("Unix initialize reply=%s err=%v", reply, err)
	}
	if err := client.WriteMCPFrame([]byte(`{"jsonrpc":"2.0","method":"notifications/initialized"}`)); err != nil {
		t.Fatal(err)
	}
	for i := range limit {
		frame := []byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"tools/call","params":{"name":"edit_file","arguments":{"lane_test_hold":true}}}`, i+10))
		if err := client.WriteMCPFrame(frame); err != nil {
			t.Fatal(err)
		}
		select {
		case <-dispatcher.started:
		case <-time.After(time.Second):
			t.Fatal("Unix mutation did not occupy shared dispatch capacity")
		}
	}
	before := daemon.MCPDispatchSnapshot()
	if before.Lanes["mutating"].InFlight != limit {
		t.Fatalf("Unix did not occupy process cap: %+v", before)
	}
	frame := `{"jsonrpc":"2.0","id":88,"method":"tools/call","params":{"name":"explore","arguments":{"operation":"outline","workspace":"lane-remote"}}}`
	busy, _ := post(sid, frame)
	var refusal struct {
		Error struct {
			Code int            `json:"code"`
			Data map[string]any `json:"data"`
		} `json:"error"`
	}
	if err := json.Unmarshal(busy, &refusal); err != nil {
		t.Fatal(err)
	}
	retry, _ := refusal.Error.Data["retry_after_ms"].(float64)
	if refusal.Error.Code != -32002 || refusal.Error.Data["delivery_unknown"] != false || refusal.Error.Data["retryable"] != true || retry < 100 || retry > 5000 {
		t.Fatalf("HTTP lane refusal=%s", busy)
	}
	if remoteCalls.Load() != 0 {
		t.Fatal("HTTP remote call bypassed occupied Unix admission cap")
	}
	after := daemon.MCPDispatchSnapshot()
	if after.Lanes["slow"].Refusals != before.Lanes["slow"].Refusals+1 {
		t.Fatalf("effective explore.outline did not use slow admission: before=%+v after=%+v", before, after)
	}
	unblock()
	for received := 0; received < limit; {
		reply, err := client.ReadMCPFrame()
		if err != nil {
			t.Fatal(err)
		}
		var message struct {
			ID    json.RawMessage `json:"id"`
			Error json.RawMessage `json:"error"`
		}
		if err := json.Unmarshal(reply, &message); err != nil {
			t.Fatal(err)
		}
		if len(message.ID) == 0 {
			continue
		}
		if len(message.Error) > 0 {
			t.Fatalf("holder failed: %s", reply)
		}
		received++
	}
	success, _ := post(sid, frame) // Busy was not a committed/replayed tool result.
	if remoteCalls.Load() != 1 || !bytes.Contains(success, []byte(`"remote":true`)) {
		t.Fatalf("admitted remote retry count=%d reply=%s", remoteCalls.Load(), success)
	}
	select {
	case identity := <-remoteIdentity:
		if identity != sid {
			t.Fatalf("remote identity=%q want=%q", identity, sid)
		}
	default:
		t.Fatal("remote received no session identity")
	}
}
