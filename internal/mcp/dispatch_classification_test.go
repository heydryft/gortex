package mcp

import (
	"testing"

	"github.com/zzet/gortex/internal/daemon"
)

func TestDispatchClassCoversRegisteredAndFacadeTools(t *testing.T) {
	for _, config := range []ToolPolicyConfig{{Preset: "full"}, {Preset: "core", Mode: "defer"}} {
		srv := setupPresetServer(t, config)
		for _, name := range srv.registeredToolNames() {
			if _, known := daemon.ToolDispatchClass(name); !known {
				t.Errorf("registered tool %q has no explicit dispatch class", name)
			}
		}
		for _, spec := range facadeOperationSpecs() {
			if _, known := daemon.ToolDispatchClass(spec.Legacy); !known {
				t.Errorf("facade %s.%s implementation %q has no dispatch class", spec.Facade, spec.Operation, spec.Legacy)
			}
		}
	}
}

func TestDispatchClassUsesEffectiveFacadeOperation(t *testing.T) {
	srv := setupPresetServer(t, ToolPolicyConfig{Preset: "full"})
	cases := []struct {
		name string
		args map[string]any
		want daemon.MCPDispatchClass
	}{
		{"edit_file", nil, daemon.MCPDispatchMutating},
		{"edit", map[string]any{"operation": "file"}, daemon.MCPDispatchMutating},
		{"refactor", map[string]any{"operation": "rename"}, daemon.MCPDispatchMutating},
		{"read", map[string]any{"operation": "file", "target": map[string]any{"file": "just-edited.go"}}, daemon.MCPDispatchFast},
		{"explore", map[string]any{"operation": "outline"}, daemon.MCPDispatchSlow},
		{"analyze", map[string]any{"kind": "dead_code"}, daemon.MCPDispatchSlow},
		{"analyze", map[string]any{"kind": "processes"}, daemon.MCPDispatchSlow},
		{"analyze", map[string]any{"kind": "help"}, daemon.MCPDispatchSlow},
		{"workspace_admin", map[string]any{"operation": "enrich_churn"}, daemon.MCPDispatchMutating},
		{"change_contract", map[string]any{"ack": true}, daemon.MCPDispatchMutating},
		{"change", map[string]any{"operation": "contract", "options": map[string]any{"ack": true}}, daemon.MCPDispatchFast},
		{"find_co_changing_symbols", nil, daemon.MCPDispatchMutating},
		{"analyze", map[string]any{"kind": "co_change", "options": map[string]any{"refresh": true}}, daemon.MCPDispatchSlow},
		{"recall", map[string]any{"operation": "surface"}, daemon.MCPDispatchFast},
		{"remember", map[string]any{"operation": "risk_ack"}, daemon.MCPDispatchMutating},
		{"overlay", map[string]any{"operation": "merge", "arguments": map[string]any{"to_disk": true}}, daemon.MCPDispatchFast},
		{"read", map[string]any{"operation": "symbol", "target": map[string]any{"symbol": "demo::f"}}, daemon.MCPDispatchFast},
		{"session", map[string]any{"operation": "subscribe", "channel": "diagnostics"}, daemon.MCPDispatchFast},
		{"edit", map[string]any{"operation": "not_registered"}, daemon.MCPDispatchFast},
		{"not_registered", nil, daemon.MCPDispatchFast},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if got := srv.DispatchClass(test.name, test.args); got != test.want {
				t.Errorf("class=%s want=%s", got, test.want)
			}
		})
	}
}
