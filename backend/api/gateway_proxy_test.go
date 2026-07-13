package api

import (
	"net/http"
	"testing"
)

func TestGatewayResponsesAliasPath(t *testing.T) {
	tests := []struct {
		name   string
		method string
		path   string
		wantOK bool
	}{
		{name: "root responses", method: http.MethodPost, path: "/responses", wantOK: true},
		{name: "ccswitch numeric prefix", method: http.MethodPost, path: "/1/responses", wantOK: true},
		{name: "custom route prefix", method: http.MethodPost, path: "/agent/responses", wantOK: true},
		{name: "native v1 responses", method: http.MethodPost, path: "/v1/responses", wantOK: true},
		{name: "get frontend route is not proxied", method: http.MethodGet, path: "/1/responses", wantOK: false},
		{name: "api route is not proxied", method: http.MethodPost, path: "/api/responses", wantOK: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := gatewayResponsesAliasPath(tt.method, tt.path)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if ok && got != "/v1/responses" {
				t.Fatalf("path = %q, want /v1/responses", got)
			}
		})
	}
}
