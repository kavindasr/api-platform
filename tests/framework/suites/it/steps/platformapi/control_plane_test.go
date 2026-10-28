/*
 * Copyright (c) 2026, WSO2 LLC. (https://www.wso2.com).
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package platformapi

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cucumber/godog"
	"github.com/stretchr/testify/require"

	"github.com/wso2/api-platform/tests/framework/core/actor"
	"github.com/wso2/api-platform/tests/framework/core/catalog/shared"
	"github.com/wso2/api-platform/tests/framework/core/cleanup"
	"github.com/wso2/api-platform/tests/framework/core/components"
	frameworkruntime "github.com/wso2/api-platform/tests/framework/core/runtime"
	"github.com/wso2/api-platform/tests/framework/core/util/httpx"
	"github.com/wso2/api-platform/tests/framework/core/util/tcontext"
)

func TestArtifactPath(t *testing.T) {
	tests := []struct {
		name    string
		kind    string
		handle  string
		want    string
		wantErr bool
	}{
		{name: "template", kind: "LlmProviderTemplate", handle: "tmpl-1", want: "/llm-provider-templates/tmpl-1"},
		{name: "provider", kind: "LlmProvider", handle: "prov-1", want: "/llm-providers/prov-1"},
		{name: "proxy", kind: "LlmProxy", handle: "proxy-1", want: "/llm-proxies/proxy-1"},
		{name: "mcp", kind: "Mcp", handle: "mcp-1", want: "/mcp-proxies/mcp-1"},
		{name: "rest api", kind: "RestApi", handle: "api-1", want: "/rest-apis/api-1"},
		{name: "gateway agent is an agent proxy", kind: "Agent", handle: "agent-1", want: "/agent-proxies/agent-1"},
		{name: "control-plane kind is not a gateway kind", kind: "AgentProxy", handle: "x", wantErr: true},
		{name: "unknown kind", kind: "WebSubApi", handle: "x", wantErr: true},
		{name: "empty kind", kind: "", handle: "x", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := artifactPath(tt.kind, tt.handle)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestDeploymentStatusMatches(t *testing.T) {
	tests := []struct {
		name string
		resp *httpx.Response
		want string
		ok   bool
	}{
		{
			name: "matching status present",
			resp: &httpx.Response{StatusCode: http.StatusOK, Body: []byte(`{"list":[{"status":"DEPLOYED"}]}`)},
			want: "DEPLOYED",
			ok:   true,
		},
		{
			name: "different status present",
			resp: &httpx.Response{StatusCode: http.StatusOK, Body: []byte(`{"list":[{"status":"UNDEPLOYED"}]}`)},
			want: "DEPLOYED",
			ok:   false,
		},
		{
			name: "multiple deployments, one matches",
			resp: &httpx.Response{StatusCode: http.StatusOK, Body: []byte(`{"list":[{"status":"ARCHIVED"},{"status":"DEPLOYED"}]}`)},
			want: "DEPLOYED",
			ok:   true,
		},
		{
			name: "empty list",
			resp: &httpx.Response{StatusCode: http.StatusOK, Body: []byte(`{"list":[]}`)},
			want: "DEPLOYED",
			ok:   false,
		},
		{
			name: "non-200 status",
			resp: &httpx.Response{StatusCode: http.StatusNotFound, Body: []byte(`{"list":[{"status":"DEPLOYED"}]}`)},
			want: "DEPLOYED",
			ok:   false,
		},
		{
			name: "malformed body",
			resp: &httpx.Response{StatusCode: http.StatusOK, Body: []byte(`not json`)},
			want: "DEPLOYED",
			ok:   false,
		},
		{
			name: "nil response",
			resp: nil,
			want: "DEPLOYED",
			ok:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.ok, deploymentStatusMatches(tt.resp, tt.want))
		})
	}
}

func TestRegisterCreatedArtifactRegistersCleanupInDependencyOrder(t *testing.T) {
	ctx, reg := trackingContext(t)
	funnel := httpx.NewFunnel(httpx.NewClient(httpx.Options{}), 1, time.Millisecond)

	for kind, id := range map[string]string{"LlmProvider": "prov-1", "LlmProxy": "proxy-1", "Mcp": "mcp-1"} {
		require.NoError(t, RegisterCreatedArtifact(ctx, nil, funnel, kind, id))
	}
	require.Equal(t, []string{"prov-1"}, pendingIDs(reg, platformProviderKind))
	require.Equal(t, []string{"proxy-1"}, pendingIDs(reg, platformProxyKind))
	require.Equal(t, []string{"mcp-1"}, pendingIDs(reg, platformMCPKind))

	// A proxy deletes before the provider it references; both delete before an associated
	// gateway, which deletes before the project.
	require.Less(t, platformProxyKind.Order, platformProviderKind.Order)
	require.Less(t, platformProviderKind.Order, platformGatewayKind.Order)
	require.Less(t, platformMCPKind.Order, platformGatewayKind.Order)
	require.Less(t, platformGatewayKind.Order, platformProjectKind.Order)

	require.ErrorContains(t, RegisterCreatedArtifact(ctx, nil, funnel, "RestApi", "api-1"), "no control-plane cleanup")
	require.ErrorContains(t, RegisterCreatedArtifact(ctx, nil, funnel, "", "x"), "no control-plane cleanup")
}

func TestIsLastProjectValidation(t *testing.T) {
	tests := []struct {
		name string
		resp *httpx.Response
		want bool
	}{
		{
			name: "last project",
			resp: &httpx.Response{StatusCode: http.StatusBadRequest,
				Body: []byte(`{"code":"VALIDATION_FAILED","message":"Organization must have at least one project"}`)},
			want: true,
		},
		{
			name: "different validation",
			resp: &httpx.Response{StatusCode: http.StatusBadRequest,
				Body: []byte(`{"code":"VALIDATION_FAILED","message":"Project has associated MCP proxies"}`)},
			want: false,
		},
		{
			name: "different status",
			resp: &httpx.Response{StatusCode: http.StatusInternalServerError,
				Body: []byte(`{"code":"VALIDATION_FAILED","message":"Organization must have at least one project"}`)},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, isLastProjectValidation(tt.resp))
		})
	}
}

func TestActorCredentials(t *testing.T) {
	for who, want := range map[string]actor.Credentials{
		"":          actor.Administrator(),
		"admin":     actor.Administrator(),
		" admin ":   actor.Administrator(),
		"publisher": actor.Publisher(),
		"developer": actor.Developer(),
	} {
		got, err := actorCredentials(who)
		require.NoError(t, err, who)
		require.Equal(t, want, got, who)
	}
	for _, who := range []string{"narrow", "consumer", "Admin"} {
		_, err := actorCredentials(who)
		require.ErrorContains(t, err, "unknown control-plane actor", who)
	}
}

// trackingContext returns a runner context with a fresh cleanup registry installed.
func trackingContext(t *testing.T) (context.Context, *cleanup.Registry) {
	t.Helper()
	ctx := tcontext.WithLocal(context.Background(), tcontext.NewLocal("runner"))
	reg := cleanup.NewRegistry(slog.Default())
	require.NoError(t, cleanup.Install(ctx, reg))
	return ctx, reg
}

func pendingIDs(reg *cleanup.Registry, kind cleanup.Kind) []string {
	var ids []string
	for _, res := range reg.Pending() {
		if res.Kind.Name == kind.Name {
			ids = append(ids, res.ID)
		}
	}
	return ids
}

func created(location string) *httpx.Response {
	return &httpx.Response{StatusCode: http.StatusCreated, Headers: http.Header{"Location": []string{location}}}
}

func TestTrackControlPlaneResourceRegistersWhatTheControlPlaneCreated(t *testing.T) {
	ctx, reg := trackingContext(t)
	s := &Steps{}

	require.NoError(t, s.trackControlPlaneResource(ctx, http.MethodPost, nil, created("/api/v0.9/agent-proxies/weather")))
	require.NoError(t, s.trackControlPlaneResource(ctx, http.MethodPost, []byte(`{"name":"d","gatewayId":"it-gateway"}`),
		created("/api/v0.9/agent-proxies/weather/deployments/dep-1")))
	require.NoError(t, s.trackControlPlaneResource(ctx, http.MethodPost, nil,
		created("/api/v0.9/agent-proxies/weather/api-keys/key%2D1")))

	require.Equal(t, []string{"weather"}, pendingIDs(reg, platformAgentProxyKind))
	require.Equal(t, []string{"weather\x00dep-1\x00it-gateway"}, pendingIDs(reg, platformAgentDeploymentKind))
	require.Equal(t, []string{"weather\x00key-1"}, pendingIDs(reg, platformAgentAPIKeyKind), "the Location is unescaped")

	// Cleanup order: keys, then deployments, then the Agent proxy that owns both.
	require.Less(t, platformAgentAPIKeyKind.Order, platformAgentDeploymentKind.Order)
	require.Less(t, platformAgentDeploymentKind.Order, platformAgentProxyKind.Order)
	require.Less(t, platformAgentProxyKind.Order, platformProjectKind.Order)
	require.Less(t, platformAgentProxyKind.Order, platformSecretKind.Order)

	// A repeated registration of the same resource is not a second record.
	require.NoError(t, s.trackControlPlaneResource(ctx, http.MethodPost, nil, created("/api/v0.9/agent-proxies/weather")))
	require.Len(t, pendingIDs(reg, platformAgentProxyKind), 1)
}

func TestTrackControlPlaneResourceIgnoresWhatIsNotATrackedCreate(t *testing.T) {
	ctx, reg := trackingContext(t)
	s := &Steps{}
	for _, tc := range []struct {
		method string
		resp   *httpx.Response
	}{
		{http.MethodPost, &httpx.Response{StatusCode: http.StatusOK, Headers: http.Header{"Location": []string{"/api/v0.9/agent-proxies/a"}}}},
		{http.MethodPost, &httpx.Response{StatusCode: http.StatusBadRequest}},
		{http.MethodPost, created("")},
		{http.MethodPut, created("/api/v0.9/agent-proxies/a")},
		{http.MethodPut, created("/api/v0.9/rest-apis/a")},
		{http.MethodPost, created("/api/v0.9/agent-proxies/a/builds/b")},
		{http.MethodPost, created("/api/v0.9/rest-apis/a/builds/b")},
		{http.MethodPost, created("/api/v0.9/rest-apis/a/api-keys/k")},
		{http.MethodPost, created("/api/v0.9/organizations/o")},
		{http.MethodPost, created("/api/v0.9/subscriptions/s")},
		{http.MethodPost, created("/api/v1/rest-apis/a")},
	} {
		require.NoError(t, s.trackControlPlaneResource(ctx, tc.method, nil, tc.resp))
	}
	require.Empty(t, reg.Pending())
}

func TestTrackControlPlaneResourceRequiresTheDeploymentGateway(t *testing.T) {
	ctx, reg := trackingContext(t)
	s := &Steps{}
	for _, body := range [][]byte{nil, []byte(`{`), []byte(`{"name":"d"}`), []byte(`{"gatewayId":"  "}`)} {
		err := s.trackControlPlaneResource(ctx, http.MethodPost, body, created("/api/v0.9/agent-proxies/a/deployments/d"))
		require.Error(t, err, string(body))
	}
	require.Empty(t, reg.Pending())
}

func TestTrackControlPlaneResourceDeregistersWhatTheScenarioDeleted(t *testing.T) {
	ctx, reg := trackingContext(t)
	s := &Steps{}
	require.NoError(t, s.trackControlPlaneResource(ctx, http.MethodPost, nil, created("/api/v0.9/agent-proxies/weather")))
	require.NoError(t, s.trackControlPlaneResource(ctx, http.MethodPost, nil, created("/api/v0.9/agent-proxies/other")))
	require.NoError(t, s.trackControlPlaneResource(ctx, http.MethodPost, []byte(`{"gatewayId":"gw"}`),
		created("/api/v0.9/agent-proxies/weather/deployments/dep-1")))
	require.NoError(t, s.trackControlPlaneResource(ctx, http.MethodPost, []byte(`{"gatewayId":"gw"}`),
		created("/api/v0.9/agent-proxies/weather/deployments/dep-12")))
	require.NoError(t, s.trackControlPlaneResource(ctx, http.MethodPost, nil, created("/api/v0.9/agent-proxies/weather/api-keys/k")))

	deleted := func(url string) *httpx.Response {
		return &httpx.Response{StatusCode: http.StatusNoContent, URL: url}
	}
	// A failed delete leaves the record in place.
	require.NoError(t, s.trackControlPlaneResource(ctx, http.MethodDelete, nil,
		&httpx.Response{StatusCode: http.StatusConflict, URL: "https://cp/api/v0.9/agent-proxies/weather/deployments/dep-1"}))
	require.Len(t, pendingIDs(reg, platformAgentDeploymentKind), 2)

	require.NoError(t, s.trackControlPlaneResource(ctx, http.MethodDelete, nil,
		deleted("https://cp:9243/api/v0.9/agent-proxies/weather/deployments/dep-1")))
	require.Equal(t, []string{"weather\x00dep-12\x00gw"}, pendingIDs(reg, platformAgentDeploymentKind),
		"only the deleted deployment is dropped, not one whose id shares its prefix")
	require.NoError(t, s.trackControlPlaneResource(ctx, http.MethodDelete, nil,
		deleted("https://cp:9243/api/v0.9/agent-proxies/weather/api-keys/k")))
	require.Empty(t, pendingIDs(reg, platformAgentAPIKeyKind))
	require.NoError(t, s.trackControlPlaneResource(ctx, http.MethodDelete, nil,
		deleted("https://cp:9243/api/v0.9/agent-proxies/weather?force=true")))
	require.Equal(t, []string{"other"}, pendingIDs(reg, platformAgentProxyKind))
}

func TestTrackControlPlaneResourceRegistersEveryTrackedCollection(t *testing.T) {
	ctx, reg := trackingContext(t)
	s := &Steps{}
	for collection := range platformCollectionKinds {
		require.NoError(t, s.trackControlPlaneResource(ctx, http.MethodPost, nil, created("/api/v0.9/"+collection+"/x")))
	}
	for collection, kind := range platformCollectionKinds {
		require.Equal(t, []string{"x"}, pendingIDs(reg, kind), collection)
	}

	for _, collection := range []string{"rest-apis", "graphql-apis", "mcp-proxies", "llm-providers", "llm-proxies"} {
		require.NoError(t, s.trackControlPlaneResource(ctx, http.MethodPost, []byte(`{"gatewayId":"gw"}`),
			created("/api/v0.9/"+collection+"/x/deployments/d")))
	}
	require.ElementsMatch(t, []string{
		"rest-apis\x00x\x00d\x00gw", "graphql-apis\x00x\x00d\x00gw", "mcp-proxies\x00x\x00d\x00gw",
		"llm-providers\x00x\x00d\x00gw", "llm-proxies\x00x\x00d\x00gw",
	}, pendingIDs(reg, platformDeploymentKind))

	// A deployment undeploys before the artifact that owns it deletes, and an artifact deletes
	// before the gateway, project and portal it depends on.
	for _, artifact := range []cleanup.Kind{platformRESTAPIKind, platformGraphQLAPIKind, platformMCPKind,
		platformProviderKind, platformProxyKind} {
		require.Less(t, platformDeploymentKind.Order, artifact.Order, artifact.Name)
		require.Less(t, artifact.Order, platformGatewayKind.Order, artifact.Name)
		require.Less(t, artifact.Order, platformProjectKind.Order, artifact.Name)
	}
	require.Less(t, platformRESTAPIKind.Order, platformAPIPortalKind.Order)
	require.Less(t, platformGraphQLAPIKind.Order, platformAPIPortalKind.Order)
}

func TestTrackControlPlaneResourceRequiresTheGatewayOfEveryDeployment(t *testing.T) {
	ctx, reg := trackingContext(t)
	s := &Steps{}
	for _, body := range [][]byte{nil, []byte(`{`), []byte(`{"name":"d"}`)} {
		err := s.trackControlPlaneResource(ctx, http.MethodPost, body, created("/api/v0.9/rest-apis/a/deployments/d"))
		require.Error(t, err, string(body))
	}
	require.Empty(t, reg.Pending())
}

func TestTrackControlPlaneResourceDeregistersDeletedPlatformResources(t *testing.T) {
	ctx, reg := trackingContext(t)
	s := &Steps{}
	require.NoError(t, s.trackControlPlaneResource(ctx, http.MethodPost, nil, created("/api/v0.9/rest-apis/a")))
	require.NoError(t, s.trackControlPlaneResource(ctx, http.MethodPost, nil, created("/api/v0.9/rest-apis/b")))
	require.NoError(t, s.trackControlPlaneResource(ctx, http.MethodPost, []byte(`{"gatewayId":"gw"}`),
		created("/api/v0.9/rest-apis/a/deployments/d1")))
	require.NoError(t, s.trackControlPlaneResource(ctx, http.MethodPost, []byte(`{"gatewayId":"gw"}`),
		created("/api/v0.9/rest-apis/a/deployments/d12")))

	deleted := func(url string) *httpx.Response {
		return &httpx.Response{StatusCode: http.StatusNoContent, URL: url}
	}
	require.NoError(t, s.trackControlPlaneResource(ctx, http.MethodDelete, nil,
		&httpx.Response{StatusCode: http.StatusConflict, URL: "https://cp/api/v0.9/rest-apis/a"}))
	require.Len(t, pendingIDs(reg, platformRESTAPIKind), 2, "a failed delete leaves the record in place")

	require.NoError(t, s.trackControlPlaneResource(ctx, http.MethodDelete, nil,
		deleted("https://cp:9243/api/v0.9/rest-apis/a/deployments/d1")))
	require.Equal(t, []string{"rest-apis\x00a\x00d12\x00gw"}, pendingIDs(reg, platformDeploymentKind),
		"only the deleted deployment is dropped, not one whose id shares its prefix")
	require.NoError(t, s.trackControlPlaneResource(ctx, http.MethodDelete, nil,
		deleted("https://cp:9243/api/v0.9/rest-apis/a")))
	require.Equal(t, []string{"b"}, pendingIDs(reg, platformRESTAPIKind))
}

func TestIsDeploymentNotActive(t *testing.T) {
	notActive := []byte(`{"code":"DEPLOYMENT_NOT_ACTIVE","message":"No active deployment"}`)
	require.True(t, isDeploymentNotActive(&httpx.Response{StatusCode: http.StatusConflict, Body: notActive}))
	for _, resp := range []*httpx.Response{
		nil,
		{StatusCode: http.StatusBadRequest, Body: notActive},
		{StatusCode: http.StatusConflict, Body: []byte(`{"code":"DEPLOYMENT_ACTIVE"}`)},
		{StatusCode: http.StatusConflict, Body: []byte(`not json`)},
		{StatusCode: http.StatusConflict},
	} {
		require.False(t, isDeploymentNotActive(resp))
	}
}

func TestImportedAgentProxyCleanup(t *testing.T) {
	ctx, reg := trackingContext(t)
	s := &Steps{}

	require.NoError(t, s.registerImportedAgentProxy(ctx, "imported"))
	require.NoError(t, s.registerImportedAgentProxy(ctx, "other"))
	require.ElementsMatch(t, []string{"imported", "other"}, pendingIDs(reg, platformImportedAgentProxyKind))
	require.Empty(t, pendingIDs(reg, platformAgentProxyKind), "an imported copy is not an authored Agent proxy")
	require.Greater(t, platformImportedAgentProxyKind.Order, cleanup.KindAgent.Order,
		"the imported copy must delete after the gateway Agent it was imported from")

	// A scenario that deletes the imported copy itself drops only that record.
	require.NoError(t, s.trackControlPlaneResource(ctx, http.MethodDelete, nil,
		&httpx.Response{StatusCode: http.StatusNoContent, URL: "https://cp/api/v0.9/agent-proxies/imported"}))
	require.Equal(t, []string{"other"}, pendingIDs(reg, platformImportedAgentProxyKind))
}

func TestRegisterImportedCopyIsIdempotentPerKind(t *testing.T) {
	ctx, reg := trackingContext(t)
	s := &Steps{}

	require.NoError(t, s.registerImportedCopy(ctx, "Agent", "imported"))
	require.NoError(t, s.registerImportedCopy(ctx, "Agent", "imported"), "a second observation of the same copy")
	require.NoError(t, s.registerImportedCopy(ctx, "Mcp", "imported"))
	require.NoError(t, s.registerImportedCopy(ctx, "RestApi", "imported"), "kinds with no imported copy are ignored")

	require.Equal(t, []string{"imported"}, pendingIDs(reg, platformImportedAgentProxyKind))
	require.Equal(t, []string{"imported"}, pendingIDs(reg, platformMCPKind))
	require.Len(t, reg.Pending(), 2)
}

func TestImportedAgentProxyCleanupNeedsARegistry(t *testing.T) {
	ctx := tcontext.WithLocal(context.Background(), tcontext.NewLocal("runner"))
	require.ErrorContains(t, (&Steps{}).registerImportedAgentProxy(ctx, "imported"), "no registry")
}

func TestTrackControlPlaneResourceNeedsARegistry(t *testing.T) {
	ctx := tcontext.WithLocal(context.Background(), tcontext.NewLocal("runner"))
	s := &Steps{}
	err := s.trackControlPlaneResource(ctx, http.MethodDelete, nil,
		&httpx.Response{StatusCode: http.StatusNoContent, URL: "https://cp/api/v0.9/agent-proxies/a"})
	require.ErrorContains(t, err, "no registry")
}

func TestRequestGatewayID(t *testing.T) {
	got, err := requestGatewayID([]byte(`{"name":"d","base":"current","gatewayId":"it-gateway"}`))
	require.NoError(t, err)
	require.Equal(t, "it-gateway", got)
	for _, body := range []string{``, `[]`, `{"gatewayId":""}`, `{"gatewayId":1}`} {
		_, err := requestGatewayID([]byte(body))
		require.Error(t, err, body)
	}
}

func TestYAMLToJSONKeepsTypedScalars(t *testing.T) {
	got, err := yamlToJSON([]byte("protocol: a2a\na2a:\n  protocolVersion: \"1.0\"\n  operationConfigs:\n    transports:\n      - protocolBinding: JSONRPC\nrewriteUrls: false\n"))
	require.NoError(t, err)
	require.JSONEq(t, `{"protocol":"a2a","a2a":{"protocolVersion":"1.0","operationConfigs":{"transports":[{"protocolBinding":"JSONRPC"}]}},"rewriteUrls":false}`, string(got))

	for _, document := range []string{"- a\n- b\n", "just a scalar\n", "a: [\n"} {
		_, err := yamlToJSON([]byte(document))
		require.Error(t, err, document)
	}
}

func TestTraverseAndRenderValue(t *testing.T) {
	doc := map[string]any{
		"spec": map[string]any{
			"a2a": map[string]any{
				"operationConfigs": map[string]any{
					"transports": []any{map[string]any{"protocolBinding": "JSONRPC"}, map[string]any{"pathPrefix": "/v1"}},
				},
				"agentCard": map[string]any{"public": map[string]any{"rewriteUrls": false}},
			},
			"matrix": []any{[]any{"x", "y"}},
		},
	}
	for path, want := range map[string]string{
		"spec.a2a.operationConfigs.transports[0].protocolBinding": "JSONRPC",
		"spec.a2a.operationConfigs.transports[1].pathPrefix":      "/v1",
		"spec.a2a.agentCard.public.rewriteUrls":                   "false",
		"spec.matrix[0][1]":                                       "y",
		"spec.a2a.agentCard.public":                               `{"rewriteUrls":false}`,
	} {
		got, ok := traverse(doc, path)
		require.True(t, ok, path)
		require.Equal(t, want, renderValue(got), path)
	}
	for _, path := range []string{
		"spec.missing", "spec.a2a.operationConfigs.transports[2]", "spec.a2a.operationConfigs.transports[-1]",
		"spec.a2a[0]", "spec.matrix[x]", "spec.matrix[0", "spec.matrix[0]junk",
	} {
		_, ok := traverse(doc, path)
		require.False(t, ok, path)
	}
	require.Equal(t, "null", renderValue(nil))
	require.Equal(t, "7", renderValue(7))
}

func TestJSONFieldEquals(t *testing.T) {
	ok := &httpx.Response{StatusCode: http.StatusOK, Body: []byte(`{"status":"DEPLOYED","count":2}`)}
	require.True(t, jsonFieldEquals(ok, "status", "DEPLOYED"))
	require.True(t, jsonFieldEquals(ok, "count", "2"))
	require.False(t, jsonFieldEquals(ok, "status", "DEPLOYING"))
	require.False(t, jsonFieldEquals(ok, "missing", "DEPLOYED"))
	require.False(t, jsonFieldEquals(&httpx.Response{StatusCode: http.StatusNotFound, Body: ok.Body}, "status", "DEPLOYED"))
	require.False(t, jsonFieldEquals(&httpx.Response{StatusCode: http.StatusOK, Body: []byte(`{`)}, "status", "DEPLOYED"))
	require.False(t, jsonFieldEquals(nil, "status", "DEPLOYED"))
}

func TestErrorCode(t *testing.T) {
	require.Equal(t, "GATEWAY_TOKEN_LIMIT_REACHED", errorCode(&httpx.Response{Body: []byte(`{"code":"GATEWAY_TOKEN_LIMIT_REACHED"}`)}))
	require.Empty(t, errorCode(&httpx.Response{Body: []byte(`not json`)}))
	require.Empty(t, errorCode(nil))
}

func TestAgentCardFixtureURLAndCount(t *testing.T) {
	require.Equal(t, "http://testbench:3013/blk/scope-1/ok", agentCardFixtureURL("http://testbench:3013/blk/", "scope-1", "ok"))

	count, err := parseFixtureCount(&httpx.Response{StatusCode: http.StatusOK, Body: []byte(`{"scope":"s1","count":3}`)}, "s1")
	require.NoError(t, err)
	require.Equal(t, 3, count)
	zero, err := parseFixtureCount(&httpx.Response{StatusCode: http.StatusOK, Body: []byte(`{"scope":"s1","count":0}`)}, "s1")
	require.NoError(t, err)
	require.Zero(t, zero)
	for _, resp := range []*httpx.Response{
		nil,
		{StatusCode: http.StatusBadRequest, Body: []byte(`{"scope":"s1","count":3}`)},
		{StatusCode: http.StatusOK, Body: []byte(`{"scope":"other","count":3}`)},
		{StatusCode: http.StatusOK, Body: []byte(`{"scope":"s1"}`)},
		{StatusCode: http.StatusOK, Body: []byte(`{`)},
	} {
		_, err := parseFixtureCount(resp, "s1")
		require.Error(t, err)
	}
}

func TestInternalArtifactID(t *testing.T) {
	listing := &httpx.Response{StatusCode: http.StatusOK, Body: []byte(`{"deployments":[
		{"artifactId":"uuid-rest","deploymentId":"dep-rest","kind":"RestApi"},
		{"artifactId":"uuid-agent","deploymentId":"dep-agent","kind":"Agent"},
		{"artifactId":"","deploymentId":"dep-blank","kind":"Agent"}]}`)}
	got, err := internalArtifactID(listing, "dep-agent")
	require.NoError(t, err)
	require.Equal(t, "uuid-agent", got)

	_, err = internalArtifactID(listing, "dep-rest")
	require.ErrorContains(t, err, "want Agent", "a control-plane AgentProxy must be listed in the gateway's vocabulary")
	_, err = internalArtifactID(listing, "dep-blank")
	require.ErrorContains(t, err, "without an artifact id")
	_, err = internalArtifactID(listing, "dep-missing")
	require.ErrorContains(t, err, "does not include")
	_, err = internalArtifactID(&httpx.Response{StatusCode: http.StatusUnauthorized}, "dep-agent")
	require.Error(t, err)
	_, err = internalArtifactID(&httpx.Response{StatusCode: http.StatusOK, Body: []byte(`[`)}, "dep-agent")
	require.Error(t, err)
}

func zipArchive(t *testing.T, entries map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	writer := zip.NewWriter(&buf)
	for name, content := range entries {
		file, err := writer.Create(name)
		require.NoError(t, err)
		_, err = file.Write([]byte(content))
		require.NoError(t, err)
	}
	require.NoError(t, writer.Close())
	return buf.Bytes()
}

func tarGzArchive(t *testing.T, entries map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	writer := tar.NewWriter(gz)
	require.NoError(t, writer.WriteHeader(&tar.Header{Name: "dep-1/", Typeflag: tar.TypeDir, Mode: 0o755}))
	for name, content := range entries {
		require.NoError(t, writer.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(content)), Typeflag: tar.TypeReg}))
		_, err := writer.Write([]byte(content))
		require.NoError(t, err)
	}
	require.NoError(t, writer.Close())
	require.NoError(t, gz.Close())
	return buf.Bytes()
}

const agentDeploymentYAML = "apiVersion: gateway.api-platform.wso2.com/v1\nkind: Agent\nspec:\n  a2a:\n    protocolVersion: \"1.0\"\n"

func publishedBody(t *testing.T, body []byte) context.Context {
	t.Helper()
	ctx := tcontext.WithLocal(context.Background(), tcontext.NewLocal("runner"))
	require.NoError(t, tcontext.Set(ctx, httpx.ResponseKey, &httpx.Response{StatusCode: http.StatusOK, Body: body}))
	return ctx
}

func TestArchiveAssertions(t *testing.T) {
	s := &Steps{}
	t.Run("zip", func(t *testing.T) {
		ctx := publishedBody(t, zipArchive(t, map[string]string{"agent-u1.yaml": agentDeploymentYAML}))
		require.NoError(t, s.archiveContainsOnly(ctx, "ZIP", "agent-u1.yaml"))
		require.ErrorContains(t, s.archiveContainsOnly(ctx, "ZIP", "agent-u2.yaml"), "want only")
		require.Error(t, s.archiveContainsOnly(ctx, "gzip tar", "agent-u1.yaml"))
		require.NoError(t, s.archivedFieldIs(ctx, "kind", "Agent"))
		require.NoError(t, s.archivedFieldIs(ctx, "spec.a2a.protocolVersion", "1.0"))
		require.ErrorContains(t, s.archivedFieldIs(ctx, "kind", "AgentProxy"), `expected "AgentProxy"`)
		require.ErrorContains(t, s.archivedFieldIs(ctx, "spec.protocol", "a2a"), "has no field")
		require.NoError(t, s.archivedFieldAbsent(ctx, "spec.protocol"))
		require.ErrorContains(t, s.archivedFieldAbsent(ctx, "kind"), "should be absent")
	})
	t.Run("gzip tar", func(t *testing.T) {
		ctx := publishedBody(t, tarGzArchive(t, map[string]string{"dep-1/agent-u1.yaml": agentDeploymentYAML}))
		require.NoError(t, s.archiveContainsOnly(ctx, "gzip tar", "dep-1/agent-u1.yaml"), "directory entries are not documents")
		require.NoError(t, s.archivedFieldIs(ctx, "kind", "Agent"))
	})
	t.Run("more than one document", func(t *testing.T) {
		ctx := publishedBody(t, zipArchive(t, map[string]string{"a.yaml": agentDeploymentYAML, "b.yaml": agentDeploymentYAML}))
		require.ErrorContains(t, s.archiveContainsOnly(ctx, "ZIP", "a.yaml"), "want only")
		require.ErrorContains(t, s.archivedFieldIs(ctx, "kind", "Agent"), "want exactly one")
	})
	t.Run("not an archive", func(t *testing.T) {
		ctx := publishedBody(t, []byte(`{"code":404}`))
		require.ErrorContains(t, s.archiveContainsOnly(ctx, "ZIP", "a.yaml"), "not a ZIP")
		require.Error(t, s.archivedFieldIs(ctx, "kind", "Agent"))
		_, err := archiveEntries("rar", nil)
		require.ErrorContains(t, err, "unsupported archive format")
	})
	t.Run("not YAML", func(t *testing.T) {
		ctx := publishedBody(t, zipArchive(t, map[string]string{"agent.yaml": "kind: [\n"}))
		require.ErrorContains(t, s.archivedFieldIs(ctx, "kind", "Agent"), "is not YAML")
	})
}

func TestAgentProxyLocationPattern(t *testing.T) {
	for raw, want := range map[string][]string{
		"/api/v0.9/agent-proxies/weather":                          {"weather", "", ""},
		"https://cp/api/v0.9/agent-proxies/weather/deployments/d1": {"weather", "deployments", "d1"},
		"/api/v0.9/agent-proxies/weather/api-keys/k1?x=1":          {"weather", "api-keys", "k1"},
	} {
		match := agentProxyLocationPattern.FindStringSubmatch(locationPath(raw))
		require.NotNil(t, match, raw)
		require.Equal(t, want, match[1:], raw)
	}
	for _, raw := range []string{"", "/api/v0.9/agent-proxies", "/api/v0.9/agent-proxies/", "/api/v0.9/mcp-proxies/a",
		"/api/v0.9/agent-proxies/a/deployments", "/api/v0.9/agent-proxies/a/deployments/d/undeploy", "%zz"} {
		require.Nil(t, agentProxyLocationPattern.FindStringSubmatch(locationPath(raw)), raw)
	}
	require.True(t, strings.HasPrefix(locationPath("https://cp/api/v0.9/agent-proxies/a%2Fb"), "/api/v0.9/agent-proxies/a/b"))
}

func valuesTable(rows ...[]string) *godog.Table {
	type cell struct {
		Value string `json:"value"`
	}
	type tableRow struct {
		Cells []cell `json:"cells"`
	}
	payload := struct {
		Rows []tableRow `json:"rows"`
	}{}
	for _, row := range rows {
		encoded := tableRow{}
		for _, value := range row {
			encoded.Cells = append(encoded.Cells, cell{Value: value})
		}
		payload.Rows = append(payload.Rows, encoded)
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		panic(fmt.Sprintf("marshal table fixture: %v", err))
	}
	var table godog.Table
	if err := json.Unmarshal(raw, &table); err != nil {
		panic(fmt.Sprintf("unmarshal table fixture: %v", err))
	}
	return &table
}

func TestPublicationTypeAndPath(t *testing.T) {
	apiType, err := publicationType("RestApi")
	require.NoError(t, err)
	require.Equal(t, "rest-api", apiType)
	for _, kind := range []string{"Mcp", "LlmProvider", "", "restapi"} {
		_, err := publicationType(kind)
		require.ErrorContains(t, err, "not supported", kind)
	}

	require.Equal(t, "/api-portals/portal-1/apis/rest-api/api-1", publicationPath("portal-1", "rest-api", "api-1"))
	require.Equal(t, "/api-portals/a%2Fb/apis/rest-api/c%20d", publicationPath("a/b", "rest-api", "c d"),
		"handles are escaped so one cannot address another path")
}

func TestSplitPublicationID(t *testing.T) {
	portal, apiType, api, err := splitPublicationID("portal-1/rest-api/api-1")
	require.NoError(t, err)
	require.Equal(t, "portal-1", portal)
	require.Equal(t, "rest-api", apiType)
	require.Equal(t, "api-1", api)

	for _, id := range []string{"", "portal-1", "portal-1/api-1", "/rest-api/api-1", "portal-1//api-1", "portal-1/rest-api/", "a/b/c/d"} {
		_, _, _, err := splitPublicationID(id)
		require.ErrorContains(t, err, "malformed publication id", id)
	}
}

func TestPublicationCleanupOrderUnpublishesBeforeTheAPIAndPortalAreDeleted(t *testing.T) {
	require.Greater(t, platformPublicationKind.Order, platformDeploymentKind.Order)
	require.Less(t, platformPublicationKind.Order, platformRESTAPIKind.Order,
		"the listing is unpublished while the API still exists")
	require.Greater(t, platformAPIPortalKind.Order, platformPublicationKind.Order,
		"the portal registration outlives the unpublish that needs it")
	require.Greater(t, platformAPIPortalKind.Order, platformRESTAPIKind.Order)
	require.Less(t, platformAPIPortalKind.Order, platformGatewayKind.Order)
}

func TestRegisterPublicationRecordsThePortalAndAPIForCleanup(t *testing.T) {
	ctx, reg := trackingContext(t)
	s := &Steps{}

	require.NoError(t, s.registerPublication(ctx, "portal-1", "rest-api", "api-1"))
	require.NoError(t, s.registerPublication(ctx, "portal-1", "rest-api", "api-2"))
	require.ElementsMatch(t, []string{"portal-1/rest-api/api-1", "portal-1/rest-api/api-2"}, pendingIDs(reg, platformPublicationKind))
}

func TestDraftFromTable(t *testing.T) {
	ctx, _ := trackingContext(t)
	local, _ := tcontext.LocalOf(ctx)
	local.Set("planHandle", "plan-1")

	body, err := draftFromTable(ctx, valuesTable(
		[]string{"displayName", "Weather"},
		[]string{"version", "v1.0.3"},
		[]string{"subscriptionPlanIds", `["${CTX:planHandle}"]`},
	))
	require.NoError(t, err)
	require.JSONEq(t, `{"displayName":"Weather","version":"v1.0.3","subscriptionPlanIds":["plan-1"]}`, string(body),
		"a JSON value is decoded and a field the table does not name is absent")

	for name, table := range map[string]*godog.Table{
		"nil table":     nil,
		"empty table":   valuesTable(),
		"three cells":   valuesTable([]string{"a", "b", "c"}),
		"empty key":     valuesTable([]string{" ", "b"}),
		"duplicate key": valuesTable([]string{"a", "1"}, []string{"a", "2"}),
	} {
		_, err := draftFromTable(ctx, table)
		require.Error(t, err, name)
	}

	body, err = draftFromTable(ctx, valuesTable(
		[]string{"displayName", "[Beta] Orders API"},
		[]string{"description", `{not json}`},
	))
	require.NoError(t, err)
	require.JSONEq(t, `{"displayName":"[Beta] Orders API","description":"{not json}"}`, string(body),
		"text that starts with a bracket but is not JSON stays a string")
}

func TestMinimalOpenAPIDefinitionIsAValidDocument(t *testing.T) {
	body, err := minimalOpenAPIDefinition("weather")
	require.NoError(t, err)

	var document struct {
		OpenAPI string `json:"openapi"`
		Info    struct {
			Title   string `json:"title"`
			Version string `json:"version"`
		} `json:"info"`
		Paths map[string]any `json:"paths"`
	}
	require.NoError(t, json.Unmarshal(body, &document))
	require.Equal(t, "3.0.3", document.OpenAPI)
	require.Equal(t, "weather", document.Info.Title)
	require.NotEmpty(t, document.Info.Version)
	require.NotEmpty(t, document.Paths)
}

func TestSelectedPlans(t *testing.T) {
	require.Equal(t, []string{"gold", "silver"}, selectedPlans([]byte(`{"displayName":"x","subscriptionPlanIds":["gold","silver"]}`)))
	for name, draft := range map[string]string{
		"no plans field":      `{"displayName":"x"}`,
		"empty list":          `{"subscriptionPlanIds":[]}`,
		"plans not a list":    `{"subscriptionPlanIds":"gold"}`,
		"not a JSON document": `not json`,
	} {
		require.Empty(t, selectedPlans([]byte(draft)), name)
	}
}

func TestTrackSelectedPlansRemovesEverySelectedPlanFromThePortal(t *testing.T) {
	var tracked []string
	s := &Steps{trackPortalPlan: func(_ context.Context, handle string) error {
		tracked = append(tracked, handle)
		return nil
	}}

	require.NoError(t, s.trackSelectedPlans(context.Background(), []byte(`{"subscriptionPlanIds":["gold","silver"]}`)))
	require.Equal(t, []string{"gold", "silver"}, tracked)

	tracked = nil
	require.NoError(t, s.trackSelectedPlans(context.Background(), []byte(`{"displayName":"x"}`)))
	require.Empty(t, tracked, "a draft that selects no plan tracks nothing")

	failing := &Steps{trackPortalPlan: func(context.Context, string) error { return errors.New("registry unavailable") }}
	require.ErrorContains(t, failing.trackSelectedPlans(context.Background(), []byte(`{"subscriptionPlanIds":["gold"]}`)),
		`plan "gold"`)

	untracked := &Steps{}
	require.ErrorContains(t, untracked.trackSelectedPlans(context.Background(), []byte(`{"subscriptionPlanIds":["gold"]}`)),
		"no API Portal steps")
	require.NoError(t, untracked.trackSelectedPlans(context.Background(), []byte(`{}`)),
		"without selected plans there is nothing to track")
}

func TestUnpublishSettled(t *testing.T) {
	stateConflict := []byte(`{"status":"error","code":"PUBLICATION_STATE_CONFLICT"}`)
	portalConflict := []byte(`{"status":"error","code":"PUBLICATION_PORTAL_CONFLICT"}`)

	for name, tc := range map[string]struct {
		status int
		body   []byte
		want   bool
	}{
		"unpublished":                      {http.StatusNoContent, nil, true},
		"unknown API":                      {http.StatusNotFound, nil, true},
		"nothing to unpublish":             {http.StatusConflict, stateConflict, true},
		"portal still holds keys":          {http.StatusConflict, portalConflict, false},
		"conflict without a readable body": {http.StatusConflict, []byte(`not json`), false},
		"conflict with an empty body":      {http.StatusConflict, nil, false},
		"portal unavailable":               {http.StatusServiceUnavailable, nil, false},
		"forbidden":                        {http.StatusForbidden, nil, false},
	} {
		require.Equal(t, tc.want, unpublishSettled(&httpx.Response{StatusCode: tc.status, Body: tc.body}), name)
	}
}

func TestDocumentIDCarriesTheAPI(t *testing.T) {
	id := documentID("rest-api", "api-1", "doc-1")
	require.Equal(t, "rest-api/api-1/doc-1", id)

	apiType, api, doc, err := splitDocumentID(id)
	require.NoError(t, err)
	require.Equal(t, []string{"rest-api", "api-1", "doc-1"}, []string{apiType, api, doc})
	require.NotEqual(t, documentID("rest-api", "api-1", "doc-1"), documentID("rest-api", "api-2", "doc-1"),
		"the same document handle on two APIs must be two cleanup resources")

	for _, bad := range []string{"", "doc-1", "api-1/doc-1", "rest-api//doc-1", "rest-api/api-1/", "a/b/c/d"} {
		_, _, _, err := splitDocumentID(bad)
		require.ErrorContains(t, err, "malformed document id", bad)
	}
}

// A block whose scenarios register gateways of their own must still deploy to its own gateway,
// so the block's gateway is fetched by handle rather than taken from the head of the list.
func TestGatewayUUIDResolvesTheBlockGatewayAmongOthers(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case apiBase + "/gateways":
			_, _ = w.Write([]byte(`{"list":[{"id":"tok-gw-abc"},{"id":"it-gateway"}]}`))
		case apiBase + "/gateways/" + blockGatewayHandle:
			_, _ = w.Write([]byte(`{"id":"it-gateway"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	s := &Steps{client: httpx.NewClient(httpx.Options{Timeout: 5 * time.Second})}
	id, err := s.gatewayUUID(context.Background(), server.URL, "token")
	require.NoError(t, err)
	require.Equal(t, "it-gateway", id)
}

func TestTokenFormSendsTheTableAndDefaultsTheGrant(t *testing.T) {
	ctx, _ := trackingContext(t)
	local, _ := tcontext.LocalOf(ctx)
	local.Set("clientId", "sa_default_x_abc123")

	form, err := tokenForm(ctx, valuesTable(
		[]string{"client_id", "${CTX:clientId}"},
		[]string{"scope", ""},
	))
	require.NoError(t, err)
	require.Equal(t, "sa_default_x_abc123", form.Get("client_id"))
	require.Equal(t, clientCredentialsGrant, form.Get("grant_type"), "the grant defaults when the table omits it")
	require.True(t, form.Has("scope"), "an empty cell is still sent")
	require.Equal(t, "", form.Get("scope"))

	form, err = tokenForm(ctx, valuesTable([]string{"grant_type", "password"}))
	require.NoError(t, err)
	require.Equal(t, "password", form.Get("grant_type"), "a grant the table names is kept")
}

func TestTokenFormRejectsMalformedTables(t *testing.T) {
	ctx, _ := trackingContext(t)
	for name, table := range map[string]*godog.Table{
		"nil table":     nil,
		"no rows":       valuesTable(),
		"one cell":      valuesTable([]string{"client_id"}),
		"three cells":   valuesTable([]string{"a", "b", "c"}),
		"blank key":     valuesTable([]string{" ", "b"}),
		"duplicate key": valuesTable([]string{"a", "1"}, []string{"a", "2"}),
		"missing value": valuesTable([]string{"client_id", "${CTX:missing}"}),
	} {
		_, err := tokenForm(ctx, table)
		require.Error(t, err, name)
	}
}

func TestBasicClientCredentialsFormEncodesEachPart(t *testing.T) {
	for _, pair := range [][2]string{
		{"sa_default_x_abc123", "apsa_0123456789abcdef"},
		{"id:with:colons", "s e+c%r/e&t="},
		{"sa_h\u00e9llo", "s\u00e9cret-\u4e16\u754c"},
	} {
		req, err := http.NewRequest(http.MethodPost, "https://cp/token", nil)
		require.NoError(t, err)
		req.Header.Set("Authorization", basicClientCredentials(pair[0], pair[1]))
		rawID, rawSecret, ok := req.BasicAuth()
		require.True(t, ok, pair[0])
		id, err := url.QueryUnescape(rawID)
		require.NoError(t, err)
		secret, err := url.QueryUnescape(rawSecret)
		require.NoError(t, err)
		require.Equal(t, pair[0], id)
		require.Equal(t, pair[1], secret)
	}
}

func testJWT(t *testing.T, header, payload map[string]any) string {
	t.Helper()
	encode := func(value map[string]any) string {
		raw, err := json.Marshal(value)
		require.NoError(t, err)
		return base64.RawURLEncoding.EncodeToString(raw)
	}
	return encode(header) + "." + encode(payload) + ".signature"
}

func rawJWT(header, payload string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(header)) + "." +
		base64.RawURLEncoding.EncodeToString([]byte(payload)) + ".signature"
}

func TestJWTSegmentDecodesHeaderAndPayload(t *testing.T) {
	token := testJWT(t,
		map[string]any{"alg": "RS256", "kid": "key-1"},
		map[string]any{"azp": "client-1", "exp": 1761500000, "aud": []string{"a", "b"}})

	header, err := jwtSegment(token, jwtHeaderSegment)
	require.NoError(t, err)
	require.Equal(t, "key-1", header["kid"])

	payload, err := jwtSegment(token, jwtPayloadSegment)
	require.NoError(t, err)
	require.Equal(t, "client-1", payload["azp"])
	exp, _ := traverse(payload, "exp")
	require.Equal(t, "1761500000", renderValue(exp), "a number keeps its digits")
	aud, _ := traverse(payload, "aud")
	require.Equal(t, `["a","b"]`, renderValue(aud))
}

func TestJWTSegmentRejectsWhatIsNotACompactJWS(t *testing.T) {
	for _, token := range []string{"", "garbage", "a.b", "a.b.c.d", "aaa.bbb.ccc"} {
		_, err := jwtSegment(token, jwtPayloadSegment)
		require.Error(t, err, token)
	}
	_, err := jwtSegment(rawJWT(`{"alg":"none"}`, "{}"), jwtPayloadSegment)
	require.NoError(t, err)
	for _, payload := range []string{"[]", "null", `"s"`} {
		_, err := jwtSegment(rawJWT(`{"alg":"none"}`, payload), jwtPayloadSegment)
		require.ErrorContains(t, err, "not a JSON object", payload)
	}
	for _, payload := range []string{`{} {}`, `{}x`, `{"a":1}{"b":2}`} {
		_, err := jwtSegment(rawJWT(`{"alg":"none"}`, payload), jwtPayloadSegment)
		require.ErrorContains(t, err, "not a JSON object", "trailing bytes: "+payload)
	}
	_, err = jwtSegment("e30.!!!.sig", jwtPayloadSegment)
	require.ErrorContains(t, err, "not base64url")
	_, err = jwtSegment(rawJWT(`{}`, `{}`), 2)
	require.Error(t, err, "only the header and the payload can be decoded")
}

func TestJWTEntryStep(t *testing.T) {
	token := testJWT(t,
		map[string]any{"alg": "RS256", "kid": "AbC_-123"},
		map[string]any{"azp": "client-1", "iss": "platform-api"})
	body := func(value string) string { return `{"access_token":` + value + `}` }
	s := &Steps{}

	t.Run("passing checks", func(t *testing.T) {
		ctx := publishedBody(t, []byte(body(`"`+token+`"`)))
		local, _ := tcontext.LocalOf(ctx)
		local.Set("clientId", "client-1")
		require.NoError(t, s.jwtEntryIs(ctx, "access_token", "claim", "iss", "equal to", "platform-api"))
		require.NoError(t, s.jwtEntryIs(ctx, "access_token", "header", "kid", "matching", "^[A-Za-z0-9_-]{8}$"))
		require.NoError(t, s.jwtEntryIs(ctx, "access_token", "claim", "azp", "equal to", "${CTX:clientId}"))
	})

	t.Run("failing checks", func(t *testing.T) {
		ctx := publishedBody(t, []byte(body(`"`+token+`"`)))
		for name, call := range map[string]func() error{
			"mismatch": func() error {
				return s.jwtEntryIs(ctx, "access_token", "claim", "iss", "equal to", "someone-else")
			},
			"no match": func() error {
				return s.jwtEntryIs(ctx, "access_token", "header", "kid", "matching", "^x+$")
			},
			"invalid pattern": func() error {
				return s.jwtEntryIs(ctx, "access_token", "claim", "iss", "matching", "(")
			},
			"missing claim": func() error {
				return s.jwtEntryIs(ctx, "access_token", "claim", "sub", "equal to", "x")
			},
			"blank name": func() error {
				return s.jwtEntryIs(ctx, "access_token", "claim", " ", "equal to", "x")
			},
		} {
			err := call()
			require.Error(t, err, name)
			require.NotContains(t, err.Error(), token, "%s: an error never carries the token", name)
		}
	})

	t.Run("wrong inputs", func(t *testing.T) {
		for name, ctx := range map[string]context.Context{
			"field absent":      publishedBody(t, []byte(`{"other":"x"}`)),
			"field not string":  publishedBody(t, []byte(body(`5`))),
			"field blank":       publishedBody(t, []byte(body(`""`))),
			"body not JSON":     publishedBody(t, []byte(`<html>`)),
			"no response":       tcontext.WithLocal(context.Background(), tcontext.NewLocal("runner")),
			"field not a token": publishedBody(t, []byte(body(`"garbage"`))),
		} {
			require.Error(t, s.jwtEntryIs(ctx, "access_token", "claim", "iss", "equal to", "platform-api"), name)
		}
	})

	t.Run("a missing field never prints the token held beside it", func(t *testing.T) {
		ctx := publishedBody(t, []byte(`{"access_token":"`+token+`","refresh":"`+token+`"}`))
		err := s.jwtEntryIs(ctx, "id_token", "claim", "iss", "equal to", "platform-api")
		require.ErrorContains(t, err, "access_token", "the keys are named")
		require.ErrorContains(t, err, "status 200")
		require.NotContains(t, err.Error(), token)
	})
}

func TestServiceAccountTemplatePreservesOmission(t *testing.T) {
	ctx, _ := trackingContext(t)
	s := &Steps{featureRoot: filepath.Join("..", "..")}
	const template = "resources/templates/service-account.yaml"
	render := func(rows ...[]string) (map[string]any, error) {
		payload, err := s.renderJSONTemplate(ctx, template, valuesTable(rows...))
		if err != nil {
			return nil, err
		}
		var document map[string]any
		require.NoError(t, json.Unmarshal(payload, &document))
		return document, nil
	}

	document, err := render([]string{"displayName", "CI deployer"}, []string{"roles", `["ap_service_account"]`})
	require.NoError(t, err)
	require.Len(t, document, 2, "a field the table does not name stays absent")
	require.Equal(t, []any{"ap_service_account"}, document["roles"])

	document, err = render(
		[]string{"displayName", "CI deployer"}, []string{"roles", `["ap_service_account"]`},
		[]string{"description", "Reads APIs"}, []string{"status", "disabled"})
	require.NoError(t, err)
	require.Len(t, document, 4)
	require.Equal(t, "disabled", document["status"])

	document, err = render([]string{"displayName", "CI deployer"}, []string{"roles", `[]`})
	require.NoError(t, err)
	require.Equal(t, []any{}, document["roles"], "an empty list is an empty array, not null")

	_, err = render([]string{"roles", `["ap_service_account"]`})
	require.ErrorContains(t, err, `no value supplied for "displayName"`)
	_, err = render([]string{"displayName", "CI deployer"})
	require.Error(t, err)
}

func TestTrackControlPlaneResourceTracksServiceAccounts(t *testing.T) {
	ctx, reg := trackingContext(t)
	s := &Steps{}

	require.NoError(t, s.trackControlPlaneResource(ctx, http.MethodPost, nil,
		created("/api/v0.9/service-accounts/ci-deployer")))
	require.Equal(t, []string{"ci-deployer"}, pendingIDs(reg, platformServiceAccountKind))

	for _, endpoint := range []string{
		"https://cp:9243/api/v0.9/service-accounts/token",
		"https://cp:9243/api/v0.9/service-accounts/ci-deployer/regenerate-secret",
	} {
		require.NoError(t, s.trackControlPlaneResource(ctx, http.MethodPost, nil,
			&httpx.Response{StatusCode: http.StatusOK, URL: endpoint, Headers: http.Header{}}))
	}
	require.Equal(t, []string{"ci-deployer"}, pendingIDs(reg, platformServiceAccountKind), "only a 201 registers")

	require.NoError(t, s.trackControlPlaneResource(ctx, http.MethodDelete, nil,
		&httpx.Response{StatusCode: http.StatusConflict, URL: "https://cp:9243/api/v0.9/service-accounts/ci-deployer"}))
	require.Equal(t, []string{"ci-deployer"}, pendingIDs(reg, platformServiceAccountKind), "a refused delete keeps the record")

	require.NoError(t, s.trackControlPlaneResource(ctx, http.MethodDelete, nil,
		&httpx.Response{StatusCode: http.StatusNoContent, URL: "https://cp:9243/api/v0.9/service-accounts/ci-deployer"}))
	require.Empty(t, pendingIDs(reg, platformServiceAccountKind))
}

// recordedRequest is what a fake control plane saw of one request.
type recordedRequest struct {
	method, path, rawPath, contentType string
	body                               []byte
	header                             http.Header
	form                               url.Values
	basicID, basicSecret               string
	hasBasic                           bool
}

const controlPlaneLoginPath = "/api/portal/v0.9/auth/login"

// fakeControlPlane serves handler over TLS as the block's platform-api and returns the steps
// wired to it, along with the requests it recorded. It uses the control plane's generated
// certificate and answers the sign-in with "token-of-<username>", so a signed-in call is
// exercised end to end; the sign-in itself is not recorded.
func fakeControlPlane(t *testing.T, handler http.HandlerFunc) (*Steps, *httptest.Server, func() []recordedRequest) {
	t.Helper()
	var mu sync.Mutex
	var seen []recordedRequest
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == controlPlaneLoginPath {
			_ = r.ParseForm()
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"token":"token-of-` + r.PostForm.Get("username") + `"}`))
			return
		}
		record := recordedRequest{
			method: r.Method, path: r.URL.Path, rawPath: r.URL.EscapedPath(),
			contentType: r.Header.Get("Content-Type"), header: r.Header.Clone(),
		}
		if r.Method == http.MethodPost && strings.HasPrefix(record.contentType, formContentType) {
			if err := r.ParseForm(); err == nil {
				record.form = r.PostForm
				record.body = []byte(r.PostForm.Encode())
			}
		} else if r.Body != nil {
			record.body, _ = io.ReadAll(r.Body)
		}
		record.basicID, record.basicSecret, record.hasBasic = r.BasicAuth()
		mu.Lock()
		seen = append(seen, record)
		mu.Unlock()
		handler(w, r)
	}))
	crypto := shared.ControlPlaneCrypto()
	certificate, err := tls.X509KeyPair(crypto["certs/cert.pem"], crypto["certs/key.pem"])
	require.NoError(t, err)
	server.TLS = &tls.Config{Certificates: []tls.Certificate{certificate}}
	server.StartTLS()
	t.Cleanup(server.Close)

	port := server.Listener.Addr().(*net.TCPAddr).Port
	definition := &components.Definition{
		Name:      "platform-api",
		Alias:     "platform-api",
		Endpoints: []components.Endpoint{{Name: "https", Port: 9243, Scheme: "https"}},
	}
	instance, err := components.NewInstance(definition, 0, 1, "127.0.0.1", map[int]int{9243: port})
	require.NoError(t, err)
	instances := components.NewSet()
	require.NoError(t, instances.Add(instance))

	client := httpx.NewClient(httpx.Options{Timeout: 5 * time.Second, InsecureSkipVerify: true})
	s := &Steps{
		topo:   &frameworkruntime.Topology{Instances: instances},
		funnel: httpx.NewFunnel(client, 0, 0),
		client: client,
	}
	return s, server, func() []recordedRequest {
		mu.Lock()
		defer mu.Unlock()
		return append([]recordedRequest(nil), seen...)
	}
}

func TestAnonymousControlPlaneRequests(t *testing.T) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}

	t.Run("form exchange", func(t *testing.T) {
		s, _, requests := fakeControlPlane(t, handler)
		ctx, _ := trackingContext(t)
		local, _ := tcontext.LocalOf(ctx)
		local.Set("clientId", "sa_default_x_abc123")

		require.NoError(t, s.exchangeServiceAccountCredentials(ctx, "", "", "", valuesTable(
			[]string{"client_id", "${CTX:clientId}"}, []string{"client_secret", "apsa_s"}, []string{"scope", ""})))

		got := requests()
		require.Len(t, got, 1)
		require.Equal(t, http.MethodPost, got[0].method)
		require.Equal(t, apiBase+serviceAccountTokenPath, got[0].path)
		require.Empty(t, got[0].header.Get("Authorization"), "no sign-in precedes a public call")
		require.Equal(t, formContentType, got[0].contentType)
		require.Equal(t, "sa_default_x_abc123", got[0].form.Get("client_id"))
		require.Equal(t, "apsa_s", got[0].form.Get("client_secret"))
		require.Equal(t, clientCredentialsGrant, got[0].form.Get("grant_type"))
		require.True(t, got[0].form.Has("scope"))
		resp, err := httpx.Published(ctx)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)
	})

	t.Run("basic exchange", func(t *testing.T) {
		s, _, requests := fakeControlPlane(t, handler)
		ctx, _ := trackingContext(t)

		require.NoError(t, s.exchangeServiceAccountCredentials(ctx, " using HTTP Basic", "id:1", "s e+c%r", valuesTable(
			[]string{"scope", "ap:project:read"})))

		got := requests()
		require.Len(t, got, 1)
		require.True(t, got[0].hasBasic)
		id, err := url.QueryUnescape(got[0].basicID)
		require.NoError(t, err)
		secret, err := url.QueryUnescape(got[0].basicSecret)
		require.NoError(t, err)
		require.Equal(t, "id:1", id)
		require.Equal(t, "s e+c%r", secret)
		require.False(t, got[0].form.Has("client_id"), "the form carries no client id")
		require.Equal(t, "ap:project:read", got[0].form.Get("scope"))
	})

	t.Run("basic and form together", func(t *testing.T) {
		s, _, requests := fakeControlPlane(t, handler)
		ctx, _ := trackingContext(t)

		require.NoError(t, s.exchangeServiceAccountCredentials(ctx, " using HTTP Basic", "id-1", "secret-1", valuesTable(
			[]string{"client_id", "id-1"}, []string{"scope", "ap:project:read"})))

		got := requests()
		require.Len(t, got, 1)
		require.True(t, got[0].hasBasic, "the server sees the Basic header")
		require.Equal(t, "id-1", got[0].form.Get("client_id"), "and the form field")
	})

	t.Run("root route", func(t *testing.T) {
		s, _, requests := fakeControlPlane(t, handler)
		ctx, _ := trackingContext(t)
		local, _ := tcontext.LocalOf(ctx)
		local.Set("etag", `"abc"`)

		require.NoError(t, s.sendControlPlaneRoot(ctx, "get", "/.well-known/jwks", "If-None-Match", "${CTX:etag}"))

		got := requests()
		require.Len(t, got, 1)
		require.Equal(t, http.MethodGet, got[0].method)
		require.Equal(t, "/.well-known/jwks", got[0].path, "no management API prefix")
		require.Empty(t, got[0].header.Get("Authorization"))
		require.Equal(t, `"abc"`, got[0].header.Get("If-None-Match"))
	})

	t.Run("an anonymous call names no actor", func(t *testing.T) {
		s, _, requests := fakeControlPlane(t, handler)
		ctx, _ := trackingContext(t)
		_, err := s.sendControlPlane(ctx, controlPlaneCall{method: http.MethodGet, path: "/x", who: "admin", anonymous: true})
		require.ErrorContains(t, err, "cannot be sent as")
		require.Empty(t, requests())
	})

}

func TestSignedInControlPlaneRequests(t *testing.T) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}
	const template = "resources/templates/service-account.yaml"
	table := valuesTable([]string{"displayName", "CI deployer"}, []string{"roles", `["ap_service_account"]`})

	t.Run("a plain call signs in as admin and uses the versioned API", func(t *testing.T) {
		s, _, requests := fakeControlPlane(t, handler)
		ctx, _ := trackingContext(t)
		require.NoError(t, s.sendControlPlaneStep(ctx, "get", "/service-accounts", "", "", "", nil))

		got := requests()
		require.Len(t, got, 1)
		require.Equal(t, apiBase+"/service-accounts", got[0].path)
		require.Equal(t, "Bearer token-of-"+actor.Administrator().Username, got[0].header.Get("Authorization"))
		require.Empty(t, got[0].contentType, "a body-less call sends no content type")
	})

	t.Run("a header override of any spelling replaces the sign-in", func(t *testing.T) {
		s, _, requests := fakeControlPlane(t, handler)
		ctx, _ := trackingContext(t)
		for _, name := range []string{"authorization", "AUTHORIZATION", "Authorization"} {
			for i := 0; i < 30; i++ {
				require.NoError(t, s.sendControlPlaneStep(ctx, "GET", "/projects", "", name, "Bearer sa-token", nil))
			}
		}
		for _, request := range requests() {
			require.Equal(t, []string{"Bearer sa-token"}, request.header.Values("Authorization"),
				"the override must win every time, not by map order")
		}
	})

	t.Run("create sends the rendered template as JSON", func(t *testing.T) {
		s, _, requests := fakeControlPlane(t, handler)
		s.featureRoot = filepath.Join("..", "..")
		ctx, _ := trackingContext(t)
		require.NoError(t, s.createFromTemplate(ctx, "service account", template, "publisher", table))

		got := requests()
		require.Len(t, got, 1)
		require.Equal(t, http.MethodPost, got[0].method)
		require.Equal(t, apiBase+"/service-accounts", got[0].path)
		require.Equal(t, "application/json", got[0].contentType)
		require.Equal(t, "Bearer token-of-"+actor.Publisher().Username, got[0].header.Get("Authorization"))
		require.JSONEq(t, `{"displayName":"CI deployer","roles":["ap_service_account"]}`, string(got[0].body))
	})

	t.Run("create and update pick the collection from the resource", func(t *testing.T) {
		s, _, requests := fakeControlPlane(t, handler)
		s.featureRoot = filepath.Join("..", "..")
		ctx, _ := trackingContext(t)
		local, _ := tcontext.LocalOf(ctx)
		local.Set("id", "ci/deployer 1")
		require.NoError(t, s.updateFromTemplate(ctx, "service account", "${CTX:id}", template, "", table))
		require.NoError(t, s.createFromTemplate(ctx, "Agent proxy", "resources/templates/agent-proxy.yaml", "", valuesTable(
			[]string{"displayName", "Weather"}, []string{"projectId", "p1"}, []string{"context", "/weather"},
			[]string{"upstreamUrl", "http://upstream"}, []string{"transports", `["http"]`})))

		got := requests()
		require.Len(t, got, 2)
		require.Equal(t, http.MethodPut, got[0].method)
		require.Equal(t, apiBase+"/service-accounts/ci%2Fdeployer%201", got[0].rawPath, "the id is path-escaped")
		require.Equal(t, apiBase+"/agent-proxies", got[1].path)
	})

	t.Run("update needs an id", func(t *testing.T) {
		s, _, requests := fakeControlPlane(t, handler)
		s.featureRoot = filepath.Join("..", "..")
		ctx, _ := trackingContext(t)
		require.ErrorContains(t, s.updateFromTemplate(ctx, "service account", "  ", template, "", table), "needs its id")
		require.Empty(t, requests())
	})

	t.Run("without signing in sends no credentials but keeps the versioned API", func(t *testing.T) {
		s, _, requests := fakeControlPlane(t, handler)
		ctx, _ := trackingContext(t)
		require.NoError(t, s.sendControlPlaneUnauthenticated(ctx, "get", "/service-accounts"))

		got := requests()
		require.Len(t, got, 1)
		require.Equal(t, apiBase+"/service-accounts", got[0].path)
		require.Empty(t, got[0].header.Get("Authorization"))
	})
}
