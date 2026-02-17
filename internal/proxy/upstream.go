package proxy

import (
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"
)

// UpstreamClient handles HTTP requests to upstream OCI registries.
type UpstreamClient struct {
	Client *http.Client
}

// NewUpstreamClient creates an UpstreamClient with a configured http.Transport.
// The default client follows redirects automatically (needed for blob redirects).
func NewUpstreamClient() *UpstreamClient {
	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout:  30 * time.Second,
		MaxIdleConns:           100,
		MaxIdleConnsPerHost:    20,
		IdleConnTimeout:        90 * time.Second,
		DisableCompression:     true,
	}
	return &UpstreamClient{
		Client: &http.Client{Transport: transport},
	}
}

// Do forwards a request to the upstream registry.
func (u *UpstreamClient) Do(r *http.Request, info requestInfo) (*http.Response, error) {
	upstreamURL := upstreamURL(info)

	req, err := http.NewRequestWithContext(r.Context(), r.Method, upstreamURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating upstream request: %w", err)
	}

	// Forward Authorization header as-is (auth passthrough)
	if auth := r.Header.Get("Authorization"); auth != "" {
		req.Header.Set("Authorization", auth)
	}

	// Forward Accept header (critical for manifest content negotiation)
	if accept := r.Header.Get("Accept"); accept != "" {
		req.Header.Set("Accept", accept)
	}

	// Forward Range/If-Range headers so upstream can return 206 Partial Content
	// for resumable downloads. The non-200 code path already forwards partial
	// responses without caching.
	if rng := r.Header.Get("Range"); rng != "" {
		req.Header.Set("Range", rng)
	}
	if ifRange := r.Header.Get("If-Range"); ifRange != "" {
		req.Header.Set("If-Range", ifRange)
	}

	return u.Client.Do(req)
}

// upstreamURL constructs the full upstream registry URL.
func upstreamURL(info requestInfo) string {
	registry := resolveRegistry(info.Registry)
	return fmt.Sprintf("https://%s/v2/%s/%s/%s", registry, info.Name, info.Kind, info.Reference)
}

// resolveRegistry maps well-known registry aliases to their API endpoints.
func resolveRegistry(registry string) string {
	// Docker Hub uses a different host for API calls
	if strings.EqualFold(registry, "docker.io") || strings.EqualFold(registry, "registry.docker.io") {
		return "registry-1.docker.io"
	}
	return registry
}
