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
	Scheme string // "https" or "http"
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
		Scheme: "https",
	}
}

// DoV2Check forwards a /v2/ version check to the upstream registry.
// This relays auth challenges (401 + Www-Authenticate) back to the client.
func (u *UpstreamClient) DoV2Check(r *http.Request, registry string) (*http.Response, error) {
	host := resolveRegistry(registry)
	url := fmt.Sprintf("%s://%s/v2/", u.Scheme, host)

	req, err := http.NewRequestWithContext(r.Context(), r.Method, url, nil)
	if err != nil {
		return nil, fmt.Errorf("creating upstream /v2/ request: %w", err)
	}

	if auth := r.Header.Get("Authorization"); auth != "" {
		req.Header.Set("Authorization", auth)
	}

	return u.Client.Do(req)
}

// Do forwards a request to the upstream registry.
func (u *UpstreamClient) Do(r *http.Request, info requestInfo) (*http.Response, error) {
	upstreamURL := u.upstreamURL(info)

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
func (u *UpstreamClient) upstreamURL(info requestInfo) string {
	registry := resolveRegistry(info.Registry)
	return fmt.Sprintf("%s://%s/v2/%s/%s/%s", u.Scheme, registry, info.Name, info.Kind, info.Reference)
}

// resolveRegistry maps well-known registry aliases to their API endpoints.
func resolveRegistry(registry string) string {
	// Docker Hub uses a different host for API calls
	if strings.EqualFold(registry, "docker.io") || strings.EqualFold(registry, "registry.docker.io") {
		return "registry-1.docker.io"
	}
	return registry
}
