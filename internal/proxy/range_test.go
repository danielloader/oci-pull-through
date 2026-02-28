package proxy

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/danielloader/oci-pull-through/internal/cache"
)

// --- test doubles ---

// seekableBody is an io.ReadCloser that also implements io.ReadSeeker (like *os.File).
type seekableBody struct {
	*bytes.Reader
}

func (s *seekableBody) Close() error { return nil }

// nonSeekableBody is an io.ReadCloser that does NOT implement io.ReadSeeker.
type nonSeekableBody struct {
	io.Reader
}

func (n *nonSeekableBody) Close() error { return nil }

// mockStore is a minimal cache.Store for testing the proxy handler.
type mockStore struct {
	result *cache.GetResult
	err    error
}

func (m *mockStore) Init(_ context.Context) error { return nil }
func (m *mockStore) Head(_ context.Context, _ string) (cache.ObjectMeta, error) {
	return cache.ObjectMeta{}, m.err
}
func (m *mockStore) GetWithMeta(_ context.Context, _ string) (*cache.GetResult, error) {
	if m.result != nil {
		return m.result, nil
	}
	return nil, m.err
}
func (m *mockStore) Put(_ context.Context, _ string, body io.Reader, _ cache.ObjectMeta) error {
	io.Copy(io.Discard, body)
	return nil
}

// --- shared fixtures ---

const testBlob = "0123456789ABCDEF" // 16 bytes

func blobMeta() cache.ObjectMeta {
	return cache.ObjectMeta{
		ContentType:         "application/octet-stream",
		DockerContentDigest: "sha256:abcdef1234567890",
		ContentLength:       int64(len(testBlob)),
		Header: http.Header{
			"Content-Type":          {"application/octet-stream"},
			"Docker-Content-Digest": {"sha256:abcdef1234567890"},
			"Content-Length":        {fmt.Sprintf("%d", len(testBlob))},
		},
	}
}

func blobPath() string {
	return "/v2/test/image/blobs/sha256:abcdef1234567890"
}

// --- tests ---

func TestRangeCacheHitSeekable(t *testing.T) {
	store := &mockStore{
		result: &cache.GetResult{
			Body: &seekableBody{bytes.NewReader([]byte(testBlob))},
			Meta: blobMeta(),
		},
	}
	h := &Handler{
		Registry: "example.com",
		Cache:    store,
		Upstream: &UpstreamClient{Client: http.DefaultClient},
	}

	req := httptest.NewRequest("GET", blobPath(), nil)
	req.Header.Set("Range", "bytes=5-9")
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusPartialContent {
		t.Fatalf("expected 206, got %d", rec.Code)
	}
	if body := rec.Body.String(); body != "56789" {
		t.Fatalf("expected %q, got %q", "56789", body)
	}
	if cr := rec.Header().Get("Content-Range"); !strings.HasPrefix(cr, "bytes 5-9/") {
		t.Fatalf("unexpected Content-Range: %q", cr)
	}
}

func TestRangeCacheHitNonSeekable(t *testing.T) {
	store := &mockStore{
		result: &cache.GetResult{
			Body: &nonSeekableBody{bytes.NewReader([]byte(testBlob))},
			Meta: blobMeta(),
		},
	}
	h := &Handler{
		Registry: "example.com",
		Cache:    store,
		Upstream: &UpstreamClient{Client: http.DefaultClient},
	}

	req := httptest.NewRequest("GET", blobPath(), nil)
	req.Header.Set("Range", "bytes=5-9")
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if body := rec.Body.String(); body != testBlob {
		t.Fatalf("expected full blob %q, got %q", testBlob, body)
	}
}

func TestRangeCacheMissForwardsHeaders(t *testing.T) {
	var gotRange, gotIfRange string

	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotRange = r.Header.Get("Range")
		gotIfRange = r.Header.Get("If-Range")
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Range", "bytes 5-9/16")
		w.WriteHeader(http.StatusPartialContent)
		w.Write([]byte("56789"))
	}))
	defer upstream.Close()

	registry := strings.TrimPrefix(upstream.URL, "https://")

	h := &Handler{
		Registry: registry,
		Cache:    &mockStore{err: fmt.Errorf("not found")},
		Upstream: &UpstreamClient{Client: upstream.Client(), Scheme: "https"},
	}

	req := httptest.NewRequest("GET", blobPath(), nil)
	req.Header.Set("Range", "bytes=5-9")
	req.Header.Set("If-Range", `"some-etag"`)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if gotRange != "bytes=5-9" {
		t.Fatalf("Range not forwarded to upstream: got %q", gotRange)
	}
	if gotIfRange != `"some-etag"` {
		t.Fatalf("If-Range not forwarded to upstream: got %q", gotIfRange)
	}
	if rec.Code != http.StatusPartialContent {
		t.Fatalf("expected 206 pass-through, got %d", rec.Code)
	}
	if body := rec.Body.String(); body != "56789" {
		t.Fatalf("expected %q, got %q", "56789", body)
	}
}

func TestNoRangeCacheHitSeekable(t *testing.T) {
	store := &mockStore{
		result: &cache.GetResult{
			Body: &seekableBody{bytes.NewReader([]byte(testBlob))},
			Meta: blobMeta(),
		},
	}
	h := &Handler{
		Registry: "example.com",
		Cache:    store,
		Upstream: &UpstreamClient{Client: http.DefaultClient},
	}

	req := httptest.NewRequest("GET", blobPath(), nil)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if body := rec.Body.String(); body != testBlob {
		t.Fatalf("expected full blob %q, got %q", testBlob, body)
	}
}

func TestParsePath(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		want    requestInfo
		wantErr bool
	}{
		{
			name: "manifest by tag",
			path: "org/image/manifests/v1.2.3",
			want: requestInfo{Name: "org/image", Kind: "manifests", Reference: "v1.2.3"},
		},
		{
			name: "manifest by digest",
			path: "org/image/manifests/sha256:abc123",
			want: requestInfo{Name: "org/image", Kind: "manifests", Reference: "sha256:abc123"},
		},
		{
			name: "blob by digest",
			path: "org/image/blobs/sha256:abc123",
			want: requestInfo{Name: "org/image", Kind: "blobs", Reference: "sha256:abc123"},
		},
		{
			name: "deeply nested image name",
			path: "org/sub/repo/manifests/latest",
			want: requestInfo{Name: "org/sub/repo", Kind: "manifests", Reference: "latest"},
		},
		{
			name: "single-segment image name",
			path: "library/manifests/latest",
			want: requestInfo{Name: "library", Kind: "manifests", Reference: "latest"},
		},
		{
			name:    "no kind keyword",
			path:    "org/image/v1.0",
			wantErr: true,
		},
		{
			name:    "no image name before kind",
			path:    "manifests/latest",
			wantErr: true,
		},
		{
			name:    "missing reference",
			path:    "org/image/manifests",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parsePath(tt.path)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("got %+v, want %+v", got, tt.want)
			}
		})
	}
}
