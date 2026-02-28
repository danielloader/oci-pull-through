package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/danielloader/oci-pull-through/internal/cache"
	"github.com/danielloader/oci-pull-through/internal/stream"
)

// requestInfo holds the parsed components of an OCI registry request.
type requestInfo struct {
	Registry  string // e.g. "ghcr.io"
	Name      string // e.g. "org/image"
	Kind      string // "manifests" or "blobs"
	Reference string // tag or digest
}

// isTagManifest returns true if the request is for a manifest by tag (not digest).
func (r requestInfo) isTagManifest() bool {
	return r.Kind == "manifests" && !strings.Contains(r.Reference, ":")
}

// shouldCache reports whether this request's response should be cached.
// Blobs and digest manifests are always cached (content-addressed, immutable).
// Tag manifests are controlled by CacheTagManifests, with an extra gate
// on the "latest" tag via CacheLatestTag.
func (h *Handler) shouldCache(info requestInfo) bool {
	if !info.isTagManifest() {
		return true
	}
	if !h.CacheTagManifests {
		return false
	}
	if info.Reference == "latest" && !h.CacheLatestTag {
		return false
	}
	return true
}

// image returns "registry/name" for logging.
func (r requestInfo) image() string {
	return r.Registry + "/" + r.Name
}

// shortRef returns a truncated reference for logging.
// Tags are returned as-is; digests are shortened to algo:first12.
func (r requestInfo) shortRef() string {
	if alg, hex, ok := strings.Cut(r.Reference, ":"); ok && len(hex) > 12 {
		return alg + ":" + hex[:12]
	}
	return r.Reference
}

// Handler is the main HTTP handler for the OCI proxy.
type Handler struct {
	Registry          string
	Cache             cache.Store
	Upstream          *UpstreamClient
	CacheTagManifests bool
	CacheLatestTag    bool
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "ok")
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/v2")
	path = strings.TrimPrefix(path, "/")

	// GET /v2/ — proxy to upstream so auth challenges (401 + Www-Authenticate) flow through
	if path == "" || path == "/" {
		h.handleV2Check(w, r)
		return
	}

	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		writeOCIError(w, http.StatusMethodNotAllowed, "UNSUPPORTED", "read-only proxy: method not allowed")
		return
	}

	info, err := parsePath(path)
	if err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}
	info.Registry = h.Registry

	slog.Debug("request", "method", r.Method, "image", info.image(), "kind", info.Kind, "ref", info.shortRef())

	// Referrers — pass through to upstream, no caching
	if info.Kind == "referrers" {
		h.handlePassthrough(w, r, info)
		return
	}

	storageKey := storageKey(info)

	// HEAD request — check cache, otherwise forward upstream
	if r.Method == http.MethodHead {
		h.handleHead(w, r, info, storageKey)
		return
	}

	// GET request — cache-first, then upstream with tee-stream
	h.handleGet(w, r, info, storageKey)
}

func (h *Handler) handleV2Check(w http.ResponseWriter, r *http.Request) {
	resp, err := h.Upstream.DoV2Check(r, h.Registry)
	if err != nil {
		slog.Debug("upstream /v2/ check failed", "error", err)
		w.Header().Set("Docker-Distribution-API-Version", "registry/2.0")
		w.WriteHeader(http.StatusOK)
		return
	}
	defer resp.Body.Close()

	copyResponseHeaders(w, resp)
	w.Header().Set("Docker-Distribution-API-Version", "registry/2.0")
	w.WriteHeader(resp.StatusCode)
	if _, err := copyToClient(w, resp.Body); err != nil {
		slog.Debug("error forwarding /v2/ response", "error", err)
	}
}

func (h *Handler) handleHead(w http.ResponseWriter, r *http.Request, info requestInfo, key string) {
	if h.shouldCache(info) {
		meta, err := h.Cache.Head(r.Context(), key)
		if err == nil {
			replayStoredHeaders(w, meta)
			w.Header().Set("Docker-Distribution-API-Version", "registry/2.0")
			setCacheControl(w, info)
			w.WriteHeader(http.StatusOK)
			return
		}
	}

	// Cache miss or tag manifest — forward HEAD to upstream
	resp, err := h.Upstream.Do(r, info)
	if err != nil {
		slog.Debug("upstream HEAD failed", "error", err)
		writeError(w, "upstream error", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	copyResponseHeaders(w, resp)
	w.Header().Set("Docker-Distribution-API-Version", "registry/2.0")
	w.WriteHeader(resp.StatusCode)
}

func (h *Handler) handlePassthrough(w http.ResponseWriter, r *http.Request, info requestInfo) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	resp, err := h.Upstream.Do(r.WithContext(ctx), info)
	if err != nil {
		slog.Debug("upstream passthrough failed", "kind", info.Kind, "error", err)
		writeError(w, "upstream unavailable", http.StatusGatewayTimeout)
		return
	}
	defer resp.Body.Close()

	copyResponseHeaders(w, resp)
	w.Header().Set("Docker-Distribution-API-Version", "registry/2.0")
	w.WriteHeader(resp.StatusCode)
	if _, err := copyToClient(w, resp.Body); err != nil {
		slog.Debug("error forwarding passthrough response", "error", err)
	}
}

func (h *Handler) handleGet(w http.ResponseWriter, r *http.Request, info requestInfo, key string) {
	// 1. Try redirect for backends that support presigned URLs (e.g. S3)
	if redirector, ok := h.Cache.(cache.Redirector); ok && h.shouldCache(info) {
		url, meta, err := redirector.RedirectURL(r.Context(), key)
		if err == nil {
			slog.Info("cache hit (redirect)", "image", info.image(), "kind", info.Kind, "ref", info.shortRef())
			replayStoredHeaders(w, meta)
			w.Header().Set("Docker-Distribution-API-Version", "registry/2.0")
			setCacheControl(w, info)
			http.Redirect(w, r, url, http.StatusTemporaryRedirect)
			return
		}
		// Fall through to upstream on error (cache miss or presign failure)
	}

	// 2. Check cache with streaming (FS backend with seekable files)
	if h.shouldCache(info) {
		result, err := h.Cache.GetWithMeta(r.Context(), key)
		if err == nil {
			slog.Info("cache hit", "image", info.image(), "kind", info.Kind, "ref", info.shortRef())
			defer result.Body.Close()
			replayStoredHeaders(w, result.Meta)
			w.Header().Set("Docker-Distribution-API-Version", "registry/2.0")
			setCacheControl(w, info)
			if seeker, ok := result.Body.(io.ReadSeeker); ok {
				// FS backend returns *os.File (seekable) — let ServeContent
				// handle Range negotiation, 206 responses, and Content-Range.
				http.ServeContent(w, r, "", time.Time{}, seeker)
			} else {
				// Non-seekable stream — serve full body.
				w.WriteHeader(http.StatusOK)
				if _, err := copyToClient(w, result.Body); err != nil {
					slog.Debug("error streaming cached response", "error", err)
				}
			}
			return
		}
	}

	// 2. Cache miss or tag manifest — fetch from upstream
	slog.Info("upstream fetch", "image", info.image(), "kind", info.Kind, "ref", info.shortRef())
	resp, err := h.Upstream.Do(r, info)
	if err != nil {
		slog.Error("upstream failed", "image", info.image(), "error", err)
		writeError(w, "upstream error", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	// Non-200 responses (401, 404, etc.) — forward as-is without caching
	if resp.StatusCode != http.StatusOK {
		slog.Debug("upstream non-200", "image", info.image(), "status", resp.StatusCode)
		copyResponseHeaders(w, resp)
		w.Header().Set("Docker-Distribution-API-Version", "registry/2.0")
		w.WriteHeader(resp.StatusCode)
		if _, err := copyToClient(w, resp.Body); err != nil {
			slog.Debug("error forwarding upstream response", "error", err)
		}
		return
	}

	// 3. 200 OK — tag manifests forward directly, everything else tee-streams to S3
	copyResponseHeaders(w, resp)
	w.Header().Set("Docker-Distribution-API-Version", "registry/2.0")
	if !h.shouldCache(info) {
		w.WriteHeader(http.StatusOK)
		if _, err := copyToClient(w, resp.Body); err != nil {
			slog.Debug("error forwarding tag manifest", "error", err)
		}
		return
	}

	setCacheControl(w, info)
	w.WriteHeader(http.StatusOK)

	putMeta := cache.ObjectMeta{
		ContentType:         resp.Header.Get("Content-Type"),
		DockerContentDigest: resp.Header.Get("Docker-Content-Digest"),
		ContentLength:       resp.ContentLength,
		Header:              cloneResponseHeaders(resp),
	}

	err = stream.TeeToStore(r.Context(), resp.Body, w, h.Cache, key, putMeta)
	if err != nil {
		slog.Debug("tee stream error", "key", key, "error", err)
	}
}

// hopByHopHeaders are headers that should not be forwarded by a proxy.
var hopByHopHeaders = map[string]struct{}{
	"Connection":          {},
	"Keep-Alive":          {},
	"Proxy-Authenticate":  {},
	"Proxy-Authorization": {},
	"Te":                  {},
	"Trailer":             {},
	"Transfer-Encoding":   {},
	"Upgrade":             {},
}

// copyResponseHeaders forwards all upstream response headers except hop-by-hop headers.
func copyResponseHeaders(w http.ResponseWriter, resp *http.Response) {
	for key, values := range resp.Header {
		if _, hop := hopByHopHeaders[http.CanonicalHeaderKey(key)]; hop {
			continue
		}
		for _, v := range values {
			w.Header().Add(key, v)
		}
	}
}

// cloneResponseHeaders returns a copy of the upstream response headers,
// excluding hop-by-hop headers, suitable for persisting in cache metadata.
func cloneResponseHeaders(resp *http.Response) http.Header {
	h := make(http.Header)
	for key, values := range resp.Header {
		if _, hop := hopByHopHeaders[http.CanonicalHeaderKey(key)]; hop {
			continue
		}
		h[http.CanonicalHeaderKey(key)] = append([]string(nil), values...)
	}
	return h
}

// replayStoredHeaders writes all headers from cached metadata onto the response.
// Headers like Content-Type, Docker-Content-Digest, and Content-Length are
// included in the stored set, so no special-casing is needed.
func replayStoredHeaders(w http.ResponseWriter, meta cache.ObjectMeta) {
	for key, values := range meta.Header {
		for _, v := range values {
			w.Header().Add(key, v)
		}
	}
	// Fallback: if Header map is nil (legacy cache entries without stored
	// headers), set the critical headers from the explicit fields.
	if meta.Header == nil {
		if meta.ContentType != "" {
			w.Header().Set("Content-Type", meta.ContentType)
		}
		if meta.DockerContentDigest != "" {
			w.Header().Set("Docker-Content-Digest", meta.DockerContentDigest)
		}
		if meta.ContentLength > 0 {
			w.Header().Set("Content-Length", fmt.Sprintf("%d", meta.ContentLength))
		}
	}
}

// writeError sends a plain-text error response with the OCI version header.
func writeError(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Docker-Distribution-API-Version", "registry/2.0")
	http.Error(w, msg, code)
}

// writeOCIError sends an OCI-compliant JSON error response.
func writeOCIError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Docker-Distribution-API-Version", "registry/2.0")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]any{
		"errors": []map[string]string{
			{"code": code, "message": message},
		},
	})
}

// setCacheControl sets Cache-Control headers based on content type.
// Blobs and digest manifests are immutable (content-addressed).
// Tag manifests get a shorter max-age since tags can move:
// "latest" gets 1 hour, other tags get 28 days.
func setCacheControl(w http.ResponseWriter, info requestInfo) {
	if info.isTagManifest() {
		if info.Reference == "latest" {
			w.Header().Set("Cache-Control", "public, max-age=3600")
		} else {
			w.Header().Set("Cache-Control", "public, max-age=2419200")
		}
	} else {
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	}
}

// parsePath parses a /v2/ sub-path into its components.
// Input path should already have "/v2/" prefix stripped.
//
// All segments before the "manifests" or "blobs" keyword form the image name.
// The Registry field is left empty — the caller sets it from config.
func parsePath(path string) (requestInfo, error) {
	path = strings.TrimPrefix(path, "/")
	path = strings.TrimSuffix(path, "/")
	segments := strings.Split(path, "/")

	// Find "manifests" or "blobs" from the end
	kindIdx := -1
	for i := len(segments) - 1; i >= 0; i-- {
		if segments[i] == "manifests" || segments[i] == "blobs" || segments[i] == "referrers" {
			kindIdx = i
			break
		}
	}

	if kindIdx < 0 {
		return requestInfo{}, fmt.Errorf("path must contain 'manifests' or 'blobs'")
	}
	if kindIdx < 1 {
		return requestInfo{}, fmt.Errorf("path must include image name before %s", segments[kindIdx])
	}
	if kindIdx+1 >= len(segments) {
		return requestInfo{}, fmt.Errorf("missing reference after %s", segments[kindIdx])
	}

	// Normalize the reference so that mangled digests (sha256-hex from
	// SeaweedFS metadata round-trip) are restored to sha256:hex.
	ref := cache.NormalizeDigest(strings.Join(segments[kindIdx+1:], "/"))

	return requestInfo{
		Name:      strings.Join(segments[:kindIdx], "/"),
		Kind:      segments[kindIdx],
		Reference: ref,
	}, nil
}

// storageKey computes the storage key for a request.
// Digest colons are replaced with hyphens (sha256:abc → sha256-abc) to keep
// keys as single path segments.
func storageKey(info requestInfo) string {
	if info.Kind == "blobs" {
		// blobs are content-addressed; key by digest only
		return "blobs/" + strings.Replace(info.Reference, ":", "-", 1)
	}

	// Manifests — check if reference is a digest or tag
	if strings.Contains(info.Reference, ":") {
		return fmt.Sprintf("manifests/%s/%s/%s", info.Registry, info.Name, strings.Replace(info.Reference, ":", "-", 1))
	}

	// Tag references are never cached — this shouldn't be reached.
	// Return a key anyway for safety, but it will never be written to or read from S3.
	return fmt.Sprintf("manifests/%s/%s/tags/%s", info.Registry, info.Name, info.Reference)
}

func copyToClient(w http.ResponseWriter, src io.Reader) (int64, error) {
	return io.Copy(w, src)
}
