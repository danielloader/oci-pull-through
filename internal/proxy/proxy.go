package proxy

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/danielloader/oci-pull-through/internal/cache"
	"github.com/danielloader/oci-pull-through/internal/stream"
)

var validRegistry = regexp.MustCompile(`^[a-zA-Z0-9]([a-zA-Z0-9.-]*[a-zA-Z0-9])?(:[0-9]{1,5})?$`)

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

	// GET /v2/ — version check
	if path == "" || path == "/" {
		w.Header().Set("Docker-Distribution-API-Version", "registry/2.0")
		w.WriteHeader(http.StatusOK)
		return
	}

	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		writeError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	info, err := parsePath(path)
	if err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}

	slog.Debug("request", "method", r.Method, "image", info.image(), "kind", info.Kind, "ref", info.shortRef())

	storageKey := storageKey(info)

	// HEAD request — check cache, otherwise forward upstream
	if r.Method == http.MethodHead {
		h.handleHead(w, r, info, storageKey)
		return
	}

	// GET request — cache-first, then upstream with tee-stream
	h.handleGet(w, r, info, storageKey)
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

func (h *Handler) handleGet(w http.ResponseWriter, r *http.Request, info requestInfo, key string) {
	// 1. Check cache (uncacheable requests skip straight to upstream)
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
				// S3 backend returns a non-seekable stream — serve full body.
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

// writeError sends an error response with the OCI version header.
func writeError(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Docker-Distribution-API-Version", "registry/2.0")
	http.Error(w, msg, code)
}

// setCacheControl sets Cache-Control headers based on content type.
// Blobs and digest manifests are immutable (content-addressed).
// Tag manifests get a short max-age since tags can move.
func setCacheControl(w http.ResponseWriter, info requestInfo) {
	if info.isTagManifest() {
		w.Header().Set("Cache-Control", "public, max-age=60")
	} else {
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	}
}

// parsePath parses a /v2/ sub-path into its components.
// Input path should already have "/v2/" prefix stripped.
func parsePath(path string) (requestInfo, error) {
	path = strings.TrimPrefix(path, "/")
	path = strings.TrimSuffix(path, "/")
	segments := strings.Split(path, "/")

	// Find "manifests" or "blobs" from the end
	kindIdx := -1
	for i := len(segments) - 1; i >= 0; i-- {
		if segments[i] == "manifests" || segments[i] == "blobs" {
			kindIdx = i
			break
		}
	}

	if kindIdx < 0 {
		return requestInfo{}, fmt.Errorf("path must contain 'manifests' or 'blobs'")
	}
	if kindIdx < 2 {
		return requestInfo{}, fmt.Errorf("path must include registry and image name before %s", segments[kindIdx])
	}
	if kindIdx+1 >= len(segments) {
		return requestInfo{}, fmt.Errorf("missing reference after %s", segments[kindIdx])
	}

	// Normalize the reference so that mangled digests (sha256-hex from
	// SeaweedFS metadata round-trip) are restored to sha256:hex.
	ref := cache.NormalizeDigest(strings.Join(segments[kindIdx+1:], "/"))

	registry := segments[0]
	if !validRegistry.MatchString(registry) {
		return requestInfo{}, fmt.Errorf("invalid registry name: %s", registry)
	}

	return requestInfo{
		Registry:  registry,
		Name:      strings.Join(segments[1:kindIdx], "/"),
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
