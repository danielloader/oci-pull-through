package cache

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// Store is the interface for OCI object storage backends.
type Store interface {
	Init(ctx context.Context) error
	Head(ctx context.Context, key string) (ObjectMeta, error)
	GetWithMeta(ctx context.Context, key string) (*GetResult, error)
	Put(ctx context.Context, key string, body io.Reader, meta ObjectMeta) error
}

// ObjectMeta holds metadata for cached objects.
type ObjectMeta struct {
	ContentType         string
	DockerContentDigest string
	ContentLength       int64
	Header              http.Header
}

// MarshalMeta serializes an ObjectMeta to JSON for sidecar storage.
// Only the Header map is persisted; the explicit struct fields are derived
// from it on read.
func MarshalMeta(m ObjectMeta) ([]byte, error) {
	return json.Marshal(m.Header)
}

// UnmarshalMeta deserializes JSON from a sidecar file into an ObjectMeta.
// The explicit struct fields (ContentType, DockerContentDigest, ContentLength)
// are extracted from the header map.
func UnmarshalMeta(data []byte) (ObjectMeta, error) {
	var h http.Header
	if err := json.Unmarshal(data, &h); err != nil {
		return ObjectMeta{}, err
	}
	cl, _ := strconv.ParseInt(h.Get("Content-Length"), 10, 64)
	return ObjectMeta{
		ContentType:         h.Get("Content-Type"),
		DockerContentDigest: h.Get("Docker-Content-Digest"),
		ContentLength:       cl,
		Header:              h,
	}, nil
}

// Redirector is an optional interface that cache stores can implement to
// support HTTP redirects for cached objects. When implemented, the proxy
// can redirect clients directly to the storage backend (e.g. via S3
// presigned URLs) instead of streaming the data through the proxy.
type Redirector interface {
	RedirectURL(ctx context.Context, key string) (url string, meta ObjectMeta, err error)
}

// GetResult holds the body and metadata from a single get call.
type GetResult struct {
	Body io.ReadCloser
	Meta ObjectMeta
}

// NormalizeDigest ensures a digest uses the standard "algorithm:hex" format.
// SeaweedFS mangles colons to hyphens in S3 metadata values, so
// "sha256:abc..." becomes "sha256-abc..." on read-back. This restores the colon.
func NormalizeDigest(s string) string {
	if strings.Contains(s, ":") {
		return s
	}
	for _, alg := range []string{"sha256", "sha512"} {
		if strings.HasPrefix(s, alg+"-") {
			return alg + ":" + s[len(alg)+1:]
		}
	}
	return s
}
