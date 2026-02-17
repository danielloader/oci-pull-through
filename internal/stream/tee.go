package stream

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"sync/atomic"

	"github.com/danielloader/oci-pull-through/internal/cache"
)

// TeeToStore streams the upstream response body to the HTTP client while
// simultaneously uploading it to the cache store via a pipe. Caching is
// best-effort: if the upload fails, the client still receives all bytes
// uninterrupted.
//
// The flow:
//
//	upstream.Body → TeeReader → io.Copy(w, tee) → client
//	                   │
//	                   └→ safeWriter → PipeWriter → PipeReader → store.Put
func TeeToStore(ctx context.Context, src io.Reader, dst http.ResponseWriter, store cache.Store, key string, meta cache.ObjectMeta) error {
	pr, pw := io.Pipe()

	// Wrap the pipe writer so errors never propagate to the TeeReader.
	// If the store stops reading or the pipe errors, writes are silently discarded.
	sw := &safeWriter{w: pw}
	tee := io.TeeReader(src, sw)

	// Start store upload in a goroutine reading from the pipe
	uploadDone := make(chan struct{})
	go func() {
		defer close(uploadDone)
		// Wrap the PipeReader to hide its concrete type from store
		// implementations that may treat *io.PipeReader specially.
		err := store.Put(context.Background(), key, readerOnly{pr}, meta)
		if err != nil {
			slog.Debug("cache upload failed", "key", key, "error", err)
			// Drain the pipe so writes from the TeeReader don't block.
			io.Copy(io.Discard, pr)
		} else {
			slog.Debug("cached", "key", key)
		}
	}()

	// Drive both streams: copy to the client, which also feeds the pipe.
	_, copyErr := io.Copy(dst, tee)

	// Signal EOF to the store uploader and wait for it to finish.
	pw.Close()
	<-uploadDone

	return copyErr
}

// readerOnly wraps an io.Reader to hide its concrete type.
type readerOnly struct{ io.Reader }

// safeWriter wraps an io.Writer and silently discards writes after any error.
// This ensures the TeeReader never sees a write failure, so the client
// stream is never interrupted by store issues.
type safeWriter struct {
	w      io.Writer
	failed atomic.Bool
}

func (s *safeWriter) Write(p []byte) (int, error) {
	if s.failed.Load() {
		return len(p), nil
	}
	n, err := s.w.Write(p)
	if err != nil {
		s.failed.Store(true)
		return len(p), nil
	}
	return n, nil
}
