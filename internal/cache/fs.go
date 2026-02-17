package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// FSStore provides filesystem-backed caching for OCI objects.
type FSStore struct {
	root string
}

// NewFSStore creates a new filesystem cache store rooted at root.
func NewFSStore(root string) *FSStore {
	return &FSStore{root: root}
}

// Init ensures the root directory exists.
func (f *FSStore) Init(_ context.Context) error {
	return os.MkdirAll(f.root, 0o755)
}

// fsMeta is the JSON structure stored in sidecar .meta.json files.
type fsMeta struct {
	ContentType         string `json:"content_type"`
	DockerContentDigest string `json:"docker_content_digest"`
	ContentLength       int64  `json:"content_length"`
}

func (f *FSStore) dataPath(key string) string {
	return filepath.Join(f.root, filepath.FromSlash(key))
}

func (f *FSStore) metaPath(key string) string {
	return f.dataPath(key) + ".meta.json"
}

// Head checks if an object exists and returns its metadata from the sidecar file.
func (f *FSStore) Head(_ context.Context, key string) (ObjectMeta, error) {
	meta, err := f.readMeta(key)
	if err != nil {
		return ObjectMeta{}, err
	}
	return meta, nil
}

// GetWithMeta retrieves an object's body and metadata.
func (f *FSStore) GetWithMeta(_ context.Context, key string) (*GetResult, error) {
	meta, err := f.readMeta(key)
	if err != nil {
		return nil, err
	}

	file, err := os.Open(f.dataPath(key))
	if err != nil {
		return nil, err
	}

	return &GetResult{Body: file, Meta: meta}, nil
}

// Put writes an object and its metadata sidecar atomically using temp file + rename.
func (f *FSStore) Put(_ context.Context, key string, body io.Reader, meta ObjectMeta) error {
	dp := f.dataPath(key)

	if err := os.MkdirAll(filepath.Dir(dp), 0o755); err != nil {
		return fmt.Errorf("creating directory: %w", err)
	}

	// Write data file atomically
	if err := atomicWrite(dp, body); err != nil {
		return fmt.Errorf("writing data: %w", err)
	}

	// Write metadata sidecar atomically
	fm := fsMeta{
		ContentType:         meta.ContentType,
		DockerContentDigest: meta.DockerContentDigest,
		ContentLength:       meta.ContentLength,
	}
	metaJSON, err := json.Marshal(fm)
	if err != nil {
		return fmt.Errorf("marshalling metadata: %w", err)
	}

	if err := atomicWriteBytes(f.metaPath(key), metaJSON); err != nil {
		return fmt.Errorf("writing metadata: %w", err)
	}

	return nil
}

func (f *FSStore) readMeta(key string) (ObjectMeta, error) {
	data, err := os.ReadFile(f.metaPath(key))
	if err != nil {
		return ObjectMeta{}, err
	}

	var fm fsMeta
	if err := json.Unmarshal(data, &fm); err != nil {
		return ObjectMeta{}, fmt.Errorf("parsing metadata: %w", err)
	}

	return ObjectMeta{
		ContentType:         fm.ContentType,
		DockerContentDigest: fm.DockerContentDigest,
		ContentLength:       fm.ContentLength,
	}, nil
}

// atomicWrite writes data from a reader to dst via a temp file + rename.
func atomicWrite(dst string, r io.Reader) error {
	tmp, err := os.CreateTemp(filepath.Dir(dst), ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()

	if _, err := io.Copy(tmp, r); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	return os.Rename(tmpName, dst)
}

// atomicWriteBytes writes bytes to dst via a temp file + rename.
func atomicWriteBytes(dst string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(dst), ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	return os.Rename(tmpName, dst)
}
