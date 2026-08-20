package workflow

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"
)

const storeSchemaVersion = 1

var (
	metaBucket        = []byte("meta")
	runsBucket        = []byte("runs")
	idempotencyBucket = []byte("idempotency")
	eventsBucket      = []byte("events")
	versionKey        = []byte("schema-version")
)

type store struct {
	db *bolt.DB
}

func openStore(runtimeDirectory string) (*store, error) {
	if strings.TrimSpace(runtimeDirectory) == "" {
		return nil, errors.New("workflow runtime directory is required")
	}
	directory := filepath.Join(runtimeDirectory, "workflows")
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, fmt.Errorf("create workflow runtime directory: %w", err)
	}
	info, err := os.Lstat(directory)
	if err != nil {
		return nil, fmt.Errorf("stat workflow runtime directory: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("workflow runtime path %q must be a directory, not a symlink", directory)
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		return nil, fmt.Errorf("protect workflow runtime directory: %w", err)
	}
	path := filepath.Join(directory, "workflows.db")
	if info, err := os.Lstat(path); err == nil && (info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular()) {
		return nil, fmt.Errorf("workflow database %q must be a regular file, not a symlink", path)
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("stat workflow database: %w", err)
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, fmt.Errorf("open workflow database: %w", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("protect workflow database: %w", err)
	}
	result := &store{db: db}
	if err := result.initialize(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return result, nil
}

func (s *store) initialize() error {
	return s.db.Update(func(tx *bolt.Tx) error {
		meta, err := tx.CreateBucketIfNotExists(metaBucket)
		if err != nil {
			return err
		}
		for _, name := range [][]byte{runsBucket, idempotencyBucket, eventsBucket} {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return err
			}
		}
		current := meta.Get(versionKey)
		if current == nil {
			var encoded [8]byte
			binary.BigEndian.PutUint64(encoded[:], storeSchemaVersion)
			return meta.Put(versionKey, encoded[:])
		}
		if len(current) != 8 || binary.BigEndian.Uint64(current) != storeSchemaVersion {
			return fmt.Errorf("workflow database schema is unsupported")
		}
		return nil
	})
}

func (s *store) create(record *runRecord, events []Event) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		runs := tx.Bucket(runsBucket)
		if runs.Get([]byte(record.Run.ID)) != nil {
			return ErrConflict
		}
		if record.Run.IdempotencyKey != "" {
			key := idempotencyKey(record.Run.WorkflowName, record.Run.IdempotencyKey)
			if existing := tx.Bucket(idempotencyBucket).Get(key); existing != nil {
				return ErrConflict
			}
			if err := tx.Bucket(idempotencyBucket).Put(key, []byte(record.Run.ID)); err != nil {
				return err
			}
		}
		if err := putRecord(runs, record); err != nil {
			return err
		}
		return putEvents(tx.Bucket(eventsBucket), events)
	})
}

func (s *store) update(record *runRecord, events []Event) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		runs := tx.Bucket(runsBucket)
		if runs.Get([]byte(record.Run.ID)) == nil {
			return ErrNotFound
		}
		if err := putRecord(runs, record); err != nil {
			return err
		}
		return putEvents(tx.Bucket(eventsBucket), events)
	})
}

func (s *store) delete(record *runRecord) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		if tx.Bucket(runsBucket).Get([]byte(record.Run.ID)) == nil {
			return ErrNotFound
		}
		if err := tx.Bucket(runsBucket).Delete([]byte(record.Run.ID)); err != nil {
			return err
		}
		if record.Run.IdempotencyKey != "" {
			if err := tx.Bucket(idempotencyBucket).Delete(idempotencyKey(record.Run.WorkflowName, record.Run.IdempotencyKey)); err != nil {
				return err
			}
		}
		prefix := append([]byte(record.Run.ID), 0)
		cursor := tx.Bucket(eventsBucket).Cursor()
		for key, _ := cursor.Seek(prefix); key != nil && bytes.HasPrefix(key, prefix); key, _ = cursor.Next() {
			if err := cursor.Delete(); err != nil {
				return err
			}
		}
		return nil
	})
}

func putRecord(bucket *bolt.Bucket, record *runRecord) error {
	encoded, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return bucket.Put([]byte(record.Run.ID), encoded)
}

func putEvents(bucket *bolt.Bucket, events []Event) error {
	for _, event := range events {
		encoded, err := json.Marshal(event)
		if err != nil {
			return err
		}
		if err := bucket.Put(eventKey(event.RunID, event.Sequence), encoded); err != nil {
			return err
		}
	}
	return nil
}

func (s *store) get(runID string) (*runRecord, error) {
	var result *runRecord
	err := s.db.View(func(tx *bolt.Tx) error {
		encoded := tx.Bucket(runsBucket).Get([]byte(runID))
		if encoded == nil {
			return ErrNotFound
		}
		var record runRecord
		if err := decodeRecordJSON(encoded, &record); err != nil {
			return fmt.Errorf("decode workflow run %q: %w", runID, err)
		}
		result = &record
		return nil
	})
	return result, err
}

func (s *store) list() ([]*runRecord, error) {
	var records []*runRecord
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(runsBucket).ForEach(func(key, value []byte) error {
			var record runRecord
			if err := decodeRecordJSON(value, &record); err != nil {
				return fmt.Errorf("decode workflow run %q: %w", key, err)
			}
			records = append(records, &record)
			return nil
		})
	})
	return records, err
}

func (s *store) idempotentRun(workflowName, key string) (string, error) {
	var runID string
	err := s.db.View(func(tx *bolt.Tx) error {
		value := tx.Bucket(idempotencyBucket).Get(idempotencyKey(workflowName, key))
		if value != nil {
			runID = string(value)
		}
		return nil
	})
	return runID, err
}

func (s *store) events(runID string, after uint64, limit int) ([]Event, error) {
	if limit <= 0 || limit > 4096 {
		limit = 4096
	}
	result := make([]Event, 0)
	err := s.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(runsBucket).Get([]byte(runID)) == nil {
			return ErrNotFound
		}
		cursor := tx.Bucket(eventsBucket).Cursor()
		prefix := append([]byte(runID), 0)
		seek := eventKey(runID, after+1)
		for key, value := cursor.Seek(seek); key != nil && bytes.HasPrefix(key, prefix) && len(result) < limit; key, value = cursor.Next() {
			var event Event
			if err := json.Unmarshal(value, &event); err != nil {
				return fmt.Errorf("decode workflow event: %w", err)
			}
			result = append(result, event)
		}
		return nil
	})
	return result, err
}

func (s *store) lastEventSequence(runID string) (uint64, error) {
	var sequence uint64
	err := s.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(runsBucket).Get([]byte(runID)) == nil {
			return ErrNotFound
		}
		prefix := append([]byte(runID), 0)
		cursor := tx.Bucket(eventsBucket).Cursor()
		key, _ := cursor.Seek(eventKey(runID, math.MaxUint64))
		if key == nil || !bytes.HasPrefix(key, prefix) {
			key, _ = cursor.Prev()
		}
		if key == nil || !bytes.HasPrefix(key, prefix) || len(key) != len(prefix)+8 {
			return nil
		}
		sequence = binary.BigEndian.Uint64(key[len(prefix):])
		return nil
	})
	return sequence, err
}

func idempotencyKey(workflowName, key string) []byte {
	return []byte(workflowName + "\x00" + key)
}

func eventKey(runID string, sequence uint64) []byte {
	key := make([]byte, len(runID)+1+8)
	copy(key, runID)
	binary.BigEndian.PutUint64(key[len(runID)+1:], sequence)
	return key
}

func (s *store) close() error { return s.db.Close() }
