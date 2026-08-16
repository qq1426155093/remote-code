package files

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	transferStateVersion              = 1
	defaultUploadSessionTTL           = 24 * time.Hour
	defaultCompletedUploadSessionTTL  = time.Hour
	defaultMaxActiveUploadSessions    = 64
	defaultUploadCheckpointBytes      = int64(4 << 20)
	defaultUploadCheckpointInterval   = time.Second
	transferStateDirectoryPermissions = 0o700
	transferStateFilePermissions      = 0o600
	maxTransferStateBytes             = 64 << 10
)

// TransferConfig controls resumable file-transfer state and resource limits.
type TransferConfig struct {
	Disabled               bool
	UploadSessionTTL       time.Duration
	CompletedSessionTTL    time.Duration
	MaxUploadSessions      int
	MaxStagingBytes        int64
	CheckpointBytes        int64
	CheckpointInterval     time.Duration
	MaxConcurrentDownloads int
}

// ValidateTransferConfig applies defaults to a copy and validates its limits.
func ValidateTransferConfig(config TransferConfig, maxUploadBytes int64) error {
	return config.applyDefaults(maxUploadBytes)
}

func (c *TransferConfig) applyDefaults(maxUploadBytes int64) error {
	if c.UploadSessionTTL == 0 {
		c.UploadSessionTTL = defaultUploadSessionTTL
	}
	if c.CompletedSessionTTL == 0 {
		c.CompletedSessionTTL = defaultCompletedUploadSessionTTL
	}
	if c.MaxUploadSessions == 0 {
		c.MaxUploadSessions = defaultMaxActiveUploadSessions
	}
	if c.MaxStagingBytes == 0 {
		if maxUploadBytes > math.MaxInt64/4 {
			c.MaxStagingBytes = maxUploadBytes
		} else {
			c.MaxStagingBytes = maxUploadBytes * 4
		}
	}
	if c.CheckpointBytes == 0 {
		c.CheckpointBytes = defaultUploadCheckpointBytes
	}
	if c.CheckpointInterval == 0 {
		c.CheckpointInterval = defaultUploadCheckpointInterval
	}
	if c.MaxConcurrentDownloads == 0 {
		c.MaxConcurrentDownloads = 16
	}
	if c.UploadSessionTTL < time.Second {
		return errors.New("upload session TTL must be at least one second")
	}
	if c.CompletedSessionTTL < time.Second {
		return errors.New("completed upload session TTL must be at least one second")
	}
	if c.MaxUploadSessions <= 0 {
		return errors.New("maximum upload sessions must be positive")
	}
	if c.MaxStagingBytes < maxUploadBytes {
		return errors.New("maximum staging bytes must be at least the maximum upload bytes")
	}
	if c.CheckpointBytes <= 0 {
		return errors.New("upload checkpoint bytes must be positive")
	}
	if c.CheckpointInterval <= 0 {
		return errors.New("upload checkpoint interval must be positive")
	}
	if c.MaxConcurrentDownloads <= 0 {
		return errors.New("maximum concurrent downloads must be positive")
	}
	return nil
}

type uploadResultRecord struct {
	Path       string    `json:"path"`
	Name       string    `json:"name"`
	Size       int64     `json:"size"`
	Mode       uint32    `json:"mode"`
	ModifiedAt time.Time `json:"modified_at"`
	SHA256     []byte    `json:"sha256"`
}

type uploadRecord struct {
	Version         int                       `json:"version"`
	WorkspaceID     string                    `json:"workspace_id"`
	UploadID        string                    `json:"upload_id"`
	RequestID       string                    `json:"request_id"`
	TargetPath      string                    `json:"target_path"`
	TempPath        string                    `json:"temp_path"`
	Size            int64                     `json:"size"`
	SHA256          []byte                    `json:"sha256"`
	Mode            uint32                    `json:"mode"`
	Overwrite       bool                      `json:"overwrite"`
	CommittedOffset int64                     `json:"committed_offset"`
	State           codev1.UploadSessionState `json:"state"`
	CreatedAt       time.Time                 `json:"created_at"`
	UpdatedAt       time.Time                 `json:"updated_at"`
	ExpiresAt       time.Time                 `json:"expires_at"`
	Result          *uploadResultRecord       `json:"result,omitempty"`
}

type uploadSession struct {
	mu     sync.Mutex
	record uploadRecord
	active bool
}

type transferStore struct {
	root           *os.Root
	workspaceID    string
	maxUploadBytes int64
	stateRoot      string
	uploadsDir     string
	ephemeralRoot  string
	disabled       bool
	config         TransferConfig
	revisionKey    []byte
	downloadSlots  chan struct{}
	lockFile       *os.File

	mu            sync.Mutex
	sessions      map[string]*uploadSession
	requests      map[string]string
	targets       map[string]string
	temps         map[string]string
	activeUploads int
	reservedBytes int64
	stop          chan struct{}
	done          chan struct{}
	closeOnce     sync.Once
}

func newTransferStore(root *os.Root, workspace, runtimeDirectory string, maxUploadBytes int64, config TransferConfig) (*transferStore, error) {
	if err := config.applyDefaults(maxUploadBytes); err != nil {
		return nil, err
	}
	store := &transferStore{
		root: root, disabled: config.Disabled, config: config, maxUploadBytes: maxUploadBytes,
		sessions: make(map[string]*uploadSession), requests: make(map[string]string),
		targets: make(map[string]string), temps: make(map[string]string),
		stop: make(chan struct{}), done: make(chan struct{}),
		downloadSlots: make(chan struct{}, config.MaxConcurrentDownloads),
	}
	workspaceDigest := sha256.Sum256([]byte(filepath.Clean(workspace)))
	store.workspaceID = hex.EncodeToString(workspaceDigest[:])
	if store.disabled {
		close(store.done)
		return store, nil
	}
	if runtimeDirectory == "" {
		ephemeral, err := os.MkdirTemp("", "remote-code-file-transfers-*")
		if err != nil {
			return nil, err
		}
		store.ephemeralRoot = ephemeral
		runtimeDirectory = ephemeral
	}
	store.stateRoot = filepath.Join(runtimeDirectory, "file-transfers")
	store.uploadsDir = filepath.Join(store.stateRoot, "uploads")
	if err := os.MkdirAll(store.uploadsDir, transferStateDirectoryPermissions); err != nil {
		store.removeEphemeralRoot()
		return nil, err
	}
	if err := os.Chmod(store.stateRoot, transferStateDirectoryPermissions); err != nil {
		store.removeEphemeralRoot()
		return nil, err
	}
	if err := os.Chmod(store.uploadsDir, transferStateDirectoryPermissions); err != nil {
		store.removeEphemeralRoot()
		return nil, err
	}
	lockFile, err := lockTransferStateDirectory(store.stateRoot)
	if err != nil {
		store.removeEphemeralRoot()
		return nil, err
	}
	store.lockFile = lockFile
	key, err := loadOrCreateRevisionKey(store.stateRoot)
	if err != nil {
		store.unlockStateDirectory()
		store.removeEphemeralRoot()
		return nil, err
	}
	store.revisionKey = key
	if err := store.loadSessions(); err != nil {
		store.unlockStateDirectory()
		store.removeEphemeralRoot()
		return nil, err
	}
	store.cleanupExpired(time.Now())
	go store.runGC()
	return store, nil
}

func (s *transferStore) close() {
	s.closeOnce.Do(func() {
		if !s.disabled {
			close(s.stop)
			<-s.done
			s.unlockStateDirectory()
		}
		s.removeEphemeralRoot()
	})
}

func lockTransferStateDirectory(stateRoot string) (*os.File, error) {
	file, err := os.OpenFile(filepath.Join(stateRoot, "controller.lock"), os.O_CREATE|os.O_RDWR|unix.O_NOFOLLOW, transferStateFilePermissions)
	if err != nil {
		return nil, err
	}
	if err := file.Chmod(transferStateFilePermissions); err != nil {
		_ = file.Close()
		return nil, err
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = file.Close()
		return nil, errors.New("file transfer state directory is already in use")
	}
	return file, nil
}

func (s *transferStore) unlockStateDirectory() {
	if s.lockFile == nil {
		return
	}
	_ = unix.Flock(int(s.lockFile.Fd()), unix.LOCK_UN)
	_ = s.lockFile.Close()
	s.lockFile = nil
}

func (s *transferStore) removeEphemeralRoot() {
	if s.ephemeralRoot != "" {
		_ = os.RemoveAll(s.ephemeralRoot)
		s.ephemeralRoot = ""
	}
}

func (s *transferStore) runGC() {
	defer close(s.done)
	interval := s.config.UploadSessionTTL / 4
	if interval > 10*time.Minute {
		interval = 10 * time.Minute
	}
	if interval < time.Minute {
		interval = time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			s.cleanupExpired(now)
		case <-s.stop:
			return
		}
	}
}

func (s *transferStore) loadSessions() error {
	entries, err := os.ReadDir(s.uploadsDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		name := filepath.Join(s.uploadsDir, entry.Name())
		data, err := readTransferStateFile(name)
		if err != nil {
			return err
		}
		var record uploadRecord
		if err := json.Unmarshal(data, &record); err != nil || !validUploadRecord(record, strings.TrimSuffix(entry.Name(), ".json"), s.workspaceID, s.maxUploadBytes) {
			_ = os.Rename(name, name+".corrupt")
			continue
		}
		session := &uploadSession{record: record}
		if err := s.recoverSession(session); err != nil {
			return fmt.Errorf("recover upload session: %w", err)
		}
		s.sessions[record.UploadID] = session
		s.requests[record.RequestID] = record.UploadID
		if isOpenUploadState(session.record.State) {
			s.targets[record.TargetPath] = record.UploadID
			s.temps[record.TempPath] = record.UploadID
			s.activeUploads++
			s.reservedBytes += record.Size
		}
	}
	return nil
}

func validUploadRecord(record uploadRecord, filenameID, workspaceID string, maxUploadBytes int64) bool {
	if record.Version != transferStateVersion || record.WorkspaceID != workspaceID || record.UploadID != filenameID ||
		len(record.UploadID) != 48 || record.RequestID == "" || len(record.RequestID) > 128 {
		return false
	}
	if _, err := hex.DecodeString(record.UploadID); err != nil {
		return false
	}
	if len(record.SHA256) != sha256.Size || record.Size < 0 || record.Size > maxUploadBytes ||
		record.CommittedOffset < 0 || record.CommittedOffset > record.Size || record.Mode&^uint32(0o777) != 0 {
		return false
	}
	if _, err := cleanMutablePath(record.TargetPath); err != nil {
		return false
	}
	if _, err := cleanMutablePath(record.TempPath); err != nil {
		return false
	}
	if path.Dir(record.TempPath) != path.Dir(record.TargetPath) || path.Base(record.TempPath) != ".remote-code-upload-"+record.UploadID {
		return false
	}
	return record.State >= codev1.UploadSessionState_UPLOAD_SESSION_STATE_OPEN && record.State <= codev1.UploadSessionState_UPLOAD_SESSION_STATE_FAILED
}

func (s *transferStore) recoverSession(session *uploadSession) error {
	record := &session.record
	if !isOpenUploadState(record.State) {
		info, err := s.root.Lstat(record.TempPath)
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			if err := s.root.Remove(record.TempPath); err != nil {
				return err
			}
			return syncRootDirectory(s.root, path.Dir(record.TempPath))
		}
		return errors.New("terminal upload staging path is not a regular file")
	}
	info, err := s.root.Lstat(record.TempPath)
	if errors.Is(err, fs.ErrNotExist) {
		if record.State == codev1.UploadSessionState_UPLOAD_SESSION_STATE_FINALIZING {
			result, verifyErr := s.verifyPublishedUpload(*record)
			if verifyErr == nil {
				if err := syncRootDirectory(s.root, path.Dir(record.TargetPath)); err != nil {
					return err
				}
				record.State = codev1.UploadSessionState_UPLOAD_SESSION_STATE_COMPLETE
				record.CommittedOffset = record.Size
				record.Result = result
				record.ExpiresAt = time.Now().Add(s.config.CompletedSessionTTL)
				return s.persistRecord(*record)
			}
		}
		record.State = codev1.UploadSessionState_UPLOAD_SESSION_STATE_FAILED
		record.ExpiresAt = time.Now().Add(s.config.CompletedSessionTTL)
		return s.persistRecord(*record)
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() < record.CommittedOffset {
		record.State = codev1.UploadSessionState_UPLOAD_SESSION_STATE_FAILED
		record.ExpiresAt = time.Now().Add(s.config.CompletedSessionTTL)
		return s.persistRecord(*record)
	}
	if record.State == codev1.UploadSessionState_UPLOAD_SESSION_STATE_FINALIZING {
		if result, verifyErr := s.verifyPublishedUpload(*record); verifyErr == nil {
			if err := s.root.Remove(record.TempPath); err != nil {
				return err
			}
			if err := syncRootDirectory(s.root, path.Dir(record.TempPath)); err != nil {
				return err
			}
			record.State = codev1.UploadSessionState_UPLOAD_SESSION_STATE_COMPLETE
			record.CommittedOffset = record.Size
			record.Result = result
			record.ExpiresAt = time.Now().Add(s.config.CompletedSessionTTL)
			return s.persistRecord(*record)
		}
	}
	file, err := s.root.OpenFile(record.TempPath, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	if info.Size() != record.CommittedOffset {
		err = file.Truncate(record.CommittedOffset)
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if record.State == codev1.UploadSessionState_UPLOAD_SESSION_STATE_FINALIZING {
		record.State = codev1.UploadSessionState_UPLOAD_SESSION_STATE_OPEN
		return s.persistRecord(*record)
	}
	return nil
}

func (s *transferStore) verifyPublishedUpload(record uploadRecord) (*uploadResultRecord, error) {
	file, err := s.root.OpenFile(record.TargetPath, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	openedInfo, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if !openedInfo.Mode().IsRegular() || openedInfo.Size() != record.Size || uint32(openedInfo.Mode().Perm()) != record.Mode {
		_ = file.Close()
		return nil, errors.New("published upload is not the expected regular file")
	}
	hash := sha256.New()
	size, copyErr := io.Copy(hash, file)
	finalInfo, statErr := file.Stat()
	closeErr := file.Close()
	if copyErr != nil {
		return nil, copyErr
	}
	if statErr != nil {
		return nil, statErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if size != record.Size || !equalBytes(hash.Sum(nil), record.SHA256) {
		return nil, errors.New("published upload does not match the session")
	}
	info, err := s.root.Lstat(record.TargetPath)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || !os.SameFile(finalInfo, info) || finalInfo.Size() != record.Size || uint32(finalInfo.Mode().Perm()) != record.Mode {
		return nil, errors.New("published upload is not a regular file")
	}
	return &uploadResultRecord{
		Path: displayPath(record.TargetPath), Name: displayName(record.TargetPath), Size: finalInfo.Size(),
		Mode: uint32(finalInfo.Mode().Perm()), ModifiedAt: finalInfo.ModTime(), SHA256: append([]byte(nil), record.SHA256...),
	}, nil
}

func (s *transferStore) cleanupExpired(now time.Time) {
	s.mu.Lock()
	sessions := make([]*uploadSession, 0, len(s.sessions))
	for _, session := range s.sessions {
		sessions = append(sessions, session)
	}
	s.mu.Unlock()

	for _, session := range sessions {
		session.mu.Lock()
		if session.active {
			session.mu.Unlock()
			continue
		}
		if session.record.State == codev1.UploadSessionState_UPLOAD_SESSION_STATE_FINALIZING {
			release, err := s.reconcileFinalizingUploadLocked(session)
			if err != nil {
				session.mu.Unlock()
				continue
			}
			if release {
				record := session.record
				session.mu.Unlock()
				s.releasePaths(record)
				continue
			}
		}
		if now.Before(session.record.ExpiresAt) {
			session.mu.Unlock()
			continue
		}
		record := session.record
		if isOpenUploadState(record.State) {
			session.record.State = codev1.UploadSessionState_UPLOAD_SESSION_STATE_EXPIRED
			session.record.UpdatedAt = now
			session.record.ExpiresAt = now.Add(s.config.CompletedSessionTTL)
			if err := s.persistRecord(session.record); err != nil {
				session.record = record
				session.mu.Unlock()
				continue
			}
			session.mu.Unlock()
			s.removeSessionTemp(record.TempPath)
			s.releasePaths(record)
			continue
		}
		session.mu.Unlock()
		if !s.removeSessionTemp(record.TempPath) {
			continue
		}
		s.mu.Lock()
		delete(s.sessions, record.UploadID)
		if s.requests[record.RequestID] == record.UploadID {
			delete(s.requests, record.RequestID)
		}
		if s.temps[record.TempPath] == record.UploadID {
			delete(s.temps, record.TempPath)
		}
		s.mu.Unlock()
		_ = os.Remove(s.recordPath(record.UploadID))
	}
}

func (s *transferStore) get(uploadID string) (*uploadSession, bool) {
	if len(uploadID) != 48 {
		return nil, false
	}
	if _, err := hex.DecodeString(uploadID); err != nil {
		return nil, false
	}
	s.mu.Lock()
	session, ok := s.sessions[uploadID]
	s.mu.Unlock()
	return session, ok
}

func (s *transferStore) newUploadID() (string, error) {
	for range 16 {
		random := make([]byte, 24)
		if _, err := rand.Read(random); err != nil {
			return "", err
		}
		id := hex.EncodeToString(random)
		if _, exists := s.sessions[id]; !exists {
			return id, nil
		}
	}
	return "", errors.New("could not allocate a unique upload ID")
}

func (s *transferStore) persistRecord(record uploadRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(s.uploadsDir, ".state-*")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	remove := true
	defer func() {
		_ = temporary.Close()
		if remove {
			_ = os.Remove(temporaryName)
		}
	}()
	if err := temporary.Chmod(transferStateFilePermissions); err != nil {
		return err
	}
	if err := writeFull(temporary, data); err != nil {
		return err
	}
	if err := temporary.Sync(); err != nil {
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryName, s.recordPath(record.UploadID)); err != nil {
		return err
	}
	remove = false
	return syncDirectory(s.uploadsDir)
}

func (s *transferStore) recordPath(uploadID string) string {
	return filepath.Join(s.uploadsDir, uploadID+".json")
}

func (s *transferStore) removeSessionTemp(rel string) bool {
	info, err := s.root.Lstat(rel)
	if errors.Is(err, fs.ErrNotExist) {
		return true
	}
	if err != nil || !info.Mode().IsRegular() {
		return false
	}
	if err := s.root.Remove(rel); err != nil {
		return false
	}
	return syncRootDirectory(s.root, path.Dir(rel)) == nil
}

func (s *transferStore) releasePaths(record uploadRecord) {
	_, tempErr := s.root.Lstat(record.TempPath)
	tempGone := errors.Is(tempErr, fs.ErrNotExist)
	s.mu.Lock()
	released := false
	if s.targets[record.TargetPath] == record.UploadID {
		delete(s.targets, record.TargetPath)
		released = true
	}
	if tempGone && s.temps[record.TempPath] == record.UploadID {
		delete(s.temps, record.TempPath)
	}
	if released {
		s.activeUploads--
		s.reservedBytes -= record.Size
	}
	s.mu.Unlock()
}

func (s *transferStore) isTemp(rel string) bool {
	s.mu.Lock()
	_, ok := s.temps[rel]
	s.mu.Unlock()
	return ok
}

func (s *transferStore) mutationConflicts(rel string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mutationConflictsLocked(rel)
}

func (s *transferStore) mutationConflictsLocked(rel string) bool {
	for target := range s.targets {
		if pathOverlaps(rel, target) {
			return true
		}
	}
	for temporary := range s.temps {
		if pathOverlaps(rel, temporary) {
			return true
		}
	}
	return false
}

func pathOverlaps(left, right string) bool {
	return left == right || strings.HasPrefix(left, right+"/") || strings.HasPrefix(right, left+"/")
}

func isOpenUploadState(state codev1.UploadSessionState) bool {
	return state == codev1.UploadSessionState_UPLOAD_SESSION_STATE_OPEN || state == codev1.UploadSessionState_UPLOAD_SESSION_STATE_FINALIZING
}

func uploadSessionProto(record uploadRecord) *codev1.UploadSession {
	result := &codev1.UploadSession{
		UploadId: record.UploadID, State: record.State, CommittedOffset: record.CommittedOffset,
		Size: record.Size, ExpiresAt: timestamppb.New(record.ExpiresAt),
	}
	if record.Result != nil {
		file := &codev1.FileInfo{
			Path: record.Result.Path, Name: record.Result.Name, Type: codev1.FileType_FILE_TYPE_REGULAR,
			Size: record.Result.Size, Mode: record.Result.Mode, ModifiedAt: timestamppb.New(record.Result.ModifiedAt),
		}
		result.Result = &codev1.UploadResponse{File: file, Size: record.Result.Size, Sha256: append([]byte(nil), record.Result.SHA256...)}
	}
	return result
}

func loadOrCreateRevisionKey(stateRoot string) ([]byte, error) {
	name := filepath.Join(stateRoot, "revision.key")
	data, err := readTransferStateFile(name)
	if err == nil {
		if len(data) != sha256.Size {
			return nil, errors.New("file transfer revision key has an invalid size")
		}
		return data, nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	data = make([]byte, sha256.Size)
	if _, err := rand.Read(data); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL|unix.O_NOFOLLOW, transferStateFilePermissions)
	if errors.Is(err, fs.ErrExist) {
		return loadOrCreateRevisionKey(stateRoot)
	}
	if err != nil {
		return nil, err
	}
	if err := writeFull(file, data); err != nil {
		_ = file.Close()
		_ = os.Remove(name)
		return nil, err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(name)
		return nil, err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(name)
		return nil, err
	}
	if err := syncDirectory(stateRoot); err != nil {
		return nil, err
	}
	return data, nil
}

func readTransferStateFile(name string) ([]byte, error) {
	file, err := os.OpenFile(name, os.O_RDONLY|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() < 0 || info.Size() > maxTransferStateBytes {
		return nil, errors.New("file transfer state is not a bounded regular file")
	}
	return io.ReadAll(io.LimitReader(file, maxTransferStateBytes+1))
}

func syncDirectory(name string) error {
	directory, err := os.Open(name)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
