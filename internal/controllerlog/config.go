package controllerlog

import (
	"fmt"
	"time"

	processservice "github.com/qq1426155093/remote-code/internal/process"
)

const (
	defaultMaxBytesPerController = int64(32 << 20)
	defaultMaxTotalBytes         = int64(128 << 20)
	defaultSegmentBytes          = int64(4 << 20)
	defaultRetention             = 7 * 24 * time.Hour
	defaultMaxObservers          = 8
)

// Config bounds the persistent controller diagnostic log. The controller has
// one log, so MaxTotalBytes is an additional guard for future multi-segment
// implementations and is normally larger than MaxBytes.
type Config struct {
	MaxBytesPerController int64
	MaxTotalBytes         int64
	SegmentBytes          int64
	RetentionAfterRestart time.Duration
	MaxObservers          int
}

// DefaultConfig returns conservative limits for a long-running controller.
func DefaultConfig() Config {
	return Config{
		MaxBytesPerController: defaultMaxBytesPerController,
		MaxTotalBytes:         defaultMaxTotalBytes,
		SegmentBytes:          defaultSegmentBytes,
		RetentionAfterRestart: defaultRetention,
		MaxObservers:          defaultMaxObservers,
	}
}

// Normalize fills zero values and validates the shared segment-store bounds.
func Normalize(config Config) (Config, error) {
	defaults := DefaultConfig()
	if config.MaxBytesPerController == 0 {
		config.MaxBytesPerController = defaults.MaxBytesPerController
	}
	if config.MaxTotalBytes == 0 {
		config.MaxTotalBytes = defaults.MaxTotalBytes
	}
	if config.SegmentBytes == 0 {
		config.SegmentBytes = defaults.SegmentBytes
	}
	if config.RetentionAfterRestart == 0 {
		config.RetentionAfterRestart = defaults.RetentionAfterRestart
	}
	if config.MaxObservers == 0 {
		config.MaxObservers = defaults.MaxObservers
	}
	if config.RetentionAfterRestart < 0 {
		return Config{}, fmt.Errorf("controller log retention must not be negative")
	}
	if err := processservice.ValidateLogConfig(processLogConfig(config)); err != nil {
		return Config{}, fmt.Errorf("invalid controller log configuration: %w", err)
	}
	return config, nil
}

// ValidateConfig checks controller-log bounds without touching the filesystem.
func ValidateConfig(config Config) error {
	_, err := Normalize(config)
	return err
}

func processLogConfig(config Config) processservice.LogConfig {
	return processservice.LogConfig{
		MaxBytesPerProcess: config.MaxBytesPerController,
		MaxTotalBytes:      config.MaxTotalBytes,
		SegmentBytes:       config.SegmentBytes,
		RetentionAfterExit: config.RetentionAfterRestart,
		MaxObservers:       config.MaxObservers,
	}
}
