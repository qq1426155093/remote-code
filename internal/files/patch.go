package files

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	maxPatchBytes       = 1 << 20
	maxPatchTargetBytes = 16 << 20
	maxPatchLines       = 100_000
)

var unifiedHunkPattern = regexp.MustCompile(`^@@ -(0|[1-9][0-9]*)(?:,([0-9]+))? \+(0|[1-9][0-9]*)(?:,([0-9]+))? @@(?: .*)?$`)

type patchOperation struct {
	kind byte
	text string
}

type patchHunk struct {
	oldStart, oldCount int
	newStart, newCount int
	oldUsed, newUsed   int
	operations         []patchOperation
}

// ApplyTextPatch applies one bounded unified diff to the explicit target path.
// Patch headers are informational and never select another filesystem path.
func (s *Service) ApplyTextPatch(ctx context.Context, name, expectedSHA256, patch string) (*codev1.UploadResponse, error) {
	if len(patch) == 0 || len(patch) > maxPatchBytes || !utf8.ValidString(patch) || strings.ContainsRune(patch, '\x00') {
		return nil, status.Errorf(codes.InvalidArgument, "patch must be valid UTF-8 without NUL and between 1 and %d bytes", maxPatchBytes)
	}
	expected, err := hex.DecodeString(expectedSHA256)
	if err != nil || len(expected) != 32 || expectedSHA256 != strings.ToLower(expectedSHA256) {
		return nil, status.Error(codes.InvalidArgument, "expected_sha256 must be a lowercase SHA-256 hexadecimal value")
	}
	readLimit := int64(maxPatchTargetBytes)
	if s.maxUploadBytes < readLimit {
		readLimit = s.maxUploadBytes
	}
	current, err := s.ReadText(ctx, name, readLimit)
	if err != nil {
		return nil, err
	}
	if !equalBytes(current.SHA256, expected) {
		return nil, status.Error(codes.FailedPrecondition, "patch target SHA-256 does not match expected_sha256")
	}
	hunks, err := parseUnifiedPatch(patch)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	updated, err := applyUnifiedHunks(current.Text, hunks)
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	if int64(len(updated)) > s.maxUploadBytes {
		return nil, status.Errorf(codes.ResourceExhausted, "patched file exceeds the %d byte limit", s.maxUploadBytes)
	}

	// Recheck immediately before publication. This provides compare-and-swap
	// semantics for controller-mediated writers and narrows the race with
	// workspace processes, which do not participate in controller locking.
	latest, err := s.ReadText(ctx, name, readLimit)
	if err != nil {
		return nil, err
	}
	if !equalBytes(latest.SHA256, expected) {
		return nil, status.Error(codes.FailedPrecondition, "patch target changed before publication")
	}
	return s.WriteText(ctx, name, updated, true, current.File.GetMode()&0o777)
}

func parseUnifiedPatch(source string) ([]patchHunk, error) {
	lines := splitLinesKeepEnd(source)
	if len(lines) > maxPatchLines {
		return nil, fmt.Errorf("patch contains more than %d lines", maxPatchLines)
	}
	hunks := make([]patchHunk, 0)
	var current *patchHunk
	headerPairs := 0
	changed := false
	for index, line := range lines {
		header := strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
		if strings.HasPrefix(header, "@@ ") {
			if current != nil && (current.oldUsed != current.oldCount || current.newUsed != current.newCount) {
				return nil, fmt.Errorf("hunk %d line counts do not match its header", len(hunks))
			}
			values := unifiedHunkPattern.FindStringSubmatch(header)
			if values == nil {
				return nil, fmt.Errorf("invalid unified diff hunk header at line %d", index+1)
			}
			hunk, err := patchHunkFromHeader(values)
			if err != nil {
				return nil, fmt.Errorf("invalid unified diff hunk header at line %d", index+1)
			}
			hunks = append(hunks, hunk)
			current = &hunks[len(hunks)-1]
			continue
		}
		if current == nil {
			switch {
			case strings.HasPrefix(header, "--- "):
				headerPairs++
				if headerPairs > 1 {
					return nil, errors.New("patch may describe only one file")
				}
			case strings.HasPrefix(header, "+++ "), strings.HasPrefix(header, "diff "), strings.HasPrefix(header, "index "), header == "":
				// Standard single-file diff metadata is accepted but never used
				// to choose the target path.
			default:
				return nil, fmt.Errorf("unexpected patch metadata at line %d", index+1)
			}
			continue
		}
		if header == `\ No newline at end of file` {
			if len(current.operations) == 0 {
				return nil, fmt.Errorf("orphan no-newline marker at line %d", index+1)
			}
			last := &current.operations[len(current.operations)-1]
			last.text = strings.TrimSuffix(last.text, "\n")
			continue
		}
		if current.oldUsed == current.oldCount && current.newUsed == current.newCount &&
			(strings.HasPrefix(header, "--- ") || strings.HasPrefix(header, "+++ ") || strings.HasPrefix(header, "diff ")) {
			return nil, errors.New("patch may describe only one file")
		}
		if len(line) == 0 || (line[0] != ' ' && line[0] != '+' && line[0] != '-') {
			return nil, fmt.Errorf("invalid hunk operation at line %d", index+1)
		}
		current.operations = append(current.operations, patchOperation{kind: line[0], text: line[1:]})
		if line[0] != '+' {
			current.oldUsed++
		}
		if line[0] != '-' {
			current.newUsed++
		}
		if current.oldUsed > current.oldCount || current.newUsed > current.newCount {
			return nil, fmt.Errorf("hunk %d line counts do not match its header", len(hunks))
		}
		if line[0] == '+' || line[0] == '-' {
			changed = true
		}
	}
	if len(hunks) == 0 {
		return nil, errors.New("patch requires at least one unified diff hunk")
	}
	if !changed {
		return nil, errors.New("patch does not change the target")
	}
	for index := range hunks {
		if hunks[index].oldUsed != hunks[index].oldCount || hunks[index].newUsed != hunks[index].newCount {
			return nil, fmt.Errorf("hunk %d line counts do not match its header", index+1)
		}
	}
	return hunks, nil
}

func patchHunkFromHeader(values []string) (patchHunk, error) {
	oldStart, err := strconv.Atoi(values[1])
	if err != nil {
		return patchHunk{}, err
	}
	newStart, err := strconv.Atoi(values[3])
	if err != nil {
		return patchHunk{}, err
	}
	oldCount, newCount := 1, 1
	if values[2] != "" {
		oldCount, err = strconv.Atoi(values[2])
		if err != nil {
			return patchHunk{}, err
		}
	}
	if values[4] != "" {
		newCount, err = strconv.Atoi(values[4])
		if err != nil {
			return patchHunk{}, err
		}
	}
	if (oldStart == 0 && oldCount != 0) || (newStart == 0 && newCount != 0) {
		return patchHunk{}, errors.New("zero hunk start requires a zero count")
	}
	return patchHunk{oldStart: oldStart, oldCount: oldCount, newStart: newStart, newCount: newCount}, nil
}

func applyUnifiedHunks(original string, hunks []patchHunk) (string, error) {
	originalLines := splitLinesKeepEnd(original)
	output := make([]string, 0, len(originalLines))
	cursor := 0
	for hunkIndex, hunk := range hunks {
		oldIndex := hunk.oldStart
		if hunk.oldCount > 0 {
			oldIndex--
		}
		if oldIndex < cursor || oldIndex > len(originalLines) {
			return "", fmt.Errorf("hunk %d starts outside the target", hunkIndex+1)
		}
		output = append(output, originalLines[cursor:oldIndex]...)
		newIndex := hunk.newStart
		if hunk.newCount > 0 {
			newIndex--
		}
		if newIndex != len(output) {
			return "", fmt.Errorf("hunk %d has an inconsistent new-file position", hunkIndex+1)
		}
		cursor = oldIndex
		for _, operation := range hunk.operations {
			switch operation.kind {
			case ' ', '-':
				if cursor >= len(originalLines) || originalLines[cursor] != operation.text {
					return "", fmt.Errorf("hunk %d context does not match the target", hunkIndex+1)
				}
				if operation.kind == ' ' {
					output = append(output, operation.text)
				}
				cursor++
			case '+':
				output = append(output, operation.text)
			}
		}
	}
	output = append(output, originalLines[cursor:]...)
	return strings.Join(output, ""), nil
}

func splitLinesKeepEnd(value string) []string {
	if value == "" {
		return nil
	}
	lines := strings.SplitAfter(value, "\n")
	if lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	return lines
}
