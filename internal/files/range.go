package files

import (
	"bufio"
	"context"
	"errors"
	"io"
	"os"
	"syscall"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	maxTextRangeLines = 10_000
	maxTextLineBytes  = 1 << 20
)

// TextRange is a bounded, line-oriented view of a regular UTF-8 file.
type TextRange struct {
	Path      string
	Content   string
	Size      int64
	StartLine uint64
	NextLine  uint64
	LineCount uint64
	EOF       bool
	Truncated bool
}

// ReadTextRange reads complete logical lines without loading the whole file.
// startLine is one-based. NextLine is zero at EOF.
func (s *Service) ReadTextRange(ctx context.Context, name string, startLine uint64, maxLines int, maxBytes int64) (*TextRange, error) {
	if startLine == 0 {
		return nil, status.Error(codes.InvalidArgument, "start_line must be at least 1")
	}
	if maxLines <= 0 || maxLines > maxTextRangeLines {
		return nil, status.Errorf(codes.InvalidArgument, "max_lines must be between 1 and %d", maxTextRangeLines)
	}
	if maxBytes <= 0 || maxBytes > 16<<20 {
		return nil, status.Error(codes.InvalidArgument, "max_bytes must be between 1 and 16777216")
	}
	rel, err := cleanPath(name)
	if err != nil {
		return nil, err
	}
	file, err := s.root.OpenFile(rel, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, fileError("read text range", rel, err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, fileError("stat text range", rel, err)
	}
	if !info.Mode().IsRegular() {
		return nil, status.Errorf(codes.FailedPrecondition, "read target %q is not a regular file", displayPath(rel))
	}

	reader := bufio.NewReaderSize(&contextReader{ctx: ctx, reader: file}, 64<<10)
	result := &TextRange{Path: displayPath(rel), Size: info.Size(), StartLine: startLine}
	var content []byte
	lineNumber := uint64(1)
	for {
		line, readErr := readBoundedLine(reader, maxTextLineBytes)
		if len(line) > 0 {
			if !utf8.Valid(line) {
				return nil, status.Error(codes.FailedPrecondition, "read target is not valid UTF-8")
			}
			if lineNumber >= startLine {
				if int64(len(content)+len(line)) > maxBytes {
					if len(content) == 0 {
						return nil, status.Error(codes.ResourceExhausted, "the next line exceeds max_bytes")
					}
					result.Truncated = true
					result.NextLine = lineNumber
					break
				}
				content = append(content, line...)
				result.LineCount++
				if result.LineCount == uint64(maxLines) {
					if readErr == io.EOF {
						result.EOF = true
					} else if _, peekErr := reader.Peek(1); errors.Is(peekErr, io.EOF) {
						result.EOF = true
					} else if peekErr != nil {
						return nil, contextError(peekErr)
					} else {
						result.Truncated = true
						result.NextLine = lineNumber + 1
					}
					break
				}
			}
			lineNumber++
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				result.EOF = true
				result.NextLine = 0
				break
			}
			return nil, contextError(readErr)
		}
	}
	result.Content = string(content)
	return result, nil
}

func readBoundedLine(reader *bufio.Reader, maximum int) ([]byte, error) {
	line := make([]byte, 0, 256)
	for {
		fragment, err := reader.ReadSlice('\n')
		if len(line)+len(fragment) > maximum {
			return nil, status.Errorf(codes.ResourceExhausted, "text line exceeds %d bytes", maximum)
		}
		line = append(line, fragment...)
		switch {
		case err == nil:
			return line, nil
		case errors.Is(err, bufio.ErrBufferFull):
			continue
		case errors.Is(err, io.EOF) && len(line) > 0:
			return line, io.EOF
		default:
			return line, err
		}
	}
}
