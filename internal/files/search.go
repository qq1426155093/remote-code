package files

import (
	"bufio"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path"
	"regexp"
	"strings"
	"syscall"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	maxSearchResults   = 1_000
	maxSearchBytes     = int64(64 << 20)
	maxSearchEntries   = 100_000
	maxSearchFiles     = 10_000
	maxSearchGlobParts = 256
	maxSearchQuery     = 4_096
	maxSearchLineBytes = 1 << 20
	maxSearchPreview   = 4_096
)

type SearchOptions struct {
	Path          string
	Glob          string
	Query         string
	CaseSensitive bool
	MaxResults    int
	MaxBytes      int64
}

type SearchMatch struct {
	Path          string
	Line          uint64
	Column        uint64
	Text          string
	TextTruncated bool
}

type SearchResult struct {
	Matches         []SearchMatch
	FilesScanned    int
	BytesScanned    int64
	SkippedFiles    int
	ResultTruncated bool
	ScanTruncated   bool
}

var errStopSearch = errors.New("stop workspace search")

// SearchText performs a deterministic, bounded literal search over regular
// UTF-8 files. Symbolic links are never followed by the directory walk.
func (s *Service) SearchText(ctx context.Context, options SearchOptions) (*SearchResult, error) {
	if options.Query == "" || len(options.Query) > maxSearchQuery || !utf8.ValidString(options.Query) || strings.ContainsRune(options.Query, '\x00') {
		return nil, status.Errorf(codes.InvalidArgument, "query must be valid UTF-8 without NUL and between 1 and %d bytes", maxSearchQuery)
	}
	if options.MaxResults <= 0 || options.MaxResults > maxSearchResults {
		return nil, status.Errorf(codes.InvalidArgument, "max_results must be between 1 and %d", maxSearchResults)
	}
	if options.MaxBytes <= 0 || options.MaxBytes > maxSearchBytes {
		return nil, status.Errorf(codes.InvalidArgument, "max_bytes must be between 1 and %d", maxSearchBytes)
	}
	rel, err := cleanPath(options.Path)
	if err != nil {
		return nil, err
	}
	pattern := options.Glob
	if pattern == "" {
		pattern = "**"
	}
	if len(pattern) > maxPathBytes || strings.HasPrefix(pattern, "/") || strings.ContainsRune(pattern, '\x00') {
		return nil, status.Error(codes.InvalidArgument, "glob must be a workspace-relative pattern")
	}
	patternParts := strings.Split(pattern, "/")
	if len(patternParts) > maxSearchGlobParts {
		return nil, status.Errorf(codes.InvalidArgument, "glob may contain at most %d path components", maxSearchGlobParts)
	}
	for _, component := range patternParts {
		if component == "**" {
			continue
		}
		if _, err := path.Match(component, "validation"); err != nil {
			return nil, status.Error(codes.InvalidArgument, "glob is invalid")
		}
	}

	result := &SearchResult{Matches: make([]SearchMatch, 0)}
	var insensitive *regexp.Regexp
	if !options.CaseSensitive {
		insensitive = regexp.MustCompile("(?i:" + regexp.QuoteMeta(options.Query) + ")")
	}
	visitedEntries := 0
	walkErr := fs.WalkDir(s.root.FS(), rel, func(name string, entry fs.DirEntry, walkErr error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if walkErr != nil {
			if name == rel {
				return walkErr
			}
			result.SkippedFiles++
			return nil
		}
		visitedEntries++
		if visitedEntries > maxSearchEntries {
			result.ScanTruncated = true
			return errStopSearch
		}
		if entry.Type()&os.ModeSymlink != 0 {
			if entry.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if entry.IsDir() {
			return nil
		}
		candidate := name
		if rel != "." && strings.HasPrefix(name, rel+"/") {
			candidate = strings.TrimPrefix(name, rel+"/")
		} else if rel == name {
			candidate = path.Base(name)
		}
		matched, err := matchDoublestar(pattern, candidate)
		if err != nil || !matched {
			return nil
		}
		if result.FilesScanned >= maxSearchFiles {
			result.ScanTruncated = true
			return errStopSearch
		}
		remaining := options.MaxBytes - result.BytesScanned
		if remaining <= 0 {
			result.ScanTruncated = true
			return errStopSearch
		}
		stop, err := s.searchFile(ctx, name, options, insensitive, remaining, result)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			result.SkippedFiles++
			return nil
		}
		if stop {
			return errStopSearch
		}
		return nil
	})
	if walkErr != nil && !errors.Is(walkErr, errStopSearch) {
		if ctx.Err() != nil {
			return nil, contextError(ctx.Err())
		}
		return nil, fileError("search workspace", rel, walkErr)
	}
	return result, nil
}

func (s *Service) searchFile(ctx context.Context, name string, options SearchOptions, insensitive *regexp.Regexp, remaining int64, result *SearchResult) (bool, error) {
	file, err := s.root.OpenFile(name, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return false, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return false, err
	}
	if !info.Mode().IsRegular() {
		result.SkippedFiles++
		return false, nil
	}
	result.FilesScanned++
	limited := &io.LimitedReader{R: &contextReader{ctx: ctx, reader: file}, N: remaining}
	scanner := bufio.NewScanner(limited)
	scanner.Buffer(make([]byte, 64<<10), maxSearchLineBytes)
	lineNumber := uint64(0)
	fileMatches := make([]SearchMatch, 0)
	validText := true
	for scanner.Scan() {
		lineNumber++
		line := scanner.Text()
		if !utf8.ValidString(line) || strings.ContainsRune(line, '\x00') {
			validText = false
			break
		}
		index := strings.Index(line, options.Query)
		if insensitive != nil {
			location := insensitive.FindStringIndex(line)
			if location == nil {
				index = -1
			} else {
				index = location[0]
			}
		}
		if index < 0 {
			continue
		}
		preview, truncated := searchPreview(line, index, maxSearchPreview)
		fileMatches = append(fileMatches, SearchMatch{
			Path: displayPath(name), Line: lineNumber,
			Column: uint64(utf8.RuneCountInString(line[:index]) + 1), Text: preview, TextTruncated: truncated,
		})
		if len(result.Matches)+len(fileMatches) >= options.MaxResults {
			result.Matches = append(result.Matches, fileMatches...)
			if len(result.Matches) > options.MaxResults {
				result.Matches = result.Matches[:options.MaxResults]
			}
			result.ResultTruncated = true
			result.BytesScanned += remaining - limited.N
			return true, nil
		}
	}
	result.BytesScanned += remaining - limited.N
	if limited.N == 0 && info.Size() > remaining {
		result.ScanTruncated = true
	}
	if scanErr := scanner.Err(); scanErr != nil {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		return false, scanErr
	}
	if validText {
		result.Matches = append(result.Matches, fileMatches...)
	} else {
		result.SkippedFiles++
	}
	return result.ScanTruncated, nil
}

func matchDoublestar(pattern, candidate string) (bool, error) {
	parts := strings.Split(pattern, "/")
	values := strings.Split(candidate, "/")
	previous := make([]bool, len(values)+1)
	previous[0] = true
	for _, part := range parts {
		current := make([]bool, len(values)+1)
		if part == "**" {
			current[0] = previous[0]
			for index := 1; index <= len(values); index++ {
				current[index] = previous[index] || current[index-1]
			}
		} else {
			for index := 1; index <= len(values); index++ {
				if !previous[index-1] {
					continue
				}
				matched, err := path.Match(part, values[index-1])
				if err != nil {
					return false, err
				}
				current[index] = matched
			}
		}
		previous = current
	}
	return previous[len(values)], nil
}

func searchPreview(line string, matchIndex, maximum int) (string, bool) {
	if len(line) <= maximum {
		return line, false
	}
	start := matchIndex - maximum/4
	if start < 0 {
		start = 0
	}
	for start > 0 && !utf8.RuneStart(line[start]) {
		start--
	}
	end := start + maximum
	if end > len(line) {
		end = len(line)
		start = end - maximum
		for start < matchIndex && !utf8.RuneStart(line[start]) {
			start++
		}
	}
	for end < len(line) && !utf8.RuneStart(line[end]) {
		end--
	}
	return line[start:end], true
}
