package agent

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"google.golang.org/protobuf/proto"
)

// The on-disk layout of one query record directory:
//
//	<root>/<query-id>/events-<base-sequence>.log
//	<root>/<query-id>/state.json
//
// Segment files are append-only sequences of framed records:
//
//	uint32 payload length | uint32 crc32c(payload) | payload
//
// The payload is a marshalled QueryResponse frame that already carries its
// query_id and sequence. A segment's name pins the sequence of its first
// record, so replay resolves a sequence to a segment by name and records are
// numbered positionally from that base — mirroring the process log's offset
// semantics. Head retention drops whole segments and advances the query's
// earliest retained sequence; nothing is rewritten in place.
const (
	queryRecordHeaderSize = 8
	querySegmentPrefix    = "events-"
	queryStateFileName    = "state.json"
	queryEventFormat      = 1

	// maxQueryRecordBytes bounds one payload allocation when scanning a
	// possibly corrupt file: a torn length field must not OOM the controller.
	maxQueryRecordBytes = 8 << 20
)

var queryRecordCRCTable = crc32.MakeTable(crc32.Castagnoli)

// querySegmentName renders the segment file name for a base sequence.
func querySegmentName(base uint64) string {
	return fmt.Sprintf("%s%020d.log", querySegmentPrefix, base)
}

// parseQuerySegmentName extracts the base sequence; ok is false for foreign
// files in the record directory.
func parseQuerySegmentName(name string) (base uint64, ok bool) {
	if !strings.HasPrefix(name, querySegmentPrefix) || !strings.HasSuffix(name, ".log") {
		return 0, false
	}
	digits := strings.TrimSuffix(strings.TrimPrefix(name, querySegmentPrefix), ".log")
	if len(digits) != 20 {
		return 0, false
	}
	value, err := strconv.ParseUint(digits, 10, 64)
	if err != nil {
		return 0, false
	}
	return value, true
}

// writeQueryRecord appends one framed record to the buffered writer and
// returns the bytes written.
func writeQueryRecord(buffered *bufio.Writer, payload []byte) (int64, error) {
	header := make([]byte, queryRecordHeaderSize)
	binary.BigEndian.PutUint32(header[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(header[4:8], crc32.Checksum(payload, queryRecordCRCTable))
	if _, err := buffered.Write(header); err != nil {
		return 0, fmt.Errorf("write record header: %w", err)
	}
	if _, err := buffered.Write(payload); err != nil {
		return 0, fmt.Errorf("write record payload: %w", err)
	}
	return int64(queryRecordHeaderSize + len(payload)), nil
}

// readQueryRecord reads the next framed record. A torn or corrupt tail is
// reported as errQueryRecordTorn so scanners stop without failing the whole
// query; io.EOF marks a clean segment end.
var errQueryRecordTorn = errors.New("query record torn or corrupt")

func readQueryRecord(reader io.Reader) (payload []byte, err error) {
	header := make([]byte, queryRecordHeaderSize)
	if _, err := io.ReadFull(reader, header); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.EOF
		}
		return nil, errQueryRecordTorn
	}
	length := binary.BigEndian.Uint32(header[0:4])
	checksum := binary.BigEndian.Uint32(header[4:8])
	if length == 0 || length > maxQueryRecordBytes {
		return nil, errQueryRecordTorn
	}
	payload = make([]byte, length)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return nil, errQueryRecordTorn
	}
	if crc32.Checksum(payload, queryRecordCRCTable) != checksum {
		return nil, errQueryRecordTorn
	}
	return payload, nil
}

// querySegmentInfo describes one on-disk segment of a query record.
type querySegmentInfo struct {
	base  uint64
	name  string
	bytes int64
}

// listQuerySegments returns the query's segments ordered by base sequence.
func listQuerySegments(directory string) ([]querySegmentInfo, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}
	segments := make([]querySegmentInfo, 0, len(entries))
	for _, entry := range entries {
		base, ok := parseQuerySegmentName(entry.Name())
		if !ok || entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		segments = append(segments, querySegmentInfo{base: base, name: entry.Name(), bytes: info.Size()})
	}
	sort.Slice(segments, func(i, j int) bool { return segments[i].base < segments[j].base })
	return segments, nil
}

// replayQuerySegments streams decoded frames of records [from, to) in order.
// Scanning stops at the first torn record — a crash-truncated tail — which is
// the honest end of the retained stream, not a read failure.
func replayQuerySegments(directory string, from, to uint64, send func(*codev1.QueryResponse) error) error {
	segments, err := listQuerySegments(directory)
	if err != nil {
		return err
	}
	sequence := from
	for _, segment := range segments {
		if sequence >= to {
			return nil
		}
		if segment.base > sequence {
			// A gap between segments means retention dropped the segment
			// holding the requested window.
			return fmt.Errorf("segment covering sequence %d was pruned: %w", sequence, ErrQueryPruned)
		}
		file, err := os.Open(filepath.Join(directory, segment.name))
		if err != nil {
			if os.IsNotExist(err) {
				return fmt.Errorf("segment covering sequence %d was pruned: %w", sequence, ErrQueryPruned)
			}
			return err
		}
		reader := bufio.NewReader(file)
		ordinal := sequence - segment.base
		skipped := uint64(0)
		torn := false
		for {
			payload, err := readQueryRecord(reader)
			if err != nil {
				if errors.Is(err, errQueryRecordTorn) {
					torn = true
					break
				}
				if errors.Is(err, io.EOF) {
					break
				}
				_ = file.Close()
				return fmt.Errorf("read segment %s: %w", segment.name, err)
			}
			if skipped < ordinal {
				skipped++
				continue
			}
			if sequence >= to {
				break
			}
			frame := &codev1.QueryResponse{}
			if err := proto.Unmarshal(payload, frame); err != nil {
				torn = true
				break
			}
			if frame.GetSequence() != sequence {
				_ = file.Close()
				return fmt.Errorf("segment %s holds sequence %d at position %d", segment.name, frame.GetSequence(), sequence)
			}
			if err := send(frame); err != nil {
				_ = file.Close()
				return err
			}
			sequence++
		}
		_ = file.Close()
		if torn {
			return nil
		}
	}
	return nil
}

// scanQueryRecordBounds walks the segments and reports the retained window
// [earliest, next) of valid records plus their total bytes. A torn record ends
// the walk: everything after it is unrecoverable.
func scanQueryRecordBounds(directory string) (earliest, next uint64, bytes int64, err error) {
	segments, err := listQuerySegments(directory)
	if err != nil {
		return 0, 0, 0, err
	}
	if len(segments) == 0 {
		return 0, 0, 0, nil
	}
	earliest = segments[0].base
	next = segments[0].base
	for _, segment := range segments {
		file, err := os.Open(filepath.Join(directory, segment.name))
		if err != nil {
			return earliest, next, bytes, nil
		}
		reader := bufio.NewReader(file)
		valid := int64(0)
		for {
			payload, err := readQueryRecord(reader)
			if err != nil {
				break
			}
			frame := codev1.QueryResponse{}
			if proto.Unmarshal(payload, &frame) != nil || frame.GetSequence() != next {
				break
			}
			valid += int64(queryRecordHeaderSize + len(payload))
			next++
		}
		_ = file.Close()
		bytes += valid
		if valid < segment.bytes {
			// Torn tail inside this segment: later segments are unreachable.
			break
		}
	}
	return earliest, next, bytes, nil
}

// truncateQueryTornTails removes crash-truncated trailing bytes from every
// segment so a recovered record holds only complete records.
func truncateQueryTornTails(directory string) error {
	segments, err := listQuerySegments(directory)
	if err != nil {
		return err
	}
	for _, segment := range segments {
		file, err := os.OpenFile(filepath.Join(directory, segment.name), os.O_RDWR, 0)
		if err != nil {
			continue
		}
		reader := bufio.NewReader(file)
		valid := int64(0)
		for {
			payload, err := readQueryRecord(reader)
			if err != nil {
				break
			}
			valid += int64(queryRecordHeaderSize + len(payload))
		}
		if info, err := file.Stat(); err == nil && info.Size() > valid {
			if err := file.Truncate(valid); err != nil {
				_ = file.Close()
				return err
			}
		}
		_ = file.Close()
	}
	return nil
}
