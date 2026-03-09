package substrate

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// fileBackend persists events as NDJSON files under a directory tree.
// Each stream is stored in its own file: <persistPath>/events/<stream_id>.ndjson
// Files are rotated to <stream_id>.ndjson.1, .2, … when they exceed
// maxFileSizeMB. flush is called while the EventStore read-lock is held so
// flushCursors can be accessed without a separate lock.
type fileBackend struct {
	persistPath   string
	maxFileSizeMB int
	serializer    Serializer
	flushCursors  map[string]int64 // stream_id → count already flushed
}

// newFileBackend creates a fileBackend that stores events under path.
func newFileBackend(path string, maxMB int, s Serializer) *fileBackend {
	return &fileBackend{
		persistPath:   path,
		maxFileSizeMB: maxMB,
		serializer:    s,
		flushCursors:  make(map[string]int64),
	}
}

// flush appends any events not yet persisted to their respective NDJSON files.
// streams is the EventStore's stream map; it is accessed under the store's
// read-lock so this method needs no additional locking.
func (f *fileBackend) flush(streams map[string][]*Event) error {
	eventsDir := filepath.Join(f.persistPath, "events")
	if err := os.MkdirAll(eventsDir, 0o750); err != nil {
		return fmt.Errorf("create events dir: %w", err)
	}

	for streamID, events := range streams {
		cursor := f.flushCursors[streamID]
		newEvents := events[cursor:]
		if len(newEvents) == 0 {
			continue
		}

		safe := sanitizeStreamID(streamID)
		baseName := filepath.Join(eventsDir, safe+".ndjson")

		for _, event := range newEvents {
			data, err := f.serializer.Serialize(event)
			if err != nil {
				return fmt.Errorf("serialize event %s: %w", event.ID, err)
			}

			// Rotate if maxFileSizeMB is set and current file is too large.
			if f.maxFileSizeMB > 0 {
				if info, statErr := os.Stat(baseName); statErr == nil {
					if info.Size() >= int64(f.maxFileSizeMB)*1024*1024 {
						if rotErr := f.rotate(eventsDir, safe); rotErr != nil {
							return fmt.Errorf("rotate %s: %w", safe, rotErr)
						}
					}
				}
			}

			file, err := os.OpenFile(baseName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o640)
			if err != nil {
				return fmt.Errorf("open %s: %w", baseName, err)
			}
			_, writeErr := fmt.Fprintf(file, "%s\n", data)
			closeErr := file.Close()
			if writeErr != nil {
				return fmt.Errorf("write event %s: %w", event.ID, writeErr)
			}
			if closeErr != nil {
				return fmt.Errorf("close %s: %w", baseName, closeErr)
			}
		}

		f.flushCursors[streamID] = int64(len(events))
	}

	return nil
}

// load reads all *.ndjson files (including rotation suffixes) from the events
// directory and returns the events in file-order.
func (f *fileBackend) load() ([]*Event, error) {
	eventsDir := filepath.Join(f.persistPath, "events")

	entries, err := os.ReadDir(eventsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read events dir: %w", err)
	}

	// Collect all .ndjson files and sort them so rotation files come first
	// (e.g. foo.ndjson.2, foo.ndjson.1, foo.ndjson).
	var files []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.Contains(entry.Name(), ".ndjson") {
			files = append(files, filepath.Join(eventsDir, entry.Name()))
		}
	}
	sort.Strings(files)

	var events []*Event
	for _, path := range files {
		evs, err := f.readNDJSON(path)
		if err != nil {
			return nil, err
		}
		events = append(events, evs...)
	}

	// Update flush cursors so a subsequent flush only appends new events.
	for _, ev := range events {
		if ev.StreamID != "" {
			f.flushCursors[ev.StreamID]++
		}
	}

	return events, nil
}

// readNDJSON reads all JSON lines from a single file.
func (f *fileBackend) readNDJSON(path string) ([]*Event, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}

	var events []*Event
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		ev, deserErr := f.serializer.Deserialize(line)
		if deserErr != nil {
			_ = file.Close()
			return nil, fmt.Errorf("deserialize line in %s: %w", path, deserErr)
		}
		events = append(events, ev)
	}
	scanErr := scanner.Err()
	if closeErr := file.Close(); closeErr != nil && scanErr == nil {
		scanErr = fmt.Errorf("close %s: %w", path, closeErr)
	}
	if scanErr != nil {
		return nil, fmt.Errorf("scan %s: %w", path, scanErr)
	}
	return events, nil
}

// rotate renames <safe>.ndjson.N to .N+1 and .ndjson to .ndjson.1.
func (f *fileBackend) rotate(eventsDir, safe string) error {
	base := filepath.Join(eventsDir, safe+".ndjson")

	// Find the highest existing rotation number.
	maxN := 0
	entries, err := os.ReadDir(eventsDir)
	if err != nil {
		return fmt.Errorf("read dir for rotate: %w", err)
	}
	prefix := safe + ".ndjson."
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, prefix) {
			var n int
			if _, scanErr := fmt.Sscanf(name[len(prefix):], "%d", &n); scanErr == nil && n > maxN {
				maxN = n
			}
		}
	}

	// Rename existing rotation files in descending order.
	for i := maxN; i >= 1; i-- {
		oldPath := fmt.Sprintf("%s.%d", base, i)
		newPath := fmt.Sprintf("%s.%d", base, i+1)
		if err := os.Rename(oldPath, newPath); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("rename %s→%s: %w", oldPath, newPath, err)
		}
	}

	// Rename the active file to .ndjson.1.
	if err := os.Rename(base, base+".1"); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("rename %s→%s.1: %w", base, base, err)
	}
	return nil
}

// sanitizeStreamID replaces path-unsafe characters with underscores.
func sanitizeStreamID(id string) string {
	return strings.NewReplacer("/", "_", "\\", "_", ":", "_").Replace(id)
}
