package historian

import (
	"time"

	"github.com/fuserobotics/statestream"
	r "gopkg.in/dancannon/gorethink.v2"
)

// Retrieve the first snapshot before timestamp. Return nil for no data.
func (s *Stream) GetSnapshotBefore(timestamp time.Time) (*stream.StreamEntry, error) {
	entry := &stream.StreamEntry{}
	query := s.dataTable.Filter(r.Row.Field("type").Eq(int(stream.StreamEntrySnapshot))).Filter(r.Row.Field("timestamp").Lt(timestamp))
	cursor, err := query.Run(s.h.rctx)
	defer cursor.Close()
	if err != nil {
		return nil, err
	}
	if err := cursor.One(entry); err != nil {
		if err.Error() == r.ErrEmptyResult.Error() {
			return nil, nil
		}
		return nil, err
	}
	return entry, nil
}

// Retrieve the first entry after timestamp. Return nil for no data.
func (s *Stream) GetEntryAfter(timestamp time.Time, filterType stream.StreamEntryType) (*stream.StreamEntry, error) {
	entry := &stream.StreamEntry{}
	query := s.dataTable
	if filterType != stream.StreamEntryAny {
		query = query.Filter(r.Row.Field("type").Eq(int(filterType)))
	}
	query = query.Filter(r.Row.Field("timestamp").Gt(timestamp))
	cursor, err := query.Run(s.h.rctx)
	defer cursor.Close()
	if err != nil {
		return nil, err
	}
	if err := cursor.One(entry); err != nil {
		if err.Error() == r.ErrEmptyResult.Error() {
			return nil, nil
		}
		return nil, err
	}
	return entry, nil
}

// Store a stream entry.
func (s *Stream) SaveEntry(entry *stream.StreamEntry) error {
	if _, err := s.dataTable.Insert(entry).RunWrite(s.h.rctx); err != nil {
		return err
	}
	return nil
}

// Amend an old entry
func (s *Stream) AmendEntry(entry *stream.StreamEntry, oldTimestamp time.Time) error {
	_, err := s.dataTable.Get(oldTimestamp).Replace(entry).RunWrite(s.h.rctx)
	return err
}
