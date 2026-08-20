package workflow

import (
	"bytes"
	"encoding/json"
	"time"
)

const runRecordVersion = 1

type runRecord struct {
	Version    int                      `json:"version"`
	Run        Run                      `json:"run"`
	Definition Definition               `json:"definition"`
	Digest     string                   `json:"definition_digest"`
	Leases     map[string]leaseRecord   `json:"leases,omitempty"`
	Commands   map[string]commandRecord `json:"commands,omitempty"`
}

type leaseRecord struct {
	Attempt   int       `json:"attempt"`
	TokenHash string    `json:"token_hash"`
	Deadline  time.Time `json:"deadline"`
}

type commandRecord struct {
	ActivityID  string `json:"activity_id"`
	Action      string `json:"action"`
	PayloadHash string `json:"payload_hash"`
}

func cloneRunRecord(record *runRecord) (*runRecord, error) {
	encoded, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	var clone runRecord
	if err := decodeRecordJSON(encoded, &clone); err != nil {
		return nil, err
	}
	initializeRecordMaps(&clone)
	return &clone, nil
}

func cloneRun(run Run) (*Run, error) {
	encoded, err := json.Marshal(run)
	if err != nil {
		return nil, err
	}
	var clone Run
	if err := decodeRecordJSON(encoded, &clone); err != nil {
		return nil, err
	}
	return &clone, nil
}

func decodeRecordJSON(encoded []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	return decoder.Decode(target)
}
