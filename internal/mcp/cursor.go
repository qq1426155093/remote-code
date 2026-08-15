package mcpserver

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
)

type cursorCodec struct {
	registryDigest [sha256.Size]byte
	key            [sha256.Size]byte
}

type cursorPayload struct {
	Version  int    `json:"v"`
	Registry string `json:"registry"`
	Offset   int    `json:"offset"`
}

func newCursorCodec(digest [sha256.Size]byte) (cursorCodec, error) {
	codec := cursorCodec{registryDigest: digest}
	if _, err := rand.Read(codec.key[:]); err != nil {
		return cursorCodec{}, fmt.Errorf("generate MCP cursor key: %w", err)
	}
	return codec, nil
}

func (c cursorCodec) encode(offset int) (string, error) {
	payload, err := json.Marshal(cursorPayload{Version: 1, Registry: base64.RawURLEncoding.EncodeToString(c.registryDigest[:]), Offset: offset})
	if err != nil {
		return "", err
	}
	encoded := base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, c.key[:])
	mac.Write([]byte(encoded))
	return encoded + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

func (c cursorCodec) decode(value string, maximum int) (int, error) {
	separator := bytes.LastIndexByte([]byte(value), '.')
	if separator <= 0 || separator == len(value)-1 {
		return 0, errors.New("invalid cursor")
	}
	payloadText, tagText := value[:separator], value[separator+1:]
	tag, err := base64.RawURLEncoding.DecodeString(tagText)
	if err != nil || base64.RawURLEncoding.EncodeToString(tag) != tagText {
		return 0, errors.New("invalid cursor")
	}
	mac := hmac.New(sha256.New, c.key[:])
	mac.Write([]byte(payloadText))
	if !hmac.Equal(tag, mac.Sum(nil)) {
		return 0, errors.New("invalid cursor")
	}
	payloadBytes, err := base64.RawURLEncoding.DecodeString(payloadText)
	if err != nil || base64.RawURLEncoding.EncodeToString(payloadBytes) != payloadText {
		return 0, errors.New("invalid cursor")
	}
	decoder := json.NewDecoder(bytes.NewReader(payloadBytes))
	decoder.DisallowUnknownFields()
	var payload cursorPayload
	if err := decoder.Decode(&payload); err != nil || ensureJSONEOF(decoder) != nil {
		return 0, errors.New("invalid cursor")
	}
	if payload.Version != 1 || payload.Registry != base64.RawURLEncoding.EncodeToString(c.registryDigest[:]) || payload.Offset < 0 || payload.Offset > maximum {
		return 0, errors.New("invalid cursor")
	}
	return payload.Offset, nil
}
