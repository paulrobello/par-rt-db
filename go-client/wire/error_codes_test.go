// ARC-017: error-codes corpus — the canonical {code, httpStatus} table
// generated from the server's ErrorCode enum. The Go errors package must
// match the fixture set-for-set, and each code's HTTPStatus must agree.
package wire_test

import (
	"encoding/json"
	"sort"
	"testing"

	"github.com/paulrobello/par-rt-db/go-client/errors"
)

func TestErrorCodesCorpus(t *testing.T) {
	raw, err := osRead(corpusPath(t, "wire-corpus", "error-codes.json"))
	if err != nil {
		t.Fatalf("read error-codes.json: %v", err)
	}
	var fixture struct {
		Codes []struct {
			Code       string `json:"code"`
			HTTPStatus int    `json:"httpStatus"`
		} `json:"codes"`
	}
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatalf("parse error-codes.json: %v", err)
	}
	if len(fixture.Codes) != len(errors.AllCodes()) {
		t.Fatalf("code count: corpus %d, client %d", len(fixture.Codes), len(errors.AllCodes()))
	}
	corpusByCode := map[string]int{}
	for _, e := range fixture.Codes {
		corpusByCode[e.Code] = e.HTTPStatus
	}
	clientCodes := errors.AllCodes()
	for _, c := range clientCodes {
		status, ok := corpusByCode[string(c)]
		if !ok {
			t.Fatalf("client code %s is missing from the corpus", c)
		}
		if got := errors.HTTPStatus(c); got != status {
			t.Fatalf("code %s: httpStatus client %d, corpus %d", c, got, status)
		}
	}
	clientSet := map[string]bool{}
	for _, c := range clientCodes {
		clientSet[string(c)] = true
	}
	var corpusCodes []string
	for code := range corpusByCode {
		corpusCodes = append(corpusCodes, code)
	}
	sort.Strings(corpusCodes)
	for _, code := range corpusCodes {
		if !clientSet[code] {
			t.Fatalf("corpus code %s is missing from the client", code)
		}
	}
}
