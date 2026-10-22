// go-client/wire/hostcapability_test.go
package wire

// Mirrors core/src/host_capability.rs's test matrix: exact tag serialization,
// strict round-trip, and fail-closed rejection of unknown types, unknown
// fields, and malformed hosts.

import (
	"encoding/json"
	"testing"
)

func TestHostCapabilityMarshalTag(t *testing.T) {
	b, err := json.Marshal(HostCapability{Type: HostCapabilityTypeParMeshV1, Host: "macdaddy"})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(b), `{"type":"parMeshV1","host":"macdaddy"}`; got != want {
		t.Fatalf("got %s want %s", got, want)
	}
}

func TestHostCapabilityRoundTrip(t *testing.T) {
	cap := HostCapability{Type: HostCapabilityTypeParMeshV1, Host: "macdaddy"}
	b, err := json.Marshal(cap)
	if err != nil {
		t.Fatal(err)
	}
	var back HostCapability
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if back != cap {
		t.Fatalf("round-trip: got %+v want %+v", back, cap)
	}
}

func TestHostCapabilityRejectsUnknownType(t *testing.T) {
	var cap HostCapability
	err := json.Unmarshal([]byte(`{"type":"parMeshV2","host":"macdaddy"}`), &cap)
	if err == nil {
		t.Fatal("unknown capability type must be rejected")
	}
}

func TestHostCapabilityRejectsUnknownField(t *testing.T) {
	var cap HostCapability
	err := json.Unmarshal([]byte(`{"type":"parMeshV1","host":"macdaddy","admin":true}`), &cap)
	if err == nil {
		t.Fatal("unknown property must be rejected")
	}
}

func TestHostCapabilityRejectsMissingTag(t *testing.T) {
	var cap HostCapability
	if err := json.Unmarshal([]byte(`{"host":"macdaddy"}`), &cap); err == nil {
		t.Fatal("missing type tag must be rejected")
	}
}

func TestHostCapabilityRejectsMissingHost(t *testing.T) {
	var cap HostCapability
	if err := json.Unmarshal([]byte(`{"type":"parMeshV1"}`), &cap); err == nil {
		t.Fatal("missing host must be rejected")
	}
}

func TestHostCapabilityRejectsNonStringHost(t *testing.T) {
	var cap HostCapability
	if err := json.Unmarshal([]byte(`{"type":"parMeshV1","host":7}`), &cap); err == nil {
		t.Fatal("non-string host must be rejected")
	}
}

func TestHostCapabilityRejectsMalformedHost(t *testing.T) {
	for _, bad := range []string{
		"Macdaddy",            // uppercase
		"-leading",            // must start with a letter/digit
		"",                    // empty
		"a b",                 // space
		"a" + repeat('a', 32), // 33 chars, over the 32-char cap
		"hôte",                // non-ASCII
	} {
		var cap HostCapability
		err := json.Unmarshal([]byte(`{"type":"parMeshV1","host":"`+bad+`"}`), &cap)
		if err == nil {
			t.Errorf("host %q must be rejected", bad)
		}
		if err := ValidateHost(bad); err == nil {
			t.Errorf("ValidateHost(%q) must fail", bad)
		}
	}
}

func TestHostGrammarBoundariesAccepted(t *testing.T) {
	for _, good := range []string{"a", "0", "macdaddy", "a-b_c9", repeat('a', 32)} {
		if err := ValidateHost(good); err != nil {
			t.Errorf("host %q must pass: %v", good, err)
		}
		var cap HostCapability
		if err := json.Unmarshal([]byte(`{"type":"parMeshV1","host":"`+good+`"}`), &cap); err != nil {
			t.Errorf("capability with host %q must decode: %v", good, err)
		}
	}
}

func repeat(c byte, n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = c
	}
	return string(b)
}
