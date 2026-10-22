// go-client/wire/hostcapability.go
package wire

// Mirrors core/src/host_capability.rs — the versioned, host-scoped
// capability vocabulary carried by machine tokens and enrollment grants.
// One variant today (parMeshV1); unknown capability types, unknown fields,
// and malformed hosts fail closed, matching the server's strict decoder.
// Clients validate or pass through but must NOT normalize.

import (
	"fmt"
)

// HostCapabilityTypeParMeshV1 is the par-mesh host capability's type tag.
const HostCapabilityTypeParMeshV1 = "parMeshV1"

// HostCapability is the wire shape `{"type":"parMeshV1","host":"macdaddy"}`.
// Anything decoded from the wire carries Type == HostCapabilityTypeParMeshV1
// — the strict UnmarshalJSON rejects every other tag.
type HostCapability struct {
	Type string `json:"type"`
	Host string `json:"host"`
}

// UnmarshalJSON rejects unknown fields, unknown capability types, and
// malformed hosts.
func (h *HostCapability) UnmarshalJSON(b []byte) error {
	v, err := StrictUnmarshal[struct {
		Type string `json:"type"`
		Host string `json:"host"`
	}](b)
	if err != nil {
		return err
	}
	if v.Type != HostCapabilityTypeParMeshV1 {
		return fmt.Errorf("wire: unknown capability type %q", v.Type)
	}
	if err := ValidateHost(v.Host); err != nil {
		return err
	}
	*h = HostCapability{Type: v.Type, Host: v.Host}
	return nil
}

// ValidateHost checks host against par-mesh's component grammar
// `^[a-z0-9][a-z0-9-_]{0,31}$`: a lowercase ASCII letter or digit first,
// then lowercase letters/digits/hyphens/underscores, at most 32 characters
// in total.
func ValidateHost(host string) error {
	if host == "" || len(host) > 32 {
		return fmt.Errorf("wire: invalid host %q: expected ^[a-z0-9][a-z0-9-_]{0,31}$", host)
	}
	for i, c := range host {
		first := i == 0
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		case (c == '-' || c == '_') && !first:
		default:
			return fmt.Errorf("wire: invalid host %q: expected ^[a-z0-9][a-z0-9-_]{0,31}$", host)
		}
	}
	return nil
}

// EnrollResult is POST /auth/enroll's body: the minted machine token plus
// the grant's host capability (mirrors server/src/auth/enrollment.rs's
// EnrollResponse).
type EnrollResult struct {
	TokenID    string         `json:"tokenId"`
	Token      string         `json:"token"`
	DB         string         `json:"db"`
	Capability HostCapability `json:"capability"`
}

// UnmarshalJSON rejects unknown fields.
func (e *EnrollResult) UnmarshalJSON(b []byte) error {
	type alias struct {
		TokenID    string         `json:"tokenId"`
		Token      string         `json:"token"`
		DB         string         `json:"db"`
		Capability HostCapability `json:"capability"`
	}
	v, err := StrictUnmarshal[alias](b)
	if err != nil {
		return err
	}
	*e = EnrollResult(v)
	return nil
}
