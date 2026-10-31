package admin

import (
	"context"
	"encoding/json"
	"testing"

	rtdberrors "github.com/paulrobello/par-rt-db/go-client/errors"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

func ptrInt(i int) *int { return &i }

func TestPurgePreview(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts *PurgeOptions
		body string
	}{
		{"defaults", nil, `{"table":"events"}`},
		{"filter_and_limit", &PurgeOptions{Filter: wire.FilterEq{Field: "kind", Value: wire.String("wake")}, Limit: ptrInt(25)}, `{"table":"events","filter":{"op":"eq","field":"kind","value":"wake"},"limit":25}`},
		{"explicit_zero_limit", &PurgeOptions{Limit: ptrInt(0)}, `{"table":"events","limit":0}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &capture{t: t, method: "POST", path: "/admin/db/app/purge", bearer: wantStr(testAdminKey), body: &tc.body,
				status: 200, respBody: `{"matching":9007199254740993,"confirmToken":"preview-token","limit":25}`}
			out, err := stub(t, c.handler).PurgePreview(context.Background(), "app", "events", tc.opts)
			if err != nil {
				t.Fatal(err)
			}
			if out.Matching != 9007199254740993 || out.ConfirmToken != "preview-token" || out.Limit != 25 {
				t.Fatalf("unexpected preview: %+v", out)
			}
		})
	}
}

func TestPurge(t *testing.T) {
	for _, tc := range []struct {
		name      string
		opts      *PurgeOptions
		confirm   string
		body      string
		response  string
		deleted   uint64
		truncated bool
	}{
		{"batch", &PurgeOptions{Filter: wire.FilterEq{Field: "kind", Value: wire.String("wake")}, Limit: ptrInt(25)}, "preview-token", `{"table":"events","filter":{"op":"eq","field":"kind","value":"wake"},"limit":25,"confirm":"preview-token"}`, `{"deleted":25,"truncated":true}`, 25, true},
		{"finished", nil, "next-token", `{"table":"events","confirm":"next-token"}`, `{"deleted":0,"truncated":false}`, 0, false},
		{"uint64", nil, "token", `{"table":"events","confirm":"token"}`, `{"deleted":18446744073709551615,"truncated":false}`, ^uint64(0), false},
		{"empty_confirm_is_not_preview", nil, "", `{"table":"events","confirm":""}`, `{"deleted":0,"truncated":false}`, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &capture{t: t, method: "POST", path: "/admin/db/app/purge", bearer: wantStr(testAdminKey), body: &tc.body,
				status: 200, respBody: tc.response}
			out, err := stub(t, c.handler).Purge(context.Background(), "app", "events", tc.confirm, tc.opts)
			if err != nil {
				t.Fatal(err)
			}
			if out.Deleted != tc.deleted || out.Truncated != tc.truncated {
				t.Fatalf("unexpected purge: %+v", out)
			}
		})
	}
}

func TestPurgeErrors(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		code   rtdberrors.ErrorCode
	}{
		{"non_admin", 401, rtdberrors.CodeUnauthorized},
		{"unknown_db", 404, rtdberrors.CodeNotFound},
		{"unknown_table", 404, rtdberrors.CodeNotFound},
		{"not_insert_only", 400, rtdberrors.CodeBadRequest},
		{"readonly", 409, rtdberrors.CodeReadOnly},
		{"quiesced", 409, rtdberrors.CodeReadOnly},
		{"stale_token", 409, rtdberrors.CodePreconditionFailed},
	} {
		for _, preview := range []bool{true, false} {
			phase := "confirm"
			if preview {
				phase = "preview"
			}
			t.Run(tc.name+"/"+phase, func(t *testing.T) {
				c := &capture{t: t, method: "POST", path: "/admin/db/app/purge", bearer: wantStr(testAdminKey),
					status: tc.status, respBody: `{"code":"` + string(tc.code) + `","message":"purge rejected"}`}
				client := stub(t, c.handler)
				var err error
				if preview {
					var out *PurgePreviewResponse
					out, err = client.PurgePreview(context.Background(), "app", "events", nil)
					if out != nil {
						t.Fatalf("error returned response: %+v", out)
					}
				} else {
					var out *PurgeResponse
					out, err = client.Purge(context.Background(), "app", "events", "stale", nil)
					if out != nil {
						t.Fatalf("error returned response: %+v", out)
					}
				}
				if !rtdberrors.IsCode(err, tc.code) {
					t.Fatalf("got %v want %s", err, tc.code)
				}
			})
		}
	}
}

func TestPurgeResponsesStrictDecode(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		out  any
	}{
		{"preview_unknown", `{"matching":1,"confirmToken":"t","limit":1,"extra":true}`, &PurgePreviewResponse{}},
		{"confirm_unknown", `{"deleted":1,"truncated":false,"extra":true}`, &PurgeResponse{}},
		{"negative_deleted", `{"deleted":-1,"truncated":false}`, &PurgeResponse{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := json.Unmarshal([]byte(tc.body), tc.out); err == nil {
				t.Fatal("expected decode error")
			}
		})
	}
}
