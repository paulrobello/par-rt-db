// go-client/admin/dbs.go
package admin

// Mirrors rust-client/src/admin/mod.rs — databases, schema, and tokens.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"

	rtdberrors "github.com/paulrobello/par-rt-db/go-client/errors"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// CreateDB: POST /admin/create-db {name} → {ok:true}.
func (c *AdminClient) CreateDB(ctx context.Context, name string) error {
	return c.ok(ctx, methodPost, "/admin/create-db", map[string]any{"name": name})
}

// DeleteDB: POST /admin/delete-db {name, confirm} → {ok:true}. The server
// rejects unless confirm == name exactly.
func (c *AdminClient) DeleteDB(ctx context.Context, name, confirm string) error {
	return c.ok(ctx, methodPost, "/admin/delete-db", map[string]any{"name": name, "confirm": confirm})
}

// PushSchema: POST /admin/push-schema {db, schema} → {ok:true}.
func (c *AdminClient) PushSchema(ctx context.Context, db string, schema wire.JSONValue) error {
	return c.ok(ctx, methodPost, "/admin/push-schema", map[string]any{"db": db, "schema": schema})
}

// PreviewSchema: POST /admin/db/{db}/schema/preview {schema} →
// SchemaPreviewDiff. Pure/advisory — validates and diffs without applying.
func (c *AdminClient) PreviewSchema(ctx context.Context, db string, schema wire.JSONValue) (*SchemaPreviewDiff, error) {
	var out SchemaPreviewDiff
	if err := c.api.Call(ctx, methodPost, fmt.Sprintf("/admin/db/%s/schema/preview", db),
		map[string]any{"schema": schema}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// MigrateSchema: POST /admin/db/{db}/migrate {directives, dryRun} →
// MigrateResult. On dryRun nothing commits; the returned Schema previews
// the derived shape.
func (c *AdminClient) MigrateSchema(ctx context.Context, db string, directives []Directive, dryRun bool) (*MigrateResult, error) {
	var out MigrateResult
	if err := c.api.Call(ctx, methodPost, fmt.Sprintf("/admin/db/%s/migrate", db),
		map[string]any{"directives": directives, "dryRun": dryRun}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ListDBs: GET /admin/dbs → the database names.
func (c *AdminClient) ListDBs(ctx context.Context) ([]string, error) {
	var resp struct {
		Databases []string `json:"databases"`
	}
	if err := c.get(ctx, "/admin/dbs", nil, &resp); err != nil {
		return nil, err
	}
	return resp.Databases, nil
}

// MintToken mints a full-access token (no expiry, read-write, all tables).
func (c *AdminClient) MintToken(ctx context.Context, db, name string) (*MintedToken, error) {
	return c.MintTokenWithOptions(ctx, db, name, MintTokenOptions{})
}

// MintTokenWithOptions: POST /admin/mint-token with the capability fields
// the caller set; omitted fields take the server defaults.
func (c *AdminClient) MintTokenWithOptions(ctx context.Context, db, name string, opts MintTokenOptions) (*MintedToken, error) {
	body := struct {
		DB   string `json:"db"`
		Name string `json:"name"`
		MintTokenOptions
	}{DB: db, Name: name, MintTokenOptions: opts}
	var out MintedToken
	if err := c.api.Call(ctx, methodPost, "/admin/mint-token", body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// RevokeToken: POST /admin/revoke-token {tokenId} → {ok:true}.
func (c *AdminClient) RevokeToken(ctx context.Context, tokenID string) error {
	return c.ok(ctx, methodPost, "/admin/revoke-token", map[string]any{"tokenId": tokenID})
}

// AllowlistAdd: POST /admin/allowlist {db, action:"add", email} → {ok:true}.
func (c *AdminClient) AllowlistAdd(ctx context.Context, db, email string) error {
	return c.allowlist(ctx, db, "add", email)
}

// AllowlistRemove: POST /admin/allowlist {db, action:"remove", email}.
func (c *AdminClient) AllowlistRemove(ctx context.Context, db, email string) error {
	return c.allowlist(ctx, db, "remove", email)
}

func (c *AdminClient) allowlist(ctx context.Context, db, action, email string) error {
	return c.ok(ctx, methodPost, "/admin/allowlist",
		map[string]any{"db": db, "action": action, "email": email})
}

// AllowlistList: GET /admin/allowlist?db= → the allowlisted emails.
func (c *AdminClient) AllowlistList(ctx context.Context, db string) ([]string, error) {
	var resp struct {
		Emails []string `json:"emails"`
	}
	if err := c.get(ctx, "/admin/allowlist", url.Values{"db": {db}}, &resp); err != nil {
		return nil, err
	}
	return resp.Emails, nil
}

// AdminsList: GET /admin/admins → the admin allowlist rows.
func (c *AdminClient) AdminsList(ctx context.Context) ([]AdminMember, error) {
	var resp struct {
		Admins []AdminMember `json:"admins"`
	}
	if err := c.get(ctx, "/admin/admins", nil, &resp); err != nil {
		return nil, err
	}
	return resp.Admins, nil
}

// AdminsAdd: POST /admin/admins {email, githubId?} → {ok:true}.
func (c *AdminClient) AdminsAdd(ctx context.Context, email string, githubID *int64) error {
	body := struct {
		Email    string `json:"email"`
		GithubID *int64 `json:"githubId,omitempty"`
	}{Email: email, GithubID: githubID}
	return c.ok(ctx, methodPost, "/admin/admins", body)
}

// AdminsRemove: DELETE /admin/admins {email} → {ok:true}.
func (c *AdminClient) AdminsRemove(ctx context.Context, email string) error {
	return c.ok(ctx, methodDelete, "/admin/admins", map[string]any{"email": email})
}

// GetSchema: GET /admin/dbs/{db}/schema → the database's pushed schema.
func (c *AdminClient) GetSchema(ctx context.Context, db string) (wire.JSONValue, error) {
	return c.jsonValue(ctx, fmt.Sprintf("/admin/dbs/%s/schema", db), nil)
}

// SchemaHistory: GET /admin/db/{db}/schema/history → newest-first snapshot
// summaries; limit/offset optional paging.
func (c *AdminClient) SchemaHistory(ctx context.Context, db string, limit, offset *int64) ([]SchemaHistorySummary, error) {
	q := url.Values{}
	if limit != nil {
		q.Set("limit", fmt.Sprintf("%d", *limit))
	}
	if offset != nil {
		q.Set("offset", fmt.Sprintf("%d", *offset))
	}
	var resp struct {
		Entries []SchemaHistorySummary `json:"entries"`
	}
	if err := c.get(ctx, fmt.Sprintf("/admin/db/%s/schema/history", db), q, &resp); err != nil {
		return nil, err
	}
	return resp.Entries, nil
}

// SchemaHistoryGet: GET /admin/db/{db}/schema/history/{version} → one full
// snapshot including the schema blob.
func (c *AdminClient) SchemaHistoryGet(ctx context.Context, db string, version int64) (*SchemaHistoryEntry, error) {
	var out SchemaHistoryEntry
	if err := c.get(ctx, fmt.Sprintf("/admin/db/%s/schema/history/%d", db, version), nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// RestoreSchema: POST /admin/db/{db}/schema/restore {version, confirm} →
// the restored version. confirm must equal the db name.
func (c *AdminClient) RestoreSchema(ctx context.Context, db string, version int64, confirm string) (int64, error) {
	var resp struct {
		RestoredTo int64 `json:"restoredTo"`
	}
	body := map[string]any{"version": version, "confirm": confirm}
	if err := c.api.Call(ctx, methodPost, fmt.Sprintf("/admin/db/%s/schema/restore", db), body, &resp); err != nil {
		return 0, err
	}
	return resp.RestoredTo, nil
}

// DBStats: GET /admin/dbs/{db}/stats → per-table counts + quota usage.
func (c *AdminClient) DBStats(ctx context.Context, db string) (*DbStats, error) {
	var out DbStats
	if err := c.get(ctx, fmt.Sprintf("/admin/dbs/%s/stats", db), nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ListTokens: GET /admin/tokens?db= → the database's machine tokens.
func (c *AdminClient) ListTokens(ctx context.Context, db string) ([]TokenInfo, error) {
	var resp struct {
		Tokens []TokenInfo `json:"tokens"`
	}
	if err := c.get(ctx, "/admin/tokens", url.Values{"db": {db}}, &resp); err != nil {
		return nil, err
	}
	return resp.Tokens, nil
}

// ExportDB: GET /admin/export-db?db= → the schema + every document as JSONL.
func (c *AdminClient) ExportDB(ctx context.Context, db string) (string, error) {
	data, err := c.raw(ctx, methodGet, withQuery("/admin/export-db", url.Values{"db": {db}}), "", nil)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// ImportDB: POST /admin/import-db?db= with an application/x-ndjson body of
// an ExportDB snapshot → {ok:true}, routed through the expect_ok analog.
func (c *AdminClient) ImportDB(ctx context.Context, db, jsonl string) error {
	data, err := c.raw(ctx, methodPost, withQuery("/admin/import-db", url.Values{"db": {db}}),
		"application/x-ndjson", []byte(jsonl))
	if err != nil {
		return err
	}
	var resp struct {
		OK bool `json:"ok"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return fmt.Errorf("admin: decode import ack: %w", err)
	}
	if !resp.OK {
		return rtdberrors.New(rtdberrors.CodeInternal, "admin request returned ok=false")
	}
	return nil
}

// CloneDB: POST /admin/clone-db?from=&to= → {ok:true}. Clones schema +
// documents server-side; storage blobs and scheduled transactions are not
// copied.
func (c *AdminClient) CloneDB(ctx context.Context, from, to string) error {
	q := url.Values{"from": {from}, "to": {to}}
	return c.ok(ctx, methodPost, withQuery("/admin/clone-db", q), nil)
}
