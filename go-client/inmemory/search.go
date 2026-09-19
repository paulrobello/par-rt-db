// Search, vector-search, and hybrid-search terminals — the Go port of rust
// in_memory/query.rs's search half (FM-31 websearch approximation, FM-30
// trgm ranking, ENH-030 ranked paging, and the _searchSnippet stand-in).
package inmemory

import (
	"encoding/json"
	"sort"
	"strings"

	rtdberrors "github.com/paulrobello/par-rt-db/go-client/errors"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

type searchOperand struct {
	phrase []string // non-nil = quoted phrase (adjacent words); nil = bare term
	term   string   // bare term (lowercased)
}

func (o searchOperand) isPhrase() bool { return o.phrase != nil }

type websearchQuery struct {
	groups   [][]searchOperand // ANDed; within a group any operand suffices
	excluded []searchOperand   // -term / -"phrase": must NOT match
}

// parseWebsearchQuery approximates the server's websearch_to_tsquery: quoted
// phrases require adjacent words, the bare word `or` unions adjacent
// operands, `-term`/`-"phrase"` excludes, everything else is an ANDed term.
func parseWebsearchQuery(text string) websearchQuery {
	out := websearchQuery{}
	var current []searchOperand
	orPending := false
	fields := strings.Fields(text)
	i := 0
	for i < len(fields) {
		tok := fields[i]
		i++
		negated := strings.HasPrefix(tok, "-")
		if negated {
			tok = tok[1:]
			if tok == "" {
				continue
			}
		}
		var operand searchOperand
		quoted := tok == "\"" || strings.HasPrefix(tok, "\"")
		if quoted {
			// Collect tokens until one ends with an unescaped closing quote.
			phrase := strings.TrimPrefix(tok, "\"")
			for i < len(fields) && (!strings.HasSuffix(phrase, "\"") || phrase == strings.TrimPrefix(tok, "\"") && !strings.HasSuffix(tok, "\"")) {
				phrase += " " + fields[i]
				i++
			}
			phrase = strings.TrimSuffix(phrase, "\"")
			words := strings.Fields(strings.ToLower(phrase))
			if len(words) == 0 {
				continue
			}
			operand = searchOperand{phrase: words}
		} else {
			word := strings.ToLower(strings.TrimSuffix(strings.TrimPrefix(tok, "\""), "\""))
			if word == "" {
				continue
			}
			operand = searchOperand{term: word}
		}
		if !negated && !operand.isPhrase() && operand.term == "or" {
			orPending = len(current) > 0
			continue
		}
		if negated {
			out.excluded = append(out.excluded, operand)
			continue
		}
		if orPending {
			current = append(current, operand)
			orPending = false
		} else {
			if len(current) > 0 {
				out.groups = append(out.groups, current)
				current = nil
			}
			current = append(current, operand)
		}
	}
	if len(current) > 0 {
		out.groups = append(out.groups, current)
	}
	return out
}

func searchOperandMatches(op searchOperand, fieldTexts []string) bool {
	if op.isPhrase() {
		joined := strings.Join(op.phrase, " ")
		for _, f := range fieldTexts {
			if strings.Contains(f, joined) {
				return true
			}
		}
		return false
	}
	for _, f := range fieldTexts {
		if strings.Contains(f, op.term) {
			return true
		}
	}
	return false
}

func positiveLexemes(parsed websearchQuery) []string {
	var out []string
	for _, group := range parsed.groups {
		for _, op := range group {
			if op.isPhrase() {
				out = append(out, op.phrase...)
			} else {
				out = append(out, op.term)
			}
		}
	}
	return out
}

// ftsStringify is the doc's text contribution for one field: absent/null
// contributes nothing, strings verbatim, numbers and bools stringify, else
// the JSON form.
func ftsStringify(v wire.JSONValue) string {
	switch t := v.(type) {
	case nil:
		return ""
	case wire.Null:
		return ""
	case wire.String:
		return string(t)
	case wire.Number:
		return jsonNumberText(t)
	case wire.Bool:
		if t {
			return "true"
		}
		return "false"
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return ""
		}
		return string(b)
	}
}

// ftsTokens lowercases then takes the maximal ASCII-alphanumeric runs (the
// ts-client's /[a-z0-9]+/g stand-in for to_tsvector lexemes).
func ftsTokens(s string) []string {
	var out []string
	var cur []byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') {
			cur = append(cur, c)
		} else if c >= 'A' && c <= 'Z' {
			cur = append(cur, c+'a'-'A')
		} else if len(cur) > 0 {
			out = append(out, string(cur))
			cur = nil
		}
	}
	if len(cur) > 0 {
		out = append(out, string(cur))
	}
	return out
}

// tsqueryScore is the relevance stand-in every client engine pins: how many
// tokens of the doc's search text are positive query lexemes.
func tsqueryScore(doc wire.Object, indexFields []string, positives map[string]bool) int64 {
	var parts []string
	for _, f := range indexFields {
		parts = append(parts, ftsStringify(doc[f]))
	}
	source := strings.Join(parts, " ")
	var score int64
	for _, tok := range ftsTokens(source) {
		if positives[tok] {
			score++
		}
	}
	return score
}

// searchFieldTexts is the lowercased string values of the index's fields.
func searchFieldTexts(doc wire.Object, indexFields []string) []string {
	var out []string
	for _, f := range indexFields {
		if s, ok := doc[f].(wire.String); ok {
			out = append(out, strings.ToLower(string(s)))
		}
	}
	return out
}

// attachSearchSnippets writes _searchSnippet on every hit: a ≤35-word excerpt
// over the index's fields, matched words wrapped in <mark>…</mark>, centered
// on the first matched word.
func attachSearchSnippets(rows wire.Array, indexFields []string, parsed websearchQuery) {
	terms := positiveLexemes(parsed)
	for _, rv := range rows {
		doc, ok := rv.(wire.Object)
		if !ok {
			continue
		}
		var texts []string
		for _, f := range indexFields {
			if s, ok := doc[f].(wire.String); ok {
				texts = append(texts, string(s))
			}
		}
		text := strings.Join(texts, " ")
		marked := func(w string) bool {
			lw := strings.ToLower(w)
			for _, t := range terms {
				if strings.Contains(lw, t) {
					return true
				}
			}
			return false
		}
		words := strings.Fields(text)
		first := 0
		for i, w := range words {
			if marked(w) {
				first = i
				break
			}
		}
		start := first - 15
		if start < 0 {
			start = 0
		}
		end := start + 35
		if end > len(words) {
			end = len(words)
		}
		var sb strings.Builder
		for i, w := range words[start:end] {
			if i > 0 {
				sb.WriteByte(' ')
			}
			if marked(w) {
				sb.WriteString("<mark>")
				sb.WriteString(w)
				sb.WriteString("</mark>")
			} else {
				sb.WriteString(w)
			}
		}
		doc["_searchSnippet"] = wire.String(sb.String())
	}
}

// executeSearchTerminal runs the search terminal: shared prologue (non-empty
// query, SEC-007 byte cap, index resolution, snippet/mode combination), then
// the mode branch. tsquery (default) matches the websearch approximation and
// narrows by the carried filter; trgm substring-matches and ranks by the
// pinned similarity stand-in.
func executeSearchTerminal(s *Store, q *wire.Query, search *wire.SearchQuery, table *TableDef) (wire.JSONValue, error) {
	if strings.TrimSpace(search.Query) == "" {
		return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "search query text must not be empty")
	}
	if len(search.Query) > maxSearchQueryBytes {
		return nil, rtdberrors.New(rtdberrors.CodeBadRequest,
			"search query text must be at most "+formatI64(maxSearchQueryBytes)+" bytes")
	}
	var indexDef *IndexDef
	for i := range table.Indexes {
		if table.Indexes[i].Name == search.Index && table.Indexes[i].Search {
			indexDef = &table.Indexes[i]
			break
		}
	}
	if indexDef == nil {
		return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "search index '"+search.Index+"' not found")
	}
	indexFields := indexDef.Fields
	snippet := search.Snippet != nil && *search.Snippet
	if snippet && search.Mode == wire.SearchModeTrgm {
		return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "snippet is only supported in tsquery mode")
	}
	if search.Filter != nil {
		if err := validateFilterExpr(search.Filter, table); err != nil {
			return nil, err
		}
	}
	rows := collectAllLocked(s, q.Table)
	trgm := search.Mode == wire.SearchModeTrgm
	if trgm {
		if search.Filter != nil {
			rows = filterDocs(rows, search.Filter, table.Fields)
		}
		needle := strings.ToLower(search.Query)
		queryLen := len(search.Query)
		type scoredRow struct {
			score     float64
			createdAt int64
			id        string
			doc       wire.Object
		}
		var scored []scoredRow
		for _, rv := range rows {
			doc := rv.(wire.Object)
			var best *float64
			for _, field := range indexFields {
				text, ok := doc[field].(wire.String)
				if !ok {
					continue
				}
				ts := string(text)
				if strings.Contains(strings.ToLower(ts), needle) {
					score := 0.0
					if len(ts) > 0 {
						score = float64(queryLen) / float64(len(ts))
					}
					if best == nil || score > *best {
						best = &score
					}
				}
			}
			if best != nil {
				sr := scoredRow{score: *best, doc: doc}
				if n, ok := doc["_creationTime"].(wire.Number); ok {
					if v, ok := parseI64(string(n)); ok {
						sr.createdAt = v
					}
				} else {
					sr.createdAt = -9223372036854775808
				}
				if id, ok := doc["_id"].(wire.String); ok {
					sr.id = string(id)
				}
				scored = append(scored, sr)
			}
		}
		sort.SliceStable(scored, func(a, b int) bool {
			if scored[a].score != scored[b].score {
				return scored[a].score > scored[b].score
			}
			if scored[a].createdAt != scored[b].createdAt {
				return scored[a].createdAt > scored[b].createdAt
			}
			return scored[a].id > scored[b].id
		})
		if q.Paginate != nil {
			var ranked []*rankedRow
			for _, sr := range scored {
				ranked = append(ranked, newRankedRow(jsonNumberFromF64(sr.score), sr.doc))
			}
			return paginateRanked(q.Paginate, ranked, true)
		}
		limit := maxTake
		if q.Take != nil {
			limit = *q.Take
		}
		out := make(wire.Array, 0, len(scored))
		for i, sr := range scored {
			if i >= limit {
				break
			}
			out = append(out, sr.doc)
		}
		return out, nil
	}
	// tsquery (default) mode.
	parsed := parseWebsearchQuery(search.Query)
	var kept wire.Array
	for _, rv := range rows {
		doc := rv.(wire.Object)
		texts := searchFieldTexts(doc, indexFields)
		excludedHit := false
		for _, op := range parsed.excluded {
			if searchOperandMatches(op, texts) {
				excludedHit = true
				break
			}
		}
		if excludedHit {
			continue
		}
		allGroups := true
		for _, group := range parsed.groups {
			any := false
			for _, op := range group {
				if searchOperandMatches(op, texts) {
					any = true
					break
				}
			}
			if !any {
				allGroups = false
				break
			}
		}
		if allGroups {
			kept = append(kept, doc)
		}
	}
	if search.Filter != nil {
		kept = filterDocs(kept, search.Filter, table.Fields)
	}
	if snippet {
		attachSearchSnippets(kept, indexFields, parsed)
	}
	if q.Paginate != nil {
		positives := map[string]bool{}
		for _, l := range positiveLexemes(parsed) {
			positives[l] = true
		}
		var ranked []*rankedRow
		for _, rv := range kept {
			doc := rv.(wire.Object)
			score := tsqueryScore(doc, indexFields, positives)
			ranked = append(ranked, newRankedRow(wire.Number(formatI64(score)), doc))
		}
		sortRankedRows(ranked)
		return paginateRanked(q.Paginate, ranked, true)
	}
	return kept, nil
}

// filterDocs retains docs matching the validated filter.
func filterDocs(rows wire.Array, expr wire.FilterExpr, fields map[string]FieldType) wire.Array {
	out := make(wire.Array, 0, len(rows))
	for _, rv := range rows {
		if doc, ok := rv.(wire.Object); ok && matchesFilter(expr, doc, fields) {
			out = append(out, doc)
		}
	}
	return out
}

// executeVectorSearchTerminal over-approximates vector search: every live
// doc is a candidate, narrowed by the carried filter, capped at limit. With
// paginate the pool is sorted [created_at desc, id desc] first so the pages
// are stable.
func executeVectorSearchTerminal(s *Store, q *wire.Query, vector *wire.VectorSearchQuery, table *TableDef) (wire.JSONValue, error) {
	if vector.Filter != nil {
		if err := validateFilterExpr(vector.Filter, table); err != nil {
			return nil, err
		}
	}
	rows := collectAllLocked(s, q.Table)
	if vector.Filter != nil {
		rows = filterDocs(rows, vector.Filter, table.Fields)
	}
	if q.Paginate != nil {
		var ranked []*rankedRow
		for _, rv := range rows {
			if doc, ok := rv.(wire.Object); ok {
				ranked = append(ranked, newRankedRow(nil, doc))
			}
		}
		sortRankedRows(ranked)
		if len(ranked) > vector.Limit {
			ranked = ranked[:vector.Limit]
		}
		return paginateRanked(q.Paginate, ranked, false)
	}
	if len(rows) > vector.Limit {
		rows = rows[:vector.Limit]
	}
	return rows, nil
}

// executeHybridSearchTerminal over-approximates the fused rank: every live
// doc is a candidate capped at limit. A paginated hybrid returns an empty
// page (no honest fused ordering exists in-memory).
func executeHybridSearchTerminal(s *Store, q *wire.Query, hybrid *wire.HybridSearchQuery) (wire.JSONValue, error) {
	if q.Paginate != nil {
		return paginateRanked(q.Paginate, nil, true)
	}
	rows := collectAllLocked(s, q.Table)
	if len(rows) > hybrid.Limit {
		rows = rows[:hybrid.Limit]
	}
	return rows, nil
}

// collectAllLocked is Store.CollectAll without re-locking (engine call sites
// already hold s.mu).
func collectAllLocked(s *Store, table string) wire.Array {
	var out wire.Array
	for key, row := range s.docs {
		if key.Table != table || row.DeletedAt != nil {
			continue
		}
		out = append(out, mergeDoc(row))
	}
	return out
}
