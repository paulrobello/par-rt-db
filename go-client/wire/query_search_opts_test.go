package wire

import (
	"encoding/json"
	"testing"
)

func TestSearchQueryRankModeMinSimilarityRoundTrip(t *testing.T) {
	in := `{"index":"i","query":"q","mode":"trgm","minSimilarity":0.5}`
	var s SearchQuery
	if err := json.Unmarshal([]byte(in), &s); err != nil {
		t.Fatal(err)
	}
	if s.MinSimilarity == nil || *s.MinSimilarity != 0.5 {
		t.Fatalf("minSimilarity lost: %#v", s)
	}
	out, _ := json.Marshal(s)
	if string(out) != in {
		t.Fatalf("round trip: %s", out)
	}
	in = `{"index":"i","query":"q","rankMode":"ts_rank_cd"}`
	s = SearchQuery{}
	if err := json.Unmarshal([]byte(in), &s); err != nil || s.RankMode != SearchRankTsRankCd {
		t.Fatalf("rankMode: %v %#v", err, s)
	}
	out, _ = json.Marshal(s)
	if string(out) != in {
		t.Fatalf("round trip: %s", out)
	}
	out, _ = json.Marshal(SearchQuery{Index: "i", Query: "q"})
	if string(out) != `{"index":"i","query":"q"}` {
		t.Fatalf("defaults not omitted: %s", out)
	}
}
