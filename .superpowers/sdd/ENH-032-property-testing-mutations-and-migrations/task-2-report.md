
## Final review corrections

- Added `score -> rating` to generated RenameField operations, exercising the default map surface.
- Restored dynamic QA-002 quoted-name assertions for every generated rename source.
- Query terminal selection is independent of directive selection; generated queries now include owner-field filters, additive-index equality queries, and full scans, with optional count terminals.
- Both collect and count deserialize and execute the same query JSON through `ClientQuery::run_query`.
- The admin app is spawned once before `runner.run` and its address is reused for all cases.

Final verification:

```text
cargo test --manifest-path /Users/probello/Repos/par-rt-db/.worktrees/enh-032/server/Cargo.toml --test main proptest_parity --all-features
5 passed; 908 filtered out
```
