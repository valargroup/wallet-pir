# Changelog

## Unreleased

- Recovery respects query and byte limits between page requests and saves checked
  fragments so one-query passes finish long histories after restarts.
- The reference SQLite store uses fresh format 6 for saved fragment progress.
  Earlier prototype companion stores must be recreated; there is no migration.
- Page requests with a finite byte budget run sequentially so at most one atomic
  response can cross the limit.
