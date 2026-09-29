# Same-anchor fixture comparison

The first candidate comparison found no changed ledger expectations or review
findings. It stopped solely because the existing re-cut tool required the new
anchor to advance: both exports end at height 3,499,341. The original failed
comparison log and JSON report remain part of the deployment evidence.

The explicit `--same-anchor` mode supports a schema replacement at unchanged
coverage. It requires equal anchor heights, identical cutoff and case definitions
(including birthdays and every checkpoint expectation), unchanged map identity
fields, and agreement on shared accepted headers. The normal re-cut mode still
requires an advancing anchor. Changing only the publication binding and source
provenance is allowed; changed ledgers, missing checkpoints and reorgs are not.

All 18 comparison tests passed, including refusal of changed expectations,
missing checkpoints, changed birthdays, cutoff or chain identity, and either
advancing or regressing anchors under the explicit mode.
[Raw test log](same-anchor-passed.log).
