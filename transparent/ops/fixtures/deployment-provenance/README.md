# Frozen deployment provenance fixtures

These are historical release identities consumed by the candidate, input-stage,
schema-product and bootstrap tests. They are not current operational configuration
or evidence of current service health. Each file retains its original bytes from
`c00c2101e85fb9518ef8509f4478ed097ce49814:transparent/evidence/<relative-path>`;
the relative paths are listed in `SHA256SUMS`. Verify from this directory with
`shasum -a 256 -c SHA256SUMS`.

The tests compare release hashes, source replacements and refusal of relabelled
historical reports. Do not regenerate these inputs from the implementation under
test or replace their identities with current releases.
