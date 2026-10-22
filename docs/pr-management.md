# PR management and labels

Labels show which products and service roles a PR touches and which kinds of
change deserve attention. Keep **at most three labels per PR**, focused on the
main areas changed. Labels do not introduce approval, specification, or
merge requirements. They do not establish that CI passed, a release was
published, or a deployment happened.

The repository still defaults to direct pushes to `main`; use a PR when Roman
requests one. This guide applies whenever a PR is created or updated.

## Author and agent process

1. Keep the PR focused on one outcome. Describe the problem and resulting
   behavior, breaking changes, and validation in plain words. Keep pending and
   skipped validation explicit. Use a draft while the change is unfinished.
2. Check the selected component labels against the main changes. The limit is
   three labels total, not three labels per category; a broad PR does not need
   one label for every touched directory.
3. Inspect the diff and apply the breaking-change and review labels below when
   relevant. An absent label is not evidence of compatibility or safety.
4. Revisit labels after scope changes. Risk labels take priority and use the
   same three slots as component labels. Remove labels that no longer apply.

Reviewers check the labels along with the diff. Labels do not replace a PR's
breaking-change explanation, validation evidence, or the existing review and
release rules. No label requires an additional approval or specification.

## Automatic component labels

The workflow reserves slots for manually applied risk or other non-component
labels, then selects the highest-scoring components for the remaining slots.
Each component scores the added and removed lines in matching files, with a
minimum of one point per file so renames are included. Ties prefer products,
then Publisher/Query Server, then supporting areas, then alphabetical order.
Component labels are recomputed on updates; labels for lesser or stale areas
are removed rather than accumulating.

If more than three manual labels are present, retain three in this order:
`pir-scheme`, `wallet-api-breaking`, `publish-api-breaking`, `journal-breaking`,
`security-review`, `privacy-review`, then other labels alphabetically. This
keeps the limit explicit without creating a merge gate.

| Labels | Meaning and automatic coverage |
| --- | --- |
| `product:enhance`, `product:status`, `product:transparent`, `product:receiver` | Product-owned source, tools, docs, evidence, and explicitly product-specific workflow and infrastructure paths. Transaction-detail/txid paths belong to Transparent. |
| `area:publisher` | Unified publisher, product publishing packages, and explicitly publisher-owned deployment configuration. Receiver's indexer and store belong here. |
| `area:query-server` | Unified query server, product query packages, and explicitly query-owned deployment configuration. Receiver's PIR server belongs here. |
| `area:shared` | Libraries and tools shared across products. |
| `area:ci` | Workflows, GitHub configuration, and CI/development tooling. |
| `area:operations` | Deployment, infrastructure, fleet administration, and operations tests. |
| `documentation` | Markdown and documentation/evidence indexes. Product labels are also candidates for product docs. |

Product and role labels describe the main touched components; they do not mean
every change affects an external API. A unified-host edit makes its role label
a candidate. A shared library edit does not
automatically receive every product label. The selected three labels summarize
the PR; its description can explain additional affected products and roles.

## Breaking-change labels

Apply these after checking behavior, rather than inferring breaks from a path
or from the size of the diff. They may overlap.

| Label | Apply when |
| --- | --- |
| `wallet-api-breaking` | Wallet/client updates are needed because requests, responses, encodings, endpoints, or protocol behavior become incompatible. Include externally consumed Rust APIs when their change requires a client update. |
| `publish-api-breaking` | The publisher-to-query-server contract becomes incompatible: discovery, manifests, publication formats, artifact downloads, or associated protocol behavior. Publisher CLI/configuration changes alone do not imply this label. |
| `journal-breaking` | Existing persisted ingestion journals become unreadable or unsafe to reuse, or require rebuilding or conversion. An internal journal refactor that preserves existing data does not qualify. |

Examples: removing a wallet response field requires `wallet-api-breaking`;
changing the distribution manifest in a way that requires coordinated publisher
and query-server updates requires `publish-api-breaking`; changing the saved
journal record encoding requires `journal-breaking`. Moving an unchanged route
handler between crates does not itself require a breaking-change label.

## Security, privacy, and PIR scheme visibility

These are attention labels, not new workflow gates. Authors and agents apply
them based on the actual change, including changes outside obvious source paths.

| Label | Apply when |
| --- | --- |
| `security-review` | The change affects authentication, authorization, trust boundaries, cryptographic verification, or security-sensitive input handling. |
| `privacy-review` | The change affects what requests, logs, metrics, identifiers, timing, or access patterns reveal about a wallet or user. |
| `pir-scheme` | The change affects PIR parameters, scheme/profile selection, client query encryption, query construction, or privacy-sensitive batching or padding. Include indirect changes through configuration, defaults, and dependency updates. |

For example, changing a PIR modulus or encrypted-query profile receives
`pir-scheme` and `privacy-review`, plus a breaking-change label if clients or
servers become incompatible. A dependency repin receives `pir-scheme` when the
upstream change affects those behaviors; an unrelated repin does not. Merely
moving encryption code without changing its behavior does not imply a scheme
change. A PR does not need a specification or approval just to carry a label.
The existing restriction on editing `docs/spec/` still applies independently.

## Automation and maintenance

The [label catalog](../.github/labels.json) owns names, colors, and descriptions.
The [path rules](../.github/labeler.yml) use JSON syntax, which is valid YAML,
so the configuration can also be checked with Python's standard JSON parser.
The [PR labels workflow](../.github/workflows/pr-labels.yml) synchronizes catalog
definitions and runs the [label selector](../tools/ci/pr_labels.py) when a PR
opens, reopens, gains commits, is edited, becomes ready for review, or has labels
added or removed. Draft PRs are included. Updates made with the workflow's own
GitHub token do not trigger another run.

Automation adds component labels only. Breaking-change, security, privacy, and
PIR-scheme labels are applied by authors, agents, or reviewers. Existing label
definitions outside the catalog, including `prepare-hosted-cache`, are left
untouched. On a PR, non-component labels count toward the three-label limit.
Adding a risk label displaces a component label when all three slots are full.
Rerunning the workflow produces the same selection for unchanged inputs.

The workflow executes code and reads configuration only from the default
branch, never from the PR head. Label-policy changes take effect after landing.
It uses the ordinary GitHub Actions token; no new credential is needed. Failure
appears in the workflow run and does not change merge requirements.

To apply current rules to an existing PR:

```sh
gh workflow run pr-labels.yml --repo valargroup/wallet-pir --ref main -f pr-numbers=123
```

For several PRs, pass their numbers separated by actual newlines in the same
input. Closed PRs are skipped. Manual dispatch applies the same three-label
limit as automatic runs.

When changing the taxonomy, update the catalog, path rules where relevant, and
this guide together. Keep rules explicit for differently named Receiver
packages and product-specific infrastructure. Do not create functional labels
for every internal module or automatically infer semantic breaking changes.
The catalog can also be synchronized from a trusted checkout:

```sh
python3 tools/ci/pr_labels.py --repo valargroup/wallet-pir
```
