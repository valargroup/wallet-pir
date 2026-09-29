# Quality parser regression inputs

Captured from production aggregate endpoints on 2026-09-29 UTC. Service
snapshots precede the quality rollout; Caddy was captured after enabling its
private HTTP metrics. These cover Transparent publisher/worker, Enhance query and
packing ingress, and Status init/query JSON. They contain public infrastructure
identities and revision digests, not wallet requests or credential values.

The parser tests verify that these deployed formats remain readable while
unbounded assignment/revision labels are excluded from retained history.
Fixtures describe their capture time only; they are not current service health.
