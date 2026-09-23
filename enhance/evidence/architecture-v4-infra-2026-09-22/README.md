# V4 infrastructure foundation validation — September 22, 2026

The isolated [Terraform root](../../../ops/infra/digitalocean/enhance-v4/README.md)
models up to four replica pairs, additive project membership, and a dedicated
firewall/tag. Workers have stable addresses and deletion/replacement guards.
The plan validator accepts only the next pair and rejects changes to established
resources. It supports interrupted creation using recorded Droplet identities.
The durable request consumer, worker bootstrap, qualification receipts, and
registration bridge are still unfinished.

## Local checks

- Terraform 1.14.8 with locked DigitalOcean provider 2.101.1:
  [schema validation](terraform-validate.log) and [four mocked fleet tests](terraform-tests.log) passed.
- [Five plan-validator tests](plan-tests.log) passed against a fixture generated
  by Terraform's mocked provider, including partial creation, pair expansion,
  forbidden mutations, and fleet limits.
- [All 41 Enhance operations tests](ops-tests.log) passed.
- Full CI now includes isolated Terraform formatting, backend-disabled init,
  validation, and mocked tests as a release dependency. Local actionlint passed.
  The remote workflow has not been run.
- Documentation links and development utility checks passed (3 filter, 4 parent
  filter, and 17 release/tooling tests).

Initialization used `-backend=false`; mocked tests use synthetic IDs and no live
cloud credentials. No backend, cloud resource, running service, DNS record, or
production inventory was changed. These checks establish software behavior only;
they do not establish hardware capacity, deployment success, or load qualification.
[Source hashes](source-sha256.json) identify the infrastructure and CI inputs.
