# Activity metadata implementation evidence

This directory retains observations from the v3/v11 activity metadata implementation
starting from wallet-pir `22fe04c5f5e6f781dbb697bea24850f2632907b5` and
zakura-core/wallet-libraries `908913c7c8ed69ee1a35cc001172a0800d236d3a`.

[Storage observation](storage.json) records the new 250 GiB coordinator volume and
unchanged live service state. The live node uses database format 29; the previous
reader used format 28. The transparent publisher dependency pin must therefore
track the observed running node before secondary ingestion.

Initial iteration found and corrected the obsolete maximum-entry-size assertion,
a missing schema destructuring field and a missing store codec dependency. These
failed checks do not establish passing validation. Final check outputs and exact
candidate identities are retained separately when available.

No loaded prototype, production cutover or sustained qualification is claimed.
