# Receiver wire records

This package owns the fixed receiver record layout and structural parsing. It
has no dependencies on chain types or cryptography.

`RawRecord` contains untrusted bytes. Parsing checks the length, version tags,
empty-slot encoding, and page bounds. It does not validate receiver addresses,
field elements, ephemeral keys, or inclusion proofs. Callers must perform those
checks before accepting a payment or publishing recovered data.

The `receiver-directory` package retains its validating `Receiver` and `Record`
APIs and uses this codec internally. An entirely zero slot is empty; a malformed
nonempty slot is an error.
