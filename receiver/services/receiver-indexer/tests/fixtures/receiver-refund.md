# Public receiver fixture

`receiver-refund.hex` is the raw mainnet transaction
`2060cf68088b55dcd9e2f91556c72528e1ab8c6834ea3f71b8c80fca9fc51653`,
retrieved on September 26, 2026 with `getrawtransaction(txid, 0)` from
`http://159.65.183.89:8232`. Its one Ironwood Action is authenticated with zero
OVK. It was mined at height 3,496,114, Action index 0, global note position 610503.
The tests embed it in synthetic block envelopes and reuse its ciphertext in a
synthetic coinbase to exercise exclusion. No wallet keys or local wallet data are
included.
