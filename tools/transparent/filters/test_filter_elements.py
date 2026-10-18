"""Public filter extraction must retain complete scripts without changing scope."""
import hashlib
import unittest
from transparent_filter_build import block_elements


def h(value):
    return hashlib.sha256(value.encode()).hexdigest()


S = "76a914" + "11" * 20 + "88ac"


class ElementSetTests(unittest.TestCase):
    """Extraction rules, independent of any encoder."""

    def test_complete_scripts_keeps_scripts_the_private_backend_cannot_serve(self):
        block = {
            "height": 1,
            "hash": h("b1"),
            "prev_hash": h("b0"),
            "transactions": [
                {
                    "index": 1,
                    "txid": h("tx"),
                    "vin": [],
                    # A bare multisig-ish script: not a supported address type,
                    # but it must still appear in a shared public filter.
                    "vout": [
                        {"script": "5152ae", "value_zat": 1, "n": 0},
                        {"script": S, "value_zat": 2, "n": 1},
                    ],
                }
            ],
        }
        self.assertEqual(block_elements(block), sorted(["5152ae", S]))

    def test_leading_op_return_excluded_but_embedded_0x6a_kept(self):
        embedded = "76a914" + "6a" * 20 + "88ac"
        block = {
            "height": 1,
            "hash": h("b1"),
            "prev_hash": h("b0"),
            "transactions": [
                {
                    "index": 1,
                    "txid": h("tx"),
                    "vin": [],
                    "vout": [
                        {"script": "6a04deadbeef", "value_zat": 0, "n": 0},
                        {"script": embedded, "value_zat": 1, "n": 1},
                        {"script": "", "value_zat": 0, "n": 2},
                    ],
                }
            ],
        }
        self.assertEqual(block_elements(block), [embedded])

    def test_spent_scripts_are_included_and_deduplicated(self):
        block = {
            "height": 1,
            "hash": h("b1"),
            "prev_hash": h("b0"),
            "transactions": [
                {
                    "index": 1,
                    "txid": h("tx"),
                    "vin": [{"script": S, "value_zat": 7, "txid": h("p"), "n": 0}],
                    "vout": [{"script": S, "value_zat": 6, "n": 0}],
                }
            ],
        }
        self.assertEqual(block_elements(block), [S])


if __name__ == "__main__":
    unittest.main()
