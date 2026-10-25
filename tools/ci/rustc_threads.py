#!/usr/bin/env python3
"""Cap codegen threads for the fast query-host crate.

Cargo does not hash this wrapper into crate fingerprints. The fast compile sets
it after the warm target directory is chosen, so every other crate stays fresh.
`-C threads` does not change the object code.
"""
import os
import sys


def main(argv):
    if len(argv) < 2:
        raise SystemExit('rustc wrapper expected the compiler path')
    compiler, args = argv[1], argv[2:]
    names = [args[index + 1] for index, arg in enumerate(args)
             if arg == '--crate-name' and index + 1 < len(args)]
    threads = os.environ.get('WALLET_PIR_QUERY_RUSTC_THREADS', '4')
    if not threads.isdecimal() or int(threads) < 1:
        raise SystemExit('WALLET_PIR_QUERY_RUSTC_THREADS must be a positive integer')
    if names == ['pir_query_server']:
        args = [*args, '-C', f'threads={threads}']
    os.execv(compiler, [compiler, *args])


if __name__ == '__main__':
    main(sys.argv)
