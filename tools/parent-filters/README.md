# Parent-filter evaluation tools

`transparent_parent_evaluate.py` selects offline finalists, runs paired isolated
HTTP waves, and summarizes results for the active opt-in experiment. It does not
deploy a publication. Its sibling unittest covers ranking and validation behavior.

Follow the [evaluation procedure](../../docs/transparent-pir/parent-filter-evaluation.md)
for dataset provenance, held-out selection, correctness and privacy requirements.
Run `python3 tools/parent-filters/transparent_parent_evaluate.py --help` for commands.
