"""Candidate oracle verification and report assembly inside the locked execution owner.

No standalone entry point: the deploy owner must supply its continuously checked
budget and hold the production lock through complete snapshot verification,
native execution, capture closure and assembly. This module never constructs a
snapshot or runs a reader. The snapshot provider is imported lazily because it
is the full-only verifier; absence refuses, never downgrades.
"""
import importlib
from pathlib import Path
import time

import activity_candidate_reports as R
import activity_oracle_anchors as A
import activity_oracle_rpc as RPC
import activity_oracle_sample as S

KEYS = {'mapping', 'execution', 'snapshot_request', 'snapshot_owner', 'snapshot_manifest',
        'rpc_attempts', 'rpc_timings', 'capture_result'}


def produce_and_write(evidence, budget, path):
    """Assembly and durable report retention within the same checked owner.

    The caller keeps its production lock, durable fence and hard deadline until
    this returns. A refusal preserves any partial evidence and is not a pass.
    There is deliberately no unowned CLI or fallback to an unchecked writer.
    """
    report = produce(evidence, budget)
    def check():
        R.require(budget.deadline > time.monotonic(), 'oracle report deadline exceeded')
        budget.check()
        R.require(budget.deadline > time.monotonic(), 'oracle report deadline exceeded')
    check()
    R.write_report(path, report, check=check)
    check()
    return report


def json_lines(reference, check):
    raw = R.blob(reference, check=check)
    lines = raw.splitlines()
    R.require(raw.endswith(b'\n') and 0 < len(lines) <= RPC.MAX_ATTEMPTS and
              all(line.strip() for line in lines),
              'oracle raw index is empty, incomplete or oversized')
    result = []
    for line in lines:
        check(); result.append(R.json_bytes(line))
    check()
    return result


def prepare_snapshot(evidence, budget):
    """Full immutable verification BEFORE any candidate reader is launched.

    The locked, hard-supervised caller reuses this verifier before dispatch and
    report assembly re-runs it afterwards. No asserted verified flag is accepted.
    """
    R.require(callable(getattr(budget, 'check', None)) and
              R.number(getattr(budget, 'deadline', None)) and budget.deadline > time.monotonic(),
              'oracle owned snapshot budget is incomplete')
    check = budget.check
    check()
    mapping = R.value(evidence['mapping'], check=check)
    publication = evidence['mapping']['sha256']
    request = R.value(evidence['snapshot_request'], check=check)
    owner = R.value(evidence['snapshot_owner'], check=check)
    manifest_ref = evidence['snapshot_manifest']
    R.blob(manifest_ref, check=check)  # Independent reference shape/hash check before provider.
    root = Path(manifest_ref['path']).parent
    J = importlib.import_module('activity_journal_snapshot')
    request = J.validate(request)
    request_sha = J.digest(request)
    R.require(root == J.SNAPSHOTS/request_sha and Path(manifest_ref['path']).name == 'manifest.json' and
              Path(evidence['snapshot_owner']['path']) == J.OWNERS/(request_sha+'.json') and
              Path(evidence['snapshot_request']['path']) == J.OWNERS/(request_sha+'.request.json') and
              isinstance(owner, dict) and owner.get('request_sha256') == request_sha and
              owner.get('status') == 'staged' and owner.get('phase') == 'retained' and
              owner.get('target') == str(root) and owner.get('manifest') == manifest_ref['sha256'] and
              isinstance(owner.get('restoration'), dict) and owner['restoration'].get('status') == 'proven',
              'snapshot owner has not retained an immutable restored snapshot')
    # The provider's full-only verifier requires its concrete budget. Nest it
    # under exactly the caller's aggregate deadline and checked owner guard;
    # neither its resource sampling nor this adaptation can widen that bound.
    # There is no manifest-only option or asserted verification flag.
    snapshot_budget = J.Budget(budget.deadline, 'oracle full snapshot verification', guard=check)
    manifest = J.verify_tree(root, manifest_ref['sha256'], request=request, budget=snapshot_budget)
    check()
    R.require(manifest.get('request_sha256') == request_sha and
              manifest.get('source_sha') == request['source_sha'] and
              manifest.get('candidate') == {'source_sha':R.C.SOURCE_SHA, 'identity':R.C.identity()} and
              manifest.get('publication') == request['publication'] and
              manifest.get('writer', {}).get('restored') == owner['restoration'] and
              request['publication']['map_sha256'] == publication and
              mapping.get('start_height') == 0 and mapping.get('genesis_hash') == request['journal']['genesis_hash'],
              'snapshot provenance, publication or restoration differs')
    rows = mapping.get('shards')
    R.require(isinstance(rows, list) and rows and rows[-1].get('end_height') == S.THROUGH and
              rows[-1].get('terminal_block_hash') == request['publication']['anchor_hash'],
              'oracle publication anchor differs')
    files = manifest['contents']['files']
    tip = manifest['journal']['tip_height']
    sample = S.derive(root/'journal/blocks.bin', files['blocks.bin']['size'],
                      files['blocks.bin']['sha256'], tip, check)
    R.require(sample['canonical_hashes'][str(S.THROUGH)] == rows[-1]['terminal_block_hash'],
              'oracle snapshot sample publication anchor differs')
    return {'root':root, 'request':request, 'manifest':manifest, 'sample':sample, 'tip':tip}


def produce(evidence, budget):
    """Re-verify actual snapshot bytes, then native/raw/temporal bindings.

    `budget.check()` must enforce the caller's lock, aggregate deadline and
    sampled resource floors. The caller's hard bound also covers noninterruptible
    filesystem/JSON operations. A manifest-only checker is never accepted.
    `capture_result` binds the capture owner's two clocks after owned closure;
    raw indexes are the capture's original immutable JSONL files.
    """
    R.require(isinstance(evidence, dict) and set(evidence) == KEYS and
              callable(getattr(budget, 'check', None)) and R.number(getattr(budget, 'deadline', None)) and
              budget.deadline > time.monotonic(), 'oracle inputs or owned budget are incomplete')
    check = budget.check
    check()
    mapping = R.value(evidence['mapping'], check=check)
    publication = evidence['mapping']['sha256']
    native = R.capture(evidence['execution'], 'event-spotcheck', publication, check=check)
    child = R.value(evidence['execution']['owner'], check=check)
    result = R.value(evidence['execution']['result'], check=check)
    R.require(type(child.get('start_ticks')) is int and child['start_ticks'] > 0 and
              type(result.get('start_ticks')) is int and result['start_ticks'] == child['start_ticks'],
              'oracle child kernel identity is missing or changed')
    native_interval = {'started_unix':child['started_unix'], 'ended_unix':result['ended_unix'],
                       'started_monotonic':child.get('started_monotonic'),
                       'ended_monotonic':result.get('ended_monotonic')}
    A.interval(native_interval)
    R.require(native_interval['ended_monotonic']-native_interval['started_monotonic'] <=
              result['timeout_seconds'], 'oracle native exceeded its monotonic execution bound')
    prepared = prepare_snapshot(evidence, budget)
    root, request, sample, tip = (prepared[k] for k in ('root','request','sample','tip'))
    S.bind_native(native, sample, journal_dir=root/'journal',
                  genesis_hash=request['journal']['genesis_hash'], covered_through=tip)
    attempts = json_lines(evidence['rpc_attempts'], check)
    timings = json_lines(evidence['rpc_timings'], check)
    capture = R.value(evidence['capture_result'], check=check)
    R.require(isinstance(capture, dict) and capture.get('schema') == 'transparent-oracle-capture-v1' and
              capture.get('status') == 'passed' and capture.get('failures') == [] and
              type(capture.get('attempt_count')) is int and capture['attempt_count'] == len(attempts) and
              type(capture.get('timing_count')) is int and capture['timing_count'] == len(timings) and
              capture.get('attempts') == evidence['rpc_attempts'] and
              capture.get('timings') == evidence['rpc_timings'], 'oracle capture closure is incomplete or failed')
    capture_interval = capture.get('interval')
    anchors = A.verify(attempts, timings, {int(h):v for h,v in sample['canonical_hashes'].items()},
                       native_interval, capture_interval, check)
    comparison = RPC.compare(attempts, native, check=check)
    check()
    report = R.base('independent-chain-oracle', publication,
                    {**evidence, 'rederived_sample':sample, 'canonical_boundary_attempts':anchors,
                     'raw_comparison':comparison})
    report.update(blocks_compared=17, blocks_disagreeing=0, events_compared=comparison['events_compared'],
                  sample=sample['recipe'], raw_pool_category_coverage=comparison['raw_pool_category_coverage'],
                  limitations=['Python raw evidence independently verifies counts and categories; pinned native '
                               'multiset comparison verifies full event fields.',
                               'Zero-count categories remain explicit coverage limitations.'])
    return R.C.verify_gate(report, 'independent-chain-oracle', publication)
