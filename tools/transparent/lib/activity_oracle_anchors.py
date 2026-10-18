"""Temporal canonical evidence for the candidate oracle's locked owner.

This checks raw responses and the capture's separate timing records. It emits
no qualification gate and trusts timing only when its caller has verified the
immutable capture owner, process identity and aggregate resource coverage.
Snapshot construction and final whole-snapshot verification remain mandatory.
"""
from activity_candidate_reports import number, require
from activity_oracle_rpc import MAX_ATTEMPTS, decode, integer, text

TIMES = {'started_unix', 'ended_unix', 'started_monotonic', 'ended_monotonic'}


def interval(value):
    require(isinstance(value, dict) and set(value) == TIMES and
            all(number(v) and v >= 0 for v in value.values()) and
            value['started_unix'] < value['ended_unix'] and
            value['started_monotonic'] < value['ended_monotonic'],
            'oracle execution interval is invalid')


def verify(attempts, timings, hashes, native_interval, owner_interval, check):
    """Require exact sample anchors surrounding the independently owned reader.

    `hashes` comes from re-derived, completely verified immutable block records,
    not an operator's list. Both clocks must surround the reader; a clock jump,
    in-reader response or missing timing cannot manufacture before/after proof.
    `check` enforces the locked owner's remaining budget and resource floors.
    Full raw block/transaction validation belongs to activity_oracle_rpc.
    """
    interval(native_interval); interval(owner_interval)
    require(callable(check) and isinstance(hashes, dict) and len(hashes) == 17 and
            all(type(h) is int and h >= 0 for h in hashes) and 0 in hashes and
            3500738 in hashes, 'canonical sample identity differs')
    for identity in hashes.values():
        text(identity)
    for clock in ('unix', 'monotonic'):
        require(owner_interval['started_'+clock] <= native_interval['started_'+clock] and
                native_interval['ended_'+clock] <= owner_interval['ended_'+clock],
                'native interval escapes oracle owner')
    require(isinstance(attempts, list) and 0 < len(attempts) <= MAX_ATTEMPTS and
            isinstance(timings, list) and len(timings) == len(attempts),
            'canonical capture timing coverage differs')
    before, after = {}, {}
    previous = {clock: owner_interval['started_'+clock] for clock in ('unix', 'monotonic')}
    for index, (attempt, timing) in enumerate(zip(attempts, timings)):
        check()
        require(isinstance(timing, dict) and set(timing) == TIMES | {'attempt_index'} and
                type(timing['attempt_index']) is int and timing['attempt_index'] == index,
                'canonical capture timing order differs')
        span = {key: timing[key] for key in TIMES}
        interval(span)
        for clock in ('unix', 'monotonic'):
            require(previous[clock] <= span['started_'+clock] and
                    span['ended_'+clock] <= owner_interval['ended_'+clock],
                    'canonical capture time escapes owner or overlaps earlier attempt')
            previous[clock] = span['ended_'+clock]
        # Verify the immutable request before deciding whether it is an anchor.
        # Transaction responses may be very large; the raw comparator owns them.
        require(isinstance(attempt, dict) and set(attempt) == {'status', 'request', 'response'} and
                type(attempt['status']) is int, 'canonical capture attempt differs')
        requests = decode(attempt['request'], check=check); check()
        calls = requests if isinstance(requests, list) else [requests]
        require(calls and all(isinstance(call, dict) for call in calls), 'canonical request differs')
        anchors = [call for call in calls if call.get('method') == 'getblockhash']
        if not anchors:
            continue
        require(attempt['status'] == 200 and len(anchors) == len(calls),
                'canonical response failed or anchor batch is mixed')
        responses = decode(attempt['response'], check=check); check()
        responses = responses if isinstance(responses, list) else [responses]
        require(len(responses) == len(calls) and all(isinstance(r, dict) and
                type(r.get('id')) in (int, str) and r.get('error') is None for r in responses),
                'canonical response is incomplete')
        by_id = {r['id']: r for r in responses}
        require(len(by_id) == len(responses) and all(type(c.get('id')) in (int, str) for c in calls) and
                len({c['id'] for c in calls}) == len(calls) and
                set(by_id) == {c['id'] for c in calls}, 'canonical response ids differ')
        is_before = all(span['ended_'+clock] <= native_interval['started_'+clock]
                        for clock in ('unix', 'monotonic'))
        is_after = all(span['started_'+clock] >= native_interval['ended_'+clock]
                       for clock in ('unix', 'monotonic'))
        for call in calls:
            require(isinstance(call.get('params'), list) and len(call['params']) == 1,
                    'canonical parameters differ')
            height = integer(call['params'][0]); identity = text(by_id[call['id']].get('result'))
            require(height in hashes and hashes[height] == identity, 'canonical hash differs from snapshot')
            # Native can issue additional canonical lookups during comparison.
            # Those are checked for consistency but count for neither boundary.
            for boundary, applies in ((before, is_before), (after, is_after)):
                if applies:
                    require(height not in boundary, 'duplicate canonical boundary height')
                    boundary[height] = index
    check()
    require(set(before) == set(hashes) and set(after) == set(hashes),
            'canonical before/after coverage is incomplete')
    return {'before': {str(h): before[h] for h in sorted(before)},
            'after': {str(h): after[h] for h in sorted(after)},
            'canonical_hashes': {str(h): hashes[h] for h in sorted(hashes)}}
