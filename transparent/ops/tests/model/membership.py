"""Exhaustive small-scope model of recent-tier membership and actuation.

An abstract state machine of transparent/docs/elastic-recent.md as the fleet
adapter (transparent-live-fleet.py) implements it, explored breadth-first
over every interleaving of:

* the foreground publisher: prepare a publication (its assignment written
  once, from the roster: enrolled and draining members), activate it when the
  archive owner and a recent replica hold it warm, withdraw when a prepared
  member dies before activation, and one reorg that either keeps the
  publication or withdraws it and revokes what workers attest;
* workers: warm the candidate or catch up to the active publication, crash,
  restart (a stopped worker stays stopped);
* the reconciler: probe a member without the routing lock, apply the
  observation under the lock only if the routing generation is unchanged
  (activating a prepared member there), re-render the router after an
  inventory change with the drain filter, lose its in-flight probes on crash;
* the actuator: one journaled operation at a time, consuming a request by
  decision id; every phase written before its side effect; a crash forgets
  in-memory progress (the journaled phase's effect runs again) and fences an
  interrupted apply until resolved; guards checked whenever an effect runs;
* the scaler: writes the configuration's requests, one after another, at any
  time (an unconsumed request may be replaced).

Abstraction, sound for the invariants checked: digests only move forward, so
a member attests the active publication or an older one (a bit) and holds a
warm candidate for the active publication or not (a bit). Which assigned
members warmed the candidate in time is chosen at activation (every subset),
since nothing else reads it. Only the equality of an observation's
generation with the current one matters, so a generation bump discards every
in-flight probe (its apply would); the `no-generation-check` mutation keeps
them instead. A reconciler crash only loses in-flight probes, which leaves a
state that not having probed also reaches.

Members: 0 is the archive owner (static); 1 and 2 are static recent replicas;
3 (and 4, when a configuration replaces 3) are elastic recent replicas.
`explore(config, mutation=None)` returns (states, violations); each mutation
is a deliberate protocol bug the tests require to be caught.
"""
from collections import deque

ABSENT, PROV, ENROLLED, DRAINING, RETIRED, QUARANTINED, DESTROYED = range(7)
INTENT_NAMES = ('absent', 'provisioned', 'enrolled', 'draining', 'retired', 'quarantined', 'destroyed')
ROSTER = (ENROLLED, DRAINING)
ARCHIVE = 0
NAMES = ('archive', 'r1', 'r2', 'e', 'f')
NONE, VALID, PREPARED, INVALID = range(4)

SCALE_OUT = ('requested', 'planned', 'applying', 'provisioned', 'enrolled', 'bootstrapping', 'installed', 'serving')
SCALE_IN = ('requested', 'draining', 'drained', 'stopped', 'retired', 'planned', 'applying', 'destroyed')
REPLACE = SCALE_OUT + ('quarantined', 'stopped', 'destroy-planned', 'destroy-applying', 'destroyed')
PHASES = {'scale_out': SCALE_OUT, 'scale_in': SCALE_IN, 'replace': REPLACE}
APPLY = ('applying', 'destroy-applying')

# State layout: one flat tuple.
(LEFT, PREPARING, WITHDRAWN, REORGED, ASSIGN_PUB, ASSIGN_NEXT, INTENT, UP, BOOTED, ATT, PREP_PUB,
 STOPPED, ROUTED, RENDERED, STALE, PROBE, OP, JOURNAL, REQ, ISSUED, DRAIN, BUDGET) = range(22)
WORKER_CRASHES, ACTUATOR_CRASHES, RECONCILER_CRASHES = range(3)


def bit(m):
    return 1 << m


class Model:
    def __init__(self, config, mutation=None):
        self.config = config
        self.mutation = mutation
        self.phases = dict(PHASES)
        if mutation == 'destroy-before-retire':
            self.phases['scale_in'] = ('requested', 'draining', 'drained', 'stopped', 'planned', 'applying',
                                       'retired', 'destroyed')
        self.members = range(config['members'])
        self.recent = range(1, config['members'])
        self.script = config['script']

    # -- derived facts ----------------------------------------------------------------
    def serving(self, s, m):
        return (m != ARCHIVE and not s[WITHDRAWN] and s[RENDERED] & bit(m) and s[INTENT][m] == ENROLLED
                and s[UP] & bit(m) and s[ATT] & bit(m))

    def others_serving(self, s, m):
        return sum(1 for o in self.recent if o != m and self.serving(s, o))

    def render(self, s):
        """Set the router from the routed set, the roster's intents and the drain filter."""
        routed, intent = s[ROUTED], s[INTENT]
        rendered = 0
        if routed & bit(ARCHIVE) and not s[WITHDRAWN]:
            members = [m for m in self.recent if routed & bit(m) and intent[m] in ROSTER]
            enrolled = [m for m in members if intent[m] == ENROLLED]
            if self.mutation == 'no-drain-filter':
                chosen = members
            elif self.mutation == 'drain-filter-always':
                chosen = enrolled
            else:
                chosen = enrolled or members
            if chosen:
                rendered = bit(ARCHIVE) | sum(bit(m) for m in chosen)
        s[RENDERED] = rendered
        s[STALE] = False
        op = s[OP]
        if op is not None and self.script[op[0]][0] == 'scale_in' and rendered & bit(self.script[op[0]][1]):
            s[DRAIN] = False

    def bump(self, s):
        """A routing-generation bump: every in-flight observation is now stale."""
        if self.mutation != 'no-generation-check':
            s[PROBE] = (NONE,) * len(self.members)

    def initial(self):
        config = self.config
        intent = tuple(config['intent'])
        enrolled = sum(bit(m) for m in self.members if intent[m] == ENROLLED)
        s = [config['publications'] - 1, False, False, False, enrolled, 0, intent, enrolled, enrolled, enrolled,
             0, 0, enrolled, 0, True, (NONE,) * len(self.members), None, 0, -1, 0, False,
             tuple(config['budget'])]
        self.render(s)
        return tuple(s)

    # -- invariants -----------------------------------------------------------------------
    def check(self, s):
        out = []
        rendered, routed, intent = s[RENDERED], s[ROUTED], s[INTENT]
        if s[WITHDRAWN]:
            if rendered:
                out.append('withdrawn but the router serves members')
        else:
            for m in self.members:
                if rendered & bit(m):
                    if not s[ATT] & bit(m):
                        out.append(f'{NAMES[m]} routed without attesting the active publication')
                    if not s[ASSIGN_PUB] & bit(m):
                        out.append(f'{NAMES[m]} routed but not in the active assignment')
            if not s[STALE]:
                enrolled_routed = any(routed & bit(m) and intent[m] == ENROLLED for m in self.recent)
                for m in self.recent:
                    if rendered & bit(m):
                        if intent[m] not in ROSTER:
                            out.append(f'{NAMES[m]} routed with intent {INTENT_NAMES[intent[m]]}')
                        elif intent[m] == DRAINING and enrolled_routed:
                            out.append(f'draining {NAMES[m]} routed while an enrolled replica is routed')
            if routed & bit(ARCHIVE) and any(routed & bit(m) and intent[m] in ROSTER for m in self.recent):
                if not any(rendered & bit(m) for m in self.recent):
                    out.append('router has no recent replica while an enrolled or draining one attests')
        if intent[ARCHIVE] != ENROLLED or s[STOPPED] & bit(ARCHIVE):
            out.append('archive member touched')
        for m in (1, 2):
            if intent[m] in (RETIRED, DESTROYED):
                out.append(f'static {NAMES[m]} {INTENT_NAMES[intent[m]]}')
        return out

    # -- transitions -------------------------------------------------------------------------
    def successors(self, state):
        """[(label, new state, transition violations)]"""
        out = []
        s0 = state

        def emit(label, s, violations=()):
            new = tuple(s)
            if new != s0:
                out.append((label, new, list(violations)))

        up, intent, budget = s0[UP], s0[INTENT], s0[BUDGET]
        # Foreground publisher.
        if not s0[PREPARING] and s0[LEFT] > 0:
            s = list(s0)
            s[PREPARING] = True
            s[LEFT] -= 1
            s[ASSIGN_NEXT] = sum(bit(m) for m in self.members if intent[m] in ROSTER)
            emit('prepare', s)
        if s0[PREPARING] and s0[ASSIGN_NEXT] & bit(ARCHIVE):
            # Activation, for each set of assigned members whose candidate
            # warmed in time (the archive owner and at least one replica): the
            # running ones activate; a prepared one that died keeps its warm
            # candidate for the reconciler; without a running quorum the
            # router is withdrawn.
            assigned = [m for m in self.recent if s0[ASSIGN_NEXT] & bit(m)]
            for k in range(1, 1 << len(assigned)):
                prepared = bit(ARCHIVE) | sum(bit(m) for i, m in enumerate(assigned) if k >> i & 1)
                ready = prepared & up
                s = list(s0)
                s[PREPARING] = False
                if ready & bit(ARCHIVE) and ready & ~bit(ARCHIVE):
                    s[ATT] = ready
                    s[PREP_PUB] = prepared & ~up
                    s[ASSIGN_PUB] = s0[ASSIGN_NEXT]
                    s[WITHDRAWN] = False
                    s[ROUTED] = ready
                    self.bump(s)
                    self.render(s)
                    emit('activate', s)
                else:
                    s[WITHDRAWN] = True
                    s[RENDERED] = 0
                    self.bump(s)
                    emit('activation failed: withdraw', s)
        if not s0[REORGED]:
            s = list(s0)
            s[REORGED] = True
            self.bump(s)
            emit('reorg: publication kept', s)
            s = list(s0)
            s[REORGED] = True
            s[WITHDRAWN] = True
            s[RENDERED] = 0
            s[PREPARING] = False
            s[ATT] = s0[ATT] & ~up  # running workers revoke; a stopped one keeps a stale claim
            s[PREP_PUB] = s0[PREP_PUB] & ~up
            self.bump(s)
            emit('reorg: withdraw and revoke', s)
        # Workers.
        for m in self.members:
            name = NAMES[m]
            if up & bit(m):
                if (m != ARCHIVE and not s0[WITHDRAWN] and intent[m] in ROSTER and s0[ASSIGN_PUB] & bit(m)
                        and not s0[ATT] & bit(m) and not s0[PREP_PUB] & bit(m)):
                    s = list(s0)
                    s[PREP_PUB] |= bit(m)
                    emit(f'catch up {name}', s)
                if budget[WORKER_CRASHES] > 0:
                    s = list(s0)
                    s[UP] &= ~bit(m)
                    s[BUDGET] = (budget[0] - 1, budget[1], budget[2])
                    emit(f'crash {name}', s)
            elif s0[BOOTED] & bit(m) and not s0[STOPPED] & bit(m) and intent[m] not in (RETIRED, DESTROYED):
                s = list(s0)
                s[UP] |= bit(m)
                emit(f'restart {name}', s)
        # Reconciler (idle while withdrawn). One observation in flight at a
        # time: applies of different members commute (each depends only on its
        # own observation, the routed set is a union and the router a function
        # of it), so this loses no reachable routing state.
        if not s0[WITHDRAWN]:
            in_flight = any(p != NONE for p in s0[PROBE])
            for m in self.recent:
                name = NAMES[m]
                observed = s0[PROBE][m]
                if observed == NONE and not in_flight and intent[m] in ROSTER:
                    if up & bit(m) and s0[ATT] & bit(m):
                        observation = VALID
                    elif up & bit(m) and s0[PREP_PUB] & bit(m):
                        observation = PREPARED
                    else:
                        observation = INVALID
                    s = list(s0)
                    probes = list(s0[PROBE])
                    probes[m] = observation
                    s[PROBE] = tuple(probes)
                    emit(f'probe {name}', s)
                if observed != NONE:
                    emit(f'apply {name}', self.apply(s0, m, observed))
            if s0[STALE]:
                s = list(s0)
                self.render(s)
                emit('render', s)
        if budget[RECONCILER_CRASHES] > 0 and any(p != NONE for p in s0[PROBE]):
            s = list(s0)
            s[PROBE] = (NONE,) * len(self.members)
            s[BUDGET] = (budget[0], budget[1], budget[2] - 1)
            emit('reconciler crash', s)
        # A drain's 120 s pass while the member is absent from the router.
        op = s0[OP]
        if op is not None:
            kind, target, new = self.script[op[0]]
            if (kind == 'scale_in' and not s0[DRAIN] and not s0[STALE]
                    and not s0[RENDERED] & bit(target)):
                s = list(s0)
                s[DRAIN] = True
                emit('drain time passes', s)
        # Scaler.
        if s0[ISSUED] < len(self.script):
            s = list(s0)
            s[REQ] = s0[ISSUED]
            s[ISSUED] += 1
            emit(f'scaler requests {self.script[s0[ISSUED]]}', s)
        # Actuator.
        if op is None and s0[REQ] >= 0 and not s0[JOURNAL] & bit(s0[REQ]):
            s = list(s0)
            s[JOURNAL] |= bit(s0[REQ])
            s[OP] = (s0[REQ], 0, False, False)
            emit('actuator journals the request', s)
        if op is not None:
            rid, phase, done, fenced = op
            kind = self.script[rid][0]
            if fenced:
                s = list(s0)
                s[OP] = (rid, phase, False, False)
                emit('resolve-apply', s)
            elif not done:
                result = self.effect(s0)
                if result is not None:
                    state, violations, note = result
                    emit(f'effect {kind} {self.phases[kind][phase]}{note}', state, violations)
            elif self.can_advance(s0):
                emit(f'advance {kind} from {self.phases[kind][phase]}', self.advance(s0))
            if budget[ACTUATOR_CRASHES] > 0:
                s = list(s0)
                s[OP] = (rid, phase, False, fenced or self.phases[kind][phase] in APPLY)
                s[BUDGET] = (budget[0], budget[1] - 1, budget[2])
                emit('actuator crash', s)
        return out

    def apply(self, s0, m, observation):
        s = list(s0)
        probes = list(s0[PROBE])
        probes[m] = NONE
        s[PROBE] = tuple(probes)
        if observation == PREPARED:
            # Activation under the routing lock; a failed control changes nothing.
            if not (s0[UP] & bit(m) and s0[PREP_PUB] & bit(m)):
                return s
            s[ATT] |= bit(m)
            s[PREP_PUB] &= ~bit(m)
        routed = s[ROUTED] | bit(m) if observation in (VALID, PREPARED) else s[ROUTED] & ~bit(m)
        if routed != s[ROUTED]:
            s[ROUTED] = routed
            self.render(s)
        return s

    # -- actuator -------------------------------------------------------------------------------
    def finish(self, s):
        s[OP] = None
        s[DRAIN] = False

    def effect(self, s0):
        """(state, violations, note) after the journaled phase's side effect, or None if it must wait."""
        s = list(s0)
        violations = []
        rid, phase, done, fenced = s0[OP]
        kind, target, new = self.script[rid]
        name = self.phases[kind][phase]
        intent = list(s0[INTENT])
        mutation = self.mutation

        def set_intent(m, value):
            intent[m] = value
            s[INTENT] = tuple(intent)
            s[STALE] = True

        if name == 'requested':
            ok = target != ARCHIVE and (target is None or target in self.recent)
            if ok and kind == 'scale_in':
                ok = target >= 3 and intent[target] == ENROLLED and self.others_serving(s0, target) >= 2
            if ok and kind == 'replace':
                ok = intent[target] == ENROLLED
            if ok and kind in ('scale_out', 'replace'):
                ok = new != ARCHIVE and intent[new] == ABSENT
            if not ok:
                self.finish(s)
                return s, violations, ': refused'
        elif kind == 'scale_in':
            v = target
            if name == 'draining':
                set_intent(v, DRAINING)
                s[DRAIN] = False
            elif name == 'stopped':
                # Checked whenever the stop runs, including after a crash.
                if mutation != 'stop-guard-at-entry-only' and self.others_serving(s0, v) < 2:
                    set_intent(v, ENROLLED)  # cancel the drain
                    s[STOPPED] &= ~bit(v)
                    if s0[BOOTED] & bit(v):
                        s[UP] |= bit(v)
                    self.finish(s)
                    return s, violations, ': drain cancelled'
                if self.others_serving(s0, v) < 2:
                    violations.append(f'scale-in stopped {NAMES[v]} with fewer than two others serving')
                s[UP] &= ~bit(v)
                s[STOPPED] |= bit(v)
            elif name == 'retired':
                if s0[RENDERED] & bit(v) and mutation != 'retire-while-routed':
                    return None
                set_intent(v, RETIRED)
            elif name == 'applying':
                if intent[v] not in (RETIRED, DESTROYED):  # a resumed destroy runs again
                    violations.append(f'destroyed {NAMES[v]} before it was retired')
                set_intent(v, DESTROYED)
                s[UP] &= ~bit(v)
        else:
            if name == 'applying':
                if intent[new] == ABSENT:
                    set_intent(new, PROV)
            elif name == 'enrolled':
                if intent[new] == PROV and mutation != 'boot-before-enroll':
                    set_intent(new, ENROLLED)
            elif name == 'installed':
                if intent[new] != ENROLLED:
                    violations.append(f'{NAMES[new]} booted before it was enrolled')
                    set_intent(new, ENROLLED)
                s[UP] |= bit(new)
                s[BOOTED] |= bit(new)
            elif kind == 'replace' and name in ('quarantined', 'stopped', 'destroy-applying'):
                x = target
                if not self.serving(s0, new):
                    if mutation != 'break-before-make':
                        return None  # make before break: wait for the replacement
                    violations.append(f'replace {name} {NAMES[x]} before {NAMES[new]} serves')
                if name == 'quarantined':
                    set_intent(x, QUARANTINED)
                elif name == 'stopped':
                    s[UP] &= ~bit(x)
                    s[STOPPED] |= bit(x)
                else:
                    if intent[x] not in (QUARANTINED, DESTROYED):
                        violations.append(f'destroyed {NAMES[x]} before it was quarantined')
                    set_intent(x, DESTROYED)
                    s[UP] &= ~bit(x)
        if target == ARCHIVE and kind != 'scale_out':
            violations.append('actuator acted on the archive member')
        s[OP] = (rid, phase, True, fenced)
        return s, violations, ''

    def can_advance(self, s):
        rid, phase, done, fenced = s[OP]
        kind, target, new = self.script[rid]
        name = self.phases[kind][phase]
        if kind == 'scale_in' and name == 'drained':
            return s[DRAIN]
        if kind in ('scale_out', 'replace') and name == 'serving':
            return self.serving(s, new)
        return True

    def advance(self, s0):
        s = list(s0)
        rid, phase, done, fenced = s0[OP]
        kind, target, new = self.script[rid]
        phases = self.phases[kind]
        if phase + 1 >= len(phases) or (kind == 'replace' and phases[phase] == 'quarantined' and target in (1, 2)):
            self.finish(s)  # a quarantined static member waits for an operator
            return s
        if (self.mutation == 'stop-guard-at-entry-only' and kind == 'scale_in'
                and phases[phase + 1] == 'stopped' and self.others_serving(s0, target) < 2):
            # The bug: the guard runs once, when the phase is entered.
            intent = list(s0[INTENT])
            intent[target] = ENROLLED
            s[INTENT] = tuple(intent)
            s[STALE] = True
            self.finish(s)
            return s
        s[OP] = (rid, phase + 1, False, fenced)
        return s


def explore(config, mutation=None, goals=None, stop_at_first=False, limit=2_000_000):
    """Breadth-first over every reachable state.

    Returns a dict: `states`, `violations` [(text, trace)], `labels` (every
    transition taken) and `reached` (the names of `goals`, predicates over
    states, that some reachable state satisfies).
    """
    model = Model(config, mutation)
    start = model.initial()
    parents = {start: None}
    queue = deque([start])
    violations, seen, labels, reached = [], set(), set(), set()
    goals = goals or {}

    def trace(state):
        path = []
        while parents[state] is not None:
            state, label = parents[state]
            path.append(label)
        return path[::-1]

    def report(texts, state, label=None):
        for text in texts:
            if text not in seen:
                seen.add(text)
                violations.append((text, trace(state) + ([label] if label else [])))

    def visit(state):
        report(model.check(state), state)
        for name, goal in goals.items():
            if name not in reached and goal(state):
                reached.add(name)

    visit(start)
    while queue and not (stop_at_first and violations):
        state = queue.popleft()
        for label, new, transition_violations in model.successors(state):
            labels.add(label)
            report(transition_violations, state, label)
            if new not in parents:
                parents[new] = (state, label)
                visit(new)
                if len(parents) > limit:
                    raise RuntimeError(f'state space exceeds {limit}')
                queue.append(new)
    return {'states': len(parents), 'violations': violations, 'labels': labels, 'reached': reached}


S, E = ENROLLED, ABSENT
# Budgets: (worker crashes, actuator crashes, reconciler crashes). Reconciler
# crashes add no behaviour in this abstraction (see the module docstring).
CONFIGS = {
    # Grow by one elastic replica, then shrink it again, across three
    # publications, a reorg, a worker crash and an actuator crash.
    'scale-out-then-in': dict(members=4, intent=(S, S, S, E), publications=3, budget=(1, 1, 0),
                              script=(('scale_out', None, 3), ('scale_in', 3, None))),
    # Shrink a serving elastic replica while a worker and the actuator crash.
    'scale-in': dict(members=4, intent=(S, S, S, S), publications=2, budget=(1, 1, 0),
                     script=(('scale_in', 3, None),)),
    # A failed static replica replaced make-before-break; it waits for an operator.
    'replace-static': dict(members=4, intent=(S, S, S, E), publications=3, budget=(1, 1, 0),
                           script=(('replace', 2, 3),)),
    # A failed elastic replica replaced, then destroyed once the replacement serves.
    'replace-elastic': dict(members=5, intent=(S, S, S, S, E), publications=2, budget=(1, 1, 0),
                            script=(('replace', 3, 4),)),
    # Requests the actuator must refuse: archive and static targets, the archive as a new member.
    'hostile-requests': dict(members=4, intent=(S, S, S, E), publications=2, budget=(1, 1, 0),
                             script=(('scale_in', 0, None), ('replace', 0, 3), ('scale_in', 1, None),
                                     ('scale_out', None, 0))),
}


def intent_is(member, value):
    return lambda s: s[INTENT][member] == value


def serving(config, member):
    model = Model(config)
    return lambda s: bool(model.serving(s, member))


# What each configuration must reach, so a clean run is not a vacuous one.
GOALS = {
    'scale-out-then-in': lambda c: {'e serves': serving(c, 3), 'e destroyed': intent_is(3, DESTROYED),
                                    'withdrawn by the reorg': lambda s: s[WITHDRAWN] and s[REORGED]},
    'scale-in': lambda c: {'e destroyed': intent_is(3, DESTROYED)},
    'replace-static': lambda c: {'r2 quarantined': intent_is(2, QUARANTINED), 'e serves': serving(c, 3)},
    'replace-elastic': lambda c: {'e destroyed': intent_is(3, DESTROYED), 'f serves': serving(c, 4)},
    'hostile-requests': lambda c: {'all refused': lambda s: s[ISSUED] == 4 and s[OP] is None
                                   and s[JOURNAL] == 0b1111 and s[INTENT] == c['intent']},
}


# Transitions each configuration must take, beyond its goals.
LABELS = {
    'scale-out-then-in': ('actuator crash', 'resolve-apply', 'reorg: withdraw and revoke', 'reorg: publication kept',
                          'activation failed: withdraw', 'apply e', 'effect scale_in stopped'),
    'scale-in': ('effect scale_in stopped: drain cancelled', 'effect scale_in retired', 'resolve-apply'),
    'replace-static': ('effect replace quarantined', 'resolve-apply'),
    'replace-elastic': ('effect replace destroy-applying', 'effect replace stopped'),
    'hostile-requests': ('effect scale_in requested: refused', 'effect replace requested: refused',
                         'effect scale_out requested: refused'),
}

# Deliberate protocol bugs, each with the configuration that exposes it.
MUTANTS = {
    'stop-guard-at-entry-only': ('scale-in', None),
    'no-drain-filter': ('scale-in', None),
    'drain-filter-always': ('scale-in', dict(budget=(2, 0, 0), publications=1)),
    'no-generation-check': ('scale-out-then-in', None),
    'break-before-make': ('replace-static', None),
    'boot-before-enroll': ('scale-out-then-in', None),
    'destroy-before-retire': ('scale-in', None),
}


def run_task(task):
    """One configuration (`('config', name)`) or mutant (`('mutant', name)`), summarised."""
    kind, name = task
    if kind == 'config':
        config = CONFIGS[name]
        result = explore(config, goals=GOALS[name](config))
    else:
        base, changes = MUTANTS[name]
        result = explore({**CONFIGS[base], **(changes or {})}, name, stop_at_first=True)
    return {'task': task, 'states': result['states'], 'violations': result['violations'][:3],
            'labels': sorted(result['labels']), 'reached': sorted(result['reached'])}
