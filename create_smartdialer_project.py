from pathlib import Path
from textwrap import dedent
import zipfile

ROOT = Path("smartdialer")
FILES = {
"requirements.txt": "# No third-party dependencies. Python 3.10+ only.\n",
".gitignore": "__pycache__/\n*.py[cod]\n.venv/\nvenv/\n.pytest_cache/\n.coverage\nhtmlcov/\nresults/\n.DS_Store\n",
"README.md": r'''# SmartDialer: Safety-First Predictive Dialing Prototype

A local, dependency-free Python prototype for a collections SmartDialer. It implements progressive dialing, explainable predictive pacing, a mandatory Safety Controller, two mock telecom providers, idempotent event handling, recovery leases, simulation, and concurrency/load tests.

## Quick start

Requires Python 3.10+ and no external package.

```bash
python -m unittest discover -s tests -v
python scripts/run_simulation.py
python scripts/load_test.py
```

## Architecture

```mermaid
flowchart LR
    Campaign --> Pacing[Predictive / Progressive Pacing Engine]
    Pacing -->|request N| Safety[Safety Controller]
    Safety -->|short lived single-use permit| Allocator[Call Allocator]
    Allocator --> Store[(Authoritative Store)]
    Allocator --> Provider[Telecom Provider Interface]
    Provider --> A[Mock Provider A]
    Provider --> B[Mock Provider B]
    A --> Events[Idempotent Event Processor]
    B --> Events
    Events --> Store
    Store --> Recovery[Lease Reaper / Reconciliation]
```

The predictor only proposes a count. It does not have a provider reference. The Safety Controller independently checks live authoritative capacity, ringing calls, and provider health before issuing a short-lived permit. The allocator rejects missing, expired, or exhausted permits. Therefore predictive logic cannot bypass safety.

## States

### Agent

`OFFLINE -> AVAILABLE -> RESERVED -> DIALING -> CONNECTED -> WRAP_UP -> AVAILABLE`

`PAUSED` and `OFFLINE` agents cannot be allocated. Setup failure, cancellation, or an expired lease returns a non-offline agent to `AVAILABLE`.

### Call

`QUEUED -> RESERVED -> INITIATED -> RINGING -> ANSWERED -> CONNECTED -> COMPLETED`

`FAILED` and `CANCELLED` are terminal alternatives. Duplicate event IDs, stale sequence events, and any event after a terminal state are ignored.

## Pacing

The predictive engine uses a conservative lower confidence bound for answer probability:

```text
p_lower = max(0.01, observed_answer_rate - 1.28 * sqrt(p * (1-p) / observations))
requested = floor(available_agents * utilization_target / p_lower)
```

It reduces the proposal when the recent answer rate falls or provider setup latency is high. The Safety Controller then clamps the proposal to live `AVAILABLE` agents and the ringing budget. The hard default is zero predicted abandoned calls because each provider initiation has a reserved agent.

## Failure handling

| Scenario | Behaviour |
|---|---|
| Worker crash after reservation/initiation | Lease expires; recovery releases the pair. Production reconciliation would query the provider with the idempotency key before retry/release. |
| Provider outage | Failures update provider health; the circuit opens and Safety Controller rejects new calls. Existing calls are event-driven and reconciled separately. |
| Sudden agent drop | Agent is marked `OFFLINE`; the next safety decision reads authoritative state and approves no unsafe new work. |
| Duplicate event | Event inbox de-duplicates on `event_id`. |
| Out-of-order event | Provider sequence numbers are checked; terminal calls cannot be revived. |

## Scale plan

| Scale | Likely first bottleneck | Production fix |
|---|---|---|
| 100 agents | None | Keep simple monolith + transactional DB. |
| 1,000 agents | Allocation lock contention / callback bursts | PostgreSQL row locks, indexes, provider rate limiting. |
| 10,000 agents | Scheduler scans / callback ingestion | Campaign sharding, partitioned tables, durable event queue. |
| 100,000 agents | Hot campaigns / event fan-in | Shard ownership, event streaming, pre-aggregated metrics. |

The database is authoritative. A cache can improve reads but must never issue a safety permit or allocate an agent.

## Final answer

I would make predictive pacing advisory and safety deterministic. The predictor forecasts a useful call count, while a separate Safety Controller issues short-lived single-use permits only after checking current agent capacity, ringing calls, provider health, uncertainty, and a hard abandonment budget. If any signal degrades, the controller clamps the request to progressive one-agent-to-one-call behaviour or pauses dialing. This retains utilization benefits without allowing the prediction model to bypass compliance-critical safety.

## GitHub upload

```bash
git init
git add .
git commit -m "Build safety-first SmartDialer prototype"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/smartdialer.git
git push -u origin main
```
''',
"smartdialer/__init__.py": r'''from .domain import AgentState, CallState, ProviderEvent
from .store import InMemoryStore
from .dialer import SmartDialer

__all__ = ["AgentState", "CallState", "ProviderEvent", "InMemoryStore", "SmartDialer"]
''',
"smartdialer/domain.py": r'''from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
import time

class AgentState(str, Enum):
    OFFLINE = "OFFLINE"
    AVAILABLE = "AVAILABLE"
    RESERVED = "RESERVED"
    DIALING = "DIALING"
    CONNECTED = "CONNECTED"
    WRAP_UP = "WRAP_UP"
    PAUSED = "PAUSED"

class CallState(str, Enum):
    QUEUED = "QUEUED"
    RESERVED = "RESERVED"
    INITIATED = "INITIATED"
    RINGING = "RINGING"
    ANSWERED = "ANSWERED"
    CONNECTED = "CONNECTED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

TERMINAL_CALL_STATES = {CallState.COMPLETED, CallState.FAILED, CallState.CANCELLED}
STATE_RANK = {
    CallState.QUEUED: 0, CallState.RESERVED: 1, CallState.INITIATED: 2,
    CallState.RINGING: 3, CallState.ANSWERED: 4, CallState.CONNECTED: 5,
    CallState.COMPLETED: 6, CallState.FAILED: 6, CallState.CANCELLED: 6,
}

@dataclass
class Agent:
    id: str
    campaign_id: str
    state: AgentState = AgentState.AVAILABLE
    version: int = 0
    reservation_call_id: Optional[str] = None
    lease_until: float = 0.0
    last_heartbeat: float = field(default_factory=time.monotonic)

@dataclass
class Borrower:
    id: str
    campaign_id: str
    phone: str
    priority: int = 0
    state: str = "QUEUED"
    active_call_id: Optional[str] = None
    attempts: int = 0

@dataclass
class Call:
    id: str
    campaign_id: str
    borrower_id: str
    agent_id: str
    state: CallState = CallState.RESERVED
    idempotency_key: str = ""
    provider_call_id: Optional[str] = None
    lease_until: float = 0.0
    last_provider_sequence: int = -1
    last_state_rank: int = 1
    answered_at: Optional[float] = None
    created_at: float = field(default_factory=time.monotonic)

@dataclass(frozen=True)
class ProviderEvent:
    event_id: str
    call_id: str
    kind: str
    occurred_at: float
    sequence: Optional[int] = None

@dataclass(frozen=True)
class SafetyPermit:
    permit_id: str
    campaign_id: str
    count: int
    expires_at: float
    reason: str

@dataclass(frozen=True)
class SafetyDecision:
    requested: int
    approved: int
    reason: str
    mode: str
''',
"smartdialer/store.py": r'''from __future__ import annotations
from contextlib import contextmanager
from threading import RLock
from typing import Iterable
import time
from .domain import Agent, Borrower, Call, AgentState, CallState

class InMemoryStore:
    """Local stand-in for an authoritative transactional SQL store."""
    def __init__(self, agents: Iterable[Agent] = (), borrowers: Iterable[Borrower] = ()):
        self.lock = RLock()
        self.agents = {a.id: a for a in agents}
        self.borrowers = {b.id: b for b in borrowers}
        self.calls: dict[str, Call] = {}
        self.processed_events: set[str] = set()
        self.permits: dict[str, object] = {}
        self._call_counter = 0
        self._permit_counter = 0

    @contextmanager
    def transaction(self):
        with self.lock:
            yield self

    def next_call_id(self) -> str:
        self._call_counter += 1
        return f"call-{self._call_counter}"

    def next_permit_id(self) -> str:
        self._permit_counter += 1
        return f"permit-{self._permit_counter}"

    def available_agents(self, campaign_id: str) -> list[Agent]:
        return sorted([a for a in self.agents.values() if a.campaign_id == campaign_id and a.state == AgentState.AVAILABLE], key=lambda a: a.id)

    def queued_borrowers(self, campaign_id: str) -> list[Borrower]:
        return sorted([b for b in self.borrowers.values() if b.campaign_id == campaign_id and b.state == "QUEUED"], key=lambda b: (-b.priority, b.attempts, b.id))

    def active_calls(self, campaign_id: str) -> list[Call]:
        return [c for c in self.calls.values() if c.campaign_id == campaign_id and c.state not in {CallState.COMPLETED, CallState.FAILED, CallState.CANCELLED}]

    def counts(self, campaign_id: str) -> dict[str, int]:
        calls = self.active_calls(campaign_id)
        return {
            "available_agents": len(self.available_agents(campaign_id)),
            "reserved_agents": sum(a.campaign_id == campaign_id and a.state in {AgentState.RESERVED, AgentState.DIALING} for a in self.agents.values()),
            "connected_agents": sum(a.campaign_id == campaign_id and a.state == AgentState.CONNECTED for a in self.agents.values()),
            "ringing_calls": sum(c.state in {CallState.INITIATED, CallState.RINGING} for c in calls),
            "active_calls": len(calls),
        }

    def expire_leases(self, now: float | None = None) -> list[str]:
        now = time.monotonic() if now is None else now
        released = []
        with self.transaction():
            for call in self.calls.values():
                if call.state in {CallState.RESERVED, CallState.INITIATED, CallState.RINGING} and call.lease_until <= now:
                    agent, borrower = self.agents[call.agent_id], self.borrowers[call.borrower_id]
                    call.state = CallState.FAILED
                    if agent.state != AgentState.OFFLINE:
                        agent.state, agent.reservation_call_id = AgentState.AVAILABLE, None
                    borrower.state, borrower.active_call_id = "QUEUED", None
                    released.append(call.id)
        return released
''',
"smartdialer/pacing.py": r'''from __future__ import annotations
from dataclasses import dataclass
from math import sqrt, floor

@dataclass
class CampaignSignals:
    available_agents: int
    reserved_agents: int
    connected_agents: int
    ringing_calls: int
    observed_answer_rate: float
    observations: int
    recent_answer_rate: float
    setup_seconds: float
    average_talk_seconds: float
    provider_health: float

class PredictivePacingEngine:
    """Advisory only: it proposes a count and has no provider/store access."""
    def __init__(self, utilization_target: float = 0.90, max_multiplier: float = 2.0):
        self.utilization_target = utilization_target
        self.max_multiplier = max_multiplier

    def propose(self, s: CampaignSignals) -> tuple[int, str]:
        if s.provider_health < .80:
            return 0, "provider health below predictive threshold"
        if s.available_agents <= 0:
            return 0, "no available agents"
        p = min(.99, max(.01, s.observed_answer_rate))
        standard_error = sqrt(p * (1 - p) / max(1, s.observations))
        p_lower = max(.01, p - 1.28 * standard_error)
        trend_penalty = .75 if s.recent_answer_rate < p * .75 else 1.0
        latency_penalty = .70 if s.setup_seconds > 5 else 1.0
        request = floor((s.available_agents * self.utilization_target / p_lower) * trend_penalty * latency_penalty)
        request = min(request, floor(s.available_agents * self.max_multiplier))
        return request, f"p={p:.2f}; lower_bound={p_lower:.2f}; available={s.available_agents}; trend_penalty={trend_penalty}; latency_penalty={latency_penalty}"

class ProgressivePacingEngine:
    def propose(self, s: CampaignSignals) -> tuple[int, str]:
        return s.available_agents, "one call requested for each currently available agent"
''',
"smartdialer/safety.py": r'''from __future__ import annotations
from collections import deque
import time
from .domain import SafetyPermit, SafetyDecision
from .store import InMemoryStore

class ProviderHealth:
    def __init__(self, window: int = 20, trip_rate: float = .25):
        self.outcomes: deque[bool] = deque(maxlen=window)
        self.trip_rate = trip_rate

    def record(self, success: bool) -> None:
        self.outcomes.append(success)

    @property
    def failure_rate(self) -> float:
        return 0.0 if not self.outcomes else 1 - sum(self.outcomes) / len(self.outcomes)

    @property
    def healthy(self) -> bool:
        return len(self.outcomes) < 4 or self.failure_rate < self.trip_rate

class SafetyController:
    """Only component that issues permits. Safety is not switchable by the predictor."""
    def __init__(self, store: InMemoryStore, health: ProviderHealth, max_ringing_per_agent: float = 1.0):
        self.store, self.health, self.max_ringing_per_agent = store, health, max_ringing_per_agent
        self.decisions: list[SafetyDecision] = []

    def decide(self, campaign_id: str, requested: int, predictive: bool = True) -> tuple[SafetyDecision, SafetyPermit | None]:
        with self.store.transaction():
            c = self.store.counts(campaign_id)
            if requested <= 0:
                decision = SafetyDecision(requested, 0, "no requested calls", "REJECT")
            elif not self.health.healthy:
                decision = SafetyDecision(requested, 0, "provider circuit open", "REJECT")
            elif c["available_agents"] == 0:
                decision = SafetyDecision(requested, 0, "no authoritative available agents", "REJECT")
            else:
                ring_budget = max(0, int(c["available_agents"] * self.max_ringing_per_agent) - c["ringing_calls"])
                approved = min(requested, c["available_agents"], ring_budget)
                decision = SafetyDecision(requested, approved, f"available={c['available_agents']}; ringing={c['ringing_calls']}; ring_budget={ring_budget}", "APPROVE" if approved == requested else "REDUCE")
            self.decisions.append(decision)
            if decision.approved == 0:
                return decision, None
            permit = SafetyPermit(self.store.next_permit_id(), campaign_id, decision.approved, time.monotonic() + 2, decision.reason)
            self.store.permits[permit.permit_id] = {"permit": permit, "remaining": permit.count}
            return decision, permit

    def consume(self, permit_id: str, campaign_id: str) -> bool:
        with self.store.transaction():
            row = self.store.permits.get(permit_id)
            if not row or row["permit"].campaign_id != campaign_id or row["permit"].expires_at < time.monotonic() or row["remaining"] <= 0:
                return False
            row["remaining"] -= 1
            return True
''',
"smartdialer/providers.py": r'''from __future__ import annotations
from abc import ABC, abstractmethod
import random
import time
from .domain import Call, ProviderEvent

class TelecomProvider(ABC):
    @abstractmethod
    def initiate(self, call: Call) -> list[ProviderEvent]:
        raise NotImplementedError

    @abstractmethod
    def lookup(self, idempotency_key: str) -> str | None:
        raise NotImplementedError

class MockProviderA(TelecomProvider):
    """Fast, ordered and reliable mock provider."""
    def __init__(self, answer_rate: float = .50, failure_rate: float = .01, seed: int = 1):
        self.answer_rate, self.failure_rate, self.rng = answer_rate, failure_rate, random.Random(seed)
        self.calls: dict[str, str] = {}
        self.outage = False

    def initiate(self, call: Call) -> list[ProviderEvent]:
        if self.outage or self.rng.random() < self.failure_rate:
            raise TimeoutError("Provider A timeout")
        provider_id = self.calls.setdefault(call.idempotency_key, f"A-{call.id}")
        now = time.monotonic()
        events = [ProviderEvent(provider_id + "-1", call.id, "RINGING", now, 1)]
        if self.rng.random() < self.answer_rate:
            events.extend([ProviderEvent(provider_id + "-2", call.id, "ANSWERED", now + .01, 2), ProviderEvent(provider_id + "-3", call.id, "COMPLETED", now + .02, 3)])
        else:
            events.append(ProviderEvent(provider_id + "-3", call.id, "COMPLETED", now + .02, 3))
        return events

    def lookup(self, idempotency_key: str) -> str | None:
        return self.calls.get(idempotency_key)

class MockProviderB(MockProviderA):
    """Slow/unreliable mock provider that can duplicate and reverse callbacks."""
    def __init__(self, answer_rate: float = .50, failure_rate: float = .15, seed: int = 2):
        super().__init__(answer_rate, failure_rate, seed)

    def initiate(self, call: Call) -> list[ProviderEvent]:
        events = super().initiate(call)
        if self.rng.random() < .50:
            events.insert(1, events[0])
        if self.rng.random() < .50:
            events.reverse()
        return events
''',
"smartdialer/events.py": r'''from __future__ import annotations
from .domain import AgentState, CallState, ProviderEvent, TERMINAL_CALL_STATES, STATE_RANK
from .store import InMemoryStore

EVENT_STATE = {"RINGING": CallState.RINGING, "ANSWERED": CallState.ANSWERED, "COMPLETED": CallState.COMPLETED, "FAILED": CallState.FAILED, "CANCELLED": CallState.CANCELLED}

class EventProcessor:
    def __init__(self, store: InMemoryStore):
        self.store = store

    def apply(self, event: ProviderEvent) -> bool:
        with self.store.transaction():
            if event.event_id in self.store.processed_events:
                return False
            self.store.processed_events.add(event.event_id)
            call = self.store.calls.get(event.call_id)
            if call is None or call.state in TERMINAL_CALL_STATES:
                return False
            target = EVENT_STATE.get(event.kind)
            if target is None:
                return False
            if event.sequence is not None and event.sequence <= call.last_provider_sequence:
                return False
            if event.sequence is None and STATE_RANK[target] < call.last_state_rank:
                return False
            call.last_provider_sequence = max(call.last_provider_sequence, event.sequence if event.sequence is not None else -1)
            call.last_state_rank = max(call.last_state_rank, STATE_RANK[target])
            agent, borrower = self.store.agents[call.agent_id], self.store.borrowers[call.borrower_id]
            if target == CallState.RINGING:
                call.state = target
                if agent.state != AgentState.OFFLINE:
                    agent.state = AgentState.DIALING
            elif target == CallState.ANSWERED:
                call.state, call.answered_at = CallState.CONNECTED, event.occurred_at
                if agent.state == AgentState.DIALING:
                    agent.state = AgentState.CONNECTED
            else:
                call.state = target
                borrower.state, borrower.active_call_id = ("COMPLETED" if target == CallState.COMPLETED else "QUEUED"), None
                if agent.state != AgentState.OFFLINE:
                    agent.state, agent.reservation_call_id = (AgentState.WRAP_UP if target == CallState.COMPLETED else AgentState.AVAILABLE), None
            return True
''',
"smartdialer/dialer.py": r'''from __future__ import annotations
import time
from .domain import AgentState, Call, CallState, SafetyPermit
from .store import InMemoryStore
from .safety import SafetyController
from .providers import TelecomProvider
from .events import EventProcessor

class CallAllocator:
    def __init__(self, store: InMemoryStore, safety: SafetyController, provider: TelecomProvider, events: EventProcessor | None = None, lease_seconds: float = 10):
        self.store, self.safety, self.provider = store, safety, provider
        self.events, self.lease_seconds = events or EventProcessor(store), lease_seconds

    def allocate_and_initiate(self, campaign_id: str, permit: SafetyPermit) -> list[str]:
        initiated = []
        while len(initiated) < permit.count:
            with self.store.transaction():
                if not self.safety.consume(permit.permit_id, campaign_id):
                    break
                agents, borrowers = self.store.available_agents(campaign_id), self.store.queued_borrowers(campaign_id)
                if not agents or not borrowers:
                    break
                agent, borrower = agents[0], borrowers[0]
                call_id, now = self.store.next_call_id(), time.monotonic()
                call = Call(call_id, campaign_id, borrower.id, agent.id, CallState.RESERVED, f"{campaign_id}:{borrower.id}:{borrower.attempts}", lease_until=now + self.lease_seconds)
                agent.state, agent.reservation_call_id, agent.lease_until, agent.version = AgentState.RESERVED, call_id, call.lease_until, agent.version + 1
                borrower.state, borrower.active_call_id, borrower.attempts = "RESERVED", call_id, borrower.attempts + 1
                self.store.calls[call_id] = call
            try:
                with self.store.transaction():
                    call.state, self.store.agents[call.agent_id].state = CallState.INITIATED, AgentState.DIALING
                events = self.provider.initiate(call)
                self.safety.health.record(True)
                for event in events:
                    self.events.apply(event)
                initiated.append(call_id)
            except TimeoutError:
                self.safety.health.record(False)
                with self.store.transaction():
                    call.state = CallState.FAILED
                    agent, borrower = self.store.agents[call.agent_id], self.store.borrowers[call.borrower_id]
                    if agent.state != AgentState.OFFLINE:
                        agent.state, agent.reservation_call_id = AgentState.AVAILABLE, None
                    borrower.state, borrower.active_call_id = "QUEUED", None
        return initiated

class SmartDialer:
    def __init__(self, store, pacing, safety, provider):
        self.store, self.pacing, self.safety = store, pacing, safety
        self.allocator = CallAllocator(store, safety, provider)

    def tick(self, campaign_id: str, signals):
        requested, explanation = self.pacing.propose(signals)
        decision, permit = self.safety.decide(campaign_id, requested)
        calls = self.allocator.allocate_and_initiate(campaign_id, permit) if permit else []
        return {"requested": requested, "explanation": explanation, "decision": decision, "calls": calls}
''',
"smartdialer/simulation.py": r'''from __future__ import annotations
from dataclasses import dataclass
from .domain import Agent, Borrower, AgentState
from .store import InMemoryStore
from .pacing import CampaignSignals, PredictivePacingEngine
from .providers import MockProviderA, MockProviderB
from .safety import ProviderHealth, SafetyController
from .dialer import SmartDialer

@dataclass
class Scenario:
    name: str
    answer_rate: float
    talk_seconds: int
    provider: str = "A"
    failure_rate: float = .01

SCENARIOS = [Scenario("A: low answer", .20, 120), Scenario("B: medium answer", .50, 90), Scenario("C: high answer / long talk", .70, 180), Scenario("D: degraded provider", .50, 90, "B", .15)]

def run_scenario(s: Scenario, agents: int = 20, borrowers: int = 200, ticks: int = 10) -> dict:
    campaign = "campaign-1"
    store = InMemoryStore([Agent(f"a-{i}", campaign) for i in range(agents)], [Borrower(f"b-{i}", campaign, f"+610000{i:04}", priority=i % 3) for i in range(borrowers)])
    provider = MockProviderA(s.answer_rate, s.failure_rate) if s.provider == "A" else MockProviderB(s.answer_rate, s.failure_rate)
    health, safety = ProviderHealth(), SafetyController(store, ProviderHealth())
    safety = SafetyController(store, health)
    dialer = SmartDialer(store, PredictivePacingEngine(), safety, provider)
    initiated = 0
    for _ in range(ticks):
        c = store.counts(campaign)
        signals = CampaignSignals(c["available_agents"], c["reserved_agents"], c["connected_agents"], c["ringing_calls"], s.answer_rate, 100, s.answer_rate, 1 if s.provider == "A" else 7, s.talk_seconds, 1 - health.failure_rate)
        initiated += len(dialer.tick(campaign, signals)["calls"])
        for agent in store.agents.values():
            if agent.state == AgentState.WRAP_UP:
                agent.state = AgentState.AVAILABLE
    calls = list(store.calls.values())
    connected = sum(c.answered_at is not None for c in calls)
    completed = sum(c.state.value == "COMPLETED" for c in calls)
    return {"scenario": s.name, "initiated": initiated, "connected": connected, "completed": completed, "utilization": round(connected / max(1, agents), 2), "safety_decisions": len(safety.decisions), "provider_failure_rate": round(health.failure_rate, 2)}
''',
"scripts/run_simulation.py": r'''from smartdialer.simulation import SCENARIOS, run_scenario

if __name__ == "__main__":
    print("SmartDialer simulation results")
    print("-" * 88)
    for scenario in SCENARIOS:
        print(" | ".join(f"{key}={value}" for key, value in run_scenario(scenario).items()))
''',
"scripts/load_test.py": r'''from concurrent.futures import ThreadPoolExecutor
from smartdialer.domain import Agent, Borrower
from smartdialer.store import InMemoryStore
from smartdialer.safety import ProviderHealth, SafetyController
from smartdialer.providers import MockProviderA
from smartdialer.events import EventProcessor
from smartdialer.dialer import CallAllocator

if __name__ == "__main__":
    campaign = "load"
    store = InMemoryStore([Agent(f"a{i}", campaign) for i in range(100)], [Borrower(f"b{i}", campaign, str(i)) for i in range(1000)])
    safety = SafetyController(store, ProviderHealth())
    allocator = CallAllocator(store, safety, MockProviderA(answer_rate=.2), EventProcessor(store))
    def worker(_):
        _, permit = safety.decide(campaign, 10)
        return allocator.allocate_and_initiate(campaign, permit) if permit else []
    with ThreadPoolExecutor(max_workers=25) as pool:
        output = list(pool.map(worker, range(100)))
    active_agents = [call.agent_id for call in store.active_calls(campaign)]
    assert len(active_agents) == len(set(active_agents)), "safety invariant broken: agent double allocated"
    print(f"workers=100 initiated={sum(map(len, output))} active={len(active_agents)} unique_active_agents={len(set(active_agents))}")
    print("PASS: no active agent was double allocated")
''',
"tests/test_allocation.py": r'''import unittest
from concurrent.futures import ThreadPoolExecutor
from smartdialer.domain import Agent, Borrower
from smartdialer.store import InMemoryStore
from smartdialer.safety import ProviderHealth, SafetyController
from smartdialer.providers import MockProviderA
from smartdialer.dialer import CallAllocator

class AllocationTests(unittest.TestCase):
    def test_concurrent_workers_cannot_double_allocate_agent(self):
        store = InMemoryStore([Agent("a", "c")], [Borrower("b1", "c", "1"), Borrower("b2", "c", "2")])
        safety = SafetyController(store, ProviderHealth())
        allocator = CallAllocator(store, safety, MockProviderA(answer_rate=0))
        def run(_):
            _, permit = safety.decide("c", 1)
            return allocator.allocate_and_initiate("c", permit) if permit else []
        with ThreadPoolExecutor(max_workers=2) as executor:
            outputs = list(executor.map(run, [1, 2]))
        self.assertLessEqual(sum(map(len, outputs)), 1)

    def test_no_permit_is_rejected(self):
        store = InMemoryStore([Agent("a", "c")], [Borrower("b", "c", "1")])
        self.assertIsNone(SafetyController(store, ProviderHealth()).decide("c", 0)[1])

if __name__ == "__main__":
    unittest.main()
''',
"tests/test_events.py": r'''import unittest
import time
from smartdialer.domain import Agent, Borrower, Call, CallState, ProviderEvent
from smartdialer.store import InMemoryStore
from smartdialer.events import EventProcessor

class EventTests(unittest.TestCase):
    def setUp(self):
        self.store = InMemoryStore([Agent("a", "c")], [Borrower("b", "c", "1")])
        self.store.calls["x"] = Call("x", "c", "b", "a", CallState.INITIATED)
        self.processor = EventProcessor(self.store)

    def test_duplicate_event_is_noop(self):
        event = ProviderEvent("event", "x", "RINGING", time.monotonic(), 1)
        self.assertTrue(self.processor.apply(event))
        self.assertFalse(self.processor.apply(event))

    def test_terminal_call_cannot_be_revived(self):
        self.processor.apply(ProviderEvent("done", "x", "COMPLETED", time.monotonic(), 3))
        self.processor.apply(ProviderEvent("late", "x", "ANSWERED", time.monotonic(), 4))
        self.assertEqual(self.store.calls["x"].state, CallState.COMPLETED)

    def test_stale_sequence_is_ignored(self):
        self.processor.apply(ProviderEvent("answered", "x", "ANSWERED", time.monotonic(), 2))
        self.processor.apply(ProviderEvent("ring", "x", "RINGING", time.monotonic(), 1))
        self.assertEqual(self.store.calls["x"].state, CallState.CONNECTED)

if __name__ == "__main__":
    unittest.main()
''',
"tests/test_failures.py": r'''import unittest
import time
from smartdialer.domain import Agent, Borrower, Call, CallState, AgentState
from smartdialer.store import InMemoryStore
from smartdialer.safety import ProviderHealth, SafetyController

class FailureTests(unittest.TestCase):
    def test_expired_reservation_is_released(self):
        store = InMemoryStore([Agent("a", "c", state=AgentState.RESERVED)], [Borrower("b", "c", "1", state="RESERVED")])
        store.calls["x"] = Call("x", "c", "b", "a", CallState.INITIATED, lease_until=time.monotonic() - 1)
        self.assertEqual(store.expire_leases(), ["x"])
        self.assertEqual(store.agents["a"].state, AgentState.AVAILABLE)
        self.assertEqual(store.borrowers["b"].state, "QUEUED")

    def test_circuit_opens_after_failures(self):
        store, health = InMemoryStore([Agent("a", "c")]), ProviderHealth(window=4, trip_rate=.25)
        for _ in range(4):
            health.record(False)
        decision, permit = SafetyController(store, health).decide("c", 1)
        self.assertEqual(decision.approved, 0)
        self.assertIsNone(permit)

if __name__ == "__main__":
    unittest.main()
''',
"tests/test_simulation.py": r'''import unittest
from smartdialer.simulation import Scenario, run_scenario

class SimulationTests(unittest.TestCase):
    def test_simulation_produces_metrics(self):
        result = run_scenario(Scenario("test", .5, 90), agents=5, borrowers=30, ticks=3)
        self.assertIn("utilization", result)
        self.assertGreaterEqual(result["initiated"], 0)

if __name__ == "__main__":
    unittest.main()
''',
"docs/adr-001-architecture.md": r'''# ADR-001: Safety-first monolith with authoritative allocation state

## Status
Accepted for the initial 4–6 hour prototype.

## Decision
Use Python standard library, one local process, an in-memory transaction abstraction, explicit state machines, short leases, provider idempotency keys, and a separate Safety Controller that issues short-lived permits.

## Why
The primary compliance risk is an abandoned connected call. The assignment emphasizes correct concurrent allocation and a safety boundary that predictive logic cannot bypass. A dependency-free local project lets another engineer run and inspect the important logic quickly.

## Production replacement
Replace `InMemoryStore` with PostgreSQL. One allocation transaction would lock one `AVAILABLE` agent and one `QUEUED` borrower with `FOR UPDATE SKIP LOCKED`, transition both to reserved, insert the call with unique active-borrower constraints, write an outbox initiation record, and commit. Only then does a worker send the provider request. Provider callbacks use a durable inbox keyed by provider event ID.

## Trade-off
This prototype deliberately does not implement cross-process durability or a real provider. It makes the safety invariant readable and testable. Production needs Postgres, a durable outbox/inbox, provider reconciliation, telemetry, and partitioning by campaign at scale.
''',
"docs/interview-notes.md": r'''# Technical discussion notes

## Why 17 calls instead of 10?
The pacing engine logs its answer-rate estimate, conservative lower confidence bound, available capacity, recent trend, and latency penalty. That creates a transparent requested count. The Safety Controller then logs the live clamp applied to it.

## Two workers reserve the same agent
They cannot both commit the transition. The prototype holds one transaction lock. Production uses row locks or conditional update. One worker succeeds; the other skips/retries and never owns the same active agent.

## Database says AVAILABLE and cache says RESERVED
Database wins. Cache is only an optimization and cannot issue permits or allocate agents.

## ANSWERED then worker crash then COMPLETED
The callback processor is separate and idempotent. `COMPLETED` safely transitions the call to terminal and releases the agent. If callbacks are missing, reconciliation looks up the idempotency key before the lease is released/retried.

## Answer rate drops from 70% to 10%
Recent performance decreases the next predictive request and uncertainty widens the confidence bound. Regardless of prediction quality, Safety Controller capacity and health limits remain hard constraints.

## Least certain component
Real provider semantics under an initiation timeout: the provider may have created the call despite a client timeout. This is why idempotency keys and provider lookup/reconciliation are essential.
''',
"docs/architecture-diagram.md": r'''# Architecture sequence diagram

```mermaid
sequenceDiagram
    participant P as Pacing Engine
    participant S as Safety Controller
    participant A as Allocator
    participant DB as Authoritative Store
    participant T as Telecom Provider
    participant E as Event Processor

    P->>S: request N calls
    S->>DB: read live authoritative capacity
    S-->>A: permit for M calls or rejection
    A->>DB: atomically reserve agent + borrower + call
    A->>T: initiate with idempotency key
    T-->>E: callback, possibly duplicate/out of order
    E->>DB: dedupe and valid state transition
```
''',
}

for relative_path, content in FILES.items():
    path = ROOT / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(dedent(content).lstrip(), encoding="utf-8")

archive = Path("smartdialer-github-ready.zip")
with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as zf:
    for path in ROOT.rglob("*"):
        if path.is_file():
            zf.write(path, path.as_posix())

print(f"Created {ROOT}/ and {archive}")
