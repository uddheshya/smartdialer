# SmartDialer: Safety-First Predictive Dialing Prototype

A local, dependency-free Python prototype for a collections SmartDialer. It implements progressive dialing, explainable predictive pacing, a mandatory Safety Controller, two mock telecom providers, idempotent event handling, recovery leases, simulation, and concurrency/load tests.

## Project goal

The system improves collections-agent utilization without creating abandoned connected calls. Predictive pacing can suggest how many calls to initiate, but it cannot call the telecom provider directly.

A separate Safety Controller checks real-time capacity and provider health before permitting a call.

## Architecture

```text
Campaign
   |
   v
Pacing Engine (Progressive / Predictive)
   |
   | dial request
   v
Safety Controller
   |
   | short-lived permit
   v
Call Allocator
   |
   v
Telecom Provider Interface
   |
   +--> Mock Provider A: fast and reliable
   |
   +--> Mock Provider B: slower, timeouts, duplicate events,
                         out-of-order events
```

## Safety guarantee

The predictive engine only produces a dial request. It has no direct telecom provider access.

The Safety Controller is the only component allowed to issue a short-lived, single-use permit. It independently checks:

- Current available-agent count
- Calls already ringing or connecting
- Existing agent reservations
- Provider health and failure rate
- Permit expiry
- Ringing-call capacity limit

The Call Allocator rejects any missing, expired, or exhausted permit. This makes the safety boundary non-bypassable.

## Agent state machine

```text
OFFLINE
  -> AVAILABLE
  -> RESERVED
  -> DIALING
  -> CONNECTED
  -> WRAP_UP
  -> AVAILABLE
```

Additional agent states:

```text
PAUSED
OFFLINE
```

Only an `AVAILABLE` agent can be atomically allocated. A setup failure, cancellation, or reservation-lease expiry returns a non-offline agent to `AVAILABLE`.

## Call state machine

```text
QUEUED
  -> RESERVED
  -> INITIATED
  -> RINGING
  -> ANSWERED
  -> CONNECTED
  -> COMPLETED
```

Terminal alternatives:

```text
FAILED
CANCELLED
```

Duplicate callbacks, stale provider events, and any event received after a terminal call state are ignored.

## Concurrency approach

The prototype uses a lock-protected in-memory transaction boundary. In production this maps to a PostgreSQL transaction:

1. Lock/select one `AVAILABLE` agent and one `QUEUED` borrower.
2. Atomically reserve both records.
3. Create one call record with an idempotency key.
4. Write an outbox/provider-initiation request.
5. Commit before calling the provider.

If two workers try to reserve the same agent, only one transaction succeeds. The other worker retries or allocates a different agent.

The authoritative database always wins over cache state. A cache may improve reads but must never allocate an agent or issue a safety permit.

## Predictive pacing logic

The system uses an explainable rule-based predictive model, not black-box ML.

```text
p_lower = max(
  0.01,
  observed_answer_rate - 1.28 * standard_error
)

requested_calls = floor(
  available_agents * utilization_target / p_lower
)
```

The proposal is reduced if:

- The recent answer rate is worsening
- Provider setup latency is high
- Provider health falls below the safety threshold

The Safety Controller then independently clamps the proposal using live authoritative capacity. Default behaviour retains progressive-like safety because every initiated call has a reserved agent.

## Failure handling

| Failure case | System behaviour |
|---|---|
| Worker crash after reservation | Lease expires and recovery releases the agent/borrower pair. Production reconciliation checks the provider using the idempotency key before retrying. |
| Provider
