# WireFlow — Project Specification
> A polyglot microservices pipeline demonstrating message broker architecture with Apache Kafka

---

## The Big Idea

Back in the late 1800s, every house that wanted a telephone had to be physically connected by wire to every other house it wanted to call. For 100 houses that meant thousands of wires covering the entire city. Then in 1878, the Bell Company opened the first **telephone switching office** — every house connected to one central exchange, and the exchange handled routing calls between them.

**Message brokers are the switching office of software.**

Without a broker, every service that needs to talk to another service requires a direct connection. Four services talking to each other = 12 connections. Add a fifth = 20 connections. The system becomes brittle, tightly coupled, and a nightmare to change.

With Kafka as the broker, every service connects to Kafka only. A service publishes an event. Any service that cares about it subscribes. They never know about each other.

---

## What We're Building

A simplified bank transaction pipeline. A customer submits a payment. That single event fans out to four independent services that each react to it in their own way, in their own language, with their own logic.

### The Flow

```
[Payment Submitted]
        │
        ▼
  ┌─────────────┐
  │    Kafka    │  ← The Switching Office
  └─────────────┘
        │
   ┌────┴────────────────────┐
   │         │               │               │
   ▼         ▼               ▼               ▼
Fraud    Balance          Notifier        Audit
Detector Updater                          Logger
(Python) (Bun/TS)        (Bun/TS)        (Bun/TS)
   │         │                               │
   ▼         ▼                               ▼
 Redis     Redis                           Redis
```

---

## Services

### 1. Producer — `producer/`
**Language:** Bun + TypeScript  
**What it does:** Accepts a transaction object and publishes it to the `transactions` Kafka topic. This is the entry point of the system. Run it manually to simulate a payment being made.  
**Connects to:** Kafka only  

---

### 2. Fraud Detector — `fraud-detector/`
**Language:** Python  
**What it does:** Consumes from the `transactions` topic. Uses a trained ML classifier (from your semester studies) to decide whether the transaction is suspicious. Writes the result to Redis with the transaction ID as the key.  
**Connects to:** Kafka, Redis  

**Suggested ML approach:** Train a simple `scikit-learn` classifier (e.g. Random Forest or Logistic Regression) on a small synthetic dataset of transactions. Features can include: amount, hour of day, sender/receiver mismatch patterns. Label: `fraud` or `legitimate`. The model gets loaded at consumer startup and runs inference on each incoming message.

**Redis key pattern:** `fraud:<transactionId>` → `"fraud"` or `"clear"`

---

### 3. Balance Updater — `balance-updater/`
**Language:** Bun + TypeScript  
**What it does:** Consumes from the `transactions` topic. Reads the sender's current balance from Redis, deducts the transaction amount, writes the new balance back.  
**Connects to:** Kafka, Redis  

**Redis key pattern:** `balance:<accountId>` → `<amount>`

**Note:** Seed some initial balances in Redis before running, e.g. Alice: 1000, Bob: 500.

---

### 4. Notifier — `notifier/`
**Language:** Bun + TypeScript  
**What it does:** Consumes from the `transactions` topic. Logs a confirmation message simulating an SMS/email being sent to the customer. No database needed — pure console output is fine here.  
**Connects to:** Kafka only  

**Example output:**
```
[NOTIFIER] SMS sent to Alice: Your payment of $250 to Bob was received. TxnID: txn_001
```

---

### 5. Audit Logger — `audit-logger/`
**Language:** Bun + TypeScript  
**What it does:** Consumes from the `transactions` topic. Appends the full transaction record to a Redis list for compliance and history purposes.  
**Connects to:** Kafka, Redis  

**Redis key pattern:** `audit:log` → Redis List (RPUSH each transaction as a JSON string)

---

## Without Kafka Version

**Location:** `without-kafka/index.ts`  
**Language:** Bun + TypeScript  

A single script that does everything the above services do — but inline and sequentially. It calls fraud check, balance update, notification, and audit logging one after another in the same process. No broker. No independence.

Run it, feel the coupling. Then run the Kafka version and feel the difference.

```
npm run without-kafka   # tight coupling, sequential, brittle
npm run with-kafka      # start all consumers, then run producer separately
```

---

## A Transaction Event

Every message published to Kafka looks like this:

```json
{
  "transactionId": "txn_001",
  "from": "Alice",
  "fromAccountId": "acc_alice",
  "to": "Bob",
  "toAccountId": "acc_bob",
  "amount": 250,
  "currency": "USD",
  "timestamp": "2026-05-06T10:00:00Z"
}
```

---

## Tech Stack

| Concern | Technology |
|---|---|
| Runtime (TS services) | Bun |
| Language (TS services) | TypeScript |
| Runtime (Fraud Detector) | Python 3.10+ |
| ML Library | scikit-learn |
| Kafka Client (TS) | kafkajs |
| Kafka Client (Python) | kafka-python |
| Broker | Apache Kafka |
| Broker Coordination | Zookeeper |
| Key-Value Store | Redis |
| Infrastructure | Docker + Docker Compose |

---

## Project Structure

```
wireflow/
│
├── docker-compose.yml          # Kafka, Zookeeper, Redis
│
├── package.json                # Root scripts: without-kafka, with-kafka
│
├── without-kafka/
│   └── index.ts                # All logic inline, sequential, tightly coupled
│
├── producer/
│   ├── index.ts
│   └── package.json
│
├── fraud-detector/
│   ├── consumer.py
│   ├── model.py                # ML model training + inference
│   ├── train.py                # Run once to generate model artifact
│   ├── model.pkl               # Saved trained model (generated)
│   └── requirements.txt
│
├── balance-updater/
│   ├── index.ts
│   └── package.json
│
├── notifier/
│   ├── index.ts
│   └── package.json
│
├── audit-logger/
│   ├── index.ts
│   └── package.json
│
└── README.md
```

---

## Kafka Topic

| Topic Name | Partitions | Purpose |
|---|---|---|
| `transactions` | 1 | All payment events flow through here |

All consumers join different **consumer groups** so each one independently receives every message:

| Service | Consumer Group |
|---|---|
| Fraud Detector | `fraud-detector-group` |
| Balance Updater | `balance-updater-group` |
| Notifier | `notifier-group` |
| Audit Logger | `audit-logger-group` |

This is important — if all consumers shared the same group, Kafka would load-balance messages between them (only one would get each message). Different groups means every consumer gets every message independently. Exactly like the switching office routing the same call to multiple recipients.

---

## Redis Data Layout

| Key | Type | Written by | Contains |
|---|---|---|---|
| `balance:<accountId>` | String | Balance Updater | Current balance as a number |
| `fraud:<transactionId>` | String | Fraud Detector | `"fraud"` or `"clear"` |
| `audit:log` | List | Audit Logger | JSON strings of each transaction |

Seed initial balances manually or via a small seed script before running.

---

## Build Order

Follow this order when building:

1. **`docker-compose.yml`** — get Kafka, Zookeeper, and Redis running first. Verify Kafka is up before writing any consumer.
2. **`without-kafka/index.ts`** — build the tightly coupled version first so you understand what problem you're solving.
3. **`producer/index.ts`** — the event publisher. Test it by checking Kafka logs.
4. **`audit-logger/`** — simplest consumer, no ML, just writes to Redis. Good first consumer to build.
5. **`notifier/`** — second simplest, no Redis, just logs.
6. **`balance-updater/`** — reads and writes Redis, slightly more logic.
7. **`fraud-detector/`** — train the model first (`train.py`), then build the consumer.

---

## Key Concepts to Internalize

**Decoupling** — The producer doesn't import, call, or know about any consumer. It just publishes to a topic. New service needed? Add a new consumer. Producer doesn't change.

**Consumer Groups** — Different groups = each group gets all messages independently. Same group = messages split between group members (useful for scaling, not what we want here).

**Persistence** — Kafka retains messages even after they're consumed. If a consumer crashes and restarts, it picks up from where it left off. The without-kafka version loses everything if it crashes mid-execution.

**Polyglot** — Kafka doesn't care what language you use. Python and TypeScript consumers sit side by side on the same topic. The broker speaks bytes, not languages.

**Fan-out** — One event, four independent reactions. This is the switching office pattern. Alice calls the exchange, the exchange rings Bob, Charlie, Dave, and Eve simultaneously.

---

## What to Observe When Running

- Start all consumers, then run the producer once.
- Watch all four terminals react to the same event simultaneously.
- Kill the notifier mid-run. Restart it. It picks up missed messages.
- Run the without-kafka version. Add a `setTimeout` delay to one of the inline calls and watch the whole script slow down.
- In the Kafka version, that same delay only affects the notifier — every other service is unaffected.