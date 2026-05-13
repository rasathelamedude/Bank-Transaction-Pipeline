# WireFlow

A bank transaction pipeline that demonstrates the value of message broker architecture using Apache Kafka.

---

## The Idea

Back in the late 1800s, every house with a telephone had to be physically connected by wire to every other house it wanted to call. For 100 houses that meant thousands of wires covering the entire city. Then in 1878 the Bell Company opened the first telephone switching office — every house connected to one central exchange, and the exchange handled routing calls between them.

**Message brokers are the switching office of software.**

Without a broker, every service that needs to talk to another service requires a direct connection. Services become tightly coupled, slow, and brittle. With Kafka as the broker, every service connects to Kafka only. A service publishes an event. Any service that cares about it subscribes. They never know about each other.

This project builds the same bank transaction pipeline twice — once without Kafka to feel the problem, and once with Kafka to see the solution.

---

## What We Concluded

### Without Kafka

Running `without-kafka/index.ts` processes transactions sequentially in a single process:

- 6 transactions took **~10 seconds** to complete
- Each transaction waits for the previous one to fully finish before starting
- Every step (fraud check → balance update → notify → log) blocks the next
- A slow fraud check delays everything — the notifier, the balance updater, all of it
- If one service crashes mid-execution, everything after it is lost
- Alice's payment to Bob is blocked by Charlie's payment to Charlie — they have nothing to do with each other

### With Kafka

Running the Kafka version, the producer publishes all transactions in **under a second** and exits. Each consumer processes independently and in parallel:

- A slow fraud check does not affect the notifier's speed
- A crashed notifier does not affect the balance updater
- Adding a new service means adding a new consumer — no existing code changes
- Python and TypeScript services consume from the same topic — Kafka is language agnostic
- Total time is the slowest single operation, not the sum of all operations

---

## Architecture

```
[Producer] → [Kafka: transactions topic] → [Balance Updater  (Bun/TS) ] → Redis
                                         → [Notifier         (Bun/TS) ]
                                         → [Audit Logger     (Bun/TS) ] → Redis
                                         → [Fraud Detector   (Python) ] → Redis
```

Each service is an independent consumer in its own consumer group, meaning every service receives every message independently.

---

## Tech Stack

| Concern                  | Technology              |
| ------------------------ | ----------------------- |
| Runtime (TS services)    | Bun                     |
| Language (TS services)   | TypeScript              |
| Runtime (Fraud Detector) | Python 3.10+            |
| Kafka Client (TS)        | kafkajs                 |
| Kafka Client (Python)    | confluent-kafka         |
| Message Broker           | Apache Kafka            |
| Broker Coordination      | Zookeeper               |
| Key-Value Store          | Redis                   |
| Infrastructure           | Docker + Docker Compose |

---

## Project Structure

```
wireflow/
│
├── package.json                        # Root scripts
│
├── without-kafka/
│   ├── index.ts                        # Tightly coupled sequential version
│   └── package.json
│
└── with-kafka/
    ├── docker-compose.yml              # Kafka, Zookeeper, Redis, Kafka UI
    ├── index.ts                        # Orchestrator — runs all services
    ├── package.json
    ├── types/
    │   └── Transaction.ts              # Shared transaction type
    ├── utils/
    │   └── redisClient.ts              # Shared Redis utility functions
    └── services/
        ├── producer/
        │   └── index.ts
        ├── balance-updater/
        │   └── index.ts
        ├── notifier/
        │   └── index.ts
        ├── audit-logger/
        │   └── index.ts
        └── fraud-detector/
            ├── main.py
            ├── Transaction.py
            └── requirements.txt
```

---

## Prerequisites

- [Bun](https://bun.sh) installed
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) running
- [Python 3.10+](https://www.python.org/downloads/) installed

---

## How to Run

### Step 1 — Install dependencies

At the root:

```bash
npm install
```

Inside `with-kafka/`:

```bash
cd with-kafka
bun install
```

Inside `fraud-detector/`:

```bash
cd with-kafka/services/fraud-detector
python -m venv venv
```

Then install dependencies into the venv without activating it:

```bash
# Windows
venv\Scripts\pip install -r requirements.txt

# macOS/Linux
venv/bin/pip install -r requirements.txt
```

> You do not need to activate the venv manually. The orchestrator calls the venv Python executable directly when you run `npm run with-kafka`.

---

### Step 2 — Start infrastructure

From inside `with-kafka/`:

```bash
docker compose up -d
```

This starts:

- **Zookeeper** on port `2181`
- **Kafka** on port `29092`
- **Redis** on port `6379`
- **Kafka UI** on port `8080` — open `localhost:8080` to inspect topics and messages

Wait a few seconds for Kafka to fully initialize before proceeding.

---

### Step 3 — Run without Kafka (feel the problem)

```bash
npm run without-kafka
```

Watch the terminal. Transactions process one by one, each waiting for the previous to complete. Observe the total time printed at the end.

---

### Step 4 — Run with Kafka (see the solution)

From the root:

```bash
npm run with-kafka
```

This starts all consumers first, waits for them to connect, then runs the producer. Watch all four services react to the same transactions simultaneously in the same terminal.

---

## Redis Data Layout

After running the Kafka version, inspect the results directly in Redis:

| Key                           | Type   | Written by      | Contains                         |
| ----------------------------- | ------ | --------------- | -------------------------------- |
| `balance:<accountId>`         | String | Balance Updater | Current account balance          |
| `transaction:<transactionId>` | String | Fraud Detector  | `fraud` or `ok`                  |
| `audit:log`                   | List   | Audit Logger    | JSON record of every transaction |

---

## Fraud Detection Rules

The Python fraud detector flags a transaction as fraud if:

- Amount exceeds **$10,000**
- Sender and receiver are the **same account**

Otherwise the transaction is marked as `ok`.

---

## Kafka UI

Open `localhost:8080` to visually inspect:

- The `transactions` topic
- All published messages with their full JSON payload
- Consumer group offsets — see how far each service has read through the topic
