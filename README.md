# Distributed Payment Infrastructure

A distributed payment system with real-time fraud detection, built with a microservices architecture. Payments are processed by a Java/Spring Boot service, events are streamed through Apache Kafka, and a Python/FastAPI fraud detection service scores every transaction in real time.

---

## Architecture Overview

```
Client
  │
  │  POST /payments
  ▼
┌─────────────────────────────┐
│     payment-service         │  Java 17 · Spring Boot 3.4.4
│     localhost:8080          │
│                             │
│  PaymentController          │
│  PaymentService             │  ──── saves ────▶  PostgreSQL :5433
│  KafkaProducerService       │  ──── publishes ─▶  Kafka topic: payment.created
└─────────────────────────────┘
                                          │
                                          ▼
                              ┌───────────────────────┐
                              │  KafkaConsumer         │
                              │  (inside payment-svc)  │
                              └───────────────────────┘
                                          │
                                          │  POST /score
                                          ▼
                              ┌───────────────────────┐
                              │     fraud-service       │  Python 3 · FastAPI
                              │     localhost:8000      │
                              └───────────────────────┘
```

**Data flow for each payment:**
1. Client sends `POST /payments` with an idempotency key
2. `payment-service` checks if the key already exists (idempotency guard)
3. If new: saves the payment as `PENDING` in PostgreSQL
4. Publishes a `payment.created` event to Kafka (JSON serialized)
5. Kafka consumer receives the event and calls `fraud-service POST /score`
6. Fraud service returns a score + flag (`fraud: true/false`)
7. Consumer logs the result (DB status update is next phase)

---

## Project Structure

```
payments-system/
├── docker-compose.yml          # PostgreSQL + Zookeeper + Kafka
├── README.md
│
├── payment-service/            # Java Spring Boot microservice
│   ├── pom.xml
│   └── src/main/java/com/payments/payment_service/
│       ├── PaymentServiceApplication.java
│       ├── controller/
│       │   └── PaymentController.java    # POST /payments
│       ├── dto/
│       │   ├── CreatePaymentRequest.java
│       │   └── PaymentResponse.java
│       ├── entity/
│       │   └── Payment.java              # JPA entity
│       ├── kafka/
│       │   ├── KafkaProducerService.java # Publishes to Kafka
│       │   └── PaymentEventConsumer.java # Calls fraud-service
│       ├── repository/
│       │   └── PaymentRepository.java    # findByIdempotencyKey
│       └── service/
│           └── PaymentService.java       # Business logic + idempotency
│
└── fraud-service/              # Python FastAPI microservice
    ├── main.py                 # /score and /health endpoints
    ├── requirements.txt        # Pinned dependencies
    └── venv/                   # Python virtual environment (git-ignored)
```

---

## Tech Stack

| Layer | Technology | Version |
|-------|-----------|---------|
| Payment Service | Java + Spring Boot | Java 17, Spring Boot 3.4.4 |
| ORM | Spring Data JPA + Hibernate | 6.6.x |
| Database | PostgreSQL | 15 (Docker) |
| Message Broker | Apache Kafka | 7.5.0 (Confluent) |
| Kafka Coordination | Apache Zookeeper | 7.5.0 (Confluent) |
| Fraud Service | Python + FastAPI | Python 3.x, FastAPI 0.135.x |
| ASGI Server | Uvicorn | 0.42.x |
| Boilerplate reduction | Lombok | via Spring Boot parent |
| Observability | Spring Actuator | via Spring Boot parent |
| Containerization | Docker + Docker Compose | — |
| Build tool | Maven Wrapper (`mvnw`) | — |

---

## Prerequisites

Before running anything, make sure the following tools are installed and available in your terminal:

| Tool | Why it's needed | Verify |
|------|----------------|--------|
| **Docker Desktop** | Runs PostgreSQL, Zookeeper, and Kafka as containers | `docker --version` |
| **Java 17** | Compiles and runs the Spring Boot payment-service | `java -version` |
| **Python 3.x** | Runs the FastAPI fraud-service | `py --version` (Windows) or `python3 --version` |
| **Maven** | Build tool for payment-service — bundled via `mvnw`, no install needed | `./mvnw --version` |

> **Windows note:** Python may be available as `py` instead of `python`. Use whichever works on your machine.

---

## Running the Project

The system has **4 components** that must be started in order:

```
1. Infrastructure  (Docker)   — PostgreSQL + Zookeeper + Kafka
2. Fraud Service   (Python)   — FastAPI on :8000
3. Payment Service (Java)     — Spring Boot on :8080
```

Each step below explains what the service is, what it does, and exactly how to start and verify it.

---

### Step 1 — Infrastructure: PostgreSQL + Kafka (Docker)

**What this is:**
Docker Compose starts three containers that the application depends on:
- **PostgreSQL 15** — stores every payment record. Hibernate auto-creates the `payments` table on first startup.
- **Apache Zookeeper** — required by Kafka for cluster coordination. You don't interact with it directly.
- **Apache Kafka** — the message broker. The payment-service publishes a `payment.created` event here after saving a payment; the Kafka consumer inside the same service picks it up and calls the fraud-service.

**Why start this first:** Both the payment-service and fraud-service are useless without a database and a message broker to connect to.

**Command** — run from the repo root (`payments-system/`):

```bash
docker compose up -d
```

The `-d` flag runs containers in the background (detached mode). Docker will pull images on first run — this may take a minute.

**Verify all 3 containers are healthy:**

```bash
docker ps
```

Expected output (all 3 should show `Up`):
```
CONTAINER ID   IMAGE                             STATUS        PORTS
...            confluentinc/cp-kafka:7.5.0       Up ...        0.0.0.0:9092->9092/tcp
...            confluentinc/cp-zookeeper:7.5.0   Up ...        0.0.0.0:2181->2181/tcp
...            postgres:15                        Up ...        0.0.0.0:5433->5432/tcp
```

**Verify Kafka is ready** (list topics — should return without error):

```bash
docker exec payments-kafka kafka-topics --bootstrap-server localhost:9092 --list
```

You should see `__consumer_offsets` (and `payment.created` if any payment has been made before).

**Container summary:**
| Container | Host Port | Purpose |
|-----------|-----------|---------|
| `payments-postgres` | `5433` | PostgreSQL (port 5433 avoids clashing with any local Postgres on 5432) |
| `payments-kafka` | `9092` | Kafka broker |
| `zookeeper` | `2181` | Kafka coordination (internal use only) |

**To stop infrastructure:**
```bash
docker compose down
```

**To stop and wipe all data (DB included):**
```bash
docker compose down -v
```

---

### Step 2 — Fraud Service (Python / FastAPI)

**What this is:**
A lightweight Python microservice built with FastAPI. It exposes a single `POST /score` endpoint that receives payment details and applies rule-based fraud detection:
- Rejects amounts over 10,000
- Rejects currencies outside `[INR, USD, EUR, GBP]`

It returns a score and a `fraud: true/false` flag. The payment-service Kafka consumer calls this automatically for every new payment event.

**Why start this before the payment-service:** The Kafka consumer inside the payment-service calls `http://localhost:8000/score` synchronously. If the fraud-service is down when a Kafka message arrives, the consumer will log an error and will not acknowledge the message (it will be redelivered). Start this service first to avoid noise.

**Setup (first time only) — install dependencies:**

```bash
cd fraud-service
```

Windows:
```powershell
python -m venv venv                  # create virtual environment (only once)
venv\Scripts\activate                # activate it
pip install -r requirements.txt      # install fastapi, uvicorn, pydantic
```

macOS / Linux:
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

> If a `venv/` folder already exists in the repo, skip the `python -m venv venv` step and just activate it.

**Start the fraud service** (open a dedicated terminal for this):

Windows (venv activated):
```powershell
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

Windows (without activating venv — use absolute path):
```powershell
.\venv\Scripts\uvicorn.exe main:app --reload --host 0.0.0.0 --port 8000
```

macOS / Linux (venv activated):
```bash
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

The `--reload` flag auto-restarts the server when you edit `main.py` (useful during development).

**Verify it's running:**

```bash
curl http://localhost:8000/health
# Expected: {"status":"ok"}
```

Or open in browser: `http://localhost:8000/docs` — FastAPI auto-generates an interactive Swagger UI where you can call every endpoint directly from your browser.

**Expected startup output:**
```
INFO:     Started server process [XXXX]
INFO:     Waiting for application startup.
INFO:     Application startup complete.
INFO:     Uvicorn running on http://0.0.0.0:8000 (Press CTRL+C to quit)
```

---

### Step 3 — Payment Service (Java / Spring Boot)

**What this is:**
The core backend service. It:
1. Exposes `POST /payments` — validates the request, saves the payment to PostgreSQL as `PENDING`, and publishes a `payment.created` event to Kafka.
2. Runs a Kafka consumer internally — listens to `payment.created`, calls the fraud-service, and logs the fraud result. (Status update back to DB is the next planned phase.)
3. Enforces idempotency — if the same `Idempotency-Key` is sent twice, it returns the existing payment without creating a duplicate.

**Why start this last:** It connects to both PostgreSQL (Step 1) and Kafka (Step 1) on startup and calls the fraud-service (Step 2) on each Kafka event. All three must be up first.

**Option A — Run with Maven (recommended for development):**

Open a new terminal, then:

```bash
cd payment-service
./mvnw spring-boot:run
```

Windows:
```powershell
cd payment-service
.\mvnw.cmd spring-boot:run
```

Maven downloads all dependencies on first run. Subsequent runs are faster.

**Option B — Build a JAR first and run it:**

```bash
cd payment-service
./mvnw clean package -DskipTests    # build the JAR
java -jar target/payment-service-0.0.1-SNAPSHOT.jar
```

> The application sets `UTC` timezone internally on startup — no JVM flags required.

**Verify it's running:**

```bash
curl http://localhost:8080/actuator/health
# Expected: {"status":"UP"}
```

**Expected startup output (key lines):**
```
Tomcat started on port 8080 (http)
Started PaymentServiceApplication in ~8 seconds
Subscribed to topic(s): payment.created
```

If PostgreSQL isn't running, you'll see a `Connection refused` error on port `5433` — go back to Step 1.  
If Kafka isn't running, you'll see a `LEADER_NOT_AVAILABLE` warning — Kafka is also Step 1.

**Spring Actuator** exposes a health endpoint that shows the status of the DB and Kafka connections:
```bash
curl http://localhost:8080/actuator/health
```

---

### Full Startup Checklist

Run these in order, each in its own terminal:

```
Terminal 1 (infrastructure):   docker compose up -d
Terminal 2 (fraud-service):    cd fraud-service && uvicorn main:app --reload --port 8000
Terminal 3 (payment-service):  cd payment-service && ./mvnw spring-boot:run
```

**Quick health check — all 3 should respond before testing:**

```bash
# Infrastructure
docker ps                                          # all 3 containers Up

# Fraud service
curl http://localhost:8000/health                  # {"status":"ok"}

# Payment service
curl http://localhost:8080/actuator/health         # {"status":"UP"}
```

Once all three return healthy responses, the system is fully operational and ready to accept payments.

---

## API Reference

### Payment Service — `localhost:8080`

#### `POST /payments` — Create a Payment

**Headers:**
| Header | Required | Description |
|--------|----------|-------------|
| `Content-Type` | Yes | `application/json` |
| `Idempotency-Key` | Yes | Unique string per payment attempt. Same key = same response, no duplicate processing. |

**Request body:**
```json
{
  "userId": "550e8400-e29b-41d4-a716-446655440000",
  "amount": 500,
  "currency": "INR"
}
```

**Success response (`200 OK`):**
```json
{
  "paymentId": "7052565e-7dfe-4760-8de3-53eb10097280",
  "status": "PENDING"
}
```

**Idempotency test** — send the same `Idempotency-Key` twice:
- First call → creates and returns new payment
- Second call → returns the exact same `paymentId` without creating a duplicate

**Example (PowerShell):**
```powershell
curl -X POST http://localhost:8080/payments `
  -H "Content-Type: application/json" `
  -H "Idempotency-Key: my-unique-key-001" `
  -d '{"userId":"550e8400-e29b-41d4-a716-446655440000","amount":500,"currency":"INR"}'
```

**Example (bash/curl):**
```bash
curl -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: my-unique-key-001" \
  -d '{"userId":"550e8400-e29b-41d4-a716-446655440000","amount":500,"currency":"INR"}'
```

#### `GET /actuator/health` — Health Check
```bash
curl http://localhost:8080/actuator/health
# {"status":"UP"}
```

---

### Fraud Service — `localhost:8000`

#### `POST /score` — Score a Transaction

**Request body:**
```json
{
  "payment_id": "abc-123",
  "user_id": "xyz-456",
  "amount": 500,
  "currency": "INR"
}
```

**Normal transaction response:**
```json
{
  "payment_id": "abc-123",
  "fraud": false,
  "reason": "ok",
  "score": 0.1
}
```

**Fraud detected response (amount > 10,000):**
```json
{
  "payment_id": "abc-123",
  "fraud": true,
  "reason": "amount_exceeds_threshold",
  "score": 0.9
}
```

**Fraud rules (current):**
| Rule | Condition | Reason |
|------|-----------|--------|
| High value | `amount > 10000` | `amount_exceeds_threshold` |
| Bad currency | Not in `[INR, USD, EUR, GBP]` | `unsupported_currency` |

#### `GET /health` — Health Check
```bash
curl http://localhost:8000/health
# {"status":"ok"}
```

#### Interactive API Docs (auto-generated by FastAPI):
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

---

## Build

### Build payment-service JAR (skip tests)
```bash
cd payment-service
./mvnw clean package -DskipTests
```

### Build and run tests (requires no DB/Kafka — uses H2 in-memory)
```bash
./mvnw clean package
```

Tests use:
- H2 in-memory database (no PostgreSQL needed)
- `KafkaAutoConfiguration` excluded (no Kafka needed)
- `KafkaProducerService` is mocked with `@MockBean`

---

## Database

**Connection (from application):**
| Property | Value |
|----------|-------|
| Host | `localhost:5433` |
| Database | `payments` |
| Username | `user` |
| Password | `pass` |

**`payments` table (auto-created by Hibernate on startup):**
| Column | Type | Notes |
|--------|------|-------|
| `id` | UUID | Primary key |
| `user_id` | UUID | |
| `amount` | DECIMAL | |
| `currency` | VARCHAR | |
| `status` | VARCHAR | `PENDING` initially |
| `idempotency_key` | VARCHAR | Unique constraint |
| `created_at` | TIMESTAMP | |

Connect directly:
```bash
docker exec -it payments-postgres psql -U user -d payments
```
```sql
SELECT * FROM payments;
```

---

## Kafka

**Topic:** `payment.created`

**Producer config:**
- Key: `String` (payment UUID)
- Value: `JSON` (serialized `Payment` object)
- Acks: `all` (strongest durability)
- Retries: `3`

**Consumer config:**
- Group: `payment-group`
- Offset reset: `earliest`
- Auto-commit: `false` (manual acknowledgment via `ack.acknowledge()`)
- Value deserializer: `JsonDeserializer` → `Payment.class`

**Check Kafka topic messages:**
```bash
docker exec -it payments-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic payment.created \
  --from-beginning
```

---

## Configuration Files

### `payment-service/src/main/resources/application.yml` — Production config
- PostgreSQL on port `5433`
- Kafka on port `9092`
- Hibernate `ddl-auto: update` (creates/alters tables automatically)
- JVM timezone forced to UTC (avoids Windows timezone issues with PostgreSQL)

### `payment-service/src/test/resources/application.yml` — Test config
- H2 in-memory database
- `ddl-auto: create-drop`
- `KafkaAutoConfiguration` excluded

---

## Testing

### 1 — Unit / Integration Tests (no Docker required)

```bash
cd payment-service
./mvnw clean package
```

Spring Boot test context loads with:
- **H2 in-memory database** — no PostgreSQL needed
- **Kafka excluded** via `spring.autoconfigure.exclude` in `src/test/resources/application.yml`
- **`KafkaProducerService` mocked** with `@MockitoBean`

Expected output:
```
Tests run: 1, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

---

### 2 — Manual API Tests (all services must be running)

> Start infrastructure, fraud-service, and payment-service first (see **Running the Project** above).

#### Health checks

```bash
# Payment service
curl http://localhost:8080/actuator/health
# Expected: {"status":"UP"}

# Fraud service
curl http://localhost:8000/health
# Expected: {"status":"ok"}
```

#### Test 1 — Normal payment (should pass fraud check)

```bash
curl -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: test-key-001" \
  -d '{"userId":"550e8400-e29b-41d4-a716-446655440000","amount":500,"currency":"USD"}'
```

**PowerShell:**
```powershell
Invoke-RestMethod http://localhost:8080/payments -Method POST `
  -Headers @{"Content-Type"="application/json"; "Idempotency-Key"="test-key-001"} `
  -Body '{"userId":"550e8400-e29b-41d4-a716-446655440000","amount":500,"currency":"USD"}'
```

Expected response:
```json
{ "paymentId": "<uuid>", "status": "PENDING" }
```

What to verify in logs:
- `payment-service` logs: `Received payment event: <uuid>`
- `fraud-service` logs: `POST /score HTTP/1.1" 200 OK`
- `payment-service` logs: `Fraud check for payment <uuid> → fraud=false, reason=ok`

---

#### Test 2 — Idempotency guard (same key, no duplicate created)

Re-send the **exact same request** with the same `Idempotency-Key`:

```bash
curl -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: test-key-001" \
  -d '{"userId":"550e8400-e29b-41d4-a716-446655440000","amount":500,"currency":"USD"}'
```

Expected: **same `paymentId`** returned — no new row inserted, no new Kafka event published.

---

#### Test 3 — Fraud detection — high-value amount

```bash
curl -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: test-key-fraud-001" \
  -d '{"userId":"550e8400-e29b-41d4-a716-446655440000","amount":15000,"currency":"USD"}'
```

Expected response: `{ "paymentId": "<uuid>", "status": "PENDING" }` (payment created)

What to verify in logs:
- `payment-service` consumer: `Fraud check for payment <uuid> → fraud=true, reason=amount_exceeds_threshold`

---

#### Test 4 — Fraud detection — unsupported currency

```bash
curl -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: test-key-fraud-002" \
  -d '{"userId":"550e8400-e29b-41d4-a716-446655440000","amount":100,"currency":"XYZ"}'
```

Logged as: `fraud=true, reason=unsupported_currency`

---

#### Test 5 — Input validation (invalid request → 400)

Negative amount:
```bash
curl -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: test-key-invalid-001" \
  -d '{"userId":"550e8400-e29b-41d4-a716-446655440000","amount":-100,"currency":"USD"}'
```

Expected: **HTTP 400** with validation error — no payment created, no Kafka event.

Missing field:
```bash
curl -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: test-key-invalid-002" \
  -d '{"amount":500,"currency":"USD"}'
```

Expected: **HTTP 400** — `userId` is required (`@NotNull`).

---

#### Test 6 — Score endpoint directly on fraud-service

```bash
curl -X POST http://localhost:8000/score \
  -H "Content-Type: application/json" \
  -d '{"payment_id":"abc-123","user_id":"xyz-456","amount":500,"currency":"USD"}'
# Expected: {"payment_id":"abc-123","fraud":false,"reason":"ok","score":0.1}

curl -X POST http://localhost:8000/score \
  -H "Content-Type: application/json" \
  -d '{"payment_id":"abc-124","user_id":"xyz-456","amount":99999,"currency":"USD"}'
# Expected: {"payment_id":"abc-124","fraud":true,"reason":"amount_exceeds_threshold","score":0.9}
```

FastAPI also ships interactive docs — open in browser:
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

---

#### Test 7 — Verify data in PostgreSQL

```bash
docker exec -it payments-postgres psql -U user -d payments
```
```sql
SELECT id, amount, currency, status, created_at FROM payments ORDER BY created_at DESC;
```

---

#### Test 8 — Inspect Kafka topic messages

```bash
docker exec -it payments-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic payment.created \
  --from-beginning
```

---

## Known Issues & Environment Notes

| Issue | Fix Applied |
|-------|-------------|
| Windows `Asia/Calcutta` timezone rejected by PostgreSQL 15 | `-Duser.timezone=UTC` in Maven plugin JVM args |
| Local PostgreSQL occupying port 5432 | Docker maps postgres to host port `5433` |
| `mvn clean package` fails without Docker | Test config uses H2 in-memory DB |
| Kafka/fraud service unavailable during tests | `KafkaAutoConfiguration` excluded + `@MockBean` |

---

## What's Next (Planned)

- [ ] Update payment status in DB after fraud check (`APPROVED` / `FLAGGED`)
- [ ] Replace rule-based fraud detection with an ML model (scikit-learn)
- [ ] Add `GET /payments/{id}` endpoint
- [ ] Add authentication (JWT)
- [ ] Add a fraud-service Kafka consumer (instead of HTTP polling)
- [ ] Dockerize both services with `Dockerfile`s
- [ ] Add retry + dead-letter queue for failed Kafka messages
- [ ] Deploy to cloud
