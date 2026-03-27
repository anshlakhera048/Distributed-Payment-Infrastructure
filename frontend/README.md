# Payments System — Frontend Dashboard

Real-time system visualization dashboard for the distributed payments platform.

## Tech Stack

- React 19 + TypeScript
- Vite 6
- Tailwind CSS 4
- Axios (HTTP client)
- Native WebSocket (real-time events)

## Quick Start

```bash
cd frontend
npm install
npm run dev
```

The dev server starts on an available port (default **3000**, auto-increments if taken).
API calls are proxied to the gateway at `localhost:8090`.

### Prerequisites

Make sure all backend services are running first:

```bash
# From the project root
docker-compose up -d postgres zookeeper kafka redis fraud-service payment-service api-gateway
```

Wait until all containers show as healthy:

```bash
docker ps --format "table {{.Names}}\t{{.Status}}"
```

## Using the Dashboard

### 1. Opening the Dashboard

Navigate to `http://localhost:3000` (or whichever port Vite allocated).
You'll see:

- **Metrics Bar** (top) — Total, Success, Pending, Failed, Fraud counts, and Success Rate
- **Live Payment Stream** — Real-time table of every event flowing through the system
- **System Status** — Green/red indicators for API Gateway + WebSocket connectivity
- **Create Payment** — Form to submit payments
- **Event Stream** — Kafka-style timeline showing `payment.created → fraud.result → payment.processed`
- **Alerts & Fraud** — Fraud rejections and failed payment alerts

### 2. Creating a Payment

1. Fill in the **Amount** field (required).
2. Select a **Currency** (USD, EUR, GBP, etc.).
3. User ID and Merchant ID auto-generate if left blank.
4. An **Idempotency Key** is pre-filled (auto-generated UUID).
5. Click **Create Payment**.

The payment will appear in the Live Payment Stream almost instantly via WebSocket.
Watch the Event Stream panel to see the full lifecycle:
`payment.created` → `fraud.result` → `payment.processed`

### 3. Triggering Fraud Detection

Two easy ways:

- **Select "XYZ (triggers fraud)" currency** — unsupported currency gets rejected
- **Set amount > $10,000** — exceeds the fraud threshold

Both will result in `FRAUD_REJECTED` status (purple badge) and appear in the Alerts panel.

### 4. Simulation Buttons

Below the Create Payment form:

- **↻ Duplicate Request** — Re-sends a payment with the **same** idempotency key. Demonstrates the idempotency guarantee (same response, no double-charge).
- **⚡ Burst (5 rapid)** — Fires 5 concurrent payments. Tests the system under parallel load. Results show how many succeeded vs. failed (rate limiting may kick in).

### 5. Data Persistence

All events and alerts are saved to **sessionStorage**. Refreshing the page restores your data.
Metrics are derived directly from stored events, so they are always accurate.

> **Note:** Closing the browser tab clears session data. This is intentional — it resets the dashboard for a fresh demo.

### 6. Status Indicators

| Indicator | Meaning |
|---|---|
| Green pulsing dot (header) | WebSocket connected — events streaming live |
| Red dot (header) | WebSocket disconnected — attempting reconnect every 3s |
| API Gateway: Online | HTTP health check to gateway succeeding |
| Last Event timestamp | Most recent event received from any source |

## Architecture

```
Browser ──► Vite Dev Server (:3000)
             ├─ /api/* ──proxy──► API Gateway (:8090) ──► Payment Service (:8080)
             └─ ws://localhost:8080/ws/payments (direct WebSocket)
```

## Project Structure

```
src/
├── components/     # Reusable UI components
│   ├── AlertPanel.tsx
│   ├── Card.tsx
│   ├── CreatePaymentForm.tsx
│   ├── EventStream.tsx
│   ├── MetricsBar.tsx
│   ├── PaymentTable.tsx
│   ├── StatusBadge.tsx
│   └── SystemStatus.tsx
├── hooks/          # Custom React hooks
│   └── usePaymentStream.ts
├── pages/          # Page-level components
│   └── Dashboard.tsx
├── services/       # API + WebSocket clients
│   ├── api.ts
│   └── websocket.ts
├── types/          # TypeScript interfaces
│   └── index.ts
├── App.tsx
└── main.tsx
```

## Production Build

```bash
npm run build    # outputs to dist/
npm run preview  # preview production build locally
```

## Docker

```bash
docker build -t payments-frontend .
docker run -p 3000:80 payments-frontend
```
