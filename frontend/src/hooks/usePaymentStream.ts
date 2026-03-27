import { useEffect, useState, useCallback, useMemo } from 'react';
import { paymentWs } from '../services/websocket';
import { pingGateway } from '../services/api';
import type { PaymentEvent, AlertItem, Metrics, SystemStatus } from '../types';

const MAX_EVENTS = 200;
const MAX_ALERTS = 50;

const STORAGE_KEYS = {
  events: 'ps_events',
  alerts: 'ps_alerts',
} as const;

// ---------- sessionStorage helpers ----------

function loadFromStorage<T>(key: string, fallback: T): T {
  try {
    const raw = sessionStorage.getItem(key);
    if (!raw) return fallback;
    return JSON.parse(raw) as T;
  } catch {
    return fallback;
  }
}

function saveToStorage<T>(key: string, value: T): void {
  try {
    sessionStorage.setItem(key, JSON.stringify(value));
  } catch {
    // quota exceeded – non-critical, just skip
  }
}

// ---------- derive metrics from events ----------

function computeMetrics(events: PaymentEvent[]): Metrics {
  // Build a map of paymentId → latest known status.
  // Events are newest-first, so iterate in reverse so newer events overwrite older.
  const statusByPayment = new Map<string, string>();
  for (let i = events.length - 1; i >= 0; i--) {
    const e = events[i];
    statusByPayment.set(e.paymentId, e.status);
  }

  let successCount = 0;
  let failedCount = 0;
  let fraudCount = 0;
  let pendingCount = 0;

  for (const status of statusByPayment.values()) {
    switch (status) {
      case 'SUCCESS':
        successCount++;
        break;
      case 'FAILED':
        failedCount++;
        break;
      case 'FRAUD_REJECTED':
        fraudCount++;
        break;
      default:
        pendingCount++;
        break;
    }
  }

  return {
    totalPayments: statusByPayment.size,
    successCount,
    failedCount,
    fraudCount,
    pendingCount,
  };
}

// ---------- hook ----------

export function usePaymentStream() {
  const [events, setEvents] = useState<PaymentEvent[]>(() =>
    loadFromStorage<PaymentEvent[]>(STORAGE_KEYS.events, [])
  );
  const [alerts, setAlerts] = useState<AlertItem[]>(() =>
    loadFromStorage<AlertItem[]>(STORAGE_KEYS.alerts, [])
  );
  const [systemStatus, setSystemStatus] = useState<SystemStatus>({
    apiOnline: false,
    wsConnected: false,
    lastEventAt: null,
  });

  // Derive metrics from events – always in sync, no drift
  const metrics: Metrics = useMemo(() => computeMetrics(events), [events]);

  // Persist events to sessionStorage whenever they change
  useEffect(() => {
    saveToStorage(STORAGE_KEYS.events, events);
  }, [events]);

  // Persist alerts to sessionStorage whenever they change
  useEffect(() => {
    saveToStorage(STORAGE_KEYS.alerts, alerts);
  }, [alerts]);

  // Restore lastEventAt from persisted events
  useEffect(() => {
    if (events.length > 0) {
      const latest = events[0];
      setSystemStatus((prev) => ({
        ...prev,
        lastEventAt: latest.eventTimestamp || latest.createdAt,
      }));
    }
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // Track WebSocket connection status
  useEffect(() => {
    paymentWs.connect();

    const unsubStatus = paymentWs.onStatusChange((connected) => {
      setSystemStatus((prev) => ({ ...prev, wsConnected: connected }));
    });

    return () => {
      unsubStatus();
    };
  }, []);

  // Poll API health
  useEffect(() => {
    const check = async () => {
      const online = await pingGateway();
      setSystemStatus((prev) => ({ ...prev, apiOnline: online }));
    };
    check();
    const interval = setInterval(check, 10000);
    return () => clearInterval(interval);
  }, []);

  // Listen to WebSocket messages
  useEffect(() => {
    const unsub = paymentWs.onMessage((data) => {
      const event = data as PaymentEvent;
      if (!event.eventId) return;

      // Add event (deduplicated, newest first)
      setEvents((prev) => {
        if (prev.some((e) => e.eventId === event.eventId)) return prev;
        return [event, ...prev].slice(0, MAX_EVENTS);
      });

      // Update last-event timestamp
      setSystemStatus((prev) => ({
        ...prev,
        lastEventAt: event.eventTimestamp || new Date().toISOString(),
      }));

      // Generate alerts for failures and fraud
      if (event.eventType === 'payment.processed') {
        if (event.status === 'FAILED') {
          setAlerts((prev) =>
            [
              {
                id: event.eventId,
                type: 'failure' as const,
                message: `Payment ${event.paymentId.slice(0, 8)}… failed`,
                paymentId: event.paymentId,
                timestamp: event.eventTimestamp,
              },
              ...prev,
            ].slice(0, MAX_ALERTS)
          );
        } else if (event.status === 'FRAUD_REJECTED') {
          setAlerts((prev) =>
            [
              {
                id: event.eventId,
                type: 'fraud' as const,
                message: `FRAUD: Payment ${event.paymentId.slice(0, 8)}… rejected (${Number(event.amount).toFixed(2)} ${event.currency})`,
                paymentId: event.paymentId,
                timestamp: event.eventTimestamp,
              },
              ...prev,
            ].slice(0, MAX_ALERTS)
          );
        }
      }

      // Fraud result events (separate from payment.processed)
      if (event.eventType === 'fraud.result' && event.status === 'FRAUD_REJECTED') {
        setAlerts((prev) => {
          if (prev.some((a) => a.id === event.eventId)) return prev;
          return [
            {
              id: event.eventId,
              type: 'fraud' as const,
              message: `Fraud detected: ${event.paymentId.slice(0, 8)}… (${Number(event.amount).toFixed(2)} ${event.currency})`,
              paymentId: event.paymentId,
              timestamp: event.eventTimestamp,
            },
            ...prev,
          ].slice(0, MAX_ALERTS);
        });
      }
    });

    return unsub;
  }, []);

  const clearAlerts = useCallback(() => setAlerts([]), []);

  return { events, alerts, metrics, systemStatus, clearAlerts };
}
