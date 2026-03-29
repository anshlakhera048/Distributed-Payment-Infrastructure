import type { PaymentEvent } from '../types';

const eventTypeColors: Record<string, string> = {
  'payment.created': 'border-blue-500',
  'fraud.result': 'border-purple-500',
  'fraud.alerts': 'border-red-500',
  'payment.processed': 'border-emerald-500',
  'payment.failed': 'border-red-400',
};

const eventTypeLabels: Record<string, string> = {
  'payment.created': 'CREATED',
  'fraud.result': 'FRAUD CHECK',
  'fraud.alerts': 'FRAUD ALERT',
  'payment.processed': 'PROCESSED',
  'payment.failed': 'FAILED',
};

function formatTime(iso: string): string {
  try {
    return new Date(iso).toLocaleTimeString();
  } catch {
    return iso;
  }
}

export function EventStream({ events }: { events: PaymentEvent[] }) {
  if (events.length === 0) {
    return (
      <div className="text-gray-500 text-sm py-6 text-center">
        No events yet — create a payment to see the stream
      </div>
    );
  }

  return (
    <div className="space-y-2 max-h-[420px] overflow-y-auto pr-1">
      {events.slice(0, 50).map((e) => (
        <div
          key={e.eventId}
          className={`border-l-2 ${eventTypeColors[e.eventType] ?? 'border-gray-600'} pl-3 py-1.5`}
        >
          <div className="flex items-center gap-2">
            <span className="text-[10px] font-bold uppercase tracking-wider text-gray-400">
              {eventTypeLabels[e.eventType] ?? e.eventType}
            </span>
            <span className="text-[10px] text-gray-600">
              {formatTime(e.eventTimestamp || e.createdAt)}
            </span>
          </div>
          <div className="text-xs text-gray-300 mt-0.5">
            <span className="font-mono">{e.paymentId.slice(0, 8)}…</span>
            {' → '}
            <span
              className={
                e.status === 'SUCCESS'
                  ? 'text-emerald-400'
                  : e.status === 'FRAUD_REJECTED'
                    ? 'text-purple-400'
                    : e.status === 'FAILED'
                      ? 'text-red-400'
                      : 'text-yellow-400'
              }
            >
              {e.status}
            </span>
            <span className="text-gray-500 ml-2">
              {Number(e.amount).toFixed(2)} {e.currency}
            </span>
          </div>
        </div>
      ))}
    </div>
  );
}
