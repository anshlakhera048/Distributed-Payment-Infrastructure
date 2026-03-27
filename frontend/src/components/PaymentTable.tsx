import type { PaymentEvent } from '../types';
import { StatusBadge } from './StatusBadge';

function formatTime(iso: string): string {
  try {
    return new Date(iso).toLocaleTimeString();
  } catch {
    return iso;
  }
}

export function PaymentTable({ events }: { events: PaymentEvent[] }) {
  if (events.length === 0) {
    return (
      <div className="text-gray-500 text-sm py-8 text-center">
        Waiting for payment events…
      </div>
    );
  }

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="text-gray-400 border-b border-gray-700/50 text-left">
            <th className="py-2 pr-4 font-medium">Payment ID</th>
            <th className="py-2 pr-4 font-medium">User</th>
            <th className="py-2 pr-4 font-medium text-right">Amount</th>
            <th className="py-2 pr-4 font-medium">Status</th>
            <th className="py-2 pr-4 font-medium">Event</th>
            <th className="py-2 font-medium">Time</th>
          </tr>
        </thead>
        <tbody>
          {events.map((e) => (
            <tr
              key={e.eventId}
              className="border-b border-gray-800/50 hover:bg-gray-800/30 transition-colors"
            >
              <td className="py-2 pr-4 font-mono text-xs text-gray-300">
                {e.paymentId.slice(0, 8)}…
              </td>
              <td className="py-2 pr-4 font-mono text-xs text-gray-400">
                {e.userId.slice(0, 8)}…
              </td>
              <td className="py-2 pr-4 text-right font-mono text-gray-200">
                {Number(e.amount).toFixed(2)}{' '}
                <span className="text-gray-500">{e.currency}</span>
              </td>
              <td className="py-2 pr-4">
                <StatusBadge status={e.status} />
              </td>
              <td className="py-2 pr-4">
                <span className="text-xs text-cyan-400/80 font-mono">
                  {e.eventType}
                </span>
              </td>
              <td className="py-2 text-xs text-gray-500">
                {formatTime(e.eventTimestamp || e.createdAt)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
