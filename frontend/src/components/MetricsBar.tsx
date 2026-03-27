import type { Metrics } from '../types';

export function MetricsBar({ metrics }: { metrics: Metrics }) {
  const successRate =
    metrics.totalPayments > 0
      ? ((metrics.successCount / metrics.totalPayments) * 100).toFixed(1)
      : '—';

  return (
    <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3">
      <Stat label="Total" value={metrics.totalPayments} color="text-gray-200" />
      <Stat label="Success" value={metrics.successCount} color="text-emerald-400" />
      <Stat label="Pending" value={metrics.pendingCount} color="text-yellow-400" />
      <Stat label="Failed" value={metrics.failedCount} color="text-red-400" />
      <Stat label="Fraud" value={metrics.fraudCount} color="text-purple-400" />
      <Stat label="Success Rate" value={`${successRate}%`} color="text-cyan-400" />
    </div>
  );
}

function Stat({
  label,
  value,
  color,
}: {
  label: string;
  value: number | string;
  color: string;
}) {
  return (
    <div className="bg-gray-800/50 rounded-lg px-4 py-3 text-center">
      <div className={`text-xl font-bold font-mono ${color}`}>{value}</div>
      <div className="text-[10px] uppercase tracking-wider text-gray-500 mt-1">
        {label}
      </div>
    </div>
  );
}
