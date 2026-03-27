const statusColors: Record<string, string> = {
  SUCCESS: 'text-emerald-400',
  PENDING: 'text-yellow-400',
  FAILED: 'text-red-400',
  FRAUD_REJECTED: 'text-purple-400',
};

const statusBg: Record<string, string> = {
  SUCCESS: 'bg-emerald-400/10',
  PENDING: 'bg-yellow-400/10',
  FAILED: 'bg-red-400/10',
  FRAUD_REJECTED: 'bg-purple-400/10',
};

export function StatusBadge({ status }: { status: string }) {
  return (
    <span
      className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-semibold ${statusColors[status] ?? 'text-gray-400'} ${statusBg[status] ?? 'bg-gray-400/10'}`}
    >
      {status}
    </span>
  );
}
