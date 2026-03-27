import type { SystemStatus as SystemStatusType } from '../types';

function Dot({ active }: { active: boolean }) {
  return (
    <span
      className={`inline-block w-2 h-2 rounded-full ${active ? 'bg-emerald-400 shadow-[0_0_6px_rgba(52,211,153,0.6)]' : 'bg-red-400 shadow-[0_0_6px_rgba(248,113,113,0.6)]'}`}
    />
  );
}

function formatTime(iso: string | null): string {
  if (!iso) return '—';
  try {
    return new Date(iso).toLocaleTimeString();
  } catch {
    return iso;
  }
}

export function SystemStatusPanel({ status }: { status: SystemStatusType }) {
  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <span className="text-sm text-gray-400">API Gateway</span>
        <div className="flex items-center gap-2">
          <Dot active={status.apiOnline} />
          <span className="text-xs text-gray-500">
            {status.apiOnline ? 'Online' : 'Offline'}
          </span>
        </div>
      </div>
      <div className="flex items-center justify-between">
        <span className="text-sm text-gray-400">WebSocket</span>
        <div className="flex items-center gap-2">
          <Dot active={status.wsConnected} />
          <span className="text-xs text-gray-500">
            {status.wsConnected ? 'Connected' : 'Disconnected'}
          </span>
        </div>
      </div>
      <div className="flex items-center justify-between">
        <span className="text-sm text-gray-400">Last Event</span>
        <span className="text-xs text-gray-500 font-mono">
          {formatTime(status.lastEventAt)}
        </span>
      </div>
    </div>
  );
}
