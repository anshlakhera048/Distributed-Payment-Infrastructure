import type { AlertItem } from '../types';

const alertStyles: Record<string, { border: string; icon: string; text: string }> = {
  fraud: { border: 'border-purple-500/50', icon: '⚠', text: 'text-purple-300' },
  failure: { border: 'border-red-500/50', icon: '✕', text: 'text-red-300' },
  webhook: { border: 'border-orange-500/50', icon: '↯', text: 'text-orange-300' },
};

function formatTime(iso: string): string {
  try {
    return new Date(iso).toLocaleTimeString();
  } catch {
    return iso;
  }
}

export function AlertPanel({
  alerts,
  onClear,
}: {
  alerts: AlertItem[];
  onClear: () => void;
}) {
  if (alerts.length === 0) {
    return (
      <div className="text-gray-500 text-sm py-4 text-center">
        No alerts — system nominal
      </div>
    );
  }

  return (
    <div>
      <div className="flex justify-end mb-2">
        <button
          onClick={onClear}
          className="text-[10px] uppercase tracking-wider text-gray-500 hover:text-gray-300 transition-colors"
        >
          Clear all
        </button>
      </div>
      <div className="space-y-1.5 max-h-60 overflow-y-auto pr-1">
        {alerts.map((a) => {
          const style = alertStyles[a.type] ?? alertStyles.failure;
          return (
            <div
              key={a.id}
              className={`border-l-2 ${style.border} pl-3 py-1.5 flex items-start gap-2`}
            >
              <span className={`${style.text} text-sm leading-none mt-0.5`}>
                {style.icon}
              </span>
              <div className="flex-1 min-w-0">
                <p className={`text-xs ${style.text} truncate`}>{a.message}</p>
                <p className="text-[10px] text-gray-600">
                  {formatTime(a.timestamp)}
                </p>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
