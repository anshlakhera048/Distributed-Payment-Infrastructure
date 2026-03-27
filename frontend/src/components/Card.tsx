import type { ReactNode } from 'react';

export function Card({
  title,
  children,
  className = '',
}: {
  title: string;
  children: ReactNode;
  className?: string;
}) {
  return (
    <div
      className={`bg-gray-900/70 border border-gray-800 rounded-xl p-5 ${className}`}
    >
      <h2 className="text-xs font-bold uppercase tracking-wider text-gray-500 mb-4">
        {title}
      </h2>
      {children}
    </div>
  );
}
