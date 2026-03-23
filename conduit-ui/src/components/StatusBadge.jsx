import clsx from 'clsx';

const VARIANTS = {
  success: 'bg-emerald-500/15 text-emerald-400 border-emerald-500/25',
  running: 'bg-blue-500/15 text-blue-400 border-blue-500/25',
  pending: 'bg-amber-500/15 text-amber-400 border-amber-500/25',
  failed: 'bg-red-500/15 text-red-400 border-red-500/25',
  skipped: 'bg-gray-500/15 text-gray-400 border-gray-500/25',
  added: 'bg-emerald-500/15 text-emerald-400 border-emerald-500/25',
  modified: 'bg-amber-500/15 text-amber-400 border-amber-500/25',
  removed: 'bg-red-500/15 text-red-400 border-red-500/25',
  unchanged: 'bg-gray-500/15 text-gray-400 border-gray-500/25',
  breaking: 'bg-red-500/15 text-red-400 border-red-500/25',
  safe: 'bg-emerald-500/15 text-emerald-400 border-emerald-500/25',
  error: 'bg-red-500/15 text-red-400 border-red-500/25',
  warning: 'bg-amber-500/15 text-amber-400 border-amber-500/25',
};

const DOT_COLORS = {
  success: 'bg-emerald-400',
  running: 'bg-blue-400 animate-live',
  pending: 'bg-amber-400',
  failed: 'bg-red-400',
  skipped: 'bg-gray-400',
};

export default function StatusBadge({ status, dot = false, className }) {
  const key = status?.toLowerCase() || 'pending';
  const variant = VARIANTS[key] || VARIANTS.pending;

  return (
    <span
      className={clsx(
        'inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-xs font-medium border',
        variant,
        className
      )}
    >
      {dot && (
        <span
          className={clsx('status-dot', DOT_COLORS[key] || 'bg-gray-400')}
        />
      )}
      {status}
    </span>
  );
}
