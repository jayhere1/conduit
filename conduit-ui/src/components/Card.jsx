import clsx from 'clsx';

export default function Card({ title, subtitle, icon: Icon, children, className, action }) {
  return (
    <div className={clsx('glass p-5', className)}>
      {(title || Icon) && (
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center gap-2">
            {Icon && <Icon size={16} className="text-conduit-400" />}
            <div>
              {title && (
                <h3 className="text-sm font-semibold text-gray-200">{title}</h3>
              )}
              {subtitle && (
                <p className="text-xs text-gray-500 mt-0.5">{subtitle}</p>
              )}
            </div>
          </div>
          {action}
        </div>
      )}
      {children}
    </div>
  );
}

export function StatCard({ label, value, sub, icon: Icon, trend }) {
  return (
    <div className="glass p-4">
      <div className="flex items-center justify-between mb-2">
        <span className="text-xs font-medium text-gray-400 uppercase tracking-wide">
          {label}
        </span>
        {Icon && <Icon size={15} className="text-conduit-500" />}
      </div>
      <div className="text-2xl font-bold text-white">{value}</div>
      {(sub || trend) && (
        <div className="mt-1 flex items-center gap-2">
          {trend && (
            <span
              className={clsx(
                'text-xs font-medium',
                trend > 0 ? 'text-emerald-400' : trend < 0 ? 'text-red-400' : 'text-gray-500'
              )}
            >
              {trend > 0 ? '+' : ''}{trend}%
            </span>
          )}
          {sub && <span className="text-xs text-gray-500">{sub}</span>}
        </div>
      )}
    </div>
  );
}
