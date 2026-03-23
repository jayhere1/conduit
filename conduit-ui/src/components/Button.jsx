import clsx from 'clsx';

const VARIANTS = {
  primary:
    'bg-conduit-600 text-white hover:bg-conduit-500 border-conduit-500/50',
  secondary:
    'bg-conduit-900/60 text-gray-300 hover:bg-conduit-800/80 border-conduit-700/50',
  danger:
    'bg-red-600/20 text-red-400 hover:bg-red-600/30 border-red-500/30',
  ghost:
    'bg-transparent text-gray-400 hover:text-gray-200 hover:bg-conduit-900/50 border-transparent',
};

const SIZES = {
  sm: 'px-2.5 py-1 text-xs',
  md: 'px-3.5 py-2 text-sm',
  lg: 'px-5 py-2.5 text-sm',
};

export default function Button({
  children,
  variant = 'primary',
  size = 'md',
  icon: Icon,
  disabled,
  loading,
  className,
  ...props
}) {
  return (
    <button
      disabled={disabled || loading}
      className={clsx(
        'inline-flex items-center justify-center gap-2 rounded-lg border font-medium transition-all duration-150',
        'disabled:opacity-40 disabled:cursor-not-allowed',
        VARIANTS[variant],
        SIZES[size],
        className
      )}
      {...props}
    >
      {loading ? (
        <span className="w-4 h-4 border-2 border-current border-t-transparent rounded-full animate-spin" />
      ) : (
        Icon && <Icon size={size === 'sm' ? 14 : 16} />
      )}
      {children}
    </button>
  );
}
