import clsx from 'clsx';

export default function Spinner({ size = 'md', className }) {
  const sizes = { sm: 'w-4 h-4', md: 'w-6 h-6', lg: 'w-10 h-10' };
  return (
    <div className={clsx('flex items-center justify-center py-12', className)}>
      <div
        className={clsx(
          'border-2 border-conduit-700 border-t-conduit-400 rounded-full animate-spin',
          sizes[size]
        )}
      />
    </div>
  );
}
