import { Inbox } from 'lucide-react';

export default function EmptyState({ icon: Icon = Inbox, title, description, action }) {
  return (
    <div className="flex flex-col items-center justify-center py-16 text-center">
      <div className="w-12 h-12 rounded-xl bg-conduit-900 border border-conduit-800/50 flex items-center justify-center mb-4">
        <Icon size={22} className="text-conduit-500" />
      </div>
      <h3 className="text-sm font-semibold text-gray-300 mb-1">{title}</h3>
      {description && (
        <p className="text-xs text-gray-500 max-w-xs">{description}</p>
      )}
      {action && <div className="mt-4">{action}</div>}
    </div>
  );
}
