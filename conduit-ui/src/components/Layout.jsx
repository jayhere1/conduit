import { NavLink, Outlet } from 'react-router-dom';
import {
  LayoutDashboard,
  GitBranch,
  Play,
  Layers,
  Map,
  Network,
  Radio,
  ShieldCheck,
  BarChart3,
  Plug,
  Server,
  Activity,
  KeyRound,
  ChevronRight,
  LogOut,
} from 'lucide-react';
import clsx from 'clsx';
import { useAuth } from './AuthProvider';

const NAV = [
  { to: '/', icon: LayoutDashboard, label: 'Dashboard' },
  { to: '/dags', icon: GitBranch, label: 'DAGs' },
  { to: '/runs', icon: Play, label: 'Runs' },
  { to: '/environments', icon: Layers, label: 'Environments' },
  { to: '/plan', icon: Map, label: 'Plan / Apply' },
  { to: '/lineage', icon: Network, label: 'Lineage' },
  { to: '/contracts', icon: ShieldCheck, label: 'Contracts' },
  { to: '/metrics', icon: BarChart3, label: 'Metrics' },
  { to: '/connections', icon: Plug, label: 'Connections' },
  { to: '/cluster', icon: Server, label: 'Cluster' },
  { to: '/api-keys', icon: KeyRound, label: 'API Keys' },
  { to: '/events', icon: Radio, label: 'Events' },
];

const ROLE_COLORS = {
  admin: 'bg-red-500/20 text-red-300 border-red-500/30',
  operator: 'bg-amber-500/20 text-amber-300 border-amber-500/30',
  viewer: 'bg-blue-500/20 text-blue-300 border-blue-500/30',
};

function SidebarLink({ to, icon: Icon, label }) {
  return (
    <NavLink
      to={to}
      end={to === '/'}
      className={({ isActive }) =>
        clsx(
          'group flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium transition-all duration-150',
          isActive
            ? 'bg-conduit-600/30 text-conduit-200 border border-conduit-500/50 shadow-lg shadow-conduit-600/20'
            : 'text-gray-400 hover:text-gray-200 hover:bg-conduit-900/50 border border-transparent'
        )
      }
    >
      <Icon size={18} className="shrink-0" />
      <span className="flex-1">{label}</span>
      <ChevronRight
        size={14}
        className="opacity-0 group-hover:opacity-50 transition-opacity"
      />
    </NavLink>
  );
}

export default function Layout() {
  const { user, logout, authState } = useAuth();
  const isAuthEnabled = authState === 'authenticated' && user?.role;

  return (
    <div className="flex h-full">
      {/* ── Sidebar ─────────────────────────────────────────── */}
      <aside className="w-60 shrink-0 flex flex-col border-r border-conduit-800/50 bg-conduit-950">
        {/* Logo with Glow Effect */}
        <div className="flex items-center gap-3 px-5 h-16 border-b border-conduit-800/50">
          <div
            className="w-8 h-8 rounded-lg bg-conduit-600 flex items-center justify-center transition-all duration-300 hover:shadow-lg hover:shadow-conduit-600/50"
            style={{
              animation: 'pulse 3s cubic-bezier(0.4, 0, 0.6, 1) infinite',
            }}
          >
            <Activity size={18} className="text-white" />
          </div>
          <div>
            <h1 className="text-base font-bold text-white tracking-tight">
              Conduit
            </h1>
            <p className="text-[10px] text-conduit-400 uppercase tracking-widest leading-none">
              Pipeline Orchestrator
            </p>
          </div>
        </div>

        {/* Navigation */}
        <nav className="flex-1 p-3 space-y-1 overflow-y-auto">
          {NAV.map((item) => (
            <SidebarLink key={item.to} {...item} />
          ))}
        </nav>

        {/* Footer */}
        <div className="p-4 border-t border-conduit-800/50 space-y-3">
          {/* System Status */}
          <div className="flex items-center gap-2 px-3 py-2 rounded-lg bg-conduit-900/30 border border-conduit-800/30">
            <div className="w-2 h-2 rounded-full bg-emerald-500 animate-pulse" />
            <div className="flex-1 min-w-0">
              <p className="text-xs font-medium text-gray-300 truncate">Scheduler</p>
              <p className="text-[10px] text-gray-500">Running</p>
            </div>
          </div>

          {isAuthEnabled ? (
            <div className="space-y-2">
              {/* User info */}
              <div className="flex items-center justify-between px-3 py-2">
                <span className="text-xs text-gray-400 truncate max-w-[120px]" title={user.keyName}>
                  {user.keyName}
                </span>
                <span
                  className={clsx(
                    'px-1.5 py-0.5 text-[10px] font-medium rounded border',
                    ROLE_COLORS[user.role] || ROLE_COLORS.viewer
                  )}
                >
                  {user.role}
                </span>
              </div>
              {/* Logout button */}
              <button
                onClick={logout}
                className="w-full flex items-center justify-center gap-2 px-3 py-1.5 text-xs text-gray-500 hover:text-gray-300 hover:bg-gray-800 rounded-lg transition-colors"
              >
                <LogOut size={12} />
                Sign out
              </button>
            </div>
          ) : (
            <div className="text-xs text-gray-500 font-mono px-3">v0.1.0</div>
          )}
        </div>
      </aside>

      {/* ── Main content ────────────────────────────────────── */}
      <main className="flex-1 overflow-y-auto">
        <Outlet />
      </main>
    </div>
  );
}
