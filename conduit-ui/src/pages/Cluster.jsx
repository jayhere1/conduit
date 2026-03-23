import { useState, useEffect } from 'react';
import { usePolling } from '../hooks/useApi';
import { getClusterStatus, drainWorker } from '../api';
import Card, { StatCard } from '../components/Card';
import StatusBadge from '../components/StatusBadge';
import Button from '../components/Button';
import PageHeader from '../components/PageHeader';
import EmptyState from '../components/EmptyState';
import clsx from 'clsx';
import {
  Server,
  Activity,
  Cpu,
  HardDrive,
  Clock,
  Zap,
  AlertTriangle,
  CheckCircle,
  XCircle,
  Pause,
  RefreshCw,
} from 'lucide-react';

// State badge colors
const STATE_COLORS = {
  Active: 'bg-emerald-500/15 text-emerald-400 border-emerald-500/25',
  Draining: 'bg-amber-500/15 text-amber-400 border-amber-500/25',
  Disconnected: 'bg-orange-500/15 text-orange-400 border-orange-500/25',
  Dead: 'bg-red-500/15 text-red-400 border-red-500/25',
};

const STATE_DOT = {
  Active: 'bg-emerald-400 animate-live',
  Draining: 'bg-amber-400',
  Disconnected: 'bg-orange-400',
  Dead: 'bg-red-400',
};

// Health status colors
const HEALTH_COLORS = {
  Healthy: 'bg-emerald-500/15 text-emerald-300 border-emerald-500/25',
  Degraded: 'bg-amber-500/15 text-amber-300 border-amber-500/25',
  Unhealthy: 'bg-red-500/15 text-red-300 border-red-500/25',
};

const HEALTH_ICONS = {
  Healthy: CheckCircle,
  Degraded: AlertTriangle,
  Unhealthy: XCircle,
};

function ClusterHealthBanner({ status }) {
  if (!status) return null;

  const health = status.health || 'Healthy';
  const HealthIcon = HEALTH_ICONS[health] || CheckCircle;

  return (
    <div className={clsx('px-6 py-4 rounded-lg border', HEALTH_COLORS[health])}>
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <HealthIcon size={24} />
          <div>
            <h3 className="text-base font-semibold">Cluster Status: {health}</h3>
            <p className="text-sm opacity-80">
              Coordinator uptime: {formatUptime(status.uptimeSecs || 0)}
            </p>
          </div>
        </div>
        <div className="text-right text-sm">
          <p className="opacity-75">{status.totalWorkers || 0} workers • {status.runningTasks || 0} running • {status.queuedTasks || 0} queued</p>
        </div>
      </div>
    </div>
  );
}

function WorkerRow({ worker, onDrain, isDraining }) {
  const capacityPercent = worker.capacity > 0 ? (worker.activeTasks / worker.capacity) * 100 : 0;
  const cpuPercent = worker.cpuUsagePercent || 0;
  const memPercent = worker.memUsagePercent || 0;

  return (
    <div className="glass px-4 py-3 rounded-lg border border-conduit-700/30 flex items-center justify-between gap-4 hover:border-conduit-600/50 transition-colors">
      {/* Worker ID & Hostname */}
      <div className="min-w-0 flex-1">
        <h4 className="text-sm font-semibold text-conduit-50 font-mono truncate">{worker.workerId}</h4>
        <p className="text-xs text-conduit-400 font-mono truncate">{worker.hostname || 'N/A'}</p>
      </div>

      {/* State Badge */}
      <div className="flex items-center gap-1.5">
        <span
          className={clsx(
            'status-dot',
            STATE_DOT[worker.state] || 'bg-gray-400'
          )}
        />
        <span
          className={clsx(
            'px-2 py-0.5 text-xs font-medium border rounded-full',
            STATE_COLORS[worker.state] || STATE_COLORS.Disconnected
          )}
        >
          {worker.state || 'Disconnected'}
        </span>
      </div>

      {/* Capacity Bar */}
      <div className="flex items-center gap-2 min-w-max">
        <div className="w-24 h-1.5 bg-conduit-800/50 rounded-full overflow-hidden border border-conduit-700/30">
          <div
            className="h-full bg-gradient-to-r from-conduit-500 to-conduit-400 transition-all"
            style={{ width: `${Math.min(capacityPercent, 100)}%` }}
          />
        </div>
        <span className="text-xs text-conduit-400 font-mono w-16 text-right">
          {worker.activeTasks}/{worker.capacity}
        </span>
      </div>

      {/* Pool Affinity Tags */}
      {worker.pools && worker.pools.length > 0 && (
        <div className="flex gap-1 flex-wrap max-w-xs">
          {worker.pools.slice(0, 2).map((pool) => (
            <span
              key={pool}
              className="px-1.5 py-0.5 text-[10px] font-medium bg-conduit-800/50 text-conduit-400 rounded border border-conduit-700/30"
            >
              {pool}
            </span>
          ))}
          {worker.pools.length > 2 && (
            <span className="px-1.5 py-0.5 text-[10px] font-medium text-conduit-500">
              +{worker.pools.length - 2}
            </span>
          )}
        </div>
      )}

      {/* CPU/Mem % */}
      <div className="flex gap-3 text-xs text-conduit-400 min-w-max">
        <div className="flex items-center gap-1">
          <Cpu size={12} className="text-blue-400" />
          <span className="w-6 text-right">{cpuPercent}%</span>
        </div>
        <div className="flex items-center gap-1">
          <HardDrive size={12} className="text-purple-400" />
          <span className="w-6 text-right">{memPercent}%</span>
        </div>
      </div>

      {/* Last Heartbeat */}
      <div className="flex items-center gap-1 text-xs text-conduit-500 min-w-max">
        <Clock size={12} />
        <span>{formatRelativeTime(worker.lastHeartbeatSecs || 0)}</span>
      </div>

      {/* Lifetime Stats */}
      <div className="text-xs text-conduit-500 min-w-max">
        <span className="text-emerald-400">{worker.tasksCompleted || 0}</span>
        <span className="text-conduit-600"> / </span>
        <span className="text-red-400">{worker.tasksFailed || 0}</span>
      </div>

      {/* Drain Button */}
      {worker.state === 'Active' && (
        <button
          onClick={() => onDrain(worker.workerId)}
          disabled={isDraining}
          className={clsx(
            'flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium rounded-lg border transition-all',
            isDraining
              ? 'bg-conduit-800/30 text-conduit-500 border-conduit-700/30 cursor-not-allowed'
              : 'bg-conduit-800/50 hover:bg-conduit-700/50 text-conduit-300 border-conduit-700/50'
          )}
        >
          <Pause size={12} />
          {isDraining ? 'Draining...' : 'Drain'}
        </button>
      )}
    </div>
  );
}

function RunningTaskRow({ task }) {
  return (
    <div className="glass px-4 py-3 rounded-lg border border-conduit-700/30 flex items-center justify-between gap-4 hover:border-conduit-600/50 transition-colors">
      <div className="min-w-0 flex-1">
        <h4 className="text-sm font-semibold text-conduit-50 font-mono truncate">
          {task.assignmentId}
        </h4>
        <p className="text-xs text-conduit-400 font-mono truncate">{task.taskId}</p>
      </div>

      <div className="flex items-center gap-4 text-sm">
        <div className="flex items-center gap-1 text-conduit-400">
          <Server size={14} />
          <span className="font-mono text-xs">{task.workerId}</span>
        </div>

        <div className="flex items-center gap-1 text-conduit-400">
          <Zap size={14} />
          <span className="text-xs">{formatDuration(task.durationSecs || 0)}</span>
        </div>

        <span className="px-2 py-0.5 text-xs font-medium bg-conduit-800/50 text-conduit-400 rounded border border-conduit-700/30">
          {task.dagId}
        </span>
      </div>
    </div>
  );
}

// Helper functions
function formatUptime(seconds) {
  if (seconds < 60) return `${Math.floor(seconds)}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h`;
  return `${Math.floor(seconds / 86400)}d`;
}

function formatRelativeTime(seconds) {
  if (seconds < 60) return `${Math.floor(seconds)}s ago`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
  return `${Math.floor(seconds / 86400)}d ago`;
}

function formatDuration(seconds) {
  if (seconds < 60) return `${Math.floor(seconds)}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m`;
  return `${Math.floor(seconds / 3600)}h`;
}

export default function Cluster() {
  const { data: status, loading, error, refetch } = usePolling(getClusterStatus, 5000);
  const [drainingWorkers, setDrainingWorkers] = useState(new Set());

  const handleDrain = async (workerId) => {
    setDrainingWorkers((prev) => new Set([...prev, workerId]));
    try {
      await drainWorker(workerId);
      // Refetch to get updated status
      setTimeout(refetch, 500);
    } catch (err) {
      console.error('Failed to drain worker:', err);
    } finally {
      setDrainingWorkers((prev) => {
        const next = new Set(prev);
        next.delete(workerId);
        return next;
      });
    }
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-screen">
        <div className="flex flex-col items-center gap-3">
          <Activity className="w-8 h-8 text-conduit-400 animate-spin" />
          <p className="text-sm text-conduit-400">Loading cluster status...</p>
        </div>
      </div>
    );
  }

  const workers = status?.workers || [];
  const runningTasks = status?.runningTasks || [];
  const totalWorkers = status?.totalWorkers || 0;
  const activeWorkers = workers.filter((w) => w.state === 'Active').length;

  return (
    <div className="min-h-screen bg-gradient-to-br from-conduit-950 via-conduit-900 to-conduit-950">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <PageHeader
          title="Distributed Cluster"
          description="Monitor workers, tasks, and cluster health"
          action={
            <Button
              onClick={refetch}
              className="flex items-center gap-2"
            >
              <RefreshCw className="w-4 h-4" />
              Refresh
            </Button>
          }
        />

        {error && (
          <div className="mb-6 p-4 bg-red-900/20 border border-red-700/50 rounded-lg text-red-200">
            Error loading cluster status: {error}
          </div>
        )}

        {/* Cluster Health Banner */}
        {status && <ClusterHealthBanner status={status} />}

        {/* Stats Row */}
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-4 my-8">
          <StatCard label="Total Workers" value={totalWorkers} icon={Server} />
          <StatCard label="Active Workers" value={activeWorkers} icon={Activity} />
          <StatCard label="Running Tasks" value={status?.runningTasks || 0} icon={Zap} />
          <StatCard
            label="Queued Tasks"
            value={status?.queuedTasks || 0}
            icon={Clock}
          />
        </div>

        {/* Workers Section */}
        <div className="mb-8">
          <h2 className="text-sm font-semibold text-conduit-400 uppercase tracking-wide mb-4 flex items-center gap-2">
            <span className="w-8 h-px bg-conduit-700/50" />
            Workers
            <span className="text-conduit-600 font-normal">({workers.length})</span>
            <span className="flex-1 h-px bg-conduit-700/50" />
          </h2>

          {workers.length === 0 ? (
            <EmptyState
              icon={Server}
              title="No workers connected"
              description="Start a worker to connect to the cluster"
              action={
                <code className="block mt-4 px-4 py-3 bg-conduit-900 border border-conduit-700/50 rounded-lg text-sm text-conduit-300 font-mono text-center">
                  conduit worker --coordinator localhost:9400 --capacity 4
                </code>
              }
            />
          ) : (
            <div className="space-y-2">
              {workers.map((worker) => (
                <WorkerRow
                  key={worker.workerId}
                  worker={worker}
                  onDrain={handleDrain}
                  isDraining={drainingWorkers.has(worker.workerId)}
                />
              ))}
            </div>
          )}
        </div>

        {/* Running Tasks Section */}
        {runningTasks.length > 0 && (
          <div>
            <h2 className="text-sm font-semibold text-conduit-400 uppercase tracking-wide mb-4 flex items-center gap-2">
              <span className="w-8 h-px bg-conduit-700/50" />
              Running Tasks
              <span className="text-conduit-600 font-normal">({runningTasks.length})</span>
              <span className="flex-1 h-px bg-conduit-700/50" />
            </h2>
            <div className="space-y-2">
              {runningTasks.map((task) => (
                <RunningTaskRow key={task.assignmentId} task={task} />
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
