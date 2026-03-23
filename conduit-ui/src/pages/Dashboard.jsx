import { useState, useEffect, useCallback } from 'react';
import { systemInfo, listAllRuns, listDags, listEnvironments, health } from '../api';
import { useApi, usePolling } from '../hooks/useApi';
import { useWebSocket } from '../hooks/useWebSocket';
import { StatCard } from '../components/Card';
import Card from '../components/Card';
import StatusBadge from '../components/StatusBadge';
import Spinner from '../components/Spinner';
import PageHeader from '../components/PageHeader';
import {
  Activity,
  GitBranch,
  Play,
  Layers,
  Clock,
  Zap,
  CheckCircle,
  AlertCircle,
  Radio,
  TrendingUp,
  Gauge,
  Server,
  Cpu,
} from 'lucide-react';
import { AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts';
import { formatMs as formatTime, formatShortTime as formatDate, formatRelativeTime } from '../utils/time';

const truncateId = (id, length = 8) => {
  if (!id) return 'N/A';
  return id.length > length ? id.substring(0, length) + '...' : id;
};

const getEventTypeBadgeColor = (eventType) => {
  const type = eventType?.toLowerCase() || '';
  if (type.includes('start')) return 'bg-blue-500/15 text-blue-400 border-blue-500/25';
  if (type.includes('complete') || type.includes('success')) return 'bg-emerald-500/15 text-emerald-400 border-emerald-500/25';
  if (type.includes('fail') || type.includes('error')) return 'bg-red-500/15 text-red-400 border-red-500/25';
  if (type.includes('skip')) return 'bg-gray-500/15 text-gray-400 border-gray-500/25';
  return 'bg-conduit-500/15 text-conduit-400 border-conduit-500/25';
};

export default function Dashboard() {
  // Polling for system info
  const { data: systemData, loading: systemLoading } = usePolling(systemInfo, 5000);

  // API calls for runs, DAGs, and environments
  const { data: runsData, loading: runsLoading } = useApi(listAllRuns);
  const { data: dagsData, loading: dagsLoading } = useApi(listDags);
  const { data: envsData, loading: envsLoading } = useApi(listEnvironments);
  const { data: healthData, loading: healthLoading } = useApi(health);

  // WebSocket for live events
  const { events: wsEvents, connected: wsConnected } = useWebSocket(10);

  // Extract recent runs (last 8)
  const recentRuns = runsData ? runsData.slice(0, 8) : [];

  // Calculate derived stats
  const totalDags = dagsData?.length || 0;
  const activeRuns = runsData?.filter((r) => r.status?.toLowerCase() === 'running').length || 0;
  const totalEnvironments = envsData?.length || 0;
  const snapshots = systemData?.snapshots || 0;

  // Calculate today's runs and success rate
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  const todaysRuns = runsData?.filter((r) => {
    const runDate = new Date(r.startedAt);
    runDate.setHours(0, 0, 0, 0);
    return runDate.getTime() === today.getTime();
  }) || [];
  const successfulRuns = todaysRuns.filter((r) => r.status?.toLowerCase() === 'success').length;
  const successRate = todaysRuns.length > 0 ? Math.round((successfulRuns / todaysRuns.length) * 100) : 0;

  // Format uptime
  const uptime = systemData?.uptime_seconds || 0;
  const uptimeFormatted = formatTime(uptime * 1000);

  // Recent runs for timeline (last 10)
  const timelineRuns = runsData ? runsData.slice(0, 10) : [];

  return (
    <div className="min-h-screen bg-conduit-950 p-6">
      {/* Page Header with Live Indicator */}
      <div className="mb-8">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-2xl font-bold text-white">Dashboard</h2>
            <p className="text-sm text-gray-400 mt-1">
              Pipeline orchestrator status and analytics
            </p>
          </div>
          <div className="flex items-center gap-2 px-3 py-1.5 rounded-full bg-conduit-900/40 border border-conduit-800/50">
            <div
              className={`w-2 h-2 rounded-full ${
                wsConnected ? 'bg-emerald-400 animate-pulse' : 'bg-gray-400'
              }`}
            />
            <span className={`text-xs font-medium ${wsConnected ? 'text-emerald-400' : 'text-gray-400'}`}>
              {wsConnected ? 'Live' : 'Offline'}
            </span>
          </div>
        </div>
      </div>

      {/* Stats Cards Row */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
        <StatCard
          label="Total DAGs"
          value={totalDags}
          icon={GitBranch}
          sub={dagsLoading ? 'Loading...' : 'configured'}
        />
        <StatCard
          label="Today's Runs"
          value={todaysRuns.length}
          icon={Play}
          sub={runsLoading ? 'Loading...' : 'executed'}
          trend={activeRuns > 0 ? 1 : 0}
        />
        <StatCard
          label="Success Rate"
          value={`${successRate}%`}
          icon={TrendingUp}
          sub={todaysRuns.length === 0 ? 'No runs' : `${successfulRuns} of ${todaysRuns.length}`}
        />
        <StatCard
          label="Active Environments"
          value={totalEnvironments}
          icon={Layers}
          sub={envsLoading ? 'Loading...' : 'configured'}
        />
      </div>

      {/* Quick Actions */}
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-4 mb-8">
        <button className="glass px-4 py-3 rounded-lg flex items-center justify-center gap-2 text-sm font-medium text-conduit-300 hover:text-conduit-200 hover:border-conduit-500/50 border border-conduit-700/50 transition-all hover:shadow-lg hover:shadow-conduit-500/10">
          <Zap className="w-4 h-4" />
          Compile All DAGs
        </button>
        <button className="glass px-4 py-3 rounded-lg flex items-center justify-center gap-2 text-sm font-medium text-conduit-300 hover:text-conduit-200 hover:border-conduit-500/50 border border-conduit-700/50 transition-all hover:shadow-lg hover:shadow-conduit-500/10">
          <Play className="w-4 h-4" />
          Generate Plan
        </button>
        <button className="glass px-4 py-3 rounded-lg flex items-center justify-center gap-2 text-sm font-medium text-conduit-300 hover:text-conduit-200 hover:border-conduit-500/50 border border-conduit-700/50 transition-all hover:shadow-lg hover:shadow-conduit-500/10">
          <Layers className="w-4 h-4" />
          Create Environment
        </button>
      </div>

      {/* Main Grid: Recent Runs & Live Events */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-8">
        {/* Recent Runs Timeline */}
        <div className="lg:col-span-2">
          <Card title="Recent Runs Timeline" icon={Clock}>
            {runsLoading ? (
              <Spinner size="sm" />
            ) : timelineRuns.length === 0 ? (
              <div className="text-center py-8">
                <p className="text-gray-400 text-sm">No runs yet</p>
              </div>
            ) : (
              <div className="space-y-3">
                {timelineRuns.map((run, idx) => (
                  <div key={run.id} className="flex items-start gap-3 pb-3 border-b border-conduit-800/30 last:border-0">
                    <div className="flex-shrink-0 mt-1">
                      <div
                        className={`w-2.5 h-2.5 rounded-full ${
                          run.status?.toLowerCase() === 'success'
                            ? 'bg-emerald-500'
                            : run.status?.toLowerCase() === 'running'
                            ? 'bg-blue-500 animate-pulse'
                            : run.status?.toLowerCase() === 'failed'
                            ? 'bg-red-500'
                            : 'bg-gray-500'
                        }`}
                      />
                    </div>
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center justify-between gap-2">
                        <div>
                          <p className="text-sm font-medium text-gray-200 truncate">
                            {run.dagId || 'Unknown DAG'}
                          </p>
                          <p className="text-xs text-gray-500 mt-0.5">
                            {formatRelativeTime(run.startedAt)}
                          </p>
                        </div>
                        <StatusBadge status={run.status} dot size="sm" />
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </Card>
        </div>

        {/* Live Events */}
        <div>
          <Card title="Live Events" icon={Radio}>
            <div className="space-y-2 max-h-96 overflow-y-auto">
              {wsEvents.length === 0 ? (
                <div className="text-center py-8">
                  <p className="text-gray-400 text-xs">Waiting for events...</p>
                </div>
              ) : (
                wsEvents.map((evt, idx) => {
                  const timestamp = new Date(evt._ts || Date.now());
                  const timeStr = timestamp.toLocaleTimeString('en-US', {
                    hour: '2-digit',
                    minute: '2-digit',
                    second: '2-digit',
                  });
                  const eventType = evt.event_type || evt.type || 'event';
                  const badgeColor = getEventTypeBadgeColor(eventType);

                  return (
                    <div
                      key={idx}
                      className="p-2.5 rounded-lg bg-conduit-900/30 border border-conduit-800/50 hover:bg-conduit-900/50 transition-colors"
                    >
                      <div className="flex items-center gap-2 mb-1">
                        <span className="text-xs text-gray-500">{timeStr}</span>
                        <span
                          className={`px-2 py-0.5 rounded-full text-xs font-medium border inline-block ${badgeColor}`}
                        >
                          {eventType}
                        </span>
                      </div>
                      {evt.details && (
                        <p className="text-xs text-gray-400 truncate">
                          {typeof evt.details === 'string'
                            ? evt.details
                            : JSON.stringify(evt.details).substring(0, 60)}
                        </p>
                      )}
                    </div>
                  );
                })
              )}
            </div>
          </Card>
        </div>
      </div>

      {/* System Health & Metrics */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <div>
          <Card title="System Health" icon={CheckCircle}>
            {healthLoading || systemLoading ? (
              <Spinner size="sm" />
            ) : (
              <div className="space-y-4">
                <div>
                  <div className="flex items-center justify-between mb-1">
                    <span className="text-xs font-medium text-gray-400">Status</span>
                    {healthData?.status === 'healthy' ? (
                      <CheckCircle size={14} className="text-emerald-400" />
                    ) : (
                      <AlertCircle size={14} className="text-red-400" />
                    )}
                  </div>
                  <div className="text-sm font-medium text-white">
                    {healthData?.status === 'healthy' ? 'Healthy' : 'Degraded'}
                  </div>
                  <p className="text-xs text-gray-500 mt-0.5">
                    {healthData?.message || 'System running normally'}
                  </p>
                </div>

                <div className="pt-3 border-t border-conduit-800/50">
                  <div className="flex items-center justify-between">
                    <span className="text-xs font-medium text-gray-400">Uptime</span>
                    <span className="text-sm font-medium text-white">
                      {uptimeFormatted}
                    </span>
                  </div>
                </div>

                {systemData?.last_snapshot && (
                  <div className="pt-3 border-t border-conduit-800/50">
                    <div className="flex items-center justify-between">
                      <span className="text-xs font-medium text-gray-400">Last Snapshot</span>
                      <span className="text-xs text-gray-400">
                        {formatDate(systemData.last_snapshot)}
                      </span>
                    </div>
                  </div>
                )}

                {healthData?.checks && Object.keys(healthData.checks).length > 0 && (
                  <div className="pt-3 border-t border-conduit-800/50">
                    <p className="text-xs font-medium text-gray-400 mb-2">Health Checks</p>
                    <div className="space-y-1">
                      {Object.entries(healthData.checks).map(([name, status]) => (
                        <div
                          key={name}
                          className="flex items-center justify-between text-xs"
                        >
                          <span className="text-gray-500 capitalize">{name}</span>
                          <div className={`w-2 h-2 rounded-full ${
                            status === 'ok' ? 'bg-emerald-400' : 'bg-red-400'
                          }`} />
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            )}
          </Card>
        </div>

        {/* Scheduler & Performance Metrics */}
        <div className="lg:col-span-2">
          <Card title="Scheduler & Performance" icon={Gauge}>
            {healthLoading || systemLoading ? (
              <Spinner size="sm" />
            ) : (
              <div className="space-y-4">
                {/* Scheduler Status */}
                <div className="flex items-center justify-between p-3 rounded-lg bg-conduit-900/20 border border-conduit-800/30">
                  <div>
                    <p className="text-xs font-medium text-gray-400 uppercase tracking-wide">Scheduler</p>
                    <p className="text-sm font-medium text-gray-200 mt-0.5">
                      {healthData?.status === 'healthy' ? 'Running' : 'Degraded'}
                    </p>
                  </div>
                  <div className={`w-3 h-3 rounded-full ${healthData?.status === 'healthy' ? 'bg-emerald-500' : 'bg-red-500'}`} />
                </div>

                {/* Compilation Speed */}
                <div className="flex items-center justify-between p-3 rounded-lg bg-conduit-900/20 border border-conduit-800/30">
                  <div>
                    <p className="text-xs font-medium text-gray-400 uppercase tracking-wide">Compilation</p>
                    <p className="text-sm font-medium text-gray-200 mt-0.5">
                      {systemData?.compilation_duration_ms
                        ? `${Math.round(systemData.compilation_duration_ms)}ms`
                        : 'N/A'}
                    </p>
                  </div>
                  <Cpu className="w-4 h-4 text-conduit-500" />
                </div>

                {/* Event Store Size */}
                <div className="flex items-center justify-between p-3 rounded-lg bg-conduit-900/20 border border-conduit-800/30">
                  <div>
                    <p className="text-xs font-medium text-gray-400 uppercase tracking-wide">Events Stored</p>
                    <p className="text-sm font-medium text-gray-200 mt-0.5">
                      {systemData?.snapshots || 0} snapshots
                    </p>
                  </div>
                  <Server className="w-4 h-4 text-conduit-500" />
                </div>

                {/* Uptime */}
                <div className="flex items-center justify-between p-3 rounded-lg bg-conduit-900/20 border border-conduit-800/30">
                  <div>
                    <p className="text-xs font-medium text-gray-400 uppercase tracking-wide">Uptime</p>
                    <p className="text-sm font-medium text-gray-200 mt-0.5">
                      {uptimeFormatted}
                    </p>
                  </div>
                  <Activity className="w-4 h-4 text-emerald-500" />
                </div>
              </div>
            )}
          </Card>
        </div>
      </div>
    </div>
  );
}
