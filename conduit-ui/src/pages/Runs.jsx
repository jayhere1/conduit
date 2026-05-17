import React, { useState, useMemo } from 'react';
import { Link, useSearchParams } from 'react-router-dom';
import { ChevronDown, Search, Activity, X } from 'lucide-react';
import { listAllRuns } from '../api';
import { useApi, usePolling } from '../hooks/useApi';
import Card from '../components/Card';
import StatusBadge from '../components/StatusBadge';
import Spinner from '../components/Spinner';
import PageHeader from '../components/PageHeader';
import EmptyState from '../components/EmptyState';
import { formatRelativeTime, formatDuration } from '../utils/time';

export default function Runs() {
  const [searchParams, setSearchParams] = useSearchParams();
  const envFilter = searchParams.get('environment') || '';

  const [statusFilter, setStatusFilter] = useState('all');
  const [dagFilter, setDagFilter] = useState('');
  const [sortConfig, setSortConfig] = useState({ key: 'startedAt', direction: 'desc' });

  // Server-side env filter (other filters stay client-side for now).
  const { data: runs, loading, error, refetch } = useApi(
    () => listAllRuns(envFilter ? { environment: envFilter } : {}),
    [envFilter]
  );

  const clearEnvFilter = () => {
    const next = new URLSearchParams(searchParams);
    next.delete('environment');
    setSearchParams(next, { replace: true });
  };

  // Set up polling for auto-refresh every 5 seconds
  // Use refetch directly as callback since it's the refetch function itself
  usePolling(() => refetch(), 5000, [refetch]);

  const filteredAndSortedRuns = useMemo(() => {
    if (!runs) return [];

    let filtered = runs;

    // Filter by status
    if (statusFilter !== 'all') {
      filtered = filtered.filter(run => run.status.toLowerCase() === statusFilter.toLowerCase());
    }

    // Filter by DAG name
    if (dagFilter.trim()) {
      const query = dagFilter.toLowerCase();
      filtered = filtered.filter(run => run.dagId.toLowerCase().includes(query));
    }

    // Sort
    filtered.sort((a, b) => {
      let aVal = a[sortConfig.key];
      let bVal = b[sortConfig.key];

      if (sortConfig.key === 'startedAt' || sortConfig.key === 'endedAt') {
        aVal = new Date(aVal || 0);
        bVal = new Date(bVal || 0);
      }

      if (aVal < bVal) return sortConfig.direction === 'asc' ? -1 : 1;
      if (aVal > bVal) return sortConfig.direction === 'asc' ? 1 : -1;
      return 0;
    });

    return filtered;
  }, [runs, statusFilter, dagFilter, sortConfig]);

  const handleSort = (key) => {
    setSortConfig(prev => ({
      key,
      direction: prev.key === key && prev.direction === 'desc' ? 'asc' : 'desc'
    }));
  };

  if (error) {
    return (
      <div className="p-6">
        <PageHeader title="Pipeline Runs" />
        <div className="mt-6 p-4 bg-red-500/10 border border-red-500/30 rounded-lg text-red-400">
          Failed to load runs: {error.message}
        </div>
      </div>
    );
  }

  return (
    <div className="p-6">
      <PageHeader title="Pipeline Runs" />

      {envFilter && (
        <div className="mt-4 inline-flex items-center gap-2 px-3 py-1.5 bg-conduit-700/40 border border-conduit-600/50 rounded-full text-sm text-conduit-100">
          <span className="text-conduit-300">environment:</span>
          <span className="font-mono">{envFilter}</span>
          <button
            onClick={clearEnvFilter}
            className="ml-1 text-conduit-400 hover:text-conduit-100 transition-colors"
            title="Clear environment filter"
          >
            <X className="w-3.5 h-3.5" />
          </button>
        </div>
      )}

      {/* Filter Bar */}
      <div className="mt-6 grid grid-cols-1 md:grid-cols-2 gap-4">
        {/* Status Filter */}
        <div>
          <label className="block text-sm font-medium text-gray-300 mb-2">Status</label>
          <div className="relative">
            <select
              value={statusFilter}
              onChange={(e) => setStatusFilter(e.target.value)}
              className="w-full bg-conduit-900/50 border border-conduit-700/50 rounded-lg px-4 py-2 text-gray-200 focus:outline-none focus:ring-2 focus:ring-conduit-500 appearance-none pr-10"
            >
              <option value="all">All</option>
              <option value="running">Running</option>
              <option value="success">Success</option>
              <option value="failed">Failed</option>
            </select>
            <ChevronDown className="absolute right-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400 pointer-events-none" />
          </div>
        </div>

        {/* DAG Filter */}
        <div>
          <label className="block text-sm font-medium text-gray-300 mb-2">DAG Name</label>
          <div className="relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-500" />
            <input
              type="text"
              placeholder="Search DAG..."
              value={dagFilter}
              onChange={(e) => setDagFilter(e.target.value)}
              className="w-full bg-conduit-900/50 border border-conduit-700/50 rounded-lg px-4 py-2 pl-10 text-gray-200 placeholder-gray-500 focus:outline-none focus:ring-2 focus:ring-conduit-500"
            />
          </div>
        </div>
      </div>

      {/* Runs Table */}
      <div className="mt-6">
        {loading && !runs?.length ? (
          <div className="flex justify-center items-center py-12">
            <Spinner />
          </div>
        ) : filteredAndSortedRuns.length === 0 ? (
          <EmptyState
            title="No pipeline runs"
            description={
              runs?.length === 0
                ? "No runs have been triggered yet."
                : "No runs match the selected filters."
            }
          />
        ) : (
          <div className="glass rounded-lg overflow-hidden">
            <table className="w-full">
              <thead>
                <tr className="border-b border-conduit-700/30 bg-conduit-900/50">
                  <th
                    className="px-6 py-4 text-left text-xs font-semibold text-gray-300 uppercase tracking-wider cursor-pointer hover:text-gray-200"
                    onClick={() => handleSort('id')}
                  >
                    Run ID
                  </th>
                  <th
                    className="px-6 py-4 text-left text-xs font-semibold text-gray-300 uppercase tracking-wider cursor-pointer hover:text-gray-200"
                    onClick={() => handleSort('dagId')}
                  >
                    DAG Name
                  </th>
                  <th
                    className="px-6 py-4 text-left text-xs font-semibold text-gray-300 uppercase tracking-wider cursor-pointer hover:text-gray-200"
                    onClick={() => handleSort('status')}
                  >
                    Status
                  </th>
                  <th className="px-6 py-4 text-left text-xs font-semibold text-gray-300 uppercase tracking-wider">
                    Environment
                  </th>
                  <th className="px-6 py-4 text-left text-xs font-semibold text-gray-300 uppercase tracking-wider">
                    Triggered By
                  </th>
                  <th
                    className="px-6 py-4 text-left text-xs font-semibold text-gray-300 uppercase tracking-wider cursor-pointer hover:text-gray-200"
                    onClick={() => handleSort('startedAt')}
                  >
                    Started At
                  </th>
                  <th className="px-6 py-4 text-left text-xs font-semibold text-gray-300 uppercase tracking-wider">
                    Duration
                  </th>
                  <th className="px-6 py-4 text-left text-xs font-semibold text-gray-300 uppercase tracking-wider">
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y divide-conduit-700/20">
                {filteredAndSortedRuns.map((run) => (
                  <tr
                    key={run.id}
                    className="hover:bg-conduit-800/30 transition-colors"
                  >
                    <td className="px-6 py-4 text-sm">
                      <Link
                        to={`/runs/${run.id}`}
                        className="font-mono text-conduit-400 hover:text-conduit-300 transition-colors"
                      >
                        {run.id.substring(0, 8)}
                      </Link>
                    </td>
                    <td className="px-6 py-4 text-sm">
                      <Link
                        to={`/dags/${run.dagId}`}
                        className="text-gray-300 hover:text-gray-200 transition-colors"
                      >
                        {run.dagId}
                      </Link>
                    </td>
                    <td className="px-6 py-4 text-sm">
                      <StatusBadge status={run.status} dot={true} />
                    </td>
                    <td className="px-6 py-4 text-sm">
                      {run.environment ? (
                        <Link
                          to={`/runs?environment=${encodeURIComponent(run.environment)}`}
                          className="font-mono text-xs text-conduit-300 hover:text-conduit-100 transition-colors"
                        >
                          {run.environment}
                        </Link>
                      ) : (
                        <span className="text-gray-500">-</span>
                      )}
                    </td>
                    <td className="px-6 py-4 text-sm text-gray-400">
                      {run.triggeredBy || '-'}
                    </td>
                    <td className="px-6 py-4 text-sm text-gray-400">
                      {formatRelativeTime(run.startedAt)}
                    </td>
                    <td className="px-6 py-4 text-sm text-gray-400">
                      {formatDuration(run.startedAt, run.endedAt)}
                    </td>
                    <td className="px-6 py-4 text-sm">
                      <Link
                        to={`/runs/${run.id}/live`}
                        className="flex items-center gap-1 text-conduit-400 hover:text-conduit-300 transition-colors"
                        title="Live execution view"
                      >
                        <Activity size={14} />
                      </Link>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* Auto-refresh indicator */}
      {!error && (
        <div className="mt-4 text-xs text-gray-500">
          Auto-refreshing every 5 seconds
        </div>
      )}
    </div>
  );
}
