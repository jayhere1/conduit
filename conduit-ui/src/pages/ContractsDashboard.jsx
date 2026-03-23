import { useState, useCallback, useMemo } from 'react';
import { Link } from 'react-router-dom';
import { listContracts, dagContracts, taskContracts } from '../api';
import { useApi } from '../hooks/useApi';
import Card, { StatCard } from '../components/Card';
import StatusBadge from '../components/StatusBadge';
import Spinner from '../components/Spinner';
import PageHeader from '../components/PageHeader';
import Button from '../components/Button';
import EmptyState from '../components/EmptyState';
import {
  ShieldCheck,
  ShieldAlert,
  ShieldX,
  AlertTriangle,
  CheckCircle,
  XCircle,
  ChevronDown,
  ChevronRight,
  Activity,
  Gauge,
  Eye,
  Search,
  Filter,
} from 'lucide-react';
import clsx from 'clsx';

// ─── Helpers ────────────────────────────────────────────────────────────────

const severityColor = (sev) => {
  switch (sev?.toLowerCase()) {
    case 'error':
      return 'bg-red-500/15 text-red-400 border-red-500/25';
    case 'warning':
      return 'bg-amber-500/15 text-amber-400 border-amber-500/25';
    default:
      return 'bg-gray-500/15 text-gray-400 border-gray-500/25';
  }
};

const checkTypeIcon = (type) => {
  const t = type?.toLowerCase() || '';
  if (t.includes('row_count') || t.includes('rowcount')) return Gauge;
  if (t.includes('freshness')) return Activity;
  if (t.includes('metric')) return Activity;
  if (t.includes('unique')) return ShieldCheck;
  if (t.includes('not_null')) return ShieldAlert;
  if (t.includes('custom')) return Eye;
  return ShieldCheck;
};

const checkTypeLabel = (type) => {
  const t = type?.toLowerCase() || '';
  if (t.includes('row_count') || t.includes('rowcount')) return 'Row Count';
  if (t.includes('freshness')) return 'Freshness';
  if (t.includes('unique')) return 'Unique';
  if (t.includes('not_null')) return 'Not Null';
  if (t.includes('accepted_values')) return 'Accepted Values';
  if (t.includes('value_range')) return 'Value Range';
  if (t.includes('referential')) return 'References';
  if (t.includes('row_count_delta') || t.includes('delta')) return 'Row Count Delta';
  if (t.includes('metric')) return 'Metric';
  if (t.includes('custom')) return 'Custom';
  return type || 'Unknown';
};

// ─── Contract Task Card ─────────────────────────────────────────────────────

function ContractTaskCard({ contract, onExpand, isExpanded }) {
  const { dag_id, task_id, check_count, checks } = contract;
  const errorChecks = checks?.filter((c) => c.severity === 'error') || [];
  const warningChecks = checks?.filter((c) => c.severity === 'warning') || [];

  return (
    <div className="glass-hover">
      <button
        onClick={() => onExpand(contract)}
        className="w-full text-left p-4 flex items-center gap-4 transition-all"
      >
        {/* Status Icon */}
        <div
          className={clsx(
            'w-10 h-10 rounded-lg flex items-center justify-center shrink-0',
            'bg-conduit-600/20 border border-conduit-600/30'
          )}
        >
          <ShieldCheck size={20} className="text-conduit-400" />
        </div>

        {/* Info */}
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <span className="text-sm font-semibold text-white truncate">
              {task_id}
            </span>
            <span className="text-xs text-gray-500 font-mono">{dag_id}</span>
          </div>
          <div className="flex items-center gap-3 mt-1">
            <span className="text-xs text-gray-400">
              {check_count} {check_count === 1 ? 'check' : 'checks'}
            </span>
            {errorChecks.length > 0 && (
              <span className="text-xs text-red-400 flex items-center gap-1">
                <XCircle size={10} />
                {errorChecks.length} error
              </span>
            )}
            {warningChecks.length > 0 && (
              <span className="text-xs text-amber-400 flex items-center gap-1">
                <AlertTriangle size={10} />
                {warningChecks.length} warning
              </span>
            )}
          </div>
        </div>

        {/* Expand Icon */}
        {isExpanded ? (
          <ChevronDown size={16} className="text-gray-400 shrink-0" />
        ) : (
          <ChevronRight size={16} className="text-gray-400 shrink-0" />
        )}
      </button>

      {/* Expanded Checks */}
      {isExpanded && checks && checks.length > 0 && (
        <div className="px-4 pb-4 pt-0">
          <div className="border-t border-conduit-800/50 pt-3 space-y-2">
            {checks.map((check, idx) => (
              <CheckRow key={idx} check={check} />
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

// ─── Check Row ──────────────────────────────────────────────────────────────

function CheckRow({ check }) {
  const Icon = checkTypeIcon(check.check_type);

  return (
    <div className="flex items-center gap-3 px-3 py-2 rounded-lg bg-conduit-900/30 border border-conduit-800/30">
      <Icon size={14} className="text-conduit-500 shrink-0" />
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2">
          <span className="text-xs font-medium text-gray-200 truncate">
            {check.name}
          </span>
        </div>
        {check.description && (
          <p className="text-xs text-gray-500 mt-0.5 truncate">
            {check.description}
          </p>
        )}
      </div>
      <span
        className={clsx(
          'inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium border shrink-0',
          severityColor(check.severity)
        )}
      >
        {check.severity}
      </span>
    </div>
  );
}

// ─── Expected Metrics Panel ─────────────────────────────────────────────────

function MetricsPanel({ metrics }) {
  if (!metrics || metrics.length === 0) return null;

  return (
    <Card title="Expected Metrics" subtitle="Metrics tasks must emit for contracts to validate" icon={Gauge}>
      <div className="flex flex-wrap gap-2">
        {metrics.map((m, i) => (
          <span
            key={i}
            className="px-2.5 py-1 rounded-md bg-conduit-900/50 border border-conduit-800/50 text-xs font-mono text-conduit-300"
          >
            {m}
          </span>
        ))}
      </div>
    </Card>
  );
}

// ─── Task Detail Panel ──────────────────────────────────────────────────────

function TaskDetailPanel({ dagId, taskId }) {
  const fetchTaskContracts = useCallback(
    () => taskContracts(dagId, taskId),
    [dagId, taskId]
  );
  const { data, loading } = useApi(fetchTaskContracts, [dagId, taskId]);

  if (loading) return <Spinner size="sm" />;
  if (!data) return null;

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex items-center gap-3">
        <div className="w-8 h-8 rounded-lg bg-conduit-600/20 border border-conduit-600/30 flex items-center justify-center">
          <ShieldCheck size={16} className="text-conduit-400" />
        </div>
        <div>
          <h3 className="text-sm font-semibold text-white">{taskId}</h3>
          <p className="text-xs text-gray-500">{dagId} &middot; {data.check_count} checks</p>
        </div>
      </div>

      {/* Expected Metrics */}
      {data.expected_metrics && data.expected_metrics.length > 0 && (
        <MetricsPanel metrics={data.expected_metrics} />
      )}

      {/* Checks */}
      <Card title="Contract Checks" icon={ShieldCheck}>
        {data.checks && data.checks.length > 0 ? (
          <div className="space-y-2">
            {data.checks.map((check, idx) => (
              <CheckRow key={idx} check={check} />
            ))}
          </div>
        ) : (
          <p className="text-xs text-gray-500">No checks defined</p>
        )}
      </Card>
    </div>
  );
}

// ─── Main Page ──────────────────────────────────────────────────────────────

export default function ContractsDashboard() {
  const { data, loading, error, refetch } = useApi(listContracts);
  const [expandedKey, setExpandedKey] = useState(null);
  const [selectedTask, setSelectedTask] = useState(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [severityFilter, setSeverityFilter] = useState('all');

  // Parse response
  const contracts = data?.contracts || [];
  const totalChecks = data?.total_checks || 0;
  const totalTasks = data?.total_tasks_with_contracts || 0;

  // Compute stats
  const stats = useMemo(() => {
    let errorCount = 0;
    let warningCount = 0;
    let uniqueMetrics = new Set();

    contracts.forEach((c) => {
      (c.checks || []).forEach((check) => {
        if (check.severity === 'error') errorCount++;
        if (check.severity === 'warning') warningCount++;
      });
    });

    return { errorCount, warningCount, uniqueMetrics: uniqueMetrics.size };
  }, [contracts]);

  // Filter
  const filtered = useMemo(() => {
    let result = contracts;

    if (searchQuery) {
      const q = searchQuery.toLowerCase();
      result = result.filter(
        (c) =>
          c.task_id?.toLowerCase().includes(q) ||
          c.dag_id?.toLowerCase().includes(q) ||
          c.checks?.some((ch) => ch.name?.toLowerCase().includes(q))
      );
    }

    if (severityFilter !== 'all') {
      result = result.filter((c) =>
        c.checks?.some((ch) => ch.severity === severityFilter)
      );
    }

    return result;
  }, [contracts, searchQuery, severityFilter]);

  const handleExpand = (contract) => {
    const key = `${contract.dag_id}.${contract.task_id}`;
    if (expandedKey === key) {
      setExpandedKey(null);
      setSelectedTask(null);
    } else {
      setExpandedKey(key);
      setSelectedTask({ dagId: contract.dag_id, taskId: contract.task_id });
    }
  };

  return (
    <div className="min-h-screen bg-conduit-950 p-6">
      <PageHeader
        title="Data Contracts"
        description="Evidence-based data quality assertions across all pipelines"
        actions={
          <Button onClick={refetch} variant="secondary" size="sm">
            Refresh
          </Button>
        }
      />

      {/* Stats Row */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
        <StatCard
          label="Tasks with Contracts"
          value={totalTasks}
          icon={ShieldCheck}
          sub="across all DAGs"
        />
        <StatCard
          label="Total Checks"
          value={totalChecks}
          icon={Gauge}
          sub="defined"
        />
        <StatCard
          label="Error Severity"
          value={stats.errorCount}
          icon={ShieldX}
          sub="block deployment"
        />
        <StatCard
          label="Warning Severity"
          value={stats.warningCount}
          icon={AlertTriangle}
          sub="non-blocking"
        />
      </div>

      {/* Search & Filter Bar */}
      <div className="flex items-center gap-3 mb-6">
        <div className="relative flex-1 max-w-md">
          <Search
            size={14}
            className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-500"
          />
          <input
            type="text"
            placeholder="Search tasks, DAGs, or checks..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="w-full pl-9 pr-3 py-2 rounded-lg bg-conduit-900/50 border border-conduit-800/50 text-sm text-gray-200 placeholder-gray-500 focus:outline-none focus:border-conduit-600/50"
          />
        </div>
        <div className="flex items-center gap-1">
          {['all', 'error', 'warning'].map((sev) => (
            <button
              key={sev}
              onClick={() => setSeverityFilter(sev)}
              className={clsx(
                'px-3 py-1.5 rounded-lg text-xs font-medium transition-colors border',
                severityFilter === sev
                  ? 'bg-conduit-600/20 text-conduit-300 border-conduit-600/30'
                  : 'text-gray-400 border-transparent hover:bg-conduit-900/50'
              )}
            >
              {sev === 'all' ? 'All' : sev.charAt(0).toUpperCase() + sev.slice(1)}
            </button>
          ))}
        </div>
      </div>

      {/* Main Content */}
      {loading ? (
        <div className="flex items-center justify-center py-20">
          <Spinner />
        </div>
      ) : error ? (
        <Card>
          <div className="text-center py-8">
            <ShieldX size={32} className="mx-auto text-red-400 mb-3" />
            <p className="text-sm text-red-400">{error}</p>
            <Button onClick={refetch} variant="secondary" size="sm" className="mt-4">
              Retry
            </Button>
          </div>
        </Card>
      ) : filtered.length === 0 ? (
        <EmptyState
          icon={ShieldCheck}
          title="No contracts found"
          description={
            searchQuery || severityFilter !== 'all'
              ? 'Try adjusting your search or filters'
              : 'Add contracts to your tasks in YAML or Python to see them here'
          }
        />
      ) : (
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Contract List */}
          <div className="lg:col-span-2 space-y-2">
            {filtered.map((contract) => {
              const key = `${contract.dag_id}.${contract.task_id}`;
              return (
                <ContractTaskCard
                  key={key}
                  contract={contract}
                  onExpand={handleExpand}
                  isExpanded={expandedKey === key}
                />
              );
            })}
          </div>

          {/* Detail Panel */}
          <div className="lg:col-span-1">
            {selectedTask ? (
              <div className="sticky top-6">
                <TaskDetailPanel
                  dagId={selectedTask.dagId}
                  taskId={selectedTask.taskId}
                />
              </div>
            ) : (
              <Card>
                <div className="text-center py-12">
                  <Eye size={24} className="mx-auto text-gray-600 mb-3" />
                  <p className="text-sm text-gray-500">
                    Click a task to view contract details
                  </p>
                  <p className="text-xs text-gray-600 mt-1">
                    Including expected metrics and check definitions
                  </p>
                </div>
              </Card>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
