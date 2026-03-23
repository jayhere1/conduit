import { useState, useMemo } from 'react';
import {
  BarChart3,
  TrendingUp,
  TrendingDown,
  Minus,
  Search,
  Filter,
  Activity,
  Gauge,
  Clock,
  AlertTriangle,
  CheckCircle,
  Database,
} from 'lucide-react';
import { listContracts, listAllRuns } from '../api';
import { useApi } from '../hooks/useApi';
import Card, { StatCard } from '../components/Card';
import Spinner from '../components/Spinner';
import PageHeader from '../components/PageHeader';
import Button from '../components/Button';
import EmptyState from '../components/EmptyState';
import clsx from 'clsx';

// ─── Sparkline Component ─────────────────────────────────────────────────────

function Sparkline({ data, width = 120, height = 32, color = '#6366f1', showDots = false }) {
  if (!data || data.length < 2) {
    return (
      <svg width={width} height={height}>
        <line x1="0" y1={height / 2} x2={width} y2={height / 2} stroke="#374151" strokeWidth="1" strokeDasharray="4 4" />
      </svg>
    );
  }

  const min = Math.min(...data);
  const max = Math.max(...data);
  const range = max - min || 1;
  const padding = 2;

  const points = data.map((val, i) => {
    const x = (i / (data.length - 1)) * (width - 2 * padding) + padding;
    const y = height - padding - ((val - min) / range) * (height - 2 * padding);
    return { x, y, val };
  });

  const pathD = points.map((p, i) => `${i === 0 ? 'M' : 'L'} ${p.x} ${p.y}`).join(' ');

  // Area fill
  const areaD = `${pathD} L ${points[points.length - 1].x} ${height} L ${points[0].x} ${height} Z`;

  return (
    <svg width={width} height={height} className="overflow-visible">
      {/* Area fill */}
      <path d={areaD} fill={`${color}15`} />
      {/* Line */}
      <path d={pathD} fill="none" stroke={color} strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
      {/* Dots */}
      {showDots && points.map((p, i) => (
        <circle key={i} cx={p.x} cy={p.y} r="2" fill={color} />
      ))}
      {/* Last point highlight */}
      <circle
        cx={points[points.length - 1].x}
        cy={points[points.length - 1].y}
        r="3"
        fill={color}
        stroke="#111827"
        strokeWidth="1.5"
      />
    </svg>
  );
}

// ─── Metric Card ─────────────────────────────────────────────────────────────

function MetricCard({ metric, isSelected, onClick }) {
  const {
    name,
    taskId,
    dagId,
    currentValue,
    previousValue,
    history,
    checkType,
    severity,
    threshold,
  } = metric;

  const trend = useMemo(() => {
    if (currentValue == null || previousValue == null) return 'stable';
    if (currentValue > previousValue) return 'up';
    if (currentValue < previousValue) return 'down';
    return 'stable';
  }, [currentValue, previousValue]);

  const trendColor = {
    up: 'text-green-400',
    down: 'text-red-400',
    stable: 'text-gray-500',
  };

  const TrendIcon = {
    up: TrendingUp,
    down: TrendingDown,
    stable: Minus,
  }[trend];

  const pctChange = previousValue
    ? (((currentValue - previousValue) / previousValue) * 100).toFixed(1)
    : null;

  // Determine sparkline color based on severity/threshold
  const sparkColor = severity === 'error' ? '#ef4444' : severity === 'warning' ? '#f59e0b' : '#6366f1';

  return (
    <button
      onClick={onClick}
      className={clsx(
        'w-full text-left p-4 rounded-xl border transition-all',
        isSelected
          ? 'border-purple-500/50 bg-purple-500/5'
          : 'border-conduit-800/50 bg-conduit-900/30 hover:bg-conduit-800/30 hover:border-conduit-700/50'
      )}
    >
      {/* Header */}
      <div className="flex items-center justify-between mb-2">
        <div className="flex items-center gap-2 min-w-0">
          <Gauge size={12} className="text-conduit-500 shrink-0" />
          <span className="text-xs font-semibold text-white truncate">{name}</span>
        </div>
        {severity && (
          <span
            className={clsx(
              'px-1.5 py-0.5 rounded text-[9px] font-medium',
              severity === 'error'
                ? 'bg-red-500/15 text-red-400'
                : 'bg-amber-500/15 text-amber-400'
            )}
          >
            {severity}
          </span>
        )}
      </div>

      {/* Task info */}
      <p className="text-[10px] text-gray-500 font-mono mb-3">
        {dagId}.{taskId}
        {checkType && <span className="ml-1 text-gray-600">({checkType})</span>}
      </p>

      {/* Value + Sparkline */}
      <div className="flex items-end justify-between">
        <div>
          <span className="text-lg font-bold text-white">
            {currentValue != null ? formatMetricValue(currentValue) : '—'}
          </span>
          {pctChange && (
            <span className={clsx('ml-2 text-xs flex items-center gap-0.5 inline-flex', trendColor[trend])}>
              <TrendIcon size={10} />
              {pctChange}%
            </span>
          )}
        </div>
        <Sparkline data={history} color={sparkColor} width={80} height={24} />
      </div>

      {/* Threshold line */}
      {threshold != null && (
        <div className="mt-2 flex items-center gap-1 text-[10px] text-gray-600">
          <AlertTriangle size={8} />
          threshold: {formatMetricValue(threshold)}
        </div>
      )}
    </button>
  );
}

// ─── Metric Detail Panel ─────────────────────────────────────────────────────

function MetricDetail({ metric }) {
  if (!metric) {
    return (
      <Card>
        <div className="text-center py-12">
          <BarChart3 size={24} className="mx-auto text-gray-600 mb-3" />
          <p className="text-sm text-gray-500">Select a metric to see details</p>
          <p className="text-xs text-gray-600 mt-1">
            View history, thresholds, and trends
          </p>
        </div>
      </Card>
    );
  }

  const { name, taskId, dagId, currentValue, history, checkType, severity, threshold, description } = metric;

  // Compute stats from history
  const stats = useMemo(() => {
    if (!history || history.length === 0) return null;
    const min = Math.min(...history);
    const max = Math.max(...history);
    const avg = history.reduce((a, b) => a + b, 0) / history.length;
    const latest = history[history.length - 1];
    return { min, max, avg, latest, count: history.length };
  }, [history]);

  return (
    <div className="space-y-4">
      {/* Header */}
      <Card>
        <div className="flex items-center gap-3 mb-3">
          <div className="w-10 h-10 rounded-lg bg-conduit-600/20 border border-conduit-600/30 flex items-center justify-center">
            <BarChart3 size={20} className="text-conduit-400" />
          </div>
          <div>
            <h3 className="text-sm font-semibold text-white">{name}</h3>
            <p className="text-xs text-gray-500 font-mono">{dagId}.{taskId}</p>
          </div>
        </div>
        {description && (
          <p className="text-xs text-gray-400 mt-2">{description}</p>
        )}
      </Card>

      {/* Large Sparkline */}
      <Card title="Trend" icon={TrendingUp}>
        <div className="py-2">
          <Sparkline
            data={history}
            width={280}
            height={80}
            color={severity === 'error' ? '#ef4444' : '#6366f1'}
            showDots
          />
        </div>
      </Card>

      {/* Statistics */}
      {stats && (
        <Card title="Statistics" icon={Activity}>
          <div className="grid grid-cols-2 gap-3">
            <div className="p-2 rounded-lg bg-conduit-900/30">
              <p className="text-[10px] text-gray-500 mb-0.5">Current</p>
              <p className="text-sm text-white font-semibold">
                {formatMetricValue(stats.latest)}
              </p>
            </div>
            <div className="p-2 rounded-lg bg-conduit-900/30">
              <p className="text-[10px] text-gray-500 mb-0.5">Average</p>
              <p className="text-sm text-gray-300 font-semibold">
                {formatMetricValue(stats.avg)}
              </p>
            </div>
            <div className="p-2 rounded-lg bg-conduit-900/30">
              <p className="text-[10px] text-gray-500 mb-0.5">Min</p>
              <p className="text-sm text-gray-300 font-semibold">
                {formatMetricValue(stats.min)}
              </p>
            </div>
            <div className="p-2 rounded-lg bg-conduit-900/30">
              <p className="text-[10px] text-gray-500 mb-0.5">Max</p>
              <p className="text-sm text-gray-300 font-semibold">
                {formatMetricValue(stats.max)}
              </p>
            </div>
          </div>
        </Card>
      )}

      {/* Threshold Info */}
      {(threshold != null || checkType) && (
        <Card title="Contract Check" icon={CheckCircle}>
          <div className="space-y-2 text-xs">
            {checkType && (
              <div className="flex justify-between">
                <span className="text-gray-500">Check Type</span>
                <span className="text-gray-300">{checkType}</span>
              </div>
            )}
            {threshold != null && (
              <div className="flex justify-between">
                <span className="text-gray-500">Threshold</span>
                <span className="text-gray-300">{formatMetricValue(threshold)}</span>
              </div>
            )}
            {severity && (
              <div className="flex justify-between">
                <span className="text-gray-500">Severity</span>
                <span className={severity === 'error' ? 'text-red-400' : 'text-amber-400'}>
                  {severity}
                </span>
              </div>
            )}
          </div>
        </Card>
      )}
    </div>
  );
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

function formatMetricValue(val) {
  if (val == null) return '—';
  if (typeof val !== 'number') return String(val);
  if (Number.isInteger(val)) return val.toLocaleString();
  if (Math.abs(val) < 0.01) return val.toExponential(2);
  if (Math.abs(val) < 1) return val.toFixed(4);
  if (Math.abs(val) > 1000000) return (val / 1000000).toFixed(1) + 'M';
  if (Math.abs(val) > 1000) return (val / 1000).toFixed(1) + 'K';
  return val.toFixed(2);
}

// Generate synthetic history data for demo (in production this comes from the API)
function generateDemoHistory(seed, length = 12) {
  const data = [];
  let val = seed || 100;
  for (let i = 0; i < length; i++) {
    val += (Math.sin(i * 0.7 + seed) * 10) + (Math.random() - 0.5) * 5;
    data.push(Math.max(0, val));
  }
  return data;
}

// ─── Main Page ───────────────────────────────────────────────────────────────

export default function MetricExplorer() {
  const { data: contractData, loading: contractsLoading, refetch } = useApi(listContracts);
  const [selectedMetric, setSelectedMetric] = useState(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [metricFilter, setMetricFilter] = useState('all');

  // Build metrics from contracts data
  const metrics = useMemo(() => {
    if (!contractData?.contracts) return [];

    const result = [];
    let seed = 42;

    contractData.contracts.forEach((contract) => {
      (contract.checks || []).forEach((check) => {
        seed += 7;
        const history = generateDemoHistory(seed);
        const current = history[history.length - 1];
        const previous = history[history.length - 2];

        result.push({
          id: `${contract.dag_id}.${contract.task_id}.${check.name}`,
          name: check.name,
          taskId: contract.task_id,
          dagId: contract.dag_id,
          checkType: check.type || check.check_type,
          severity: check.severity,
          description: check.description,
          currentValue: Math.round(current * 100) / 100,
          previousValue: Math.round(previous * 100) / 100,
          history: history.map((v) => Math.round(v * 100) / 100),
          threshold: check.severity === 'error' ? Math.round(history[0] * 0.8 * 100) / 100 : null,
        });
      });
    });

    return result;
  }, [contractData]);

  // Filter metrics
  const filtered = useMemo(() => {
    let result = metrics;

    if (searchQuery) {
      const q = searchQuery.toLowerCase();
      result = result.filter(
        (m) =>
          m.name.toLowerCase().includes(q) ||
          m.taskId.toLowerCase().includes(q) ||
          m.dagId.toLowerCase().includes(q)
      );
    }

    if (metricFilter !== 'all') {
      result = result.filter((m) => m.severity === metricFilter);
    }

    return result;
  }, [metrics, searchQuery, metricFilter]);

  // Stats
  const stats = useMemo(() => ({
    total: metrics.length,
    error: metrics.filter((m) => m.severity === 'error').length,
    warning: metrics.filter((m) => m.severity === 'warning').length,
    dags: new Set(metrics.map((m) => m.dagId)).size,
  }), [metrics]);

  return (
    <div className="min-h-screen bg-conduit-950 p-6">
      <PageHeader
        title="Metric Explorer"
        description="Browse and analyze pipeline metrics from contract checks"
        actions={
          <Button onClick={refetch} variant="secondary" size="sm">
            Refresh
          </Button>
        }
      />

      {/* Stats Row */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4 mb-8">
        <StatCard label="Total Metrics" value={stats.total} icon={Gauge} sub="tracked" />
        <StatCard label="DAGs" value={stats.dags} icon={Database} sub="with metrics" />
        <StatCard label="Error Checks" value={stats.error} icon={AlertTriangle} sub="blocking" />
        <StatCard label="Warning Checks" value={stats.warning} icon={Clock} sub="non-blocking" />
      </div>

      {/* Search & Filter */}
      <div className="flex items-center gap-3 mb-6">
        <div className="relative flex-1 max-w-md">
          <Search size={14} className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-500" />
          <input
            type="text"
            placeholder="Search metrics, tasks, or DAGs..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="w-full pl-9 pr-3 py-2 rounded-lg bg-conduit-900/50 border border-conduit-800/50 text-sm text-gray-200 placeholder-gray-500 focus:outline-none focus:border-conduit-600/50"
          />
        </div>
        <div className="flex items-center gap-1">
          {['all', 'error', 'warning'].map((f) => (
            <button
              key={f}
              onClick={() => setMetricFilter(f)}
              className={clsx(
                'px-3 py-1.5 rounded-lg text-xs font-medium transition-colors border',
                metricFilter === f
                  ? 'bg-conduit-600/20 text-conduit-300 border-conduit-600/30'
                  : 'text-gray-400 border-transparent hover:bg-conduit-900/50'
              )}
            >
              {f === 'all' ? 'All' : f.charAt(0).toUpperCase() + f.slice(1)}
            </button>
          ))}
        </div>
      </div>

      {/* Main Content */}
      {contractsLoading ? (
        <div className="flex items-center justify-center py-20">
          <Spinner />
        </div>
      ) : filtered.length === 0 ? (
        <EmptyState
          icon={BarChart3}
          title="No metrics found"
          description={
            searchQuery || metricFilter !== 'all'
              ? 'Try adjusting your search or filters'
              : 'Metrics appear when tasks emit evidence via the CONDUIT::METRIC protocol'
          }
        />
      ) : (
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Metric Cards Grid */}
          <div className="lg:col-span-2">
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
              {filtered.map((metric) => (
                <MetricCard
                  key={metric.id}
                  metric={metric}
                  isSelected={selectedMetric?.id === metric.id}
                  onClick={() => setSelectedMetric(metric)}
                />
              ))}
            </div>
          </div>

          {/* Detail Panel */}
          <div className="lg:col-span-1">
            <div className="sticky top-6">
              <MetricDetail metric={selectedMetric} />
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
