import { useState, useCallback } from 'react';
import { generatePlan, applyPlan, listEnvironments } from '../api';
import { useApi } from '../hooks/useApi';
import Card from '../components/Card';
import StatusBadge from '../components/StatusBadge';
import Button from '../components/Button';
import Spinner from '../components/Spinner';
import PageHeader from '../components/PageHeader';
import {
  Map,
  Play,
  Check,
  X,
  AlertTriangle,
  ArrowRight,
  RotateCcw,
  Zap,
  FileCode,
  Plus,
  Minus,
  Edit3,
  Trash2,
} from 'lucide-react';

// ─── CSS Animations ──────────────────────────────────────────────────────────

const animationStyles = `
  @keyframes slide-in {
    from { opacity: 0; transform: translateY(10px); }
    to { opacity: 1; transform: translateY(0); }
  }
  .slide-in-animation { animation: slide-in 0.3s ease-out; }
`;

const truncateHash = (hash, length = 12) => {
  if (!hash) return 'N/A';
  return hash.length > length ? hash.substring(0, length) : hash;
};

const formatTimestamp = (dateString) => {
  if (!dateString) return 'N/A';
  const date = new Date(dateString);
  return date.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' });
};

const getActionTypeColor = (actionType) => {
  switch (actionType?.toLowerCase()) {
    case 'execute':
      return 'running';
    case 'reusesnapshot':
      return 'success';
    case 'skip':
      return 'skipped';
    case 'remove':
      return 'removed';
    default:
      return 'pending';
  }
};

const getActionTypeLabel = (actionType) => {
  switch (actionType) {
    case 'Execute':
      return 'Execute';
    case 'ReuseSnapshot':
      return 'Reuse Snapshot';
    case 'Skip':
      return 'Skip';
    case 'Remove':
      return 'Remove';
    default:
      return actionType || 'Unknown';
  }
};

const getActionIcon = (actionType) => {
  switch (actionType?.toLowerCase()) {
    case 'execute':
      return Play;
    case 'reusesnapshot':
      return Check;
    case 'remove':
      return Trash2;
    case 'skip':
      return RotateCcw;
    default:
      return FileCode;
  }
};

// ─── Summary Banner ──────────────────────────────────────────────────────────

function SummaryBanner({ planData }) {
  if (!planData?.summary) return null;

  const { summary, timestamp } = planData;
  const total = summary.execute + summary.reusesnapshot + summary.remove + summary.skip;
  const affected = summary.execute + summary.remove;

  return (
    <div className="slide-in-animation mb-6 glass p-6 rounded-xl border border-amber-500/20 bg-amber-500/5">
      <div className="flex flex-col md:flex-row items-start md:items-center justify-between gap-4">
        <div className="flex-1">
          <div className="flex items-center gap-2 mb-2">
            <AlertTriangle size={18} className="text-amber-400" />
            <h3 className="text-sm font-semibold text-amber-400">Plan Summary</h3>
          </div>
          <p className="text-sm text-gray-300 mb-2">
            <span className="font-mono font-bold text-blue-400">{affected}</span>
            {' tasks affected • '}
            <span className="font-mono font-bold text-emerald-400">{summary.execute}</span>
            {' to execute • '}
            <span className="font-mono font-bold text-amber-400">{summary.reusesnapshot}</span>
            {' reuse snapshot • '}
            <span className="font-mono font-bold text-red-400">{summary.remove}</span>
            {' to remove'}
          </p>
          {timestamp && (
            <p className="text-xs text-gray-500">
              Plan generated {new Date(timestamp).toLocaleString()}
            </p>
          )}
        </div>
        <div className="flex gap-2">
          <div className="px-3 py-1 rounded bg-emerald-500/10 border border-emerald-500/25">
            <span className="text-xs font-mono text-emerald-400">{summary.execute} Execute</span>
          </div>
          <div className="px-3 py-1 rounded bg-amber-500/10 border border-amber-500/25">
            <span className="text-xs font-mono text-amber-400">{summary.reusesnapshot} Reuse</span>
          </div>
          <div className="px-3 py-1 rounded bg-red-500/10 border border-red-500/25">
            <span className="text-xs font-mono text-red-400">{summary.remove} Remove</span>
          </div>
        </div>
      </div>
    </div>
  );
}

// ─── Diff Visualization ─────────────────────────────────────────────────────

function DiffVisualization({ planData }) {
  if (!planData?.actions) return null;

  // Group actions by type
  const grouped = {
    execute: [],
    reusesnapshot: [],
    skip: [],
    remove: [],
  };

  planData.actions.forEach((action) => {
    const type = action.action_type?.toLowerCase() || 'skip';
    if (grouped[type]) {
      grouped[type].push(action);
    }
  });

  return (
    <div className="space-y-4 mb-6">
      {/* Execute Section */}
      {grouped.execute.length > 0 && (
        <Card title="New / Modified Tasks" subtitle={`${grouped.execute.length} task(s) to execute`}>
          <div className="space-y-2">
            {grouped.execute.map((action, idx) => {
              const Icon = Plus;
              return (
                <div
                  key={idx}
                  className="flex items-start gap-3 p-3 rounded-lg bg-emerald-500/5 border border-emerald-500/20 hover:bg-emerald-500/10 transition-colors"
                >
                  <Icon size={16} className="text-emerald-400 flex-shrink-0 mt-0.5" />
                  <div className="flex-1 min-w-0">
                    <p className="text-sm font-mono text-emerald-400">+ {action.task_name}</p>
                    <div className="flex gap-2 mt-1">
                      <span className="text-xs text-gray-500">fingerprint:</span>
                      <span className="text-xs font-mono text-gray-400">{truncateHash(action.fingerprint)}</span>
                    </div>
                    {action.reason && (
                      <p className="text-xs text-gray-400 mt-1">Reason: {action.reason}</p>
                    )}
                  </div>
                </div>
              );
            })}
          </div>
        </Card>
      )}

      {/* Reuse Snapshot Section */}
      {grouped.reusesnapshot.length > 0 && (
        <Card title="Cached / Unchanged Tasks" subtitle={`${grouped.reusesnapshot.length} task(s) can reuse snapshot`}>
          <div className="space-y-2">
            {grouped.reusesnapshot.map((action, idx) => (
              <div
                key={idx}
                className="flex items-start gap-3 p-3 rounded-lg bg-gray-500/5 border border-gray-500/20 hover:bg-gray-500/10 transition-colors opacity-75"
              >
                <Check size={16} className="text-gray-400 flex-shrink-0 mt-0.5" />
                <div className="flex-1 min-w-0">
                  <p className="text-sm font-mono text-gray-400">= {action.task_name}</p>
                  <div className="flex gap-2 mt-1">
                    <span className="text-xs text-gray-600">fingerprint:</span>
                    <span className="text-xs font-mono text-gray-500">{truncateHash(action.fingerprint)}</span>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </Card>
      )}

      {/* Remove Section */}
      {grouped.remove.length > 0 && (
        <Card title="Removed Tasks" subtitle={`${grouped.remove.length} task(s) will be removed`}>
          <div className="space-y-2">
            {grouped.remove.map((action, idx) => (
              <div
                key={idx}
                className="flex items-start gap-3 p-3 rounded-lg bg-red-500/5 border border-red-500/20 hover:bg-red-500/10 transition-colors"
              >
                <Trash2 size={16} className="text-red-400 flex-shrink-0 mt-0.5" />
                <div className="flex-1 min-w-0">
                  <p className="text-sm font-mono text-red-400">- {action.task_name}</p>
                  <div className="flex gap-2 mt-1">
                    <span className="text-xs text-gray-500">fingerprint:</span>
                    <span className="text-xs font-mono text-gray-400">{truncateHash(action.fingerprint)}</span>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </Card>
      )}

      {/* Skip Section */}
      {grouped.skip.length > 0 && (
        <Card title="Skipped Tasks" subtitle={`${grouped.skip.length} task(s) skipped`}>
          <div className="space-y-2">
            {grouped.skip.map((action, idx) => (
              <div
                key={idx}
                className="flex items-start gap-3 p-3 rounded-lg bg-gray-500/5 border border-gray-500/20 opacity-60"
              >
                <RotateCcw size={16} className="text-gray-500 flex-shrink-0 mt-0.5" />
                <div className="flex-1 min-w-0">
                  <p className="text-sm font-mono text-gray-500">~ {action.task_name}</p>
                </div>
              </div>
            ))}
          </div>
        </Card>
      )}
    </div>
  );
}

// ─── Main Component ──────────────────────────────────────────────────────────

export default function PlanApply() {
  const { data: environmentsData, loading: envsLoading } = useApi(listEnvironments);
  const environments = environmentsData || [];

  const [selectedEnv, setSelectedEnv] = useState(environments[0]?.name || '');
  const [planData, setPlanData] = useState(null);
  const [applyStatus, setApplyStatus] = useState(null);
  const [planLoading, setPlanLoading] = useState(false);
  const [applyLoading, setApplyLoading] = useState(false);
  const [showConfirmation, setShowConfirmation] = useState(false);
  const [error, setError] = useState(null);

  // Inject animations
  useState(() => {
    const style = document.createElement('style');
    style.textContent = animationStyles;
    document.head.appendChild(style);
    return () => style.remove();
  });

  // Update selectedEnv when environments load
  if (environments.length > 0 && !selectedEnv) {
    setSelectedEnv(environments[0].name || environments[0].id);
  }

  const handleGeneratePlan = useCallback(async () => {
    if (!selectedEnv) {
      setError('Please select an environment');
      return;
    }

    setPlanLoading(true);
    setError(null);
    setApplyStatus(null);

    try {
      const result = await generatePlan(selectedEnv);

      const actions = result.actions || [];
      setPlanData({
        id: result.planId || result.plan_id || `plan-${Date.now()}`,
        environment: selectedEnv,
        timestamp: new Date().toISOString(),
        actions,
        summary: {
          execute: actions.filter(a => a.action_type === 'Execute').length,
          reusesnapshot: actions.filter(a => a.action_type === 'ReuseSnapshot').length,
          skip: actions.filter(a => a.action_type === 'Skip').length,
          remove: actions.filter(a => a.action_type === 'Remove').length,
        },
      });
    } catch (err) {
      setError(err.message || 'Failed to generate plan');
      setPlanData(null);
    } finally {
      setPlanLoading(false);
    }
  }, [selectedEnv]);

  const handleConfirmApply = useCallback(() => {
    setShowConfirmation(true);
  }, []);

  const handleApplyPlan = useCallback(async () => {
    if (!planData?.id) {
      setError('No plan available to apply');
      return;
    }

    setShowConfirmation(false);
    setApplyLoading(true);
    setError(null);

    try {
      const result = await applyPlan(planData.id);

      const tasks = planData.actions || [];
      const executeCount = tasks.filter(a => a.action_type === 'Execute').length;

      setApplyStatus({
        phase: 'complete',
        successCount: result?.tasksExecuted ?? executeCount,
        failedCount: result?.tasksFailed ?? 0,
        timestamp: new Date().toISOString(),
      });

      setPlanData(null);
    } catch (err) {
      setError(err.message || 'Failed to apply plan');
      setApplyStatus({
        phase: 'error',
        error: err.message,
      });
    } finally {
      setApplyLoading(false);
    }
  }, [planData]);

  const handleDiscard = useCallback(() => {
    setPlanData(null);
    setApplyStatus(null);
    setError(null);
  }, []);

  const handleReset = useCallback(() => {
    setPlanData(null);
    setApplyStatus(null);
    setError(null);
  }, []);

  return (
    <div className="min-h-screen bg-conduit-950 p-6">
      {/* Page Header */}
      <PageHeader
        title="Plan / Apply"
        description="Review and deploy pipeline changes with confidence"
      />

      {/* Error Alert */}
      {error && (
        <div className="mb-6 p-4 rounded-lg bg-red-500/10 border border-red-500/25 flex items-start gap-3">
          <AlertTriangle size={18} className="text-red-400 flex-shrink-0 mt-0.5" />
          <div>
            <p className="text-sm font-medium text-red-400">Error</p>
            <p className="text-xs text-red-300 mt-1">{error}</p>
          </div>
        </div>
      )}

      {/* PHASE 1: Generate Plan */}
      {!planData && !applyStatus && (
        <Card
          title="Phase 1: Generate Plan"
          icon={Map}
          className="mb-6"
        >
          <div className="space-y-4">
            <div>
              <label className="block text-sm font-medium text-gray-300 mb-2">
                Select Environment
              </label>
              <select
                value={selectedEnv}
                onChange={(e) => setSelectedEnv(e.target.value)}
                disabled={envsLoading || planLoading}
                className="w-full px-3 py-2 bg-conduit-900/50 border border-conduit-700/50 rounded-lg text-white text-sm focus:outline-none focus:border-conduit-500 transition-colors disabled:opacity-50"
              >
                {envsLoading ? (
                  <option>Loading environments...</option>
                ) : environments.length === 0 ? (
                  <option disabled>No environments available</option>
                ) : (
                  environments.map((env) => (
                    <option key={env.name || env.id} value={env.name || env.id}>
                      {env.name || env.id}
                    </option>
                  ))
                )}
              </select>
            </div>

            <div className="flex gap-3 pt-2">
              <Button
                onClick={handleGeneratePlan}
                icon={Zap}
                loading={planLoading}
                disabled={!selectedEnv || envsLoading}
              >
                Generate Plan
              </Button>
              {planLoading && (
                <div className="flex items-center gap-2 text-sm text-gray-400">
                  <Spinner size="sm" className="py-0" />
                  <span>Generating plan...</span>
                </div>
              )}
            </div>
          </div>
        </Card>
      )}

      {/* PHASE 2: Review Plan */}
      {planData && !applyStatus && (
        <>
          {/* Summary Banner */}
          <SummaryBanner planData={planData} />

          {/* Diff Visualization */}
          <DiffVisualization planData={planData} />

          {/* Detailed Table View */}
          <Card
            title="Phase 2: Review Detailed Plan"
            subtitle={`Plan ID: ${planData.id}`}
            icon={FileCode}
            className="mb-6"
          >
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-conduit-800/50">
                    <th className="text-left px-4 py-3 text-xs font-semibold text-gray-400 uppercase tracking-wide">
                      Task
                    </th>
                    <th className="text-left px-4 py-3 text-xs font-semibold text-gray-400 uppercase tracking-wide">
                      Action
                    </th>
                    <th className="text-left px-4 py-3 text-xs font-semibold text-gray-400 uppercase tracking-wide">
                      Fingerprint
                    </th>
                    <th className="text-left px-4 py-3 text-xs font-semibold text-gray-400 uppercase tracking-wide">
                      Reason
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {(planData.actions || []).map((action, idx) => {
                    const Icon = getActionIcon(action.action_type);
                    const actionType = getActionTypeLabel(action.action_type);
                    const bgColor = {
                      execute: 'bg-emerald-500/5 hover:bg-emerald-500/10',
                      reusesnapshot: 'bg-gray-500/5 hover:bg-gray-500/10',
                      remove: 'bg-red-500/5 hover:bg-red-500/10',
                      skip: 'bg-gray-500/5 hover:bg-gray-500/10',
                    }[action.action_type?.toLowerCase()] || 'hover:bg-conduit-900/50';

                    const textColor = {
                      execute: 'text-emerald-400',
                      reusesnapshot: 'text-gray-400',
                      remove: 'text-red-400',
                      skip: 'text-gray-500',
                    }[action.action_type?.toLowerCase()] || 'text-gray-300';

                    return (
                      <tr
                        key={idx}
                        className={`border-b border-conduit-800/25 transition-colors ${bgColor}`}
                      >
                        <td className={`px-4 py-3 font-mono ${textColor}`}>
                          {action.task_name}
                        </td>
                        <td className="px-4 py-3">
                          <div className="flex items-center gap-2">
                            <Icon size={14} className={textColor} />
                            <span className="text-xs font-medium text-gray-300">
                              {actionType}
                            </span>
                          </div>
                        </td>
                        <td className="px-4 py-3 text-conduit-300 font-mono text-xs">
                          {truncateHash(action.fingerprint)}
                        </td>
                        <td className="px-4 py-3 text-gray-400 text-xs">
                          {action.reason || 'automatic'}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </Card>

          {/* Action Buttons */}
          <div className="flex gap-3 mb-8">
            <Button
              onClick={handleConfirmApply}
              icon={Play}
              disabled={applyLoading}
            >
              Apply Changes
            </Button>
            <Button
              onClick={handleDiscard}
              variant="secondary"
              disabled={applyLoading}
            >
              Discard Plan
            </Button>
          </div>

          {/* Confirmation Dialog */}
          {showConfirmation && (
            <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
              <Card className="w-full max-w-md slide-in-animation">
                <div className="space-y-4">
                  <div className="flex items-start gap-3">
                    <AlertTriangle size={20} className="text-amber-400 flex-shrink-0 mt-0.5" />
                    <div>
                      <h3 className="font-semibold text-white">Confirm Apply</h3>
                      <p className="text-sm text-gray-400 mt-2">
                        This will deploy changes to <span className="font-mono font-bold text-conduit-300">{planData.summary?.execute || 0}</span> task{planData.summary?.execute !== 1 ? 's' : ''} affecting downstream dependencies.
                      </p>
                      {planData.summary?.remove > 0 && (
                        <p className="text-sm text-red-400 mt-2">
                          <span className="font-mono font-bold">{planData.summary.remove}</span> task{planData.summary.remove !== 1 ? 's' : ''} will be removed.
                        </p>
                      )}
                      <p className="text-xs text-gray-500 mt-3">
                        This action cannot be undone.
                      </p>
                    </div>
                  </div>

                  <div className="pt-4 flex gap-3 justify-end border-t border-conduit-800/50">
                    <Button
                      variant="secondary"
                      onClick={() => setShowConfirmation(false)}
                    >
                      Cancel
                    </Button>
                    <Button
                      onClick={handleApplyPlan}
                      loading={applyLoading}
                    >
                      Continue
                    </Button>
                  </div>
                </div>
              </Card>
            </div>
          )}
        </>
      )}

      {/* PHASE 3: Apply Progress / Success */}
      {applyStatus && applyStatus.phase !== 'error' && (
        <Card
          title="Phase 3: Apply Complete"
          icon={Check}
          className="mb-6"
        >
          <div className="space-y-4">
            {applyStatus.phase === 'applying' && (
              <>
                <div>
                  <div className="flex items-center justify-between mb-3">
                    <span className="text-sm font-medium text-gray-300">
                      Executing tasks...
                    </span>
                    <span className="text-xs text-gray-400">
                      {applyStatus.completedCount} of {applyStatus.totalCount}
                    </span>
                  </div>
                  <div className="w-full bg-conduit-900/50 rounded-full h-2 border border-conduit-700/50 overflow-hidden">
                    <div
                      className="bg-blue-500 h-full transition-all duration-300"
                      style={{
                        width: `${(applyStatus.completedCount / applyStatus.totalCount) * 100}%`,
                      }}
                    />
                  </div>
                </div>

                {applyStatus.tasks && applyStatus.tasks.length > 0 && (
                  <div className="max-h-48 overflow-y-auto space-y-2 pt-4 border-t border-conduit-800/50">
                    {applyStatus.tasks.map((task, idx) => (
                      <div
                        key={idx}
                        className="flex items-center gap-3 p-2 rounded bg-conduit-900/30"
                      >
                        <Check size={16} className="text-emerald-400 flex-shrink-0" />
                        <span className="text-sm text-gray-300">{task.task_name}</span>
                        <span className="text-xs text-gray-500 ml-auto">
                          {getActionTypeLabel(task.action_type)}
                        </span>
                      </div>
                    ))}
                  </div>
                )}
              </>
            )}

            {applyStatus.phase === 'complete' && (
              <div className="space-y-4">
                <div className="p-4 rounded-lg bg-emerald-500/10 border border-emerald-500/25">
                  <div className="flex items-center gap-3">
                    <Check size={18} className="text-emerald-400" />
                    <div>
                      <p className="font-medium text-emerald-400">Changes Deployed Successfully</p>
                      <p className="text-xs text-emerald-300 mt-1">
                        <span className="font-mono font-bold">{applyStatus.successCount}</span> task{applyStatus.successCount !== 1 ? 's' : ''} executed
                        {applyStatus.failedCount > 0 && `, ${applyStatus.failedCount} failed`}
                      </p>
                    </div>
                  </div>
                </div>

                <div className="grid grid-cols-2 gap-3 pt-2">
                  <div className="p-3 rounded-lg bg-conduit-900/50 border border-conduit-700/50">
                    <p className="text-xs text-gray-400">Deployment Time</p>
                    <p className="text-sm font-medium text-white mt-1">
                      {applyStatus.timestamp ? formatTimestamp(applyStatus.timestamp) : 'N/A'}
                    </p>
                  </div>
                  <div className="p-3 rounded-lg bg-conduit-900/50 border border-conduit-700/50">
                    <p className="text-xs text-gray-400">Environment</p>
                    <p className="text-sm font-medium text-white mt-1">
                      {planData?.environment || 'N/A'}
                    </p>
                  </div>
                </div>
              </div>
            )}
          </div>
        </Card>
      )}

      {/* Error State */}
      {applyStatus && applyStatus.phase === 'error' && (
        <Card
          title="Phase 3: Apply Failed"
          icon={X}
          className="mb-6"
        >
          <div className="p-4 rounded-lg bg-red-500/10 border border-red-500/25">
            <div className="flex items-start gap-3">
              <X size={18} className="text-red-400 flex-shrink-0 mt-0.5" />
              <div>
                <p className="font-medium text-red-400">Deployment Failed</p>
                <p className="text-xs text-red-300 mt-1">
                  {applyStatus.error || 'Unknown error occurred'}
                </p>
              </div>
            </div>
          </div>
        </Card>
      )}

      {/* Reset Button */}
      {applyStatus && (
        <div className="mb-8">
          <Button
            onClick={handleReset}
            variant="secondary"
            icon={RotateCcw}
          >
            Start Over
          </Button>
        </div>
      )}
    </div>
  );
}
