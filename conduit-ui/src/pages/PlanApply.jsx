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
} from 'lucide-react';

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
        description="Review and deploy pipeline changes"
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

      {/* Step 1: Generate Plan */}
      {!planData && !applyStatus && (
        <Card
          title="Step 1: Generate Plan"
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

      {/* Step 2: Review Plan */}
      {planData && !applyStatus && (
        <>
          {/* Summary Bar */}
          <div className="mb-6 grid grid-cols-4 gap-3">
            <div className="glass p-4 rounded-lg">
              <div className="flex items-center justify-between">
                <span className="text-xs font-medium text-gray-400 uppercase">Execute</span>
                <Play size={14} className="text-blue-400" />
              </div>
              <p className="text-2xl font-bold text-white mt-2">
                {planData.summary?.execute || 0}
              </p>
            </div>

            <div className="glass p-4 rounded-lg">
              <div className="flex items-center justify-between">
                <span className="text-xs font-medium text-gray-400 uppercase">Reuse Snapshot</span>
                <Check size={14} className="text-emerald-400" />
              </div>
              <p className="text-2xl font-bold text-white mt-2">
                {planData.summary?.reusesnapshot || 0}
              </p>
            </div>

            <div className="glass p-4 rounded-lg">
              <div className="flex items-center justify-between">
                <span className="text-xs font-medium text-gray-400 uppercase">Skip</span>
                <RotateCcw size={14} className="text-gray-400" />
              </div>
              <p className="text-2xl font-bold text-white mt-2">
                {planData.summary?.skip || 0}
              </p>
            </div>

            <div className="glass p-4 rounded-lg">
              <div className="flex items-center justify-between">
                <span className="text-xs font-medium text-gray-400 uppercase">Remove</span>
                <X size={14} className="text-red-400" />
              </div>
              <p className="text-2xl font-bold text-white mt-2">
                {planData.summary?.remove || 0}
              </p>
            </div>
          </div>

          {/* Plan Actions Table */}
          <Card
            title="Step 2: Review Plan"
            subtitle={`Plan ID: ${planData.id}`}
            icon={FileCode}
            className="mb-6"
          >
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-conduit-800/50">
                    <th className="text-left px-3 py-2 text-xs font-semibold text-gray-400 uppercase tracking-wide">
                      Task Name
                    </th>
                    <th className="text-left px-3 py-2 text-xs font-semibold text-gray-400 uppercase tracking-wide">
                      Action
                    </th>
                    <th className="text-left px-3 py-2 text-xs font-semibold text-gray-400 uppercase tracking-wide">
                      Fingerprint
                    </th>
                    <th className="text-left px-3 py-2 text-xs font-semibold text-gray-400 uppercase tracking-wide">
                      Reason
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {(planData.actions || []).map((action, idx) => (
                    <tr
                      key={idx}
                      className="border-b border-conduit-800/25 hover:bg-conduit-900/30 transition-colors"
                    >
                      <td className="px-3 py-3 text-gray-200 font-medium">
                        {action.task_name}
                      </td>
                      <td className="px-3 py-3">
                        <StatusBadge
                          status={getActionTypeLabel(action.action_type)}
                          className={getActionTypeColor(action.action_type)}
                        />
                      </td>
                      <td className="px-3 py-3 text-conduit-300 font-mono text-xs">
                        {truncateHash(action.fingerprint)}
                      </td>
                      <td className="px-3 py-3 text-gray-400 text-xs">
                        {action.reason || 'N/A'}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </Card>

          {/* Action Buttons */}
          <div className="flex gap-3">
            <Button
              onClick={handleConfirmApply}
              icon={Play}
              disabled={applyLoading}
            >
              Apply Plan
            </Button>
            <Button
              onClick={handleDiscard}
              variant="secondary"
              disabled={applyLoading}
            >
              Discard
            </Button>
          </div>

          {/* Confirmation Dialog */}
          {showConfirmation && (
            <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
              <Card className="w-full max-w-md">
                <div className="space-y-4">
                  <div className="flex items-start gap-3">
                    <AlertTriangle size={20} className="text-amber-400 flex-shrink-0 mt-0.5" />
                    <div>
                      <h3 className="font-semibold text-white">Confirm Apply</h3>
                      <p className="text-sm text-gray-400 mt-1">
                        This will execute {planData.summary?.execute || 0} task{planData.summary?.execute !== 1 ? 's' : ''}.
                        This action cannot be undone.
                      </p>
                    </div>
                  </div>

                  <div className="pt-4 flex gap-3 justify-end">
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

      {/* Step 3: Apply Progress */}
      {applyStatus && applyStatus.phase !== 'error' && (
        <Card
          title="Step 3: Apply Progress"
          icon={Play}
          className="mb-6"
        >
          {applyStatus.phase === 'applying' && (
            <div className="space-y-4">
              <div>
                <div className="flex items-center justify-between mb-2">
                  <span className="text-sm font-medium text-gray-300">
                    Executing tasks...
                  </span>
                  <span className="text-xs text-gray-400">
                    {applyStatus.completedCount} of {applyStatus.totalCount}
                  </span>
                </div>
                <div className="w-full bg-conduit-900/50 rounded-full h-2 border border-conduit-700/50 overflow-hidden">
                  <div
                    className="bg-conduit-500 h-full transition-all duration-300"
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
            </div>
          )}

          {applyStatus.phase === 'complete' && (
            <div className="space-y-4">
              <div className="p-4 rounded-lg bg-emerald-500/10 border border-emerald-500/25">
                <div className="flex items-center gap-3">
                  <Check size={18} className="text-emerald-400" />
                  <div>
                    <p className="font-medium text-emerald-400">Plan Applied Successfully</p>
                    <p className="text-xs text-emerald-300 mt-1">
                      {applyStatus.successCount} task{applyStatus.successCount !== 1 ? 's' : ''} executed
                      {applyStatus.failedCount > 0 && `, ${applyStatus.failedCount} failed`}
                    </p>
                  </div>
                </div>
              </div>

              <div className="grid grid-cols-2 gap-3 pt-2">
                <div className="p-3 rounded-lg bg-conduit-900/50 border border-conduit-700/50">
                  <p className="text-xs text-gray-400">Completed At</p>
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
        </Card>
      )}

      {/* Error State */}
      {applyStatus && applyStatus.phase === 'error' && (
        <Card
          title="Step 3: Apply Failed"
          icon={X}
          className="mb-6"
        >
          <div className="p-4 rounded-lg bg-red-500/10 border border-red-500/25">
            <div className="flex items-start gap-3">
              <X size={18} className="text-red-400 flex-shrink-0 mt-0.5" />
              <div>
                <p className="font-medium text-red-400">Apply Failed</p>
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
