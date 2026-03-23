import { useState, useCallback } from 'react';
import { useApi } from '../hooks/useApi';
import {
  listEnvironments,
  createEnvironment,
  deleteEnvironment,
  promoteEnvironment,
  diffEnvironments,
} from '../api';
import Card from '../components/Card';
import StatusBadge from '../components/StatusBadge';
import Button from '../components/Button';
import Spinner from '../components/Spinner';
import PageHeader from '../components/PageHeader';
import EmptyState from '../components/EmptyState';
import {
  Layers,
  Plus,
  Trash2,
  ArrowUpRight,
  GitCompare,
  Shield,
  Clock,
  X,
} from 'lucide-react';

function formatRelativeTime(isoDate) {
  if (!isoDate) return 'Never';
  const date = new Date(isoDate);
  const now = new Date();
  const seconds = Math.floor((now - date) / 1000);

  if (seconds < 60) return 'Just now';
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
  if (seconds < 604800) return `${Math.floor(seconds / 86400)}d ago`;
  return date.toLocaleDateString();
}

export default function Environments() {
  const { data: environments, loading, error, refetch } = useApi(listEnvironments);
  const [showCreateForm, setShowCreateForm] = useState(false);
  const [createFormData, setCreateFormData] = useState({ name: '', basedOn: '' });
  const [isCreating, setIsCreating] = useState(false);
  const [createError, setCreateError] = useState(null);

  const [promoteSource, setPromoteSource] = useState('');
  const [promoteTarget, setPromoteTarget] = useState('');
  const [showPromoteModal, setShowPromoteModal] = useState(false);
  const [isPromoting, setIsPromoting] = useState(false);
  const [promoteError, setPromoteError] = useState(null);

  const [diffSourceEnv, setDiffSourceEnv] = useState('');
  const [diffTargetEnv, setDiffTargetEnv] = useState('');
  const [diffResults, setDiffResults] = useState(null);
  const [isLoadingDiff, setIsLoadingDiff] = useState(false);
  const [diffError, setDiffError] = useState(null);

  const [deletingEnv, setDeletingEnv] = useState(null);
  const [isDeleting, setIsDeleting] = useState(false);

  const handleCreateClick = () => {
    setShowCreateForm(true);
    setCreateError(null);
    setCreateFormData({ name: '', basedOn: '' });
  };

  const handleCreateCancel = () => {
    setShowCreateForm(false);
    setCreateFormData({ name: '', basedOn: '' });
    setCreateError(null);
  };

  const handleCreateSubmit = async () => {
    if (!createFormData.name.trim()) {
      setCreateError('Environment name is required');
      return;
    }

    setIsCreating(true);
    setCreateError(null);
    try {
      await createEnvironment(
        createFormData.name.trim(),
        createFormData.basedOn || null
      );
      setShowCreateForm(false);
      setCreateFormData({ name: '', basedOn: '' });
      await refetch();
    } catch (err) {
      setCreateError(err.message);
    } finally {
      setIsCreating(false);
    }
  };

  const handlePromoteClick = (envName) => {
    setPromoteSource(envName);
    setPromoteTarget('');
    setPromoteError(null);
    setShowPromoteModal(true);
  };

  const handlePromoteConfirm = async () => {
    if (!promoteTarget) {
      setPromoteError('Target environment is required');
      return;
    }
    if (promoteSource === promoteTarget) {
      setPromoteError('Source and target must be different');
      return;
    }

    setIsPromoting(true);
    setPromoteError(null);
    try {
      await promoteEnvironment(promoteSource, promoteTarget);
      setShowPromoteModal(false);
      setPromoteSource('');
      setPromoteTarget('');
      await refetch();
    } catch (err) {
      setPromoteError(err.message);
    } finally {
      setIsPromoting(false);
    }
  };

  const handleDiffClick = (envName) => {
    setDiffSourceEnv(envName);
    setDiffTargetEnv('');
    setDiffResults(null);
    setDiffError(null);
  };

  const handleDiffConfirm = async () => {
    if (!diffTargetEnv) {
      setDiffError('Target environment is required');
      return;
    }
    if (diffSourceEnv === diffTargetEnv) {
      setDiffError('Source and target must be different');
      return;
    }

    setIsLoadingDiff(true);
    setDiffError(null);
    try {
      const results = await diffEnvironments(diffSourceEnv, diffTargetEnv);
      setDiffResults(results);
    } catch (err) {
      setDiffError(err.message);
    } finally {
      setIsLoadingDiff(false);
    }
  };

  const handleDiffClose = () => {
    setDiffSourceEnv('');
    setDiffTargetEnv('');
    setDiffResults(null);
    setDiffError(null);
  };

  const handleDeleteClick = (envName) => {
    setDeletingEnv(envName);
  };

  const handleDeleteConfirm = async () => {
    setIsDeleting(true);
    try {
      await deleteEnvironment(deletingEnv);
      setDeletingEnv(null);
      await refetch();
    } catch (err) {
      console.error('Delete failed:', err);
    } finally {
      setIsDeleting(false);
    }
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-screen">
        <Spinner />
      </div>
    );
  }

  const envList = environments || [];

  return (
    <div className="min-h-screen bg-gradient-to-br from-conduit-950 via-conduit-900 to-conduit-950">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <PageHeader
          title="Environments"
          description="Manage deployment environments and snapshots"
          action={
            <Button
              onClick={handleCreateClick}
              className="flex items-center gap-2"
            >
              <Plus className="w-4 h-4" />
              Create Environment
            </Button>
          }
        />

        {error && (
          <div className="mb-6 p-4 bg-red-900/20 border border-red-700/50 rounded-lg text-red-200">
            Error loading environments: {error}
          </div>
        )}

        {/* Create Environment Form */}
        {showCreateForm && (
          <Card className="mb-6 border-conduit-600/50">
            <div className="space-y-4">
              <h3 className="text-lg font-semibold text-conduit-50">
                Create New Environment
              </h3>

              {createError && (
                <div className="p-3 bg-red-900/20 border border-red-700/50 rounded text-red-200 text-sm">
                  {createError}
                </div>
              )}

              <div>
                <label className="block text-sm font-medium text-conduit-200 mb-2">
                  Environment Name
                </label>
                <input
                  type="text"
                  placeholder="e.g., staging, development"
                  value={createFormData.name}
                  onChange={(e) =>
                    setCreateFormData({
                      ...createFormData,
                      name: e.target.value,
                    })
                  }
                  className="w-full px-3 py-2 bg-conduit-800/50 border border-conduit-700/50 rounded-lg text-conduit-50 placeholder-conduit-500 focus:outline-none focus:border-conduit-500 focus:ring-1 focus:ring-conduit-500 glass transition-all"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-conduit-200 mb-2">
                  Based On (Optional)
                </label>
                <select
                  value={createFormData.basedOn}
                  onChange={(e) =>
                    setCreateFormData({
                      ...createFormData,
                      basedOn: e.target.value,
                    })
                  }
                  className="w-full px-3 py-2 bg-conduit-800/50 border border-conduit-700/50 rounded-lg text-conduit-50 focus:outline-none focus:border-conduit-500 focus:ring-1 focus:ring-conduit-500 glass transition-all"
                >
                  <option value="">None - Start from scratch</option>
                  {envList.map((env) => (
                    <option key={(env.name || env.id)} value={(env.name || env.id)}>
                      {(env.name || env.id)}
                    </option>
                  ))}
                </select>
              </div>

              <div className="flex gap-3 justify-end pt-2">
                <Button
                  variant="secondary"
                  onClick={handleCreateCancel}
                  disabled={isCreating}
                >
                  Cancel
                </Button>
                <Button
                  onClick={handleCreateSubmit}
                  loading={isCreating}
                  disabled={isCreating}
                >
                  Create Environment
                </Button>
              </div>
            </div>
          </Card>
        )}

        {envList.length === 0 ? (
          <EmptyState
            icon={Layers}
            title="No environments yet"
            description="Create your first environment to get started with deployment management."
            action={
              <Button onClick={handleCreateClick} size="md">
                <Plus className="w-4 h-4" />
                Create Environment
              </Button>
            }
          />
        ) : (
          <>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 mb-8">
              {envList.map((env) => (
                <Card key={(env.name || env.id)} className="h-full">
                  <div className="flex flex-col h-full">
                    {/* Header */}
                    <div className="flex items-start justify-between mb-4">
                      <div className="flex items-center gap-2 flex-1">
                        <h3 className="text-lg font-semibold text-conduit-50">
                          {(env.name || env.id)}
                        </h3>
                        {(env.name || env.id) === 'production' && (
                          <Shield className="w-4 h-4 text-amber-400" />
                        )}
                      </div>
                    </div>

                    {/* Details */}
                    {env.basedOn && (
                      <p className="text-sm text-conduit-400 mb-3">
                        Based on: <span className="text-conduit-300">{env.basedOn}</span>
                      </p>
                    )}

                    {/* Stats */}
                    <div className="grid grid-cols-2 gap-4 mb-4 pb-4 border-t border-conduit-700/50">
                      <div>
                        <p className="text-xs text-conduit-500 uppercase tracking-wide">
                          Snapshots
                        </p>
                        <p className="text-xl font-bold text-conduit-200 mt-1">
                          {env.snapshotCount || 0}
                        </p>
                      </div>
                      <div>
                        <p className="text-xs text-conduit-500 uppercase tracking-wide">
                          Last Updated
                        </p>
                        <p className="text-sm text-conduit-300 mt-1">
                          {formatRelativeTime(env.updatedAt)}
                        </p>
                      </div>
                    </div>

                    {/* Actions */}
                    <div className="flex gap-2 pt-2 border-t border-conduit-700/50">
                      <button
                        onClick={() => handlePromoteClick((env.name || env.id))}
                        className="flex-1 flex items-center justify-center gap-1.5 px-3 py-2 bg-conduit-800/50 hover:bg-conduit-700/50 text-conduit-300 text-xs font-medium rounded-lg border border-conduit-700/50 transition-all"
                      >
                        <ArrowUpRight className="w-3.5 h-3.5" />
                        Promote
                      </button>
                      <button
                        onClick={() => handleDiffClick((env.name || env.id))}
                        className="flex-1 flex items-center justify-center gap-1.5 px-3 py-2 bg-conduit-800/50 hover:bg-conduit-700/50 text-conduit-300 text-xs font-medium rounded-lg border border-conduit-700/50 transition-all"
                      >
                        <GitCompare className="w-3.5 h-3.5" />
                        Diff
                      </button>
                      {(env.name || env.id) !== 'production' && (
                        <button
                          onClick={() => handleDeleteClick((env.name || env.id))}
                          className="flex items-center justify-center gap-1.5 px-3 py-2 bg-red-900/20 hover:bg-red-900/30 text-red-400 text-xs font-medium rounded-lg border border-red-700/50 transition-all"
                        >
                          <Trash2 className="w-3.5 h-3.5" />
                        </button>
                      )}
                    </div>
                  </div>
                </Card>
              ))}
            </div>

            {/* Promote Modal */}
            {showPromoteModal && (
              <div className="fixed inset-0 bg-black/50 backdrop-blur-sm flex items-center justify-center z-50 p-4">
                <Card className="w-full max-w-md border-conduit-600/50">
                  <div className="space-y-4">
                    <div className="flex items-center justify-between mb-2">
                      <h3 className="text-lg font-semibold text-conduit-50">
                        Promote Environment
                      </h3>
                      <button
                        onClick={() => setShowPromoteModal(false)}
                        className="text-conduit-400 hover:text-conduit-200 transition-colors"
                      >
                        <X className="w-5 h-5" />
                      </button>
                    </div>

                    {promoteError && (
                      <div className="p-3 bg-red-900/20 border border-red-700/50 rounded text-red-200 text-sm">
                        {promoteError}
                      </div>
                    )}

                    <div>
                      <label className="block text-sm font-medium text-conduit-200 mb-2">
                        Source Environment
                      </label>
                      <input
                        type="text"
                        disabled
                        value={promoteSource}
                        className="w-full px-3 py-2 bg-conduit-800/50 border border-conduit-700/50 rounded-lg text-conduit-50 disabled:opacity-60 glass"
                      />
                    </div>

                    <div>
                      <label className="block text-sm font-medium text-conduit-200 mb-2">
                        Target Environment
                      </label>
                      <select
                        value={promoteTarget}
                        onChange={(e) => setPromoteTarget(e.target.value)}
                        disabled={isPromoting}
                        className="w-full px-3 py-2 bg-conduit-800/50 border border-conduit-700/50 rounded-lg text-conduit-50 focus:outline-none focus:border-conduit-500 focus:ring-1 focus:ring-conduit-500 glass transition-all"
                      >
                        <option value="">Select target environment</option>
                        {envList
                          .filter((env) => (env.name || env.id) !== promoteSource)
                          .map((env) => (
                            <option key={(env.name || env.id)} value={(env.name || env.id)}>
                              {(env.name || env.id)}
                            </option>
                          ))}
                      </select>
                    </div>

                    <div className="flex gap-3 justify-end pt-2">
                      <Button
                        variant="secondary"
                        onClick={() => setShowPromoteModal(false)}
                        disabled={isPromoting}
                      >
                        Cancel
                      </Button>
                      <Button
                        onClick={handlePromoteConfirm}
                        loading={isPromoting}
                        disabled={isPromoting || !promoteTarget}
                      >
                        Promote
                      </Button>
                    </div>
                  </div>
                </Card>
              </div>
            )}

            {/* Diff Viewer Modal */}
            {diffSourceEnv && (
              <div className="fixed inset-0 bg-black/50 backdrop-blur-sm flex items-center justify-center z-50 p-4">
                <Card className="w-full max-w-2xl border-conduit-600/50 max-h-[90vh] overflow-y-auto">
                  <div className="space-y-4">
                    <div className="flex items-center justify-between mb-2">
                      <h3 className="text-lg font-semibold text-conduit-50">
                        Compare Environments
                      </h3>
                      <button
                        onClick={handleDiffClose}
                        className="text-conduit-400 hover:text-conduit-200 transition-colors"
                      >
                        <X className="w-5 h-5" />
                      </button>
                    </div>

                    {!diffResults && (
                      <>
                        {diffError && (
                          <div className="p-3 bg-red-900/20 border border-red-700/50 rounded text-red-200 text-sm">
                            {diffError}
                          </div>
                        )}

                        <div>
                          <label className="block text-sm font-medium text-conduit-200 mb-2">
                            Source Environment
                          </label>
                          <input
                            type="text"
                            disabled
                            value={diffSourceEnv}
                            className="w-full px-3 py-2 bg-conduit-800/50 border border-conduit-700/50 rounded-lg text-conduit-50 disabled:opacity-60 glass"
                          />
                        </div>

                        <div>
                          <label className="block text-sm font-medium text-conduit-200 mb-2">
                            Target Environment
                          </label>
                          <select
                            value={diffTargetEnv}
                            onChange={(e) => setDiffTargetEnv(e.target.value)}
                            disabled={isLoadingDiff}
                            className="w-full px-3 py-2 bg-conduit-800/50 border border-conduit-700/50 rounded-lg text-conduit-50 focus:outline-none focus:border-conduit-500 focus:ring-1 focus:ring-conduit-500 glass transition-all"
                          >
                            <option value="">Select target environment</option>
                            {envList
                              .filter((env) => (env.name || env.id) !== diffSourceEnv)
                              .map((env) => (
                                <option key={(env.name || env.id)} value={(env.name || env.id)}>
                                  {(env.name || env.id)}
                                </option>
                              ))}
                          </select>
                        </div>

                        <div className="flex gap-3 justify-end pt-2">
                          <Button
                            variant="secondary"
                            onClick={handleDiffClose}
                            disabled={isLoadingDiff}
                          >
                            Cancel
                          </Button>
                          <Button
                            onClick={handleDiffConfirm}
                            loading={isLoadingDiff}
                            disabled={isLoadingDiff || !diffTargetEnv}
                          >
                            Compare
                          </Button>
                        </div>
                      </>
                    )}

                    {diffResults && (
                      <>
                        <div className="flex items-center gap-2 text-conduit-300 text-sm mb-4 pb-4 border-b border-conduit-700/50">
                          <span className="font-medium">{diffSourceEnv}</span>
                          <ArrowUpRight className="w-4 h-4" />
                          <span className="font-medium">{diffTargetEnv}</span>
                        </div>

                        <div className="overflow-x-auto">
                          <table className="w-full text-sm">
                            <thead>
                              <tr className="border-b border-conduit-700/50">
                                <th className="text-left py-3 px-3 text-conduit-400 font-medium">
                                  Task
                                </th>
                                <th className="text-left py-3 px-3 text-conduit-400 font-medium">
                                  {diffSourceEnv} Snapshot
                                </th>
                                <th className="text-left py-3 px-3 text-conduit-400 font-medium">
                                  {diffTargetEnv} Snapshot
                                </th>
                                <th className="text-center py-3 px-3 text-conduit-400 font-medium">
                                  Status
                                </th>
                              </tr>
                            </thead>
                            <tbody>
                              {diffResults?.items && diffResults.items.length > 0 ? (
                                diffResults.items.map((item, idx) => (
                                  <tr
                                    key={idx}
                                    className="border-b border-conduit-700/30 hover:bg-conduit-800/20 transition-colors"
                                  >
                                    <td className="py-3 px-3 text-conduit-200 font-medium">
                                      {item.task || 'N/A'}
                                    </td>
                                    <td className="py-3 px-3 text-conduit-400 font-mono text-xs break-all">
                                      {item.sourceSnapshot || '-'}
                                    </td>
                                    <td className="py-3 px-3 text-conduit-400 font-mono text-xs break-all">
                                      {item.targetSnapshot || '-'}
                                    </td>
                                    <td className="py-3 px-3 text-center">
                                      <StatusBadge
                                        status={item.status || 'Unchanged'}
                                      />
                                    </td>
                                  </tr>
                                ))
                              ) : (
                                <tr>
                                  <td
                                    colSpan="4"
                                    className="py-6 px-3 text-center text-conduit-400"
                                  >
                                    No differences
                                  </td>
                                </tr>
                              )}
                            </tbody>
                          </table>
                        </div>

                        <div className="flex gap-3 justify-end pt-4 border-t border-conduit-700/50">
                          <Button
                            variant="secondary"
                            onClick={handleDiffClose}
                          >
                            Close
                          </Button>
                        </div>
                      </>
                    )}
                  </div>
                </Card>
              </div>
            )}

            {/* Delete Confirmation Modal */}
            {deletingEnv && (
              <div className="fixed inset-0 bg-black/50 backdrop-blur-sm flex items-center justify-center z-50 p-4">
                <Card className="w-full max-w-md border-red-600/30">
                  <div className="space-y-4">
                    <h3 className="text-lg font-semibold text-conduit-50">
                      Delete Environment?
                    </h3>
                    <p className="text-conduit-300">
                      Are you sure you want to delete the{' '}
                      <span className="font-semibold">{deletingEnv}</span>{' '}
                      environment? This action cannot be undone.
                    </p>
                    <div className="flex gap-3 justify-end pt-2">
                      <Button
                        variant="secondary"
                        onClick={() => setDeletingEnv(null)}
                        disabled={isDeleting}
                      >
                        Cancel
                      </Button>
                      <Button
                        variant="danger"
                        onClick={handleDeleteConfirm}
                        loading={isDeleting}
                        disabled={isDeleting}
                      >
                        Delete
                      </Button>
                    </div>
                  </div>
                </Card>
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}
