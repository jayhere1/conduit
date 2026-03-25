import React, { useState, useMemo } from 'react';
import { Link } from 'react-router-dom';
import { Search, Zap, Play, Clock, Terminal, Code, Database } from 'lucide-react';
import clsx from 'clsx';
import { useApi } from '../hooks/useApi';
import { listDags, compileDags, triggerRun } from '../api';
import Card from '../components/Card';
import StatusBadge from '../components/StatusBadge';
import Button from '../components/Button';
import Spinner from '../components/Spinner';
import PageHeader from '../components/PageHeader';
import EmptyState from '../components/EmptyState';
import { humanCron } from '../utils/cron';
import { formatRelativeTime } from '../utils/time';

// Helper: Map task type to icon and color
const getTaskTypeIcon = (type) => {
  const lower = (type || '').toLowerCase();
  if (lower.includes('shell') || lower.includes('bash')) return { icon: Terminal, color: 'bg-blue-500/20 text-blue-400' };
  if (lower.includes('python')) return { icon: Code, color: 'bg-green-500/20 text-green-400' };
  if (lower.includes('sql')) return { icon: Database, color: 'bg-purple-500/20 text-purple-400' };
  return { icon: Code, color: 'bg-conduit-600/20 text-conduit-400' };
};

// Helper: Count task types in a DAG
const countTaskTypes = (tasks) => {
  if (!tasks || tasks.length === 0) return [];
  const counts = {};
  tasks.forEach((task) => {
    const type = task.type || 'unknown';
    counts[type] = (counts[type] || 0) + 1;
  });
  return Object.entries(counts).map(([type, count]) => ({ type, count }));
};

export default function DagList() {
  const [searchTerm, setSearchTerm] = useState('');
  const [isCompiling, setIsCompiling] = useState(false);
  const [runningDagId, setRunningDagId] = useState(null);

  const { data: dags, loading, error, refetch } = useApi(listDags);

  const filteredDags = useMemo(() => {
    if (!dags) return [];
    return dags.filter((dag) => {
      const name = dag.name || dag.id || '';
      return name.toLowerCase().includes(searchTerm.toLowerCase());
    });
  }, [dags, searchTerm]);

  const handleCompileAll = async () => {
    setIsCompiling(true);
    try {
      await compileDags();
    } finally {
      setIsCompiling(false);
    }
  };

  const handleRunDag = async (e, dagId) => {
    e.preventDefault();
    e.stopPropagation();
    setRunningDagId(dagId);
    try {
      await triggerRun(dagId);
      // Optional: show toast notification here
    } catch (err) {
      console.error('Failed to trigger run:', err);
    } finally {
      setRunningDagId(null);
    }
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-screen">
        <Spinner />
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-conduit-950 via-conduit-900 to-conduit-950">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <PageHeader
          title="DAGs"
          subtitle="Directed Acyclic Graphs - Define your data pipelines"
          action={
            <Button
              onClick={handleCompileAll}
              disabled={isCompiling}
              className="flex items-center gap-2"
            >
              <Zap className="w-4 h-4" />
              {isCompiling ? 'Compiling...' : 'Compile All'}
            </Button>
          }
        />

        <div className="mt-8 mb-6">
          <div className="relative">
            <Search className="absolute left-3 top-3 w-5 h-5 text-conduit-400" />
            <input
              type="text"
              placeholder="Search DAGs by name..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="w-full pl-10 pr-4 py-2 bg-conduit-800/50 border border-conduit-700/50 rounded-lg text-conduit-50 placeholder-conduit-500 focus:outline-none focus:border-conduit-500 focus:ring-1 focus:ring-conduit-500 glass transition-all"
            />
          </div>
        </div>

        {error && (
          <div className="mb-6 p-4 bg-red-900/20 border border-red-700/50 rounded-lg text-red-200">
            Error loading DAGs: {error.message}
          </div>
        )}

        {filteredDags.length === 0 ? (
          <EmptyState
            title={searchTerm ? 'No DAGs found' : 'No DAGs yet'}
            description={
              searchTerm
                ? `No DAGs match "${searchTerm}". Try a different search term.`
                : 'Create your first DAG to get started building data pipelines.'
            }
            icon="Grid"
          />
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-6 auto-rows-max">
            {filteredDags.map((dag) => {
              const taskTypes = countTaskTypes(dag.tasks);
              const lastRunRelative = dag.lastRunAt ? formatRelativeTime(dag.lastRunAt) : null;
              const statusColor = dag.lastRunStatus === 'success'
                ? 'bg-emerald-500/20'
                : dag.lastRunStatus === 'failed'
                ? 'bg-red-500/20'
                : 'bg-gray-500/20';
              const statusDot = dag.lastRunStatus === 'success'
                ? 'bg-emerald-500 shadow-emerald-500/50 shadow-sm'
                : dag.lastRunStatus === 'failed'
                ? 'bg-red-500 animate-pulse shadow-red-500/50 shadow-sm'
                : 'bg-gray-500';

              return (
                <Link key={dag.id} to={`/dags/${dag.id}`}>
                  <Card
                    className={clsx(
                      'h-full cursor-pointer transition-all duration-200',
                      'hover:border-conduit-500/70 hover:-translate-y-0.5 hover:shadow-xl hover:shadow-conduit-500/15'
                    )}
                  >
                    <div className="flex flex-col h-full">
                      {/* Header with Status Dot and Name */}
                      <div className="flex items-start justify-between mb-3">
                        <div className="flex items-center gap-2 flex-1">
                          <div className={clsx('w-3 h-3 rounded-full transition-all', statusDot)} />
                          <h3 className="text-lg font-semibold text-conduit-50 truncate">
                            {dag.name || dag.id}
                          </h3>
                        </div>
                      </div>

                      {/* Description */}
                      <p className="text-sm text-conduit-400 mb-4 line-clamp-2 flex-1">
                        {dag.description || 'No description'}
                      </p>

                      {/* Stats Grid */}
                      <div className="grid grid-cols-2 gap-3 mb-4 pb-4 border-t border-conduit-700/30">
                        <div>
                          <p className="text-xs text-conduit-500 uppercase tracking-wide font-medium">
                            Tasks
                          </p>
                          <p className="text-xl font-bold text-conduit-200 mt-1">
                            {dag.taskCount || dag.task_count || 0}
                          </p>
                        </div>
                        <div>
                          <p className="text-xs text-conduit-500 uppercase tracking-wide font-medium">
                            Schedule
                          </p>
                          <p className="text-xs text-conduit-300 mt-1 truncate" title={dag.schedule || 'Manual'}>
                            {humanCron(dag.schedule || '@manual')}
                          </p>
                        </div>
                      </div>

                      {/* Task Type Breakdown */}
                      {taskTypes.length > 0 && (
                        <div className="mb-4 pb-4 border-t border-conduit-700/30">
                          <p className="text-xs text-conduit-500 uppercase tracking-wide font-medium mb-2">
                            Task Types
                          </p>
                          <div className="flex flex-wrap gap-2">
                            {taskTypes.map(({ type, count }) => {
                              const { icon: TypeIcon, color } = getTaskTypeIcon(type);
                              return (
                                <span
                                  key={type}
                                  className={clsx(
                                    'px-2.5 py-1 text-xs font-medium rounded-lg border border-opacity-40',
                                    color
                                  )}
                                >
                                  {count} {type}
                                </span>
                              );
                            })}
                          </div>
                        </div>
                      )}

                      {/* Tags */}
                      {dag.tags && dag.tags.length > 0 && (
                        <div className="mb-4 pb-4 border-t border-conduit-700/30">
                          <div className="flex flex-wrap gap-2">
                            {dag.tags.map((tag) => (
                              <span
                                key={tag}
                                className="px-2 py-1 text-xs bg-conduit-700/40 text-conduit-300 rounded-full border border-conduit-600/40"
                              >
                                {tag}
                              </span>
                            ))}
                          </div>
                        </div>
                      )}

                      {/* Last Run Time */}
                      <div className="mb-4 pb-4 border-t border-conduit-700/30">
                        <p className="text-xs text-conduit-500 uppercase tracking-wide font-medium mb-1">
                          Last Run
                        </p>
                        <p className="text-sm text-conduit-400">
                          {lastRunRelative || 'Never run'}
                        </p>
                      </div>

                      {/* Quick Action Button */}
                      <div
                        onClick={(e) => e.preventDefault()}
                        className="mt-auto"
                      >
                        <button
                          onClick={(e) => handleRunDag(e, dag.id)}
                          disabled={runningDagId === dag.id}
                          className={clsx(
                            'w-full flex items-center justify-center gap-2 px-3 py-2.5 rounded-lg text-sm font-medium transition-all',
                            'border border-conduit-600/40 hover:border-conduit-500/60',
                            'bg-conduit-600/15 hover:bg-conduit-600/25 text-conduit-300 hover:text-conduit-200',
                            'disabled:opacity-50 disabled:cursor-not-allowed'
                          )}
                        >
                          <Play className="w-4 h-4" />
                          {runningDagId === dag.id ? 'Running...' : 'Run'}
                        </button>
                      </div>
                    </div>
                  </Card>
                </Link>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
}
