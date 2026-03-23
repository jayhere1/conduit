import React, { useMemo } from 'react';
import { useParams, useNavigate, Link } from 'react-router-dom';
import { ArrowLeft, Clock, Calendar, CheckCircle, AlertCircle, PlayCircle, Activity } from 'lucide-react';
import { getRun } from '../api';
import { useApi, usePolling } from '../hooks/useApi';
import Card from '../components/Card';
import StatusBadge from '../components/StatusBadge';
import Spinner from '../components/Spinner';
import PageHeader from '../components/PageHeader';
import EmptyState from '../components/EmptyState';
import { formatRelativeTime, formatAbsoluteTime, formatDuration } from '../utils/time';

const getStatusIcon = (status) => {
  switch (status?.toLowerCase()) {
    case 'success':
      return <CheckCircle className="w-6 h-6 text-green-400" />;
    case 'failed':
      return <AlertCircle className="w-6 h-6 text-red-400" />;
    case 'running':
      return <PlayCircle className="w-6 h-6 text-blue-400" />;
    default:
      return <Clock className="w-6 h-6 text-gray-400" />;
  }
};

export default function RunDetail() {
  const { runId } = useParams();
  const navigate = useNavigate();

  const { data: run, loading, error, refetch } = useApi(() => getRun(runId), [runId]);

  // Auto-refresh if the run is still running
  usePolling(
    () => refetch(),
    run?.status?.toLowerCase() === 'running' ? 5000 : null,
    [run?.status, refetch]
  );

  if (error) {
    return (
      <div className="p-6">
        <button
          onClick={() => navigate('/runs')}
          className="flex items-center gap-2 text-conduit-400 hover:text-conduit-300 mb-6 transition-colors"
        >
          <ArrowLeft className="w-4 h-4" />
          Back to Runs
        </button>
        <div className="p-4 bg-red-500/10 border border-red-500/30 rounded-lg text-red-400">
          Failed to load run details: {error.message}
        </div>
      </div>
    );
  }

  if (loading && !run) {
    return (
      <div className="p-6">
        <button
          onClick={() => navigate('/runs')}
          className="flex items-center gap-2 text-conduit-400 hover:text-conduit-300 mb-6 transition-colors"
        >
          <ArrowLeft className="w-4 h-4" />
          Back to Runs
        </button>
        <div className="flex justify-center items-center py-12">
          <Spinner />
        </div>
      </div>
    );
  }

  if (!run) {
    return (
      <div className="p-6">
        <button
          onClick={() => navigate('/runs')}
          className="flex items-center gap-2 text-conduit-400 hover:text-conduit-300 mb-6 transition-colors"
        >
          <ArrowLeft className="w-4 h-4" />
          Back to Runs
        </button>
        <EmptyState
          title="Run not found"
          description="The requested pipeline run could not be found."
        />
      </div>
    );
  }

  return (
    <div className="p-6">
      {/* Header with back button */}
      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center gap-4">
          <button
            onClick={() => navigate('/runs')}
            className="flex items-center gap-2 text-conduit-400 hover:text-conduit-300 transition-colors"
          >
            <ArrowLeft className="w-4 h-4" />
            Back to Runs
          </button>
          <PageHeader title={`Run ${run.id.substring(0, 8)}`} />
        </div>
        <Link
          to={`/runs/${runId}/live`}
          className="flex items-center gap-2 px-3 py-2 rounded-lg bg-conduit-600/20 border border-conduit-600/30 text-conduit-300 text-sm hover:bg-conduit-600/30 transition-colors"
        >
          <Activity size={14} />
          Live Execution View
        </Link>
      </div>

      {/* Status Overview Card */}
      <Card className="mb-6">
        <div className="p-6">
          <div className="flex items-start gap-6">
            {/* Status Icon and Badge */}
            <div className="flex flex-col items-center gap-3">
              {getStatusIcon(run.status)}
              <StatusBadge status={run.status} dot={true} />
            </div>

            {/* Details Grid */}
            <div className="flex-1 grid grid-cols-2 md:grid-cols-4 gap-6">
              {/* DAG Name */}
              <div>
                <p className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-2">
                  DAG Name
                </p>
                <p className="text-gray-200">{run.dagId}</p>
              </div>

              {/* Trigger Source */}
              <div>
                <p className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-2">
                  Triggered By
                </p>
                <p className="text-gray-200">{run.triggeredBy || '-'}</p>
              </div>

              {/* Start Time */}
              <div>
                <p className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-2">
                  Started
                </p>
                <p className="text-gray-200 text-sm">
                  {formatAbsoluteTime(run.startedAt)}
                </p>
                <p className="text-gray-500 text-xs mt-1">
                  {formatRelativeTime(run.startedAt)}
                </p>
              </div>

              {/* End Time / Duration */}
              <div>
                <p className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-2">
                  Duration
                </p>
                <p className="text-gray-200">
                  {formatDuration(run.startedAt, run.endedAt)}
                </p>
                {run.endedAt && (
                  <p className="text-gray-500 text-xs mt-1">
                    Ended {formatRelativeTime(run.endedAt)}
                  </p>
                )}
              </div>
            </div>
          </div>
        </div>
      </Card>

      {/* Task Execution Timeline */}
      <div>
        <h2 className="text-lg font-semibold text-gray-200 mb-4">Task Execution</h2>

        {!run.tasks || run.tasks.length === 0 ? (
          // Show task states if available, even if tasks array is empty
          !run.taskStates || Object.keys(run.taskStates).length === 0 ? (
            <EmptyState
              title="No tasks"
              description="No tasks have been executed for this run yet."
            />
          ) : (
            <div className="space-y-4">
              {Object.entries(run.taskStates).map(([taskId, status], index) => (
                <Card key={taskId} className="relative">
                  {/* Timeline connector */}
                  {index < Object.keys(run.taskStates).length - 1 && (
                    <div className="absolute left-[27px] top-[60px] w-0.5 h-12 bg-conduit-700/30" />
                  )}

                  <div className="p-6">
                    <div className="flex gap-6">
                      {/* Timeline dot */}
                      <div className="flex flex-col items-center pt-1">
                        <div className="w-3 h-3 rounded-full bg-conduit-500 border-2 border-conduit-900" />
                      </div>

                      {/* Task Content */}
                      <div className="flex-1 min-w-0">
                        {/* Task header */}
                        <div className="flex items-start justify-between gap-4 mb-4">
                          <div>
                            <h3 className="text-gray-200 font-semibold mb-2">
                              {taskId}
                            </h3>
                            <StatusBadge status={status} dot={true} size="sm" />
                          </div>
                        </div>

                        {/* Task metadata */}
                        <div className="grid grid-cols-3 gap-4 text-sm">
                          <div>
                            <p className="text-gray-500 text-xs mb-1">Status</p>
                            <p className="text-gray-300 capitalize">{status}</p>
                          </div>
                        </div>
                      </div>
                    </div>
                  </div>
                </Card>
              ))}
            </div>
          )
        ) : (
          <div className="space-y-4">
            {run.tasks.map((task, index) => (
              <Card key={index} className="relative">
                {/* Timeline connector */}
                {index < run.tasks.length - 1 && (
                  <div className="absolute left-[27px] top-[60px] w-0.5 h-12 bg-conduit-700/30" />
                )}

                <div className="p-6">
                  <div className="flex gap-6">
                    {/* Timeline dot */}
                    <div className="flex flex-col items-center pt-1">
                      <div className="w-3 h-3 rounded-full bg-conduit-500 border-2 border-conduit-900" />
                    </div>

                    {/* Task Content */}
                    <div className="flex-1 min-w-0">
                      {/* Task header */}
                      <div className="flex items-start justify-between gap-4 mb-4">
                        <div>
                          <h3 className="text-gray-200 font-semibold mb-2">
                            {task.name}
                          </h3>
                          <StatusBadge status={task.status} dot={true} size="sm" />
                        </div>
                      </div>

                      {/* Task metadata */}
                      <div className="grid grid-cols-3 gap-4 mb-4 text-sm">
                        <div>
                          <p className="text-gray-500 text-xs mb-1">Start Time</p>
                          <p className="text-gray-300">
                            {formatAbsoluteTime(task.startedAt)}
                          </p>
                        </div>
                        <div>
                          <p className="text-gray-500 text-xs mb-1">Duration</p>
                          <p className="text-gray-300">
                            {formatDuration(task.startedAt, task.endedAt)}
                          </p>
                        </div>
                        <div>
                          <p className="text-gray-500 text-xs mb-1">Task ID</p>
                          <p className="text-gray-300 font-mono text-xs">
                            {task.id ? task.id.substring(0, 8) : '-'}
                          </p>
                        </div>
                      </div>

                      {/* Log output */}
                      {task.logs && task.logs.length > 0 && (
                        <div className="mt-4">
                          <p className="text-gray-500 text-xs mb-2 uppercase tracking-wider font-semibold">
                            Log Output
                          </p>
                          <div className="bg-conduit-950/80 border border-conduit-700/50 rounded-md p-3 overflow-auto max-h-64">
                            <pre className="font-mono text-xs text-gray-300 whitespace-pre-wrap break-words">
                              {task.logs
                                .split('\n')
                                .slice(0, 10)
                                .join('\n')}
                              {task.logs.split('\n').length > 10 && (
                                <div className="text-gray-600 mt-2">
                                  ... ({task.logs.split('\n').length - 10} more lines)
                                </div>
                              )}
                            </pre>
                          </div>
                        </div>
                      )}
                    </div>
                  </div>
                </div>
              </Card>
            ))}
          </div>
        )}
      </div>

      {/* Auto-refresh indicator */}
      {run.status?.toLowerCase() === 'running' && (
        <div className="mt-6 text-xs text-gray-500">
          Auto-refreshing every 5 seconds
        </div>
      )}
    </div>
  );
}
