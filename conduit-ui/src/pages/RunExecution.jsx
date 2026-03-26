import { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import { useParams, useNavigate, Link } from 'react-router-dom';
import {
  ArrowLeft,
  Play,
  Pause,
  CheckCircle,
  XCircle,
  Clock,
  Loader,
  Timer,
  Zap,
  Activity,
  AlertTriangle,
  ChevronRight,
  Terminal,
  BarChart3,
  RefreshCw,
} from 'lucide-react';
import { getRun, getDag, getDagGraph, connectEvents } from '../api';
import { useApi } from '../hooks/useApi';
import Card, { StatCard } from '../components/Card';
import StatusBadge from '../components/StatusBadge';
import Spinner from '../components/Spinner';
import PageHeader from '../components/PageHeader';
import Button from '../components/Button';
import clsx from 'clsx';

// ─── CSS Animations ──────────────────────────────────────────────────────────

const animationStyles = `
  @keyframes pulse-glow {
    0%, 100% { box-shadow: 0 0 8px rgba(59, 130, 246, 0.4); }
    50% { box-shadow: 0 0 16px rgba(59, 130, 246, 0.8); }
  }
  @keyframes flash-green {
    0% { box-shadow: 0 0 0 rgba(16, 185, 129, 0); }
    50% { box-shadow: 0 0 20px rgba(16, 185, 129, 1); }
    100% { box-shadow: 0 0 0 rgba(16, 185, 129, 0); }
  }
  @keyframes shake {
    0%, 100% { transform: translateX(0); }
    25% { transform: translateX(-4px); }
    75% { transform: translateX(4px); }
  }
  @keyframes grow-bar {
    0% { width: 0%; }
    100% { width: 100%; }
  }
  .pulse-glow-animation { animation: pulse-glow 1.5s ease-in-out infinite; }
  .flash-green-animation { animation: flash-green 0.6s ease-out; }
  .shake-animation { animation: shake 0.4s ease-in-out; }
  .gantt-bar-running { animation: grow-bar 10s linear; }
`;

// ─── Time Helpers ────────────────────────────────────────────────────────────

const formatDuration = (startedAt, endedAt) => {
  if (!startedAt) return '—';
  const start = new Date(startedAt);
  const end = endedAt ? new Date(endedAt) : new Date();
  const seconds = Math.floor((end - start) / 1000);
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ${seconds % 60}s`;
  const hours = Math.floor(minutes / 60);
  return `${hours}h ${minutes % 60}m`;
};

const formatTime = (ts) => {
  if (!ts) return '—';
  return new Date(ts).toLocaleTimeString();
};

// ─── Status Helpers ──────────────────────────────────────────────────────────

const STATUS_CONFIG = {
  success: {
    color: '#10b981',
    bgClass: 'bg-green-500/15 border-green-500/30',
    textClass: 'text-green-400',
    Icon: CheckCircle,
    pulse: false,
  },
  failed: {
    color: '#ef4444',
    bgClass: 'bg-red-500/15 border-red-500/30',
    textClass: 'text-red-400',
    Icon: XCircle,
    pulse: false,
  },
  running: {
    color: '#3b82f6',
    bgClass: 'bg-blue-500/15 border-blue-500/30',
    textClass: 'text-blue-400',
    Icon: Loader,
    pulse: true,
  },
  queued: {
    color: '#f59e0b',
    bgClass: 'bg-amber-500/15 border-amber-500/30',
    textClass: 'text-amber-400',
    Icon: Clock,
    pulse: false,
  },
  pending: {
    color: '#6b7280',
    bgClass: 'bg-gray-500/10 border-gray-600/30',
    textClass: 'text-gray-500',
    Icon: Clock,
    pulse: false,
  },
};

const getStatusConfig = (status) =>
  STATUS_CONFIG[status?.toLowerCase()] || STATUS_CONFIG.pending;

// ─── Progress Indicator ──────────────────────────────────────────────────

function ProgressIndicator({ taskStates }) {
  if (!taskStates) return null;

  const entries = Object.entries(taskStates);
  const total = entries.length;
  const completed = entries.filter(([, s]) => s.toLowerCase() === 'success').length;
  const failed = entries.filter(([, s]) => s.toLowerCase() === 'failed').length;
  const running = entries.filter(([, s]) => s.toLowerCase() === 'running').length;
  const pending = total - completed - failed - running;

  const pct = total > 0 ? Math.round(((completed + failed) / total) * 100) : 0;

  return (
    <Card>
      <div className="space-y-4">
        {/* Summary text */}
        <div className="flex items-baseline gap-2">
          <span className="text-lg font-bold text-white">
            {completed}/{total}
          </span>
          <span className="text-sm text-gray-400">
            tasks complete
          </span>
        </div>

        {/* Segmented progress bar */}
        <div className="flex-1 h-3 bg-conduit-900/50 rounded-full overflow-hidden border border-conduit-800/30 flex">
          <div
            className="bg-emerald-500 transition-all duration-500"
            style={{ width: `${total > 0 ? (completed / total) * 100 : 0}%` }}
            title={`Completed: ${completed}`}
          />
          <div
            className="bg-blue-500 transition-all duration-500 animate-pulse"
            style={{ width: `${total > 0 ? (running / total) * 100 : 0}%` }}
            title={`Running: ${running}`}
          />
          <div
            className="bg-red-500 transition-all duration-500"
            style={{ width: `${total > 0 ? (failed / total) * 100 : 0}%` }}
            title={`Failed: ${failed}`}
          />
          <div
            className="bg-gray-600 transition-all duration-500"
            style={{ width: `${total > 0 ? (pending / total) * 100 : 0}%` }}
            title={`Pending: ${pending}`}
          />
        </div>

        {/* Legend */}
        <div className="flex items-center gap-4 flex-wrap text-xs">
          <div className="flex items-center gap-2">
            <div className="w-3 h-3 rounded bg-emerald-500" />
            <span className="text-gray-400">Completed: {completed}</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="w-3 h-3 rounded bg-blue-500 animate-pulse" />
            <span className="text-gray-400">Running: {running}</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="w-3 h-3 rounded bg-red-500" />
            <span className="text-gray-400">Failed: {failed}</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="w-3 h-3 rounded bg-gray-600" />
            <span className="text-gray-400">Pending: {pending}</span>
          </div>
        </div>
      </div>
    </Card>
  );
}

// ─── Gantt Timeline ──────────────────────────────────────────────────────

function GanttTimeline({ taskStates, run, onSelectTask, selectedTask }) {
  if (!taskStates || !run?.tasks) return null;

  const tasks = run.tasks.filter(t => t.name && taskStates[t.name]);
  if (tasks.length === 0) return null;

  const runStart = run.startedAt ? new Date(run.startedAt) : new Date();
  const now = new Date();
  const timeSpan = Math.max((now - runStart) / 1000, 1);

  // Find critical path (longest cumulative duration chain)
  const taskDurations = {};
  tasks.forEach(t => {
    const s = t.startedAt ? new Date(t.startedAt) : null;
    const e = t.endedAt ? new Date(t.endedAt) : (taskStates[t.name]?.toLowerCase() === 'running' ? now : null);
    taskDurations[t.name] = s && e ? (e - s) / 1000 : 0;
  });
  const maxTaskDuration = Math.max(...Object.values(taskDurations), 0);
  const criticalThreshold = maxTaskDuration * 0.8;

  return (
    <Card title="Execution Timeline" subtitle={`All ${tasks.length} tasks — click to inspect`}>
      <div className="space-y-1.5 max-h-[420px] overflow-y-auto pr-1 -mr-1">
        {tasks.map((task) => {
          const status = taskStates[task.name]?.toLowerCase() || 'pending';
          const taskStart = task.startedAt ? new Date(task.startedAt) : null;
          const taskEnd = task.endedAt ? new Date(task.endedAt) : null;
          const isSelected = selectedTask === task.name;
          const isCritical = taskDurations[task.name] >= criticalThreshold && criticalThreshold > 0;

          let startOffset = 0;
          let barWidth = 0;

          if (taskStart) {
            startOffset = ((taskStart - runStart) / 1000) / timeSpan * 100;
            if (taskEnd) {
              barWidth = ((taskEnd - taskStart) / 1000) / timeSpan * 100;
            } else if (status === 'running') {
              barWidth = ((now - taskStart) / 1000) / timeSpan * 100;
            }
          }

          const bgColor = {
            success: isCritical ? 'bg-amber-500' : 'bg-emerald-500',
            running: 'bg-blue-500',
            failed: 'bg-red-500',
            pending: 'bg-gray-700',
            skipped: 'bg-gray-600',
          }[status] || 'bg-gray-700';

          return (
            <div
              key={task.name}
              className={clsx('space-y-0.5 px-2 py-1 rounded-lg cursor-pointer transition-all', isSelected ? 'bg-purple-500/10 ring-1 ring-purple-500/30' : 'hover:bg-conduit-800/20')}
              onClick={() => onSelectTask?.(task.name)}
            >
              <div className="flex items-center justify-between">
                <span className="text-[11px] font-medium text-gray-300 truncate flex-1 pr-2">
                  {task.name}
                  {isCritical && status === 'success' && <span className="ml-1.5 text-[9px] text-amber-400/70 font-normal">slow</span>}
                </span>
                <span className="text-[11px] text-gray-500 font-mono tabular-nums">
                  {status === 'pending' || status === 'skipped' ? '—' : taskEnd ? ((taskEnd - (taskStart || runStart)) / 1000).toFixed(1) + 's' : ((now - (taskStart || runStart)) / 1000).toFixed(1) + 's'}
                </span>
              </div>
              <div className="h-[6px] bg-conduit-900/50 rounded-full overflow-hidden border border-conduit-800/20 relative">
                <div
                  className={clsx(bgColor, 'h-full rounded-full transition-all', status === 'running' && 'gantt-bar-running')}
                  style={{
                    marginLeft: `${startOffset}%`,
                    width: `${Math.max(barWidth, status === 'pending' ? 0 : 1.5)}%`,
                  }}
                />
              </div>
            </div>
          );
        })}
      </div>
    </Card>
  );
}

// ─── DAG Execution Graph (SVG) ──────────────────────────────────────────────

function ExecutionGraph({ dagGraph, taskStates, selectedTask, onSelectTask }) {
  if (!dagGraph?.nodes || dagGraph.nodes.length === 0) return null;

  const { nodes, edges } = dagGraph;

  // Calculate layer depths via topological sort
  const nodeLayerMap = {};
  nodes.forEach((n) => { nodeLayerMap[n.id] = 0; });

  const visited = new Set();
  const visiting = new Set();

  function calcDepth(nodeId) {
    if (visited.has(nodeId)) return nodeLayerMap[nodeId];
    if (visiting.has(nodeId)) return 0;
    visiting.add(nodeId);

    const incoming = edges.filter((e) => e.to === nodeId);
    if (incoming.length > 0) {
      nodeLayerMap[nodeId] = Math.max(...incoming.map((e) => calcDepth(e.from))) + 1;
    }

    visiting.delete(nodeId);
    visited.add(nodeId);
    return nodeLayerMap[nodeId];
  }

  nodes.forEach((n) => calcDepth(n.id));

  // Group by layer
  const layers = {};
  nodes.forEach((n) => {
    const l = nodeLayerMap[n.id];
    if (!layers[l]) layers[l] = [];
    layers[l].push(n);
  });

  const NODE_W = 180;
  const NODE_H = 72;
  const LAYER_GAP = 240;
  const VERT_GAP = 100;
  const PAD = 50;

  const layerKeys = Object.keys(layers).map(Number);
  const layerCount = layerKeys.length > 0 ? Math.max(...layerKeys) + 1 : 1;
  const layerValues = Object.values(layers).map((l) => l.length);
  const maxNodesInLayer = layerValues.length > 0 ? Math.max(...layerValues) : 1;

  const positions = {};
  Object.entries(layers).forEach(([layer, layerNodes]) => {
    const li = parseInt(layer);
    const x = PAD + li * LAYER_GAP + NODE_W / 2;
    const totalH = layerNodes.length * VERT_GAP;
    const maxH = maxNodesInLayer * VERT_GAP;
    const startY = PAD + (maxH - totalH) / 2;

    layerNodes.forEach((node, idx) => {
      positions[node.id] = { x, y: startY + idx * VERT_GAP + NODE_H / 2 };
    });
  });

  const svgW = layerCount * LAYER_GAP + 2 * PAD;
  const svgH = Math.max(350, maxNodesInLayer * VERT_GAP + 2 * PAD);

  return (
    <div className="overflow-x-auto rounded-xl border border-conduit-800/50 bg-conduit-950/50">
      <svg width={svgW} height={svgH}>
        <defs>
          <marker id="exec-arrow" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto">
            <polygon points="0 0, 8 3, 0 6" fill="#4b5563" />
          </marker>
          <marker id="exec-arrow-active" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto">
            <polygon points="0 0, 8 3, 0 6" fill="#3b82f6" />
          </marker>
          <filter id="glow-blue">
            <feGaussianBlur stdDeviation="3" result="blur" />
            <feMerge>
              <feMergeNode in="blur" />
              <feMergeNode in="SourceGraphic" />
            </feMerge>
          </filter>
          <filter id="glow-green">
            <feGaussianBlur stdDeviation="2" result="blur" />
            <feMerge>
              <feMergeNode in="blur" />
              <feMergeNode in="SourceGraphic" />
            </feMerge>
          </filter>
        </defs>

        {/* Edges */}
        {edges.map((edge, idx) => {
          const from = positions[edge.from];
          const to = positions[edge.to];
          if (!from || !to) return null;

          const x1 = from.x + NODE_W / 2;
          const y1 = from.y;
          const x2 = to.x - NODE_W / 2;
          const y2 = to.y;
          const midX = (x1 + x2) / 2;

          const fromStatus = taskStates?.[edge.from]?.toLowerCase();
          const isActive = fromStatus === 'success' || fromStatus === 'running';

          return (
            <path
              key={`edge-${idx}`}
              d={`M ${x1} ${y1} C ${midX} ${y1}, ${midX} ${y2}, ${x2} ${y2}`}
              fill="none"
              stroke={isActive ? '#3b82f6' : '#374151'}
              strokeWidth={isActive ? 2.5 : 1.5}
              strokeDasharray={fromStatus === 'running' ? '6 4' : 'none'}
              opacity={isActive ? 0.8 : 0.4}
              markerEnd={isActive ? 'url(#exec-arrow-active)' : 'url(#exec-arrow)'}
            />
          );
        })}

        {/* Nodes */}
        {nodes.map((node) => {
          const pos = positions[node.id];
          if (!pos) return null;

          const status = taskStates?.[node.id]?.toLowerCase() || 'pending';
          const cfg = getStatusConfig(status);
          const isSelected = selectedTask === node.id;

          const x = pos.x - NODE_W / 2;
          const y = pos.y - NODE_H / 2;

          return (
            <g
              key={node.id}
              onClick={() => onSelectTask(node.id)}
              className="cursor-pointer"
              filter={status === 'running' ? 'url(#glow-blue)' : undefined}
            >
              {/* Selection ring */}
              {isSelected && (
                <rect
                  x={x - 3}
                  y={y - 3}
                  width={NODE_W + 6}
                  height={NODE_H + 6}
                  rx="14"
                  fill="none"
                  stroke="#8b5cf6"
                  strokeWidth="2"
                  strokeDasharray="4 2"
                />
              )}

              {/* Node background */}
              <rect
                x={x}
                y={y}
                width={NODE_W}
                height={NODE_H}
                rx="12"
                fill={status === 'running' ? '#1e3a5f' : '#111827'}
                stroke={cfg.color}
                strokeWidth={status === 'running' ? 2 : 1.5}
                opacity={status === 'pending' ? 0.5 : 1}
              />

              {/* Status indicator dot */}
              <circle
                cx={x + 16}
                cy={pos.y - 8}
                r={4}
                fill={cfg.color}
              >
                {cfg.pulse && (
                  <animate
                    attributeName="opacity"
                    values="1;0.3;1"
                    dur="1.5s"
                    repeatCount="indefinite"
                  />
                )}
              </circle>

              {/* Task name */}
              <text
                x={x + 28}
                y={pos.y - 5}
                fill="#e5e7eb"
                fontSize="12"
                fontWeight="600"
                className="select-none"
              >
                {node.name?.length > 18 ? node.name.substring(0, 18) + '...' : node.name}
              </text>

              {/* Status text */}
              <text
                x={x + 28}
                y={pos.y + 12}
                fill={cfg.color}
                fontSize="10"
                fontWeight="500"
                className="select-none"
              >
                {status.charAt(0).toUpperCase() + status.slice(1)}
              </text>

              {/* Type badge */}
              <text
                x={x + NODE_W - 12}
                y={pos.y + 12}
                fill="#6b7280"
                fontSize="9"
                textAnchor="end"
                className="select-none"
              >
                {node.type || 'task'}
              </text>

              {/* Running spinner animation */}
              {status === 'running' && (
                <g transform={`translate(${x + NODE_W - 20}, ${pos.y - 14})`}>
                  <circle
                    r="6"
                    fill="none"
                    stroke="#3b82f6"
                    strokeWidth="2"
                    strokeDasharray="12 20"
                    strokeLinecap="round"
                  >
                    <animateTransform
                      attributeName="transform"
                      type="rotate"
                      from="0"
                      to="360"
                      dur="1s"
                      repeatCount="indefinite"
                    />
                  </circle>
                </g>
              )}
            </g>
          );
        })}
      </svg>
    </div>
  );
}

// ─── Task Detail Sidebar ─────────────────────────────────────────────────

function TaskSidebar({ taskId, taskState, run }) {
  if (!taskId) {
    return (
      <Card>
        <div className="text-center py-12">
          <Activity size={24} className="mx-auto text-gray-600 mb-3" />
          <p className="text-sm text-gray-500">Click a task node to inspect it</p>
          <p className="text-xs text-gray-600 mt-1">View status, timing, and logs</p>
        </div>
      </Card>
    );
  }

  const status = taskState?.toLowerCase() || 'pending';
  const cfg = getStatusConfig(status);
  const StatusIcon = cfg.Icon;

  // Find task info from run data
  const taskInfo = run?.tasks?.find((t) => t.name === taskId || t.id === taskId);

  return (
    <div className="space-y-4">
      {/* Task Header */}
      <Card>
        <div className="flex items-center gap-3 mb-4">
          <div
            className={clsx(
              'w-10 h-10 rounded-lg flex items-center justify-center border',
              cfg.bgClass
            )}
          >
            <StatusIcon size={20} className={cfg.textClass} />
          </div>
          <div>
            <h3 className="text-sm font-semibold text-white">{taskId}</h3>
            <StatusBadge status={status} dot size="sm" />
          </div>
        </div>

        {/* Timing */}
        <div className="grid grid-cols-2 gap-3 mt-4">
          <div>
            <p className="text-xs text-gray-500 mb-1">Started</p>
            <p className="text-xs text-gray-300 font-mono">
              {formatTime(taskInfo?.startedAt)}
            </p>
          </div>
          <div>
            <p className="text-xs text-gray-500 mb-1">Duration</p>
            <p className="text-xs text-gray-300 font-mono">
              {formatDuration(taskInfo?.startedAt, taskInfo?.endedAt)}
            </p>
          </div>
        </div>
      </Card>

      {/* Task Metrics (if available) */}
      {taskInfo?.metrics && Object.keys(taskInfo.metrics).length > 0 && (
        <Card title="Metrics" icon={BarChart3}>
          <div className="space-y-2">
            {Object.entries(taskInfo.metrics).map(([key, val]) => (
              <div key={key} className="flex items-center justify-between py-1.5 border-b border-conduit-800/30 last:border-0">
                <span className="text-xs text-gray-400 font-mono">{key}</span>
                <span className="text-xs text-conduit-300 font-semibold">{val}</span>
              </div>
            ))}
          </div>
        </Card>
      )}

      {/* Task Logs Preview */}
      {taskInfo?.logs && (
        <Card title="Log Output" icon={Terminal}>
          <div className="bg-conduit-950/80 border border-conduit-800/30 rounded-lg p-3 max-h-48 overflow-y-auto">
            <pre className="font-mono text-xs text-gray-400 whitespace-pre-wrap break-words">
              {taskInfo.logs.split('\n').slice(0, 20).join('\n')}
              {taskInfo.logs.split('\n').length > 20 && (
                <span className="text-gray-600 block mt-2">
                  ... {taskInfo.logs.split('\n').length - 20} more lines
                </span>
              )}
            </pre>
          </div>
        </Card>
      )}

      {/* Retry Info */}
      {taskInfo?.retries > 0 && (
        <Card>
          <div className="flex items-center gap-2">
            <RefreshCw size={14} className="text-amber-400" />
            <span className="text-xs text-amber-400">
              Retried {taskInfo.retries} time{taskInfo.retries > 1 ? 's' : ''}
            </span>
          </div>
        </Card>
      )}
    </div>
  );
}

// ─── Main Page ───────────────────────────────────────────────────────────────

export default function RunExecution() {
  const { runId } = useParams();
  const navigate = useNavigate();
  const [selectedTask, setSelectedTask] = useState(null);
  const [elapsedTick, setElapsedTick] = useState(0);
  const wsRef = useRef(null);

  // Inject animations
  useEffect(() => {
    const style = document.createElement('style');
    style.textContent = animationStyles;
    document.head.appendChild(style);
    return () => style.remove();
  }, []);

  // Fetch run data
  const { data: run, loading, error, refetch } = useApi(() => getRun(runId), [runId]);

  // Fetch DAG graph for visualization
  const dagId = run?.dagId || run?.dag_id;
  const fetchGraph = useCallback(
    () => (dagId ? getDagGraph(dagId) : Promise.resolve(null)),
    [dagId]
  );
  const { data: dagGraph } = useApi(fetchGraph, [dagId]);

  // Auto-poll while running
  const isRunning = run?.status?.toLowerCase() === 'running' || run?.status?.toLowerCase() === 'queued';

  useEffect(() => {
    if (!isRunning) return;
    const id = setInterval(refetch, 3000);
    return () => clearInterval(id);
  }, [isRunning, refetch]);

  // Elapsed time ticker for running runs
  useEffect(() => {
    if (!isRunning) return;
    const id = setInterval(() => setElapsedTick((t) => t + 1), 1000);
    return () => clearInterval(id);
  }, [isRunning]);

  // WebSocket for live events
  useEffect(() => {
    const ws = connectEvents((event) => {
      if (event.run_id === runId || event.runId === runId) {
        refetch();
      }
    });
    wsRef.current = ws;
    return () => ws?.close();
  }, [runId, refetch]);

  // Parse task states from run data
  const taskStates = useMemo(() => {
    if (!run) return {};
    if (run.task_states) return run.task_states;
    if (run.taskStates) return run.taskStates;
    if (run.tasks) {
      const states = {};
      run.tasks.forEach((t) => {
        states[t.name || t.id] = t.status || 'pending';
      });
      return states;
    }
    return {};
  }, [run]);

  // Stats
  const stats = useMemo(() => {
    const entries = Object.entries(taskStates);
    return {
      total: entries.length,
      success: entries.filter(([, s]) => s.toLowerCase() === 'success').length,
      failed: entries.filter(([, s]) => s.toLowerCase() === 'failed').length,
      running: entries.filter(([, s]) => s.toLowerCase() === 'running').length,
    };
  }, [taskStates]);

  if (loading && !run) {
    return (
      <div className="min-h-screen bg-conduit-950 p-6 flex items-center justify-center">
        <Spinner />
      </div>
    );
  }

  if (error) {
    return (
      <div className="min-h-screen bg-conduit-950 p-6">
        <button
          onClick={() => navigate('/runs')}
          className="flex items-center gap-2 text-conduit-400 hover:text-conduit-300 mb-6"
        >
          <ArrowLeft size={16} /> Back to Runs
        </button>
        <Card>
          <div className="text-center py-8">
            <XCircle size={32} className="mx-auto text-red-400 mb-3" />
            <p className="text-sm text-red-400">{error}</p>
            <Button onClick={refetch} variant="secondary" size="sm" className="mt-4">
              Retry
            </Button>
          </div>
        </Card>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-conduit-950 p-6">
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center gap-4">
          <button
            onClick={() => navigate('/runs')}
            className="flex items-center gap-2 text-conduit-400 hover:text-conduit-300 transition-colors"
          >
            <ArrowLeft size={16} />
          </button>
          <PageHeader
            title={`Run ${(run?.run_id || run?.id || runId).substring(0, 12)}`}
            description={
              dagId ? (
                <span>
                  DAG:{' '}
                  <Link to={`/dags/${dagId}`} className="text-conduit-400 hover:text-conduit-300">
                    {dagId}
                  </Link>
                </span>
              ) : undefined
            }
          />
        </div>
        <div className="flex items-center gap-4">
          <StatusBadge status={run?.status} dot />
          <div className="text-right">
            <span className="text-lg font-mono font-bold text-conduit-200 tabular-nums tracking-tight">
              {formatDuration(run?.started_at || run?.startedAt, run?.status?.toLowerCase() === 'running' ? null : (run?.endedAt || run?.ended_at))}
            </span>
            {isRunning && (
              <span className="block text-[10px] text-blue-400 animate-pulse">elapsed</span>
            )}
          </div>
          <Button onClick={refetch} variant="secondary" size="sm">
            <RefreshCw size={14} />
          </Button>
        </div>
      </div>

      {/* Stats Row */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4 mb-6">
        <StatCard
          label="Total Tasks"
          value={stats.total}
          icon={Activity}
          sub="in this run"
        />
        <StatCard
          label="Completed"
          value={stats.success}
          icon={CheckCircle}
          sub={stats.total > 0 ? `${Math.round((stats.success / stats.total) * 100)}%` : '0%'}
        />
        <StatCard
          label="Running"
          value={stats.running}
          icon={Zap}
          sub="in progress"
        />
        <StatCard
          label="Failed"
          value={stats.failed}
          icon={AlertTriangle}
          sub={stats.failed > 0 ? 'needs attention' : 'none'}
        />
      </div>

      {/* Progress Indicator */}
      <div className="mb-6">
        <ProgressIndicator taskStates={taskStates} />
      </div>

      {/* Gantt Timeline */}
      <div className="mb-6">
        <GanttTimeline taskStates={taskStates} run={run} onSelectTask={setSelectedTask} selectedTask={selectedTask} />
      </div>

      {/* Main Content: Graph + Sidebar */}
      <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
        {/* DAG Execution Graph */}
        <div className="lg:col-span-3">
          <Card title="Execution Graph" subtitle="Click a node to inspect" icon={Activity}>
            {dagGraph ? (
              <ExecutionGraph
                dagGraph={dagGraph}
                taskStates={taskStates}
                selectedTask={selectedTask}
                onSelectTask={setSelectedTask}
              />
            ) : (
              <div className="py-8 text-center">
                <p className="text-sm text-gray-500">
                  {dagId ? 'Loading graph...' : 'No DAG graph available'}
                </p>
              </div>
            )}
          </Card>

          {/* Task Status Grid (compact) */}
          {Object.keys(taskStates).length > 0 && (
            <div className="mt-4">
              <Card title="Task Status" icon={Timer}>
                <div className="grid grid-cols-3 sm:grid-cols-4 md:grid-cols-5 lg:grid-cols-6 gap-1">
                  {Object.entries(taskStates).map(([taskId, status]) => {
                    const cfg = getStatusConfig(status);
                    const StatusIcon = cfg.Icon;
                    return (
                      <button
                        key={taskId}
                        onClick={() => setSelectedTask(taskId)}
                        className={clsx(
                          'flex items-center gap-1.5 px-2 py-1.5 rounded border transition-all text-left',
                          selectedTask === taskId
                            ? 'border-purple-500/50 bg-purple-500/10'
                            : 'border-conduit-800/30 bg-conduit-900/30 hover:bg-conduit-800/30'
                        )}
                      >
                        <StatusIcon
                          size={10}
                          className={clsx(cfg.textClass, cfg.pulse && 'animate-spin')}
                        />
                        <span className="text-[11px] text-gray-300 truncate">{taskId}</span>
                      </button>
                    );
                  })}
                </div>
              </Card>
            </div>
          )}
        </div>

        {/* Task Detail Sidebar */}
        <div className="lg:col-span-1">
          <div className="sticky top-6">
            <TaskSidebar
              taskId={selectedTask}
              taskState={taskStates[selectedTask]}
              run={run}
            />
          </div>
        </div>
      </div>

      {/* Live indicator */}
      {isRunning && (
        <div className="mt-6 flex items-center gap-2 text-xs text-gray-500">
          <span className="w-2 h-2 rounded-full bg-green-500 animate-pulse" />
          Live — auto-refreshing every 3s + WebSocket events
        </div>
      )}
    </div>
  );
}
