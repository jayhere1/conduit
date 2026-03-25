import React, { useState, useMemo, useRef, useEffect } from 'react';
import { useParams, useNavigate, Link } from 'react-router-dom';
import { ChevronLeft, Play, Activity, Database, Code, Terminal, Radio, ZoomIn, ZoomOut, Maximize2, Clock, TrendingUp, CalendarClock, Hash, Shield, Timer } from 'lucide-react';
import TriggerRunModal from '../components/TriggerRunModal';
import { useApi } from '../hooks/useApi';
import { getDag, getDagGraph, listRuns, triggerRun } from '../api';
import { formatRelativeTime, formatDuration } from '../utils/time';
import Card from '../components/Card';
import StatusBadge from '../components/StatusBadge';
import Button from '../components/Button';
import Spinner from '../components/Spinner';
import PageHeader from '../components/PageHeader';
import EmptyState from '../components/EmptyState';

const TAB_OVERVIEW = 'overview';
const TAB_GRAPH = 'graph';
const TAB_RUNS = 'runs';

// Task type configuration
const TASK_TYPE_CONFIG = {
  shell: { color: '#3b82f6', Icon: Terminal, label: 'Shell' },
  python: { color: '#10b981', Icon: Code, label: 'Python' },
  sql: { color: '#a855f7', Icon: Database, label: 'SQL' },
  sensor: { color: '#f59e0b', Icon: Radio, label: 'Sensor' },
  default: { color: '#6b7280', Icon: Activity, label: 'Task' },
};

const getTaskConfig = (type) =>
  TASK_TYPE_CONFIG[type?.toLowerCase()] || TASK_TYPE_CONFIG.default;

// ─── Run History Heatmap ──────────────────────────────────────────────────

function RunHeatmap({ runs }) {
  if (!runs || runs.length === 0) return null;

  // Show last 42 slots (6 weeks) — pad empty if fewer runs
  const slots = [];
  const sorted = [...runs].sort((a, b) => new Date(b.startedAt) - new Date(a.startedAt));
  for (let i = 0; i < 42; i++) {
    slots.push(sorted[i] || null);
  }
  slots.reverse();

  const statusColor = (status) => {
    switch (status?.toLowerCase()) {
      case 'success': return 'bg-emerald-500';
      case 'failed': return 'bg-red-500';
      case 'running': return 'bg-blue-500 animate-pulse';
      case 'pending': return 'bg-amber-500/60';
      default: return 'bg-conduit-800/40';
    }
  };

  return (
    <div>
      <div className="flex items-center gap-2 mb-3">
        <CalendarClock size={13} className="text-conduit-500" />
        <span className="text-xs font-semibold text-gray-400 uppercase tracking-wide">Run History</span>
        <span className="text-xs text-gray-600 ml-auto">Last {runs.length} runs</span>
      </div>
      <div className="flex gap-[3px] flex-wrap">
        {slots.map((slot, i) => (
          <div
            key={i}
            className={`w-[14px] h-[14px] rounded-[3px] transition-all ${
              slot ? `${statusColor(slot.status)} hover:ring-1 hover:ring-white/30 cursor-pointer` : 'bg-conduit-800/20'
            }`}
            title={
              slot
                ? `${slot.status} — ${new Date(slot.startedAt).toLocaleDateString()} ${formatDuration(slot.startedAt, slot.endedAt)}`
                : 'No run'
            }
          />
        ))}
      </div>
      <div className="flex items-center gap-3 mt-2 text-[10px] text-gray-600">
        <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-sm bg-emerald-500 inline-block" /> Pass</span>
        <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-sm bg-red-500 inline-block" /> Fail</span>
        <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-sm bg-blue-500 inline-block" /> Running</span>
        <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-sm bg-conduit-800/40 inline-block" /> Empty</span>
      </div>
    </div>
  );
}

// ─── DAG Graph Visualization ──────────────────────────────────────────────

function DagGraphVisualization({ dagGraph, selectedNode, onSelectNode }) {
  const svgRef = useRef(null);
  const [transform, setTransform] = useState({ x: 0, y: 0, scale: 1 });
  const isPanning = useRef(false);
  const panStart = useRef({ x: 0, y: 0 });

  if (!dagGraph || !dagGraph.nodes || dagGraph.nodes.length === 0) {
    return (
      <EmptyState
        title="No graph data"
        description="This DAG has no tasks to visualize."
        icon="Activity"
      />
    );
  }

  const { nodes, edges } = dagGraph;

  useEffect(() => {
    const svg = svgRef.current;
    if (!svg) return;

    const handleWheel = (e) => {
      e.preventDefault();
      const delta = e.deltaY > 0 ? -0.1 : 0.1;
      setTransform((t) => ({
        ...t,
        scale: Math.max(0.5, Math.min(2.5, t.scale + delta)),
      }));
    };

    const handleMouseDown = (e) => {
      if (e.button === 0 && e.shiftKey) {
        isPanning.current = true;
        panStart.current = { x: e.clientX - transform.x, y: e.clientY - transform.y };
        svg.style.cursor = 'grabbing';
      }
    };

    const handleMouseMove = (e) => {
      if (!isPanning.current) return;
      setTransform((t) => ({
        ...t,
        x: e.clientX - panStart.current.x,
        y: e.clientY - panStart.current.y,
      }));
    };

    const handleMouseUp = () => {
      isPanning.current = false;
      if (svg) svg.style.cursor = 'default';
    };

    svg.addEventListener('wheel', handleWheel, { passive: false });
    svg.addEventListener('mousedown', handleMouseDown);
    window.addEventListener('mousemove', handleMouseMove);
    window.addEventListener('mouseup', handleMouseUp);

    return () => {
      svg.removeEventListener('wheel', handleWheel);
      svg.removeEventListener('mousedown', handleMouseDown);
      window.removeEventListener('mousemove', handleMouseMove);
      window.removeEventListener('mouseup', handleMouseUp);
    };
  }, [transform.x, transform.y]);

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

  const layers = {};
  nodes.forEach((n) => {
    const l = nodeLayerMap[n.id];
    if (!layers[l]) layers[l] = [];
    layers[l].push(n);
  });

  const NODE_W = 180;
  const NODE_H = 70;
  const LAYER_GAP = 260;
  const VERT_GAP = 110;
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
  const svgH = Math.max(450, maxNodesInLayer * VERT_GAP + 2 * PAD);

  const selectedUpstream = new Set();
  const selectedDownstream = new Set();

  if (selectedNode) {
    const queue = [selectedNode];
    while (queue.length > 0) {
      const current = queue.shift();
      edges.filter((e) => e.to === current).forEach((e) => {
        if (!selectedUpstream.has(e.from)) {
          selectedUpstream.add(e.from);
          queue.push(e.from);
        }
      });
    }
    const queue2 = [selectedNode];
    while (queue2.length > 0) {
      const current = queue2.shift();
      edges.filter((e) => e.from === current).forEach((e) => {
        if (!selectedDownstream.has(e.to)) {
          selectedDownstream.add(e.to);
          queue2.push(e.to);
        }
      });
    }
  }

  const isHighlighted = (nodeId) => {
    if (!selectedNode) return true;
    return nodeId === selectedNode || selectedUpstream.has(nodeId) || selectedDownstream.has(nodeId);
  };

  const isEdgeHighlighted = (edge) => {
    if (!selectedNode) return false;
    return (
      (selectedUpstream.has(edge.from) && (selectedUpstream.has(edge.to) || edge.to === selectedNode)) ||
      (edge.from === selectedNode && selectedDownstream.has(edge.to)) ||
      (selectedDownstream.has(edge.from) && selectedDownstream.has(edge.to))
    );
  };

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between px-3">
        <div className="text-xs text-conduit-500">
          Scroll to zoom · Shift+drag to pan · Click node to inspect
        </div>
        <div className="flex items-center gap-1">
          <button onClick={() => setTransform((t) => ({ ...t, scale: Math.min(2.5, t.scale + 0.2) }))} className="p-1.5 rounded bg-conduit-900/50 border border-conduit-700/50 text-conduit-400 hover:text-conduit-300 transition-colors" title="Zoom in"><ZoomIn size={14} /></button>
          <button onClick={() => setTransform((t) => ({ ...t, scale: Math.max(0.5, t.scale - 0.2) }))} className="p-1.5 rounded bg-conduit-900/50 border border-conduit-700/50 text-conduit-400 hover:text-conduit-300 transition-colors" title="Zoom out"><ZoomOut size={14} /></button>
          <button onClick={() => setTransform({ x: 0, y: 0, scale: 1 })} className="p-1.5 rounded bg-conduit-900/50 border border-conduit-700/50 text-conduit-400 hover:text-conduit-300 transition-colors" title="Reset view"><Maximize2 size={14} /></button>
        </div>
      </div>

      <div className="relative overflow-hidden rounded-xl border border-conduit-700/30 bg-conduit-950/40 backdrop-blur-sm" style={{ height: '500px' }}>
        <svg ref={svgRef} width="100%" height="100%" viewBox={`0 0 ${svgW} ${svgH}`} style={{ cursor: 'default' }} className="bg-gradient-to-br from-conduit-900/10 to-transparent">
          <g transform={`translate(${transform.x}, ${transform.y}) scale(${transform.scale})`}>
            <defs>
              <marker id="dag-arrow" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto"><polygon points="0 0, 8 3, 0 6" fill="#4b5563" /></marker>
              <marker id="dag-arrow-hl" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto"><polygon points="0 0, 8 3, 0 6" fill="#a855f7" /></marker>
              <filter id="node-shadow"><feDropShadow dx="0" dy="2" stdDeviation="3" floodColor="#000" floodOpacity="0.4" /></filter>
              <pattern id="grid" width="40" height="40" patternUnits="userSpaceOnUse"><path d="M 40 0 L 0 0 0 40" fill="none" stroke="#1f2937" strokeWidth="0.5" /></pattern>
            </defs>
            <rect width={svgW} height={svgH} fill="url(#grid)" opacity="0.2" />

            {edges.map((edge, idx) => {
              const from = positions[edge.from];
              const to = positions[edge.to];
              if (!from || !to) return null;
              const x1 = from.x + NODE_W / 2, y1 = from.y, x2 = to.x - NODE_W / 2, y2 = to.y;
              const midX = (x1 + x2) / 2;
              const hl = isEdgeHighlighted(edge);
              const dimmed = selectedNode && !hl;
              return (
                <path key={`edge-${idx}`} d={`M ${x1} ${y1} C ${midX} ${y1}, ${midX} ${y2}, ${x2} ${y2}`} fill="none" stroke={hl ? '#a855f7' : '#374151'} strokeWidth={hl ? 2.5 : 1.5} opacity={dimmed ? 0.15 : hl ? 0.85 : 0.45} markerEnd={hl ? 'url(#dag-arrow-hl)' : 'url(#dag-arrow)'} className="transition-all duration-200" />
              );
            })}

            {nodes.map((node) => {
              const pos = positions[node.id];
              if (!pos) return null;
              const cfg = getTaskConfig(node.type);
              const isSelected = selectedNode === node.id;
              const highlighted = isHighlighted(node.id);
              const dimmed = selectedNode && !highlighted;
              const x = pos.x - NODE_W / 2, y = pos.y - NODE_H / 2;
              const deps = edges.filter((e) => e.to === node.id).length;

              return (
                <g key={node.id} onClick={(e) => { e.stopPropagation(); onSelectNode(isSelected ? null : node.id); }} className="cursor-pointer" opacity={dimmed ? 0.25 : 1} filter={isSelected ? undefined : 'url(#node-shadow)'}>
                  {isSelected && <rect x={x - 4} y={y - 4} width={NODE_W + 8} height={NODE_H + 8} rx="14" fill="none" stroke="#a855f7" strokeWidth="2" />}
                  <rect x={x} y={y} width={NODE_W} height={NODE_H} rx="12" fill="#111827" stroke={isSelected ? '#a855f7' : cfg.color} strokeWidth={isSelected ? 2 : 1.5} />
                  <rect x={x} y={y} width="3" height={NODE_H} rx="2" fill={cfg.color} />
                  <text x={x + 14} y={pos.y - 8} fill="#e5e7eb" fontSize="12" fontWeight="600">{node.name?.length > 20 ? node.name.substring(0, 20) + '...' : node.name}</text>
                  <text x={x + 14} y={pos.y + 8} fill={cfg.color} fontSize="10" fontWeight="500">{cfg.label}</text>
                  {deps > 0 && <text x={x + NODE_W - 12} y={pos.y - 8} fill="#9ca3af" fontSize="9" textAnchor="end">{deps} dep{deps > 1 ? 's' : ''}</text>}
                  <text x={x + NODE_W - 12} y={pos.y + 8} fill="#374151" fontSize="8" textAnchor="end">L{nodeLayerMap[node.id]}</text>
                </g>
              );
            })}
          </g>
        </svg>
      </div>

      <div className="flex flex-wrap gap-4 justify-center pt-2 text-xs">
        {Object.entries(TASK_TYPE_CONFIG).filter(([k]) => k !== 'default').map(([key, cfg]) => {
          const TypeIcon = cfg.Icon;
          return (
            <div key={key} className="flex items-center gap-2">
              <div className="w-3 h-3 rounded" style={{ backgroundColor: cfg.color }} />
              <TypeIcon size={12} style={{ color: cfg.color }} />
              <span className="text-conduit-400">{cfg.label}</span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

// Helper to convert cron to human readable
function cronToHuman(cron) {
  if (!cron || cron === '@manual') return 'Manual';
  const parts = cron.trim().split(/\s+/);
  if (parts.length !== 5) return cron;
  const [minute, hour] = parts;
  if (cron === '0 * * * *') return 'Hourly';
  if (cron === '0 0 * * *') return 'Daily at midnight';
  if (cron === '0 6 * * *') return 'Daily at 6:00 AM';
  if (cron === '0 12 * * *') return 'Daily at noon';
  if (cron === '0 0 * * 1') return 'Mondays at midnight';
  if (cron === '0 0 1 * *') return 'Monthly on the 1st';
  if (cron === '0 0 * * 0') return 'Sundays at midnight';
  if (hour !== '*') return `Daily at ${parseInt(hour)}:${minute === '*' ? '00' : minute.padStart(2, '0')}`;
  return cron;
}

// ─── Overview Tab ─────────────────────────────────────────────────────────

function OverviewTab({ dag, runs }) {
  // Compute derived stats from runs
  const stats = useMemo(() => {
    if (!runs || runs.length === 0) return { successRate: null, avgDuration: null, lastSuccess: null };
    const completed = runs.filter((r) => r.status?.toLowerCase() === 'success' || r.status?.toLowerCase() === 'failed');
    const successes = completed.filter((r) => r.status?.toLowerCase() === 'success');
    const successRate = completed.length > 0 ? Math.round((successes.length / completed.length) * 100) : null;

    const durations = completed
      .filter((r) => r.startedAt && r.endedAt)
      .map((r) => (new Date(r.endedAt) - new Date(r.startedAt)) / 1000);
    const avgDuration = durations.length > 0 ? Math.round(durations.reduce((a, b) => a + b, 0) / durations.length) : null;

    const lastSuccess = successes.sort((a, b) => new Date(b.startedAt) - new Date(a.startedAt))[0];

    return { successRate, avgDuration, lastSuccess };
  }, [runs]);

  const fmtAvgDuration = (secs) => {
    if (secs === null) return '—';
    if (secs < 60) return `${secs}s`;
    const m = Math.floor(secs / 60);
    if (m < 60) return `${m}m ${secs % 60}s`;
    return `${Math.floor(m / 60)}h ${m % 60}m`;
  };

  return (
    <div className="space-y-6">
      {/* Stat Cards */}
      <div className="grid grid-cols-2 lg:grid-cols-5 gap-3">
        <Card>
          <p className="text-[10px] text-conduit-500 uppercase tracking-wider font-semibold">Schedule</p>
          <p className="text-sm font-bold text-emerald-400 mt-1.5">{cronToHuman(dag.schedule)}</p>
          {dag.schedule && dag.schedule !== '@manual' && (
            <p className="text-[10px] text-conduit-600 mt-0.5 font-mono">{dag.schedule}</p>
          )}
        </Card>
        <Card>
          <p className="text-[10px] text-conduit-500 uppercase tracking-wider font-semibold">Tasks</p>
          <p className="text-xl font-bold text-conduit-50 mt-1.5">{dag.taskCount || dag.tasks?.length || 0}</p>
        </Card>
        <Card>
          <p className="text-[10px] text-conduit-500 uppercase tracking-wider font-semibold">Success Rate</p>
          <p className={`text-xl font-bold mt-1.5 ${stats.successRate === null ? 'text-gray-600' : stats.successRate >= 90 ? 'text-emerald-400' : stats.successRate >= 70 ? 'text-amber-400' : 'text-red-400'}`}>
            {stats.successRate !== null ? `${stats.successRate}%` : '—'}
          </p>
        </Card>
        <Card>
          <p className="text-[10px] text-conduit-500 uppercase tracking-wider font-semibold">Avg Duration</p>
          <p className="text-sm font-bold text-conduit-300 mt-1.5 font-mono">{fmtAvgDuration(stats.avgDuration)}</p>
        </Card>
        <Card>
          <p className="text-[10px] text-conduit-500 uppercase tracking-wider font-semibold">Last Success</p>
          <p className="text-sm font-bold text-conduit-300 mt-1.5">
            {stats.lastSuccess ? formatRelativeTime(stats.lastSuccess.startedAt) : '—'}
          </p>
        </Card>
      </div>

      {/* Heatmap */}
      {runs && runs.length > 0 && (
        <Card>
          <RunHeatmap runs={runs} />
        </Card>
      )}

      {/* Description & Tags */}
      {(dag.description || (dag.tags && dag.tags.length > 0)) && (
        <Card>
          {dag.description && (
            <div className="mb-4">
              <h3 className="text-[10px] font-semibold text-conduit-500 uppercase tracking-wider mb-2">Description</h3>
              <p className="text-sm text-conduit-200 leading-relaxed">{dag.description}</p>
            </div>
          )}
          {dag.tags && dag.tags.length > 0 && (
            <div>
              <h3 className="text-[10px] font-semibold text-conduit-500 uppercase tracking-wider mb-2">Tags</h3>
              <div className="flex flex-wrap gap-1.5">
                {dag.tags.map((tag) => (
                  <span key={tag} className="px-2.5 py-0.5 text-xs bg-conduit-700/30 text-conduit-300 rounded-full border border-conduit-600/30">{tag}</span>
                ))}
              </div>
            </div>
          )}
        </Card>
      )}

      {/* Tasks Table */}
      <Card>
        <div className="flex items-center gap-2 mb-4">
          <Hash size={13} className="text-conduit-500" />
          <h3 className="text-xs font-semibold text-gray-400 uppercase tracking-wide">Tasks</h3>
          <span className="text-xs text-gray-600">{dag.tasks?.length || 0}</span>
        </div>
        {dag.tasks && dag.tasks.length > 0 ? (
          <div className="overflow-x-auto -mx-5 px-5">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-conduit-700/50">
                  <th className="text-left py-2.5 pr-4 text-[10px] font-semibold text-conduit-500 uppercase tracking-wider">Task</th>
                  <th className="text-left py-2.5 pr-4 text-[10px] font-semibold text-conduit-500 uppercase tracking-wider">Type</th>
                  <th className="text-left py-2.5 pr-4 text-[10px] font-semibold text-conduit-500 uppercase tracking-wider">Dependencies</th>
                  <th className="text-left py-2.5 pr-4 text-[10px] font-semibold text-conduit-500 uppercase tracking-wider">Pool</th>
                  <th className="text-left py-2.5 pr-4 text-[10px] font-semibold text-conduit-500 uppercase tracking-wider">Timeout</th>
                  <th className="text-left py-2.5 pr-4 text-[10px] font-semibold text-conduit-500 uppercase tracking-wider">Trigger</th>
                  <th className="text-right py-2.5 text-[10px] font-semibold text-conduit-500 uppercase tracking-wider">Retries</th>
                </tr>
              </thead>
              <tbody>
                {dag.tasks.map((task, idx) => {
                  const cfg = getTaskConfig(task.type);
                  const TypeIcon = cfg.Icon;
                  return (
                    <tr key={idx} className={`border-b border-conduit-700/20 hover:bg-conduit-800/20 transition-colors ${idx % 2 === 1 ? 'bg-conduit-900/20' : ''}`}>
                      <td className="py-2.5 pr-4 text-conduit-50 font-medium text-xs">{task.name}</td>
                      <td className="py-2.5 pr-4">
                        <span className="inline-flex items-center gap-1.5 px-2 py-0.5 text-[11px] rounded border" style={{ color: cfg.color, borderColor: `${cfg.color}33`, backgroundColor: `${cfg.color}11` }}>
                          <TypeIcon size={11} />
                          {cfg.label}
                        </span>
                      </td>
                      <td className="py-2.5 pr-4 text-conduit-400 text-xs font-mono">
                        {task.dependencies && task.dependencies.length > 0 ? task.dependencies.join(', ') : '—'}
                      </td>
                      <td className="py-2.5 pr-4 text-conduit-400 text-xs">{task.pool || '—'}</td>
                      <td className="py-2.5 pr-4 text-conduit-400 text-xs font-mono">{task.timeout || '—'}</td>
                      <td className="py-2.5 pr-4 text-conduit-400 text-xs">
                        {task.triggerRule && task.triggerRule !== 'AllSuccess' ? (
                          <span className="px-1.5 py-0.5 text-[10px] rounded bg-amber-500/10 text-amber-400 border border-amber-500/20">{task.triggerRule}</span>
                        ) : (
                          <span className="text-conduit-600">default</span>
                        )}
                      </td>
                      <td className="py-2.5 text-conduit-400 text-xs text-right font-mono">{task.retries || 0}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        ) : (
          <p className="text-conduit-500 text-sm">No tasks defined</p>
        )}
      </Card>
    </div>
  );
}

function GraphTab({ dagId }) {
  const { data: dagGraph, loading } = useApi(() => getDagGraph(dagId));
  const [selectedNode, setSelectedNode] = useState(null);

  if (loading) return <div className="flex items-center justify-center py-12"><Spinner /></div>;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h3 className="text-sm font-semibold text-conduit-300">Task Dependency Graph</h3>
        <Link to={`/dags/${dagId}/graph`} className="flex items-center gap-2 px-3 py-1.5 rounded-lg bg-conduit-600/20 border border-conduit-600/30 text-conduit-300 text-xs hover:bg-conduit-600/30 transition-colors">Open Full View</Link>
      </div>
      <Card className="p-6">
        <DagGraphVisualization dagGraph={dagGraph} selectedNode={selectedNode} onSelectNode={setSelectedNode} />
      </Card>
    </div>
  );
}

function RunsTab({ dagId, runs: initialRuns }) {
  const { data: runs, loading, error } = useApi(() => listRuns(dagId), [dagId]);

  if (loading) return <div className="flex items-center justify-center py-12"><Spinner /></div>;
  if (error) return <Card><div className="p-4 bg-red-500/10 border border-red-500/30 rounded-lg text-red-400 text-sm">Failed to load runs: {error.message}</div></Card>;
  if (!runs || runs.length === 0) return <EmptyState title="No runs yet" description="This DAG hasn't been triggered. Use the Trigger Run button to start one." />;

  const maxDuration = Math.max(...runs.map((r) => {
    if (!r.startedAt) return 0;
    const start = new Date(r.startedAt);
    const end = r.endedAt ? new Date(r.endedAt) : new Date();
    return (end - start) / 1000;
  }));

  return (
    <div className="space-y-4">
      <Card title="Recent Runs Timeline" subtitle="Visual execution duration">
        <div className="space-y-2.5">
          {runs.slice(0, 12).map((run) => {
            const duration = run.startedAt ? Math.floor(((run.endedAt ? new Date(run.endedAt) : new Date()) - new Date(run.startedAt)) / 1000) : 0;
            const durationPct = maxDuration > 0 ? (duration / maxDuration) * 100 : 0;
            const statusColor = { success: 'bg-emerald-500/60', failed: 'bg-red-500/60', running: 'bg-blue-500/60 animate-pulse', pending: 'bg-amber-500/40' }[run.status?.toLowerCase()] || 'bg-gray-500/40';

            return (
              <div key={run.id} className="space-y-1">
                <div className="flex items-center justify-between text-xs">
                  <Link to={`/runs/${run.id}`} className="font-mono text-conduit-400 hover:text-conduit-300 transition-colors">{run.id.substring(0, 12)}</Link>
                  <div className="flex items-center gap-3">
                    <StatusBadge status={run.status} dot={true} />
                    <span className="text-conduit-500 font-mono w-14 text-right">{formatDuration(run.startedAt, run.endedAt)}</span>
                  </div>
                </div>
                <div className="flex-1 h-5 bg-conduit-900/50 rounded border border-conduit-700/30 overflow-hidden">
                  <div className={`h-full ${statusColor} rounded transition-all`} style={{ width: `${Math.max(durationPct, 2)}%` }} />
                </div>
              </div>
            );
          })}
        </div>
      </Card>

      <Card title="Run Details" subtitle={`${runs.length} total run${runs.length !== 1 ? 's' : ''}`}>
        <div className="overflow-x-auto -mx-5 px-5">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-conduit-700/50">
                <th className="text-left py-2.5 pr-4 text-[10px] font-semibold text-conduit-500 uppercase tracking-wider">Run ID</th>
                <th className="text-left py-2.5 pr-4 text-[10px] font-semibold text-conduit-500 uppercase tracking-wider">Status</th>
                <th className="text-left py-2.5 pr-4 text-[10px] font-semibold text-conduit-500 uppercase tracking-wider">Triggered By</th>
                <th className="text-left py-2.5 pr-4 text-[10px] font-semibold text-conduit-500 uppercase tracking-wider">Started</th>
                <th className="text-left py-2.5 pr-4 text-[10px] font-semibold text-conduit-500 uppercase tracking-wider">Duration</th>
                <th className="text-left py-2.5 text-[10px] font-semibold text-conduit-500 uppercase tracking-wider"></th>
              </tr>
            </thead>
            <tbody>
              {runs.map((run, idx) => (
                <tr key={run.id} className={`border-b border-conduit-700/20 hover:bg-conduit-800/20 transition-colors ${idx % 2 === 1 ? 'bg-conduit-900/20' : ''}`}>
                  <td className="py-2.5 pr-4"><Link to={`/runs/${run.id}`} className="font-mono text-conduit-400 hover:text-conduit-300 transition-colors text-xs">{run.id.substring(0, 8)}</Link></td>
                  <td className="py-2.5 pr-4"><StatusBadge status={run.status} dot={true} /></td>
                  <td className="py-2.5 pr-4 text-conduit-400 text-xs">{run.triggeredBy || '-'}</td>
                  <td className="py-2.5 pr-4 text-conduit-400 text-xs">{formatRelativeTime(run.startedAt)}</td>
                  <td className="py-2.5 pr-4 text-conduit-400 text-xs font-mono">{formatDuration(run.startedAt, run.endedAt)}</td>
                  <td className="py-2.5"><Link to={`/runs/${run.id}/live`} className="flex items-center gap-1 text-conduit-400 hover:text-conduit-300 transition-colors" title="Live execution view"><Activity size={14} /></Link></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Card>
    </div>
  );
}

// ─── Main Page ───────────────────────────────────────────────────────────

export default function DagDetail() {
  const { dagId } = useParams();
  const navigate = useNavigate();
  const [activeTab, setActiveTab] = useState(TAB_OVERVIEW);
  const [showTriggerModal, setShowTriggerModal] = useState(false);

  const { data: dag, loading, error } = useApi(() => getDag(dagId));
  const { data: runs } = useApi(() => listRuns(dagId), [dagId]);

  if (loading) return <div className="flex items-center justify-center min-h-screen"><Spinner /></div>;

  if (error || !dag) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-conduit-950 via-conduit-900 to-conduit-950">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
          <button onClick={() => navigate('/dags')} className="inline-flex items-center gap-2 text-conduit-400 hover:text-conduit-300 mb-6"><ChevronLeft className="w-4 h-4" />Back to DAGs</button>
          <EmptyState title="DAG not found" description="The DAG you're looking for doesn't exist or couldn't be loaded." icon="AlertCircle" />
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-conduit-950 via-conduit-900 to-conduit-950">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <button onClick={() => navigate('/dags')} className="inline-flex items-center gap-2 text-conduit-400 hover:text-conduit-300 mb-6 transition-colors"><ChevronLeft className="w-4 h-4" />Back to DAGs</button>

        <PageHeader
          title={dag.name}
          subtitle={dag.description || 'Data pipeline definition'}
          action={
            <Button onClick={() => setShowTriggerModal(true)} className="flex items-center gap-2">
              <Play className="w-4 h-4" />Trigger Run
            </Button>
          }
        />

        {/* Tab Navigation */}
        <div className="mt-8 border-b border-conduit-700/50">
          <div className="flex gap-8">
            {[
              { id: TAB_OVERVIEW, label: 'Overview' },
              { id: TAB_GRAPH, label: 'Graph' },
              { id: TAB_RUNS, label: 'Runs' },
            ].map((tab) => (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                className={`py-4 px-1 text-sm font-medium border-b-2 transition-colors ${
                  activeTab === tab.id
                    ? 'text-conduit-50 border-conduit-500'
                    : 'text-conduit-400 border-transparent hover:text-conduit-300'
                }`}
              >
                {tab.label}
              </button>
            ))}
          </div>
        </div>

        <div className="mt-8">
          {activeTab === TAB_OVERVIEW && <OverviewTab dag={dag} runs={runs} />}
          {activeTab === TAB_GRAPH && <GraphTab dagId={dagId} />}
          {activeTab === TAB_RUNS && <RunsTab dagId={dagId} runs={runs} />}
        </div>
      </div>

      {showTriggerModal && (
        <TriggerRunModal
          dagId={dagId}
          dagName={dag?.name}
          onClose={() => setShowTriggerModal(false)}
          onTriggered={() => {}}
        />
      )}
    </div>
  );
}
