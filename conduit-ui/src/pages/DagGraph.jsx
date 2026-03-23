import { useState, useCallback, useMemo, useRef, useEffect } from 'react';
import { useParams, useNavigate, Link } from 'react-router-dom';
import {
  ArrowLeft,
  ZoomIn,
  ZoomOut,
  Maximize2,
  Database,
  Code,
  Terminal,
  GitBranch,
  ChevronRight,
  Shield,
  Clock,
  Cpu,
  Layers,
  AlertTriangle,
  Eye,
  X,
} from 'lucide-react';
import { getDag, getDagGraph } from '../api';
import { useApi } from '../hooks/useApi';
import Card from '../components/Card';
import Spinner from '../components/Spinner';
import PageHeader from '../components/PageHeader';
import Button from '../components/Button';
import EmptyState from '../components/EmptyState';
import clsx from 'clsx';

// ─── Helpers ─────────────────────────────────────────────────────────────────

const TASK_TYPE_CONFIG = {
  sql: { color: '#3b82f6', Icon: Database, label: 'SQL' },
  python: { color: '#10b981', Icon: Code, label: 'Python' },
  shell: { color: '#f59e0b', Icon: Terminal, label: 'Shell' },
  default: { color: '#6b7280', Icon: Cpu, label: 'Task' },
};

const getTaskConfig = (type) =>
  TASK_TYPE_CONFIG[type?.toLowerCase()] || TASK_TYPE_CONFIG.default;

// ─── Zoomable SVG Container ─────────────────────────────────────────────────

function useZoomPan(svgRef) {
  const [transform, setTransform] = useState({ x: 0, y: 0, scale: 1 });
  const isPanning = useRef(false);
  const panStart = useRef({ x: 0, y: 0 });

  const zoom = useCallback((delta) => {
    setTransform((t) => {
      const newScale = Math.max(0.3, Math.min(3, t.scale + delta));
      return { ...t, scale: newScale };
    });
  }, []);

  const resetView = useCallback(() => {
    setTransform({ x: 0, y: 0, scale: 1 });
  }, []);

  useEffect(() => {
    const svg = svgRef.current;
    if (!svg) return;

    const handleWheel = (e) => {
      e.preventDefault();
      const delta = e.deltaY > 0 ? -0.1 : 0.1;
      zoom(delta);
    };

    const handleMouseDown = (e) => {
      if (e.button === 1 || (e.button === 0 && e.shiftKey)) {
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
      svg.style.cursor = 'default';
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
  }, [svgRef, zoom, transform.x, transform.y]);

  return { transform, zoom, resetView };
}

// ─── Interactive Graph ───────────────────────────────────────────────────────

function InteractiveDagGraph({ dagGraph, dag, selectedNode, onSelectNode }) {
  const svgRef = useRef(null);
  const { transform, zoom, resetView } = useZoomPan(svgRef);

  if (!dagGraph?.nodes || dagGraph.nodes.length === 0) {
    return (
      <EmptyState
        icon={GitBranch}
        title="No graph data"
        description="This DAG has no tasks to visualize."
      />
    );
  }

  const { nodes, edges } = dagGraph;

  // Build task lookup from dag data for enrichment
  const taskLookup = useMemo(() => {
    if (!dag?.tasks) return {};
    const lookup = {};
    dag.tasks.forEach((t) => { lookup[t.name] = t; });
    return lookup;
  }, [dag]);

  // Calculate layout via topological sort
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

  const NODE_W = 200;
  const NODE_H = 80;
  const LAYER_GAP = 280;
  const VERT_GAP = 110;
  const PAD = 60;

  const layerKeys = Object.keys(layers).map(Number);
  const layerCount = layerKeys.length > 0 ? Math.max(...layerKeys) + 1 : 1;
  const layerVals = Object.values(layers).map((l) => l.length);
  const maxNodesInLayer = layerVals.length > 0 ? Math.max(...layerVals) : 1;

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
  const svgH = Math.max(400, maxNodesInLayer * VERT_GAP + 2 * PAD);

  // Find upstream/downstream of selected node
  const selectedUpstream = new Set();
  const selectedDownstream = new Set();

  if (selectedNode) {
    // Upstream
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
    // Downstream
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
    <div className="relative">
      {/* Zoom controls */}
      <div className="absolute top-3 right-3 z-10 flex items-center gap-1">
        <button
          onClick={() => zoom(0.2)}
          className="p-1.5 rounded-lg bg-conduit-900/80 border border-conduit-700/50 text-gray-400 hover:text-white transition-colors"
          title="Zoom in"
        >
          <ZoomIn size={14} />
        </button>
        <button
          onClick={() => zoom(-0.2)}
          className="p-1.5 rounded-lg bg-conduit-900/80 border border-conduit-700/50 text-gray-400 hover:text-white transition-colors"
          title="Zoom out"
        >
          <ZoomOut size={14} />
        </button>
        <button
          onClick={resetView}
          className="p-1.5 rounded-lg bg-conduit-900/80 border border-conduit-700/50 text-gray-400 hover:text-white transition-colors"
          title="Reset view"
        >
          <Maximize2 size={14} />
        </button>
      </div>

      {/* Hint */}
      <div className="absolute bottom-3 left-3 z-10 text-xs text-gray-600">
        Scroll to zoom &middot; Shift+drag to pan &middot; Click to inspect
      </div>

      <div className="overflow-hidden rounded-xl border border-conduit-800/50 bg-conduit-950/50" style={{ height: Math.min(svgH + 20, 600) }}>
        <svg
          ref={svgRef}
          width="100%"
          height="100%"
          viewBox={`0 0 ${svgW} ${svgH}`}
          style={{ cursor: 'default' }}
        >
          <g transform={`translate(${transform.x}, ${transform.y}) scale(${transform.scale})`}>
            <defs>
              <marker id="dag-arrow" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto">
                <polygon points="0 0, 8 3, 0 6" fill="#4b5563" />
              </marker>
              <marker id="dag-arrow-hl" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto">
                <polygon points="0 0, 8 3, 0 6" fill="#8b5cf6" />
              </marker>
              <filter id="node-shadow">
                <feDropShadow dx="0" dy="2" stdDeviation="4" floodColor="#000" floodOpacity="0.3" />
              </filter>
            </defs>

            {/* Grid pattern (subtle) */}
            <defs>
              <pattern id="grid" width="40" height="40" patternUnits="userSpaceOnUse">
                <path d="M 40 0 L 0 0 0 40" fill="none" stroke="#1f2937" strokeWidth="0.5" />
              </pattern>
            </defs>
            <rect width={svgW} height={svgH} fill="url(#grid)" opacity="0.3" />

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

              const hl = isEdgeHighlighted(edge);
              const dimmed = selectedNode && !hl;

              return (
                <path
                  key={`edge-${idx}`}
                  d={`M ${x1} ${y1} C ${midX} ${y1}, ${midX} ${y2}, ${x2} ${y2}`}
                  fill="none"
                  stroke={hl ? '#8b5cf6' : '#374151'}
                  strokeWidth={hl ? 2.5 : 1.5}
                  opacity={dimmed ? 0.15 : hl ? 0.9 : 0.5}
                  markerEnd={hl ? 'url(#dag-arrow-hl)' : 'url(#dag-arrow)'}
                  className="transition-all duration-200"
                />
              );
            })}

            {/* Nodes */}
            {nodes.map((node) => {
              const pos = positions[node.id];
              if (!pos) return null;

              const taskInfo = taskLookup[node.id] || taskLookup[node.name] || {};
              const cfg = getTaskConfig(taskInfo.type || node.type);
              const isSelected = selectedNode === node.id;
              const highlighted = isHighlighted(node.id);
              const dimmed = selectedNode && !highlighted;

              const x = pos.x - NODE_W / 2;
              const y = pos.y - NODE_H / 2;

              const deps = taskInfo.dependencies?.length || 0;
              const hasContracts = taskInfo.contracts > 0;

              return (
                <g
                  key={node.id}
                  onClick={(e) => {
                    e.stopPropagation();
                    onSelectNode(isSelected ? null : node.id);
                  }}
                  className="cursor-pointer"
                  opacity={dimmed ? 0.25 : 1}
                  filter={isSelected ? undefined : 'url(#node-shadow)'}
                >
                  {/* Selection glow */}
                  {isSelected && (
                    <rect
                      x={x - 4}
                      y={y - 4}
                      width={NODE_W + 8}
                      height={NODE_H + 8}
                      rx="16"
                      fill="none"
                      stroke="#8b5cf6"
                      strokeWidth="2"
                    />
                  )}

                  {/* Node bg */}
                  <rect
                    x={x}
                    y={y}
                    width={NODE_W}
                    height={NODE_H}
                    rx="12"
                    fill="#111827"
                    stroke={isSelected ? '#8b5cf6' : cfg.color}
                    strokeWidth={isSelected ? 2 : 1.5}
                  />

                  {/* Type color stripe */}
                  <rect
                    x={x}
                    y={y}
                    width="4"
                    height={NODE_H}
                    rx="2"
                    fill={cfg.color}
                  />

                  {/* Task name */}
                  <text
                    x={x + 16}
                    y={pos.y - 10}
                    fill="#e5e7eb"
                    fontSize="12"
                    fontWeight="600"
                  >
                    {node.name?.length > 22 ? node.name.substring(0, 22) + '...' : node.name}
                  </text>

                  {/* Type badge */}
                  <text
                    x={x + 16}
                    y={pos.y + 8}
                    fill={cfg.color}
                    fontSize="10"
                    fontWeight="500"
                  >
                    {cfg.label}
                  </text>

                  {/* Info badges */}
                  {deps > 0 && (
                    <text
                      x={x + NODE_W - 14}
                      y={pos.y - 10}
                      fill="#6b7280"
                      fontSize="9"
                      textAnchor="end"
                    >
                      {deps} dep{deps > 1 ? 's' : ''}
                    </text>
                  )}

                  {/* Contracts indicator */}
                  {hasContracts && (
                    <g transform={`translate(${x + NODE_W - 22}, ${pos.y + 2})`}>
                      <rect x="0" y="0" width="14" height="14" rx="3" fill="#10b981" opacity="0.2" />
                      <text x="7" y="11" fill="#10b981" fontSize="8" textAnchor="middle" fontWeight="600">
                        C
                      </text>
                    </g>
                  )}

                  {/* Layer depth indicator */}
                  <text
                    x={x + NODE_W - 14}
                    y={pos.y + 26}
                    fill="#374151"
                    fontSize="8"
                    textAnchor="end"
                  >
                    L{nodeLayerMap[node.id]}
                  </text>
                </g>
              );
            })}
          </g>
        </svg>
      </div>
    </div>
  );
}

// ─── Node Inspector Panel ────────────────────────────────────────────────────

function NodeInspector({ nodeId, dag, dagGraph, onClose }) {
  if (!nodeId) return null;

  // Find task info
  const task = dag?.tasks?.find((t) => t.name === nodeId);
  const cfg = getTaskConfig(task?.type);
  const TypeIcon = cfg.Icon;

  // Find dependencies and dependents
  const edges = dagGraph?.edges || [];
  const upstream = edges.filter((e) => e.to === nodeId).map((e) => e.from);
  const downstream = edges.filter((e) => e.from === nodeId).map((e) => e.to);

  return (
    <div className="space-y-4">
      {/* Header */}
      <Card>
        <div className="flex items-center justify-between mb-3">
          <div className="flex items-center gap-3">
            <div
              className="w-10 h-10 rounded-lg flex items-center justify-center border"
              style={{ backgroundColor: `${cfg.color}15`, borderColor: `${cfg.color}40` }}
            >
              <TypeIcon size={20} style={{ color: cfg.color }} />
            </div>
            <div>
              <h3 className="text-sm font-semibold text-white">{nodeId}</h3>
              <span
                className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium"
                style={{ backgroundColor: `${cfg.color}15`, color: cfg.color }}
              >
                {cfg.label}
              </span>
            </div>
          </div>
          <button onClick={onClose} className="p-1 rounded hover:bg-conduit-800/50 text-gray-500">
            <X size={14} />
          </button>
        </div>

        {task?.pool && (
          <div className="flex items-center gap-2 text-xs text-gray-400 mt-2">
            <Layers size={10} />
            Pool: <span className="text-gray-300">{task.pool}</span>
          </div>
        )}

        {task?.retries > 0 && (
          <div className="flex items-center gap-2 text-xs text-gray-400 mt-1">
            <Clock size={10} />
            Retries: <span className="text-gray-300">{task.retries}</span>
          </div>
        )}
      </Card>

      {/* Dependencies */}
      {upstream.length > 0 && (
        <Card title="Upstream Dependencies" icon={GitBranch}>
          <div className="space-y-1.5">
            {upstream.map((dep) => (
              <div
                key={dep}
                className="flex items-center gap-2 px-3 py-1.5 rounded-lg bg-conduit-900/30 border border-conduit-800/30"
              >
                <ChevronRight size={10} className="text-gray-600" />
                <span className="text-xs text-gray-300">{dep}</span>
              </div>
            ))}
          </div>
        </Card>
      )}

      {/* Dependents */}
      {downstream.length > 0 && (
        <Card title="Downstream Dependents" icon={GitBranch}>
          <div className="space-y-1.5">
            {downstream.map((dep) => (
              <div
                key={dep}
                className="flex items-center gap-2 px-3 py-1.5 rounded-lg bg-conduit-900/30 border border-conduit-800/30"
              >
                <ChevronRight size={10} className="text-conduit-500" />
                <span className="text-xs text-gray-300">{dep}</span>
              </div>
            ))}
          </div>
        </Card>
      )}

      {/* Contracts Summary */}
      {task?.contracts > 0 && (
        <Card title="Contracts" icon={Shield}>
          <div className="flex items-center gap-2">
            <Shield size={14} className="text-green-400" />
            <span className="text-xs text-gray-300">
              {task.contracts} contract check{task.contracts > 1 ? 's' : ''} defined
            </span>
          </div>
          <Link
            to="/contracts"
            className="mt-2 inline-flex items-center gap-1 text-xs text-conduit-400 hover:text-conduit-300"
          >
            View in Contracts Dashboard <ChevronRight size={10} />
          </Link>
        </Card>
      )}

      {/* Quick Info */}
      <Card title="Graph Position" icon={Eye}>
        <div className="grid grid-cols-2 gap-3 text-xs">
          <div>
            <p className="text-gray-500 mb-0.5">Upstream</p>
            <p className="text-gray-300 font-semibold">{upstream.length} tasks</p>
          </div>
          <div>
            <p className="text-gray-500 mb-0.5">Downstream</p>
            <p className="text-gray-300 font-semibold">{downstream.length} tasks</p>
          </div>
        </div>
      </Card>
    </div>
  );
}

// ─── Main Page ───────────────────────────────────────────────────────────────

export default function DagGraph() {
  const { dagId } = useParams();
  const navigate = useNavigate();
  const [selectedNode, setSelectedNode] = useState(null);

  const { data: dag, loading: dagLoading } = useApi(() => getDag(dagId), [dagId]);
  const { data: dagGraph, loading: graphLoading } = useApi(() => getDagGraph(dagId), [dagId]);

  const loading = dagLoading || graphLoading;

  return (
    <div className="min-h-screen bg-conduit-950 p-6">
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center gap-4">
          <button
            onClick={() => navigate(`/dags/${dagId}`)}
            className="flex items-center gap-2 text-conduit-400 hover:text-conduit-300 transition-colors"
          >
            <ArrowLeft size={16} />
          </button>
          <PageHeader
            title={`${dagId} — Graph`}
            description={dag?.description || 'Interactive dependency graph'}
          />
        </div>
        <div className="flex items-center gap-2">
          {dag?.taskCount && (
            <span className="text-xs text-gray-500">
              {dag.taskCount} tasks
            </span>
          )}
          <Button
            onClick={() => navigate(`/dags/${dagId}`)}
            variant="secondary"
            size="sm"
          >
            Back to DAG
          </Button>
        </div>
      </div>

      {loading ? (
        <div className="flex items-center justify-center py-20">
          <Spinner />
        </div>
      ) : (
        <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
          {/* Graph */}
          <div className="lg:col-span-3">
            <InteractiveDagGraph
              dagGraph={dagGraph}
              dag={dag}
              selectedNode={selectedNode}
              onSelectNode={setSelectedNode}
            />

            {/* Legend */}
            <div className="mt-4 flex flex-wrap items-center gap-4 px-2">
              {Object.entries(TASK_TYPE_CONFIG).filter(([k]) => k !== 'default').map(([key, cfg]) => {
                const Icon = cfg.Icon;
                return (
                  <div key={key} className="flex items-center gap-2">
                    <div
                      className="w-3 h-3 rounded"
                      style={{ backgroundColor: cfg.color }}
                    />
                    <Icon size={12} style={{ color: cfg.color }} />
                    <span className="text-xs text-gray-400">{cfg.label}</span>
                  </div>
                );
              })}
              <div className="flex items-center gap-2 ml-4">
                <div className="w-3 h-3 rounded bg-purple-500" />
                <span className="text-xs text-gray-400">Selected + lineage</span>
              </div>
            </div>
          </div>

          {/* Inspector Panel */}
          <div className="lg:col-span-1">
            <div className="sticky top-6">
              {selectedNode ? (
                <NodeInspector
                  nodeId={selectedNode}
                  dag={dag}
                  dagGraph={dagGraph}
                  onClose={() => setSelectedNode(null)}
                />
              ) : (
                <Card>
                  <div className="text-center py-12">
                    <Eye size={24} className="mx-auto text-gray-600 mb-3" />
                    <p className="text-sm text-gray-500">Click a node to inspect</p>
                    <p className="text-xs text-gray-600 mt-1">
                      View dependencies, type, contracts, and position
                    </p>
                  </div>
                </Card>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
