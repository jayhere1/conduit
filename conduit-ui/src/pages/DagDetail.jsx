import React, { useState, useMemo } from 'react';
import { useParams, useNavigate, Link } from 'react-router-dom';
import { ChevronLeft, Play, Activity } from 'lucide-react';
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

function DagGraphVisualization({ dagGraph }) {
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

  // Calculate layer depth for each node based on dependencies
  const nodeLayerMap = {};
  const nodeMap = {};
  nodes.forEach((node) => {
    nodeMap[node.id] = node;
    nodeLayerMap[node.id] = 0;
  });

  // Topological sort to determine layer depth
  const visited = new Set();
  const visiting = new Set();

  function calculateDepth(nodeId) {
    if (visited.has(nodeId)) return nodeLayerMap[nodeId];
    if (visiting.has(nodeId)) return 0; // Cycle detection

    visiting.add(nodeId);

    const incomingEdges = edges.filter((e) => e.to === nodeId);
    if (incomingEdges.length === 0) {
      nodeLayerMap[nodeId] = 0;
    } else {
      const maxDepth = Math.max(...incomingEdges.map((e) => calculateDepth(e.from)));
      nodeLayerMap[nodeId] = maxDepth + 1;
    }

    visiting.delete(nodeId);
    visited.add(nodeId);
    return nodeLayerMap[nodeId];
  }

  nodes.forEach((node) => calculateDepth(node.id));

  // Group nodes by layer
  const layers = {};
  nodes.forEach((node) => {
    const layer = nodeLayerMap[node.id];
    if (!layers[layer]) layers[layer] = [];
    layers[layer].push(node);
  });

  // Calculate positions
  const layerKeysArr = Object.keys(layers).map(Number);
  const layerCount = layerKeysArr.length > 0 ? Math.max(...layerKeysArr) + 1 : 1;
  const nodeHeight = 60;
  const nodeWidth = 140;
  const layerWidth = 200;
  const verticalSpacing = 100;
  const horizontalPadding = 40;
  const verticalPadding = 40;

  const canvasWidth = layerCount * layerWidth + 2 * horizontalPadding;
  const positions = {};

  Object.entries(layers).forEach(([layer, layerNodes]) => {
    const layerIndex = parseInt(layer);
    const x = horizontalPadding + layerIndex * layerWidth;
    const totalHeight = layerNodes.length * verticalSpacing;
    const startY = verticalPadding + (Math.max(300, totalHeight) - totalHeight) / 2;

    layerNodes.forEach((node, index) => {
      const y = startY + index * verticalSpacing;
      positions[node.id] = { x, y };
    });
  });

  const canvasHeight = Math.max(400, Math.max(...Object.values(positions).map((p) => p.y)) + nodeHeight + verticalPadding);

  // Determine node color by type
  const getNodeColor = (type) => {
    switch (type?.toLowerCase()) {
      case 'extract':
        return '#3b82f6'; // blue
      case 'transform':
        return '#f59e0b'; // amber
      case 'load':
        return '#10b981'; // green
      default:
        return '#6b7280'; // gray
    }
  };

  return (
    <div className="overflow-x-auto">
      <svg
        width={canvasWidth}
        height={canvasHeight}
        className="mx-auto bg-conduit-800/30 rounded-lg border border-conduit-700/50"
      >
        {/* Draw edges first */}
        {edges.map((edge, idx) => {
          const fromPos = positions[edge.from];
          const toPos = positions[edge.to];
          if (!fromPos || !toPos) return null;

          const x1 = fromPos.x + nodeWidth / 2;
          const y1 = fromPos.y + nodeHeight / 2;
          const x2 = toPos.x - nodeWidth / 2;
          const y2 = toPos.y + nodeHeight / 2;

          const midX = (x1 + x2) / 2;

          return (
            <g key={`edge-${idx}`}>
              {/* Path */}
              <path
                d={`M ${x1} ${y1} Q ${midX} ${y1} ${midX} ${(y1 + y2) / 2} Q ${midX} ${y2} ${x2} ${y2}`}
                fill="none"
                stroke="#4b5563"
                strokeWidth="2"
              />
              {/* Arrowhead */}
              <defs>
                <marker
                  id="arrowhead"
                  markerWidth="10"
                  markerHeight="10"
                  refX="9"
                  refY="3"
                  orient="auto"
                >
                  <polygon points="0 0, 10 3, 0 6" fill="#4b5563" />
                </marker>
              </defs>
              <line
                x1={x2 - 10}
                y1={y2}
                x2={x2}
                y2={y2}
                stroke="#4b5563"
                strokeWidth="2"
                markerEnd="url(#arrowhead)"
              />
            </g>
          );
        })}

        {/* Draw nodes */}
        {nodes.map((node) => {
          const pos = positions[node.id];
          if (!pos) return null;

          const bgColor = getNodeColor(node.type);
          const x = pos.x - nodeWidth / 2;
          const y = pos.y - nodeHeight / 2;

          return (
            <g key={node.id}>
              {/* Rounded rectangle background */}
              <rect
                x={x}
                y={y}
                width={nodeWidth}
                height={nodeHeight}
                rx="8"
                fill={bgColor}
                opacity="0.2"
                stroke={bgColor}
                strokeWidth="2"
              />
              {/* Node label */}
              <text
                x={pos.x}
                y={pos.y - 8}
                textAnchor="middle"
                fill="#e5e7eb"
                fontSize="12"
                fontWeight="600"
              >
                {node.name}
              </text>
              {/* Type label */}
              <text
                x={pos.x}
                y={pos.y + 12}
                textAnchor="middle"
                fill="#9ca3af"
                fontSize="10"
              >
                {node.type || 'task'}
              </text>
            </g>
          );
        })}
      </svg>

      {/* Legend */}
      <div className="mt-6 flex flex-wrap gap-6 justify-center text-sm">
        <div className="flex items-center gap-2">
          <div className="w-4 h-4 rounded bg-blue-500"></div>
          <span className="text-conduit-300">Extract</span>
        </div>
        <div className="flex items-center gap-2">
          <div className="w-4 h-4 rounded bg-amber-500"></div>
          <span className="text-conduit-300">Transform</span>
        </div>
        <div className="flex items-center gap-2">
          <div className="w-4 h-4 rounded bg-green-500"></div>
          <span className="text-conduit-300">Load</span>
        </div>
        <div className="flex items-center gap-2">
          <div className="w-4 h-4 rounded bg-gray-500"></div>
          <span className="text-conduit-300">Other</span>
        </div>
      </div>
    </div>
  );
}

function OverviewTab({ dag }) {
  return (
    <div className="space-y-6">
      {/* Metadata Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <Card>
          <p className="text-xs text-conduit-500 uppercase tracking-wide">Owner</p>
          <p className="text-lg font-semibold text-conduit-50 mt-2">{dag.owner || '—'}</p>
        </Card>
        <Card>
          <p className="text-xs text-conduit-500 uppercase tracking-wide">Schedule</p>
          <p className="text-lg font-semibold text-conduit-50 mt-2">
            {dag.schedule === '@manual' || !dag.schedule ? 'Manual' : dag.schedule}
          </p>
        </Card>
        <Card>
          <p className="text-xs text-conduit-500 uppercase tracking-wide">Task Count</p>
          <p className="text-lg font-semibold text-conduit-50 mt-2">{dag.taskCount || 0}</p>
        </Card>
        <Card>
          <p className="text-xs text-conduit-500 uppercase tracking-wide">Status</p>
          <div className="mt-2">
            <StatusBadge status={dag.status || 'unknown'} />
          </div>
        </Card>
      </div>

      {/* Description */}
      {dag.description && (
        <Card>
          <h3 className="text-sm font-semibold text-conduit-300 uppercase tracking-wide mb-3">
            Description
          </h3>
          <p className="text-conduit-200">{dag.description}</p>
        </Card>
      )}

      {/* Tags */}
      {dag.tags && dag.tags.length > 0 && (
        <Card>
          <h3 className="text-sm font-semibold text-conduit-300 uppercase tracking-wide mb-3">
            Tags
          </h3>
          <div className="flex flex-wrap gap-2">
            {dag.tags.map((tag) => (
              <span
                key={tag}
                className="px-3 py-1 text-sm bg-conduit-700/30 text-conduit-300 rounded-full border border-conduit-600/30"
              >
                {tag}
              </span>
            ))}
          </div>
        </Card>
      )}

      {/* Tasks Table */}
      <Card>
        <h3 className="text-sm font-semibold text-conduit-300 uppercase tracking-wide mb-4">
          Tasks
        </h3>
        {dag.tasks && dag.tasks.length > 0 ? (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-conduit-700/50">
                  <th className="text-left py-3 px-4 text-xs font-semibold text-conduit-400 uppercase">
                    Task
                  </th>
                  <th className="text-left py-3 px-4 text-xs font-semibold text-conduit-400 uppercase">
                    Type
                  </th>
                  <th className="text-left py-3 px-4 text-xs font-semibold text-conduit-400 uppercase">
                    Dependencies
                  </th>
                  <th className="text-left py-3 px-4 text-xs font-semibold text-conduit-400 uppercase">
                    Pool
                  </th>
                  <th className="text-left py-3 px-4 text-xs font-semibold text-conduit-400 uppercase">
                    Retries
                  </th>
                </tr>
              </thead>
              <tbody>
                {dag.tasks.map((task, idx) => (
                  <tr key={idx} className="border-b border-conduit-700/30 hover:bg-conduit-800/20">
                    <td className="py-3 px-4 text-conduit-50 font-medium">{task.name}</td>
                    <td className="py-3 px-4">
                      <span className="px-2 py-1 text-xs bg-conduit-700/40 text-conduit-300 rounded">
                        {task.type || 'unknown'}
                      </span>
                    </td>
                    <td className="py-3 px-4 text-conduit-300">
                      {task.dependencies && task.dependencies.length > 0
                        ? task.dependencies.join(', ')
                        : '—'}
                    </td>
                    <td className="py-3 px-4 text-conduit-300">{task.pool || '—'}</td>
                    <td className="py-3 px-4 text-conduit-300">{task.retries || 0}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <p className="text-conduit-400">No tasks defined</p>
        )}
      </Card>
    </div>
  );
}

function GraphTab({ dagId }) {
  const { data: dagGraph, loading } = useApi(() => getDagGraph(dagId));

  if (loading) {
    return (
      <div className="flex items-center justify-center py-12">
        <Spinner />
      </div>
    );
  }

  return (
    <div>
      <div className="flex justify-end mb-3">
        <Link
          to={`/dags/${dagId}/graph`}
          className="flex items-center gap-2 px-3 py-1.5 rounded-lg bg-conduit-600/20 border border-conduit-600/30 text-conduit-300 text-xs hover:bg-conduit-600/30 transition-colors"
        >
          Open Interactive Graph
        </Link>
      </div>
      <Card className="p-6">
        <DagGraphVisualization dagGraph={dagGraph} />
      </Card>
    </div>
  );
}

function RunsTab({ dagId }) {
  const { data: runs, loading, error } = useApi(() => listRuns(dagId), [dagId]);

  if (loading) {
    return (
      <div className="flex items-center justify-center py-12">
        <Spinner />
      </div>
    );
  }

  if (error) {
    return (
      <Card>
        <div className="p-4 bg-red-500/10 border border-red-500/30 rounded-lg text-red-400 text-sm">
          Failed to load runs: {error.message}
        </div>
      </Card>
    );
  }

  if (!runs || runs.length === 0) {
    return (
      <EmptyState
        title="No runs yet"
        description="This DAG hasn't been triggered. Use the Trigger Run button to start one."
      />
    );
  }

  return (
    <Card>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-conduit-700/50">
              <th className="text-left py-3 px-4 text-xs font-semibold text-conduit-400 uppercase">Run ID</th>
              <th className="text-left py-3 px-4 text-xs font-semibold text-conduit-400 uppercase">Status</th>
              <th className="text-left py-3 px-4 text-xs font-semibold text-conduit-400 uppercase">Triggered By</th>
              <th className="text-left py-3 px-4 text-xs font-semibold text-conduit-400 uppercase">Started</th>
              <th className="text-left py-3 px-4 text-xs font-semibold text-conduit-400 uppercase">Duration</th>
              <th className="text-left py-3 px-4 text-xs font-semibold text-conduit-400 uppercase"></th>
            </tr>
          </thead>
          <tbody>
            {runs.map((run) => (
              <tr key={run.id} className="border-b border-conduit-700/30 hover:bg-conduit-800/20 transition-colors">
                <td className="py-3 px-4">
                  <Link
                    to={`/runs/${run.id}`}
                    className="font-mono text-conduit-400 hover:text-conduit-300 transition-colors text-xs"
                  >
                    {run.id.substring(0, 8)}
                  </Link>
                </td>
                <td className="py-3 px-4">
                  <StatusBadge status={run.status} dot={true} />
                </td>
                <td className="py-3 px-4 text-conduit-300">{run.triggeredBy || '-'}</td>
                <td className="py-3 px-4 text-conduit-400 text-xs">{formatRelativeTime(run.startedAt)}</td>
                <td className="py-3 px-4 text-conduit-400 text-xs">{formatDuration(run.startedAt, run.endedAt)}</td>
                <td className="py-3 px-4">
                  <Link
                    to={`/runs/${run.id}/live`}
                    className="flex items-center gap-1 text-conduit-400 hover:text-conduit-300 transition-colors"
                    title="Live execution view"
                  >
                    <Activity size={14} />
                  </Link>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Card>
  );
}

export default function DagDetail() {
  const { dagId } = useParams();
  const navigate = useNavigate();
  const [activeTab, setActiveTab] = useState(TAB_OVERVIEW);
  const [showTriggerModal, setShowTriggerModal] = useState(false);

  const { data: dag, loading, error } = useApi(() => getDag(dagId));

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-screen">
        <Spinner />
      </div>
    );
  }

  if (error || !dag) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-conduit-950 via-conduit-900 to-conduit-950">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
          <button
            onClick={() => navigate('/dags')}
            className="inline-flex items-center gap-2 text-conduit-400 hover:text-conduit-300 mb-6"
          >
            <ChevronLeft className="w-4 h-4" />
            Back to DAGs
          </button>
          <EmptyState
            title="DAG not found"
            description="The DAG you're looking for doesn't exist or couldn't be loaded."
            icon="AlertCircle"
          />
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-conduit-950 via-conduit-900 to-conduit-950">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <button
          onClick={() => navigate('/dags')}
          className="inline-flex items-center gap-2 text-conduit-400 hover:text-conduit-300 mb-6 transition-colors"
        >
          <ChevronLeft className="w-4 h-4" />
          Back to DAGs
        </button>

        <PageHeader
          title={dag.name}
          subtitle={dag.description || 'Data pipeline definition'}
          action={
            <Button
              onClick={() => setShowTriggerModal(true)}
              className="flex items-center gap-2"
            >
              <Play className="w-4 h-4" />
              Trigger Run
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

        {/* Tab Content */}
        <div className="mt-8">
          {activeTab === TAB_OVERVIEW && <OverviewTab dag={dag} />}
          {activeTab === TAB_GRAPH && <GraphTab dagId={dagId} />}
          {activeTab === TAB_RUNS && <RunsTab dagId={dagId} />}
        </div>
      </div>

      {/* Trigger Run Modal */}
      {showTriggerModal && (
        <TriggerRunModal
          dagId={dagId}
          dagName={dag?.name}
          onClose={() => setShowTriggerModal(false)}
          onTriggered={() => {
            // Could navigate to run or refresh
          }}
        />
      )}
    </div>
  );
}
