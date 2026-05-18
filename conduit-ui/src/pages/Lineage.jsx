import React, { useState, useCallback, useMemo, useRef, useEffect } from 'react';
import {
  extractSqlLineage,
  traceUpstream,
  traceDownstream,
  lineageGraph,
  schemaDiff,
  validateContract,
  getUnifiedDatasetView,
  getOpenLineageStats,
  getPlanCacheStats,
  invalidatePlanCache,
} from '../api';
import Card from '../components/Card';
import StatusBadge from '../components/StatusBadge';
import Button from '../components/Button';
import Spinner from '../components/Spinner';
import PageHeader from '../components/PageHeader';
import {
  Network,
  Search,
  ArrowUp,
  ArrowDown,
  Code,
  GitCompare,
  Shield,
  Columns,
  Database,
  Table2,
} from 'lucide-react';

export default function Lineage() {
  const [tabIndex, setTabIndex] = useState(0);

  // SQL Lineage Tab State
  const [sqlInput, setSqlInput] = useState('');
  const [sqlSourceTaskId, setSqlSourceTaskId] = useState('');
  const [sqlLineageResult, setSqlLineageResult] = useState(null);
  const [sqlLoading, setSqlLoading] = useState(false);

  // Column Trace Tab State
  const [traceTaskId, setTraceTaskId] = useState('');
  const [traceColumnName, setTraceColumnName] = useState('');
  const [traceDirection, setTraceDirection] = useState('upstream');
  const [traceEdges, setTraceEdges] = useState([]);
  const [traceEdgeForm, setTraceEdgeForm] = useState({
    from_task: '',
    from_column: '',
    to_task: '',
    to_column: '',
    transform: 'pass_through',
  });
  const [traceResult, setTraceResult] = useState(null);
  const [traceLoading, setTraceLoading] = useState(false);

  // Schema Diff Tab State
  const [diffSchemaA, setDiffSchemaA] = useState({
    task_id: '',
    columns: [],
  });
  const [diffSchemaB, setDiffSchemaB] = useState({
    task_id: '',
    columns: [],
  });
  const [diffResult, setDiffResult] = useState(null);
  const [diffLoading, setDiffLoading] = useState(false);

  // Contracts Tab State
  const [contractSchema, setContractSchema] = useState({
    task_id: '',
    columns: [],
  });
  const [contractRequiredColumns, setContractRequiredColumns] = useState([]);
  const [contractForbiddenColumns, setContractForbiddenColumns] = useState([]);
  const [contractMaxColumns, setContractMaxColumns] = useState('');
  const [contractRequireDocs, setContractRequireDocs] = useState(false);
  const [contractNoUnknownTypes, setContractNoUnknownTypes] = useState(false);
  const [contractResult, setContractResult] = useState(null);
  const [contractLoading, setContractLoading] = useState(false);

  // Datasets Tab State
  const [datasetNamespace, setDatasetNamespace] = useState('');
  const [datasetName, setDatasetName] = useState('');
  const [datasetView, setDatasetView] = useState(null);
  const [datasetLoading, setDatasetLoading] = useState(false);
  const [datasetError, setDatasetError] = useState(null);
  const [datasetStats, setDatasetStats] = useState(null);
  const [planCacheStats, setPlanCacheStats] = useState(null);
  const [invalidating, setInvalidating] = useState(false);

  const refreshCacheStats = useCallback(() => {
    getPlanCacheStats().then(setPlanCacheStats).catch(() => {});
  }, []);

  const handleInvalidateCache = useCallback(async () => {
    setInvalidating(true);
    try {
      await invalidatePlanCache();
      refreshCacheStats();
    } catch (err) {
      // Non-fatal; surface in console for now.
      console.error('Cache invalidate failed:', err);
    } finally {
      setInvalidating(false);
    }
  }, [refreshCacheStats]);

  const handleLoadDatasetView = useCallback(async () => {
    if (!datasetNamespace.trim() || !datasetName.trim()) {
      setDatasetError('Both namespace and name are required.');
      return;
    }
    setDatasetLoading(true);
    setDatasetError(null);
    setDatasetView(null);
    try {
      const view = await getUnifiedDatasetView(
        datasetNamespace.trim(),
        datasetName.trim()
      );
      setDatasetView(view);
      // Refresh cache stats after the query so users can see hit-rate.
      refreshCacheStats();
    } catch (err) {
      setDatasetError(err.message);
    } finally {
      setDatasetLoading(false);
    }
  }, [datasetNamespace, datasetName, refreshCacheStats]);

  // Load store stats + cache stats when the Datasets tab is opened.
  useEffect(() => {
    if (tabIndex !== 4) return;
    getOpenLineageStats().then(setDatasetStats).catch(() => {});
    refreshCacheStats();
  }, [tabIndex, refreshCacheStats]);

  // SQL Lineage Handlers
  const handleExtractLineage = useCallback(async () => {
    if (!sqlInput.trim()) return;
    setSqlLoading(true);
    try {
      const result = await extractSqlLineage({
        sql: sqlInput,
        source_task_id: sqlSourceTaskId || undefined,
      });
      setSqlLineageResult(result);
    } catch (error) {
      setSqlLineageResult({ error: error.message });
    } finally {
      setSqlLoading(false);
    }
  }, [sqlInput, sqlSourceTaskId]);

  // Column Trace Handlers
  const handleAddTraceEdge = useCallback(() => {
    if (
      !traceEdgeForm.from_task.trim() ||
      !traceEdgeForm.from_column.trim() ||
      !traceEdgeForm.to_task.trim() ||
      !traceEdgeForm.to_column.trim()
    ) {
      return;
    }
    setTraceEdges([...traceEdges, { ...traceEdgeForm }]);
    setTraceEdgeForm({
      from_task: '',
      from_column: '',
      to_task: '',
      to_column: '',
      transform: 'pass_through',
    });
  }, [traceEdgeForm, traceEdges]);

  const handleRemoveTraceEdge = useCallback((index) => {
    setTraceEdges(traceEdges.filter((_, i) => i !== index));
  }, [traceEdges]);

  const handleTrace = useCallback(async () => {
    if (!traceTaskId.trim() || !traceColumnName.trim()) return;
    setTraceLoading(true);
    try {
      const traceFn = traceDirection === 'upstream' ? traceUpstream : traceDownstream;
      const result = await traceFn({
        task_id: traceTaskId,
        column_name: traceColumnName,
        edges: traceEdges,
      });
      setTraceResult(result);
    } catch (error) {
      setTraceResult({ error: error.message });
    } finally {
      setTraceLoading(false);
    }
  }, [traceTaskId, traceColumnName, traceDirection, traceEdges]);

  // Schema Diff Handlers
  const handleAddDiffColumnA = useCallback(() => {
    setDiffSchemaA({
      ...diffSchemaA,
      columns: [
        ...diffSchemaA.columns,
        { name: '', type: 'string', nullable: false, description: '' },
      ],
    });
  }, [diffSchemaA]);

  const handleAddDiffColumnB = useCallback(() => {
    setDiffSchemaB({
      ...diffSchemaB,
      columns: [
        ...diffSchemaB.columns,
        { name: '', type: 'string', nullable: false, description: '' },
      ],
    });
  }, [diffSchemaB]);

  const handleUpdateDiffColumnA = useCallback((index, field, value) => {
    const updated = [...diffSchemaA.columns];
    updated[index] = { ...updated[index], [field]: value };
    setDiffSchemaA({ ...diffSchemaA, columns: updated });
  }, [diffSchemaA]);

  const handleUpdateDiffColumnB = useCallback((index, field, value) => {
    const updated = [...diffSchemaB.columns];
    updated[index] = { ...updated[index], [field]: value };
    setDiffSchemaB({ ...diffSchemaB, columns: updated });
  }, [diffSchemaB]);

  const handleRemoveDiffColumnA = useCallback((index) => {
    setDiffSchemaA({
      ...diffSchemaA,
      columns: diffSchemaA.columns.filter((_, i) => i !== index),
    });
  }, [diffSchemaA]);

  const handleRemoveDiffColumnB = useCallback((index) => {
    setDiffSchemaB({
      ...diffSchemaB,
      columns: diffSchemaB.columns.filter((_, i) => i !== index),
    });
  }, [diffSchemaB]);

  const handleCompareDiff = useCallback(async () => {
    if (!diffSchemaA.task_id.trim() || !diffSchemaB.task_id.trim()) return;
    setDiffLoading(true);
    try {
      const result = await schemaDiff({
        schema_a: diffSchemaA,
        schema_b: diffSchemaB,
      });
      setDiffResult(result);
    } catch (error) {
      setDiffResult({ error: error.message });
    } finally {
      setDiffLoading(false);
    }
  }, [diffSchemaA, diffSchemaB]);

  // Contract Handlers
  const handleAddContractColumn = useCallback(() => {
    setContractSchema({
      ...contractSchema,
      columns: [
        ...contractSchema.columns,
        { name: '', type: 'string', nullable: false, description: '' },
      ],
    });
  }, [contractSchema]);

  const handleUpdateContractColumn = useCallback((index, field, value) => {
    const updated = [...contractSchema.columns];
    updated[index] = { ...updated[index], [field]: value };
    setContractSchema({ ...contractSchema, columns: updated });
  }, [contractSchema]);

  const handleRemoveContractColumn = useCallback((index) => {
    setContractSchema({
      ...contractSchema,
      columns: contractSchema.columns.filter((_, i) => i !== index),
    });
  }, [contractSchema]);

  const handleAddRequiredColumn = useCallback(() => {
    setContractRequiredColumns([
      ...contractRequiredColumns,
      { name: '', type: '' },
    ]);
  }, [contractRequiredColumns]);

  const handleUpdateRequiredColumn = useCallback((index, field, value) => {
    const updated = [...contractRequiredColumns];
    updated[index] = { ...updated[index], [field]: value };
    setContractRequiredColumns(updated);
  }, [contractRequiredColumns]);

  const handleRemoveRequiredColumn = useCallback((index) => {
    setContractRequiredColumns(
      contractRequiredColumns.filter((_, i) => i !== index)
    );
  }, [contractRequiredColumns]);

  const handleAddForbiddenColumn = useCallback(() => {
    setContractForbiddenColumns([...contractForbiddenColumns, '']);
  }, [contractForbiddenColumns]);

  const handleUpdateForbiddenColumn = useCallback((index, value) => {
    const updated = [...contractForbiddenColumns];
    updated[index] = value;
    setContractForbiddenColumns(updated);
  }, [contractForbiddenColumns]);

  const handleRemoveForbiddenColumn = useCallback((index) => {
    setContractForbiddenColumns(
      contractForbiddenColumns.filter((_, i) => i !== index)
    );
  }, [contractForbiddenColumns]);

  const handleValidateContract = useCallback(async () => {
    if (!contractSchema.task_id.trim()) return;
    setContractLoading(true);
    try {
      const result = await validateContract({
        task_id: contractSchema.task_id,
        schema: contractSchema,
        required_columns: contractRequiredColumns,
        forbidden_columns: contractForbiddenColumns,
        max_columns: contractMaxColumns ? parseInt(contractMaxColumns) : undefined,
        require_docs: contractRequireDocs,
        no_unknown_types: contractNoUnknownTypes,
      });
      setContractResult(result);
    } catch (error) {
      setContractResult({ error: error.message });
    } finally {
      setContractLoading(false);
    }
  }, [
    contractSchema,
    contractRequiredColumns,
    contractForbiddenColumns,
    contractMaxColumns,
    contractRequireDocs,
    contractNoUnknownTypes,
  ]);

  // Trace visualization component
  const TraceVisualization = ({ result }) => {
    if (!result || !result.traced_columns) return null;

    const maxDepth = Math.max(...result.traced_columns.map((c) => c.depth || 0));
    const nodeRadius = 20;
    const levelHeight = 80;
    const width = Math.max(400, (maxDepth + 1) * 120);
    const height = Math.max(300, (result.traced_columns.length || 1) * levelHeight);

    return (
      <div className="mt-4 p-4 bg-conduit-dark/30 rounded-lg border border-conduit-accent/20 overflow-x-auto">
        <svg width={width} height={height} className="mx-auto">
          {/* Draw edges */}
          {result.edges &&
            result.edges.map((edge, idx) => {
              const fromIdx = result.traced_columns.findIndex(
                (c) => c.task_id === edge.from_task && c.column_name === edge.from_column
              );
              const toIdx = result.traced_columns.findIndex(
                (c) => c.task_id === edge.to_task && c.column_name === edge.to_column
              );
              if (fromIdx === -1 || toIdx === -1) return null;

              const fromDepth = result.traced_columns[fromIdx].depth || 0;
              const toDepth = result.traced_columns[toIdx].depth || 0;
              const x1 = 60 + fromDepth * 120;
              const y1 = 50 + fromIdx * levelHeight;
              const x2 = 60 + toDepth * 120;
              const y2 = 50 + toIdx * levelHeight;

              return (
                <line
                  key={idx}
                  x1={x1}
                  y1={y1}
                  x2={x2}
                  y2={y2}
                  stroke="#6366f1"
                  strokeWidth="2"
                  opacity="0.6"
                />
              );
            })}

          {/* Draw nodes */}
          {result.traced_columns.map((col, idx) => {
            const depth = col.depth || 0;
            const x = 60 + depth * 120;
            const y = 50 + idx * levelHeight;
            const isTarget = col.task_id === traceTaskId && col.column_name === traceColumnName;

            return (
              <g key={idx}>
                <circle
                  cx={x}
                  cy={y}
                  r={nodeRadius}
                  fill={isTarget ? '#3b82f6' : '#6366f1'}
                  opacity={isTarget ? 1 : 0.7}
                />
                <text
                  x={x}
                  y={y}
                  textAnchor="middle"
                  dy="0.3em"
                  className="text-xs font-mono fill-white"
                >
                  {depth}
                </text>
              </g>
            );
          })}
        </svg>
        <div className="mt-4 text-sm text-conduit-light/70">
          <p className="font-semibold text-conduit-light mb-2">Traced Columns:</p>
          <div className="space-y-1">
            {result.traced_columns.map((col, idx) => (
              <div
                key={idx}
                className={`px-3 py-1 rounded ${
                  col.task_id === traceTaskId && col.column_name === traceColumnName
                    ? 'bg-blue-500/30 text-blue-200'
                    : 'bg-conduit-accent/20 text-conduit-light'
                }`}
              >
                <span className="font-mono">
                  {col.task_id}.{col.column_name}
                </span>
                <span className="text-xs opacity-70 ml-2">(depth: {col.depth})</span>
              </div>
            ))}
          </div>
        </div>
      </div>
    );
  };

  const tabs = [
    { label: 'SQL Lineage', icon: Code },
    { label: 'Column Trace', icon: Network },
    { label: 'Schema Diff', icon: GitCompare },
    { label: 'Contracts', icon: Shield },
    { label: 'Datasets', icon: Database },
  ];

  return (
    <div className="space-y-6">
      <PageHeader
        title="Data Lineage"
        description="Trace data flow, extract SQL lineage, compare schemas, and validate contracts"
        icon={Network}
      />

      {/* Tab Navigation */}
      <div className="glass rounded-lg p-1 flex gap-1 w-fit">
        {tabs.map((tab, idx) => {
          const Icon = tab.icon;
          return (
            <button
              key={idx}
              onClick={() => setTabIndex(idx)}
              className={`flex items-center gap-2 px-4 py-2 rounded-md transition ${
                tabIndex === idx
                  ? 'bg-conduit-accent text-conduit-dark font-semibold'
                  : 'text-conduit-light hover:bg-conduit-accent/20'
              }`}
            >
              <Icon size={16} />
              {tab.label}
            </button>
          );
        })}
      </div>

      {/* Tab 1: SQL Lineage */}
      {tabIndex === 0 && (
        <div className="space-y-4">
          <Card className="space-y-4">
            <div className="flex items-center gap-2 mb-4">
              <Code size={20} className="text-conduit-accent" />
              <h3 className="text-lg font-semibold text-conduit-light">SQL Input</h3>
            </div>

            <div>
              <label className="block text-sm font-medium text-conduit-light mb-2">
                SQL Query
              </label>
              <textarea
                value={sqlInput}
                onChange={(e) => setSqlInput(e.target.value)}
                placeholder="SELECT col1, col2 FROM table1..."
                className="w-full h-40 px-3 py-2 bg-conduit-dark border border-conduit-accent/30 rounded-md text-conduit-light font-mono text-sm placeholder-conduit-light/40 focus:outline-none focus:border-conduit-accent"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-conduit-light mb-2">
                Source Task ID (Optional)
              </label>
              <input
                type="text"
                value={sqlSourceTaskId}
                onChange={(e) => setSqlSourceTaskId(e.target.value)}
                placeholder="task_id"
                className="w-full px-3 py-2 bg-conduit-dark border border-conduit-accent/30 rounded-md text-conduit-light placeholder-conduit-light/40 focus:outline-none focus:border-conduit-accent"
              />
            </div>

            <Button
              onClick={handleExtractLineage}
              disabled={sqlLoading || !sqlInput.trim()}
              className="w-full"
            >
              {sqlLoading ? (
                <>
                  <Spinner size={16} className="mr-2" />
                  Extracting...
                </>
              ) : (
                <>
                  <Code size={16} className="mr-2" />
                  Extract Lineage
                </>
              )}
            </Button>
          </Card>

          {sqlLineageResult && (
            <Card className="space-y-4">
              {sqlLineageResult.error ? (
                <div className="p-4 bg-red-500/20 border border-red-500/50 rounded text-red-200 font-mono text-sm">
                  {sqlLineageResult.error}
                </div>
              ) : (
                <>
                  {sqlLineageResult.output_columns && (
                    <div>
                      <h4 className="text-md font-semibold text-conduit-light mb-3">
                        Output Columns
                      </h4>
                      <div className="space-y-2">
                        {sqlLineageResult.output_columns.map((col, idx) => (
                          <div
                            key={idx}
                            className="p-3 bg-conduit-accent/10 border border-conduit-accent/20 rounded-md"
                          >
                            <div className="flex items-start justify-between">
                              <div className="flex-1">
                                <p className="font-mono text-conduit-light font-semibold">
                                  {col.name}
                                </p>
                                <p className="text-xs text-conduit-light/60 mt-1">
                                  {col.expression}
                                </p>
                              </div>
                              {col.computed && (
                                <StatusBadge status="success" label="Computed" size="sm" />
                              )}
                            </div>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}

                  {sqlLineageResult.source_tables && (
                    <div>
                      <h4 className="text-md font-semibold text-conduit-light mb-3">
                        Source Tables
                      </h4>
                      <div className="flex flex-wrap gap-2">
                        {sqlLineageResult.source_tables.map((table, idx) => (
                          <div
                            key={idx}
                            className="px-3 py-1 bg-conduit-dark border border-conduit-accent/30 rounded-full text-conduit-light text-sm font-mono"
                          >
                            <Database size={14} className="inline mr-1" />
                            {table}
                          </div>
                        ))}
                      </div>
                    </div>
                  )}

                  {sqlLineageResult.column_mappings && (
                    <div>
                      <h4 className="text-md font-semibold text-conduit-light mb-3">
                        Column Mappings
                      </h4>
                      <div className="space-y-2">
                        {Object.entries(sqlLineageResult.column_mappings).map(
                          ([output, inputs], idx) => (
                            <div
                              key={idx}
                              className="p-3 bg-conduit-accent/10 border border-conduit-accent/20 rounded-md text-sm"
                            >
                              <p className="text-conduit-light mb-2">
                                <span className="font-mono font-semibold">{output}</span>
                                <ArrowDown size={14} className="inline mx-2 opacity-60" />
                              </p>
                              <div className="ml-4 space-y-1">
                                {Array.isArray(inputs) ? (
                                  inputs.map((input, i) => (
                                    <p key={i} className="text-conduit-light/70 font-mono">
                                      {input}
                                    </p>
                                  ))
                                ) : (
                                  <p className="text-conduit-light/70 font-mono">{inputs}</p>
                                )}
                              </div>
                            </div>
                          )
                        )}
                      </div>
                    </div>
                  )}
                </>
              )}
            </Card>
          )}
        </div>
      )}

      {/* Tab 2: Column Trace */}
      {tabIndex === 1 && (
        <div className="space-y-4">
          <Card className="space-y-4">
            <div className="flex items-center gap-2 mb-4">
              <Network size={20} className="text-conduit-accent" />
              <h3 className="text-lg font-semibold text-conduit-light">Trace Configuration</h3>
            </div>

            <div className="grid grid-cols-2 gap-4">
              <div>
                <label className="block text-sm font-medium text-conduit-light mb-2">
                  Task ID
                </label>
                <input
                  type="text"
                  value={traceTaskId}
                  onChange={(e) => setTraceTaskId(e.target.value)}
                  placeholder="task_id"
                  className="w-full px-3 py-2 bg-conduit-dark border border-conduit-accent/30 rounded-md text-conduit-light placeholder-conduit-light/40 focus:outline-none focus:border-conduit-accent"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-conduit-light mb-2">
                  Column Name
                </label>
                <input
                  type="text"
                  value={traceColumnName}
                  onChange={(e) => setTraceColumnName(e.target.value)}
                  placeholder="column_name"
                  className="w-full px-3 py-2 bg-conduit-dark border border-conduit-accent/30 rounded-md text-conduit-light placeholder-conduit-light/40 focus:outline-none focus:border-conduit-accent"
                />
              </div>
            </div>

            <div>
              <label className="block text-sm font-medium text-conduit-light mb-2">
                Direction
              </label>
              <div className="flex gap-3">
                <button
                  onClick={() => setTraceDirection('upstream')}
                  className={`flex items-center gap-2 px-4 py-2 rounded-md transition ${
                    traceDirection === 'upstream'
                      ? 'bg-conduit-accent text-conduit-dark'
                      : 'bg-conduit-dark border border-conduit-accent/30 text-conduit-light hover:border-conduit-accent'
                  }`}
                >
                  <ArrowUp size={16} />
                  Upstream
                </button>
                <button
                  onClick={() => setTraceDirection('downstream')}
                  className={`flex items-center gap-2 px-4 py-2 rounded-md transition ${
                    traceDirection === 'downstream'
                      ? 'bg-conduit-accent text-conduit-dark'
                      : 'bg-conduit-dark border border-conduit-accent/30 text-conduit-light hover:border-conduit-accent'
                  }`}
                >
                  <ArrowDown size={16} />
                  Downstream
                </button>
              </div>
            </div>
          </Card>

          <Card className="space-y-4">
            <div className="flex items-center gap-2 mb-4">
              <Table2 size={20} className="text-conduit-accent" />
              <h3 className="text-lg font-semibold text-conduit-light">Lineage Edges</h3>
            </div>

            <div className="space-y-3 p-4 bg-conduit-dark/30 rounded-lg border border-conduit-accent/20">
              <div className="grid grid-cols-2 gap-3">
                <input
                  type="text"
                  value={traceEdgeForm.from_task}
                  onChange={(e) =>
                    setTraceEdgeForm({ ...traceEdgeForm, from_task: e.target.value })
                  }
                  placeholder="From Task"
                  className="px-3 py-2 bg-conduit-dark border border-conduit-accent/30 rounded-md text-conduit-light placeholder-conduit-light/40 focus:outline-none focus:border-conduit-accent text-sm"
                />
                <input
                  type="text"
                  value={traceEdgeForm.from_column}
                  onChange={(e) =>
                    setTraceEdgeForm({ ...traceEdgeForm, from_column: e.target.value })
                  }
                  placeholder="From Column"
                  className="px-3 py-2 bg-conduit-dark border border-conduit-accent/30 rounded-md text-conduit-light placeholder-conduit-light/40 focus:outline-none focus:border-conduit-accent text-sm"
                />
                <input
                  type="text"
                  value={traceEdgeForm.to_task}
                  onChange={(e) =>
                    setTraceEdgeForm({ ...traceEdgeForm, to_task: e.target.value })
                  }
                  placeholder="To Task"
                  className="px-3 py-2 bg-conduit-dark border border-conduit-accent/30 rounded-md text-conduit-light placeholder-conduit-light/40 focus:outline-none focus:border-conduit-accent text-sm"
                />
                <input
                  type="text"
                  value={traceEdgeForm.to_column}
                  onChange={(e) =>
                    setTraceEdgeForm({ ...traceEdgeForm, to_column: e.target.value })
                  }
                  placeholder="To Column"
                  className="px-3 py-2 bg-conduit-dark border border-conduit-accent/30 rounded-md text-conduit-light placeholder-conduit-light/40 focus:outline-none focus:border-conduit-accent text-sm"
                />
              </div>

              <select
                value={traceEdgeForm.transform}
                onChange={(e) =>
                  setTraceEdgeForm({ ...traceEdgeForm, transform: e.target.value })
                }
                className="w-full px-3 py-2 bg-conduit-dark border border-conduit-accent/30 rounded-md text-conduit-light focus:outline-none focus:border-conduit-accent text-sm"
              >
                <option value="pass_through">pass_through</option>
                <option value="computed">computed</option>
                <option value="joined">joined</option>
                <option value="filtered">filtered</option>
                <option value="aggregated">aggregated</option>
              </select>

              <Button
                onClick={handleAddTraceEdge}
                disabled={
                  !traceEdgeForm.from_task.trim() ||
                  !traceEdgeForm.from_column.trim() ||
                  !traceEdgeForm.to_task.trim() ||
                  !traceEdgeForm.to_column.trim()
                }
                className="w-full"
                variant="secondary"
              >
                Add Edge
              </Button>
            </div>

            {traceEdges.length > 0 && (
              <div className="space-y-2">
                <p className="text-sm font-medium text-conduit-light">Added Edges:</p>
                {traceEdges.map((edge, idx) => (
                  <div
                    key={idx}
                    className="flex items-center justify-between p-3 bg-conduit-accent/10 border border-conduit-accent/20 rounded-md"
                  >
                    <div className="text-sm font-mono text-conduit-light">
                      <span>{edge.from_task}.{edge.from_column}</span>
                      <span className="mx-2 opacity-60">→</span>
                      <span>{edge.to_task}.{edge.to_column}</span>
                      <span className="ml-2 text-xs opacity-60">({edge.transform})</span>
                    </div>
                    <button
                      onClick={() => handleRemoveTraceEdge(idx)}
                      className="text-red-400 hover:text-red-300 text-sm font-semibold"
                    >
                      Remove
                    </button>
                  </div>
                ))}
              </div>
            )}

            <Button
              onClick={handleTrace}
              disabled={traceLoading || !traceTaskId.trim() || !traceColumnName.trim()}
              className="w-full"
            >
              {traceLoading ? (
                <>
                  <Spinner size={16} className="mr-2" />
                  Tracing...
                </>
              ) : (
                <>
                  <Search size={16} className="mr-2" />
                  Trace
                </>
              )}
            </Button>
          </Card>

          {traceResult && (
            <Card className="space-y-4">
              {traceResult.error ? (
                <div className="p-4 bg-red-500/20 border border-red-500/50 rounded text-red-200 font-mono text-sm">
                  {traceResult.error}
                </div>
              ) : (
                <TraceVisualization result={traceResult} />
              )}
            </Card>
          )}
        </div>
      )}

      {/* Tab 3: Schema Diff */}
      {tabIndex === 2 && (
        <div className="space-y-4">
          <div className="grid grid-cols-2 gap-4">
            <Card className="space-y-4">
              <div className="flex items-center gap-2 mb-4">
                <Columns size={20} className="text-conduit-accent" />
                <h3 className="text-lg font-semibold text-conduit-light">Schema A</h3>
              </div>

              <div>
                <label className="block text-sm font-medium text-conduit-light mb-2">
                  Task ID
                </label>
                <input
                  type="text"
                  value={diffSchemaA.task_id}
                  onChange={(e) => setDiffSchemaA({ ...diffSchemaA, task_id: e.target.value })}
                  placeholder="task_id"
                  className="w-full px-3 py-2 bg-conduit-dark border border-conduit-accent/30 rounded-md text-conduit-light placeholder-conduit-light/40 focus:outline-none focus:border-conduit-accent"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-conduit-light mb-2">
                  Columns
                </label>
                <div className="space-y-2 max-h-96 overflow-y-auto">
                  {diffSchemaA.columns.map((col, idx) => (
                    <div
                      key={idx}
                      className="p-3 bg-conduit-dark/50 border border-conduit-accent/20 rounded-md space-y-2"
                    >
                      <div className="grid grid-cols-2 gap-2">
                        <input
                          type="text"
                          value={col.name}
                          onChange={(e) =>
                            handleUpdateDiffColumnA(idx, 'name', e.target.value)
                          }
                          placeholder="Column name"
                          className="px-2 py-1 bg-conduit-dark border border-conduit-accent/30 rounded text-conduit-light placeholder-conduit-light/40 focus:outline-none focus:border-conduit-accent text-sm"
                        />
                        <select
                          value={col.type}
                          onChange={(e) =>
                            handleUpdateDiffColumnA(idx, 'type', e.target.value)
                          }
                          className="px-2 py-1 bg-conduit-dark border border-conduit-accent/30 rounded text-conduit-light focus:outline-none focus:border-conduit-accent text-sm"
                        >
                          <option value="string">string</option>
                          <option value="integer">integer</option>
                          <option value="float">float</option>
                          <option value="boolean">boolean</option>
                          <option value="date">date</option>
                          <option value="timestamp">timestamp</option>
                          <option value="array">array</option>
                          <option value="struct">struct</option>
                        </select>
                      </div>
                      <div className="flex items-center gap-2">
                        <input
                          type="checkbox"
                          checked={col.nullable}
                          onChange={(e) =>
                            handleUpdateDiffColumnA(idx, 'nullable', e.target.checked)
                          }
                          className="w-4 h-4 bg-conduit-dark border border-conduit-accent/30 rounded focus:outline-none"
                        />
                        <label className="text-sm text-conduit-light">Nullable</label>
                      </div>
                      <input
                        type="text"
                        value={col.description}
                        onChange={(e) =>
                          handleUpdateDiffColumnA(idx, 'description', e.target.value)
                        }
                        placeholder="Description"
                        className="w-full px-2 py-1 bg-conduit-dark border border-conduit-accent/30 rounded text-conduit-light placeholder-conduit-light/40 focus:outline-none focus:border-conduit-accent text-sm"
                      />
                      <button
                        onClick={() => handleRemoveDiffColumnA(idx)}
                        className="w-full px-2 py-1 text-red-400 hover:text-red-300 bg-red-500/10 hover:bg-red-500/20 rounded text-sm font-semibold transition"
                      >
                        Remove
                      </button>
                    </div>
                  ))}
                </div>
                <Button
                  onClick={handleAddDiffColumnA}
                  variant="secondary"
                  className="w-full mt-2"
                >
                  Add Column
                </Button>
              </div>
            </Card>

            <Card className="space-y-4">
              <div className="flex items-center gap-2 mb-4">
                <Columns size={20} className="text-conduit-accent" />
                <h3 className="text-lg font-semibold text-conduit-light">Schema B</h3>
              </div>

              <div>
                <label className="block text-sm font-medium text-conduit-light mb-2">
                  Task ID
                </label>
                <input
                  type="text"
                  value={diffSchemaB.task_id}
                  onChange={(e) => setDiffSchemaB({ ...diffSchemaB, task_id: e.target.value })}
                  placeholder="task_id"
                  className="w-full px-3 py-2 bg-conduit-dark border border-conduit-accent/30 rounded-md text-conduit-light placeholder-conduit-light/40 focus:outline-none focus:border-conduit-accent"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-conduit-light mb-2">
                  Columns
                </label>
                <div className="space-y-2 max-h-96 overflow-y-auto">
                  {diffSchemaB.columns.map((col, idx) => (
                    <div
                      key={idx}
                      className="p-3 bg-conduit-dark/50 border border-conduit-accent/20 rounded-md space-y-2"
                    >
                      <div className="grid grid-cols-2 gap-2">
                        <input
                          type="text"
                          value={col.name}
                          onChange={(e) =>
                            handleUpdateDiffColumnB(idx, 'name', e.target.value)
                          }
                          placeholder="Column name"
                          className="px-2 py-1 bg-conduit-dark border border-conduit-accent/30 rounded text-conduit-light placeholder-conduit-light/40 focus:outline-none focus:border-conduit-accent text-sm"
                        />
                        <select
                          value={col.type}
                          onChange={(e) =>
                            handleUpdateDiffColumnB(idx, 'type', e.target.value)
                          }
                          className="px-2 py-1 bg-conduit-dark border border-conduit-accent/30 rounded text-conduit-light focus:outline-none focus:border-conduit-accent text-sm"
                        >
                          <option value="string">string</option>
                          <option value="integer">integer</option>
                          <option value="float">float</option>
                          <option value="boolean">boolean</option>
                          <option value="date">date</option>
                          <option value="timestamp">timestamp</option>
                          <option value="array">array</option>
                          <option value="struct">struct</option>
                        </select>
                      </div>
                      <div className="flex items-center gap-2">
                        <input
                          type="checkbox"
                          checked={col.nullable}
                          onChange={(e) =>
                            handleUpdateDiffColumnB(idx, 'nullable', e.target.checked)
                          }
                          className="w-4 h-4 bg-conduit-dark border border-conduit-accent/30 rounded focus:outline-none"
                        />
                        <label className="text-sm text-conduit-light">Nullable</label>
                      </div>
                      <input
                        type="text"
                        value={col.description}
                        onChange={(e) =>
                          handleUpdateDiffColumnB(idx, 'description', e.target.value)
                        }
                        placeholder="Description"
                        className="w-full px-2 py-1 bg-conduit-dark border border-conduit-accent/30 rounded text-conduit-light placeholder-conduit-light/40 focus:outline-none focus:border-conduit-accent text-sm"
                      />
                      <button
                        onClick={() => handleRemoveDiffColumnB(idx)}
                        className="w-full px-2 py-1 text-red-400 hover:text-red-300 bg-red-500/10 hover:bg-red-500/20 rounded text-sm font-semibold transition"
                      >
                        Remove
                      </button>
                    </div>
                  ))}
                </div>
                <Button
                  onClick={handleAddDiffColumnB}
                  variant="secondary"
                  className="w-full mt-2"
                >
                  Add Column
                </Button>
              </div>
            </Card>
          </div>

          <Card>
            <Button
              onClick={handleCompareDiff}
              disabled={
                diffLoading ||
                !diffSchemaA.task_id.trim() ||
                !diffSchemaB.task_id.trim()
              }
              className="w-full"
            >
              {diffLoading ? (
                <>
                  <Spinner size={16} className="mr-2" />
                  Comparing...
                </>
              ) : (
                <>
                  <GitCompare size={16} className="mr-2" />
                  Compare
                </>
              )}
            </Button>
          </Card>

          {diffResult && (
            <Card className="space-y-4">
              {diffResult.error ? (
                <div className="p-4 bg-red-500/20 border border-red-500/50 rounded text-red-200 font-mono text-sm">
                  {diffResult.error}
                </div>
              ) : (
                <>
                  {diffResult.changes && (
                    <div>
                      <h4 className="text-md font-semibold text-conduit-light mb-3">
                        Changes
                      </h4>
                      <div className="space-y-2">
                        {diffResult.changes.map((change, idx) => (
                          <div
                            key={idx}
                            className="p-3 bg-conduit-accent/10 border border-conduit-accent/20 rounded-md"
                          >
                            <div className="flex items-start justify-between mb-2">
                              <span className="font-mono text-conduit-light font-semibold">
                                {change.column_name}
                              </span>
                              <div className="flex gap-2">
                                <StatusBadge
                                  status="info"
                                  label={change.change_kind}
                                  size="sm"
                                />
                                {change.breaking && (
                                  <div className="px-2 py-1 bg-red-500/20 border border-red-500/50 rounded text-red-200 text-xs font-semibold">
                                    Breaking
                                  </div>
                                )}
                                {!change.breaking && (
                                  <div className="px-2 py-1 bg-green-500/20 border border-green-500/50 rounded text-green-200 text-xs font-semibold">
                                    Non-Breaking
                                  </div>
                                )}
                              </div>
                            </div>
                            {change.description && (
                              <p className="text-xs text-conduit-light/60">{change.description}</p>
                            )}
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </>
              )}
            </Card>
          )}
        </div>
      )}

      {/* Tab 4: Contracts */}
      {tabIndex === 3 && (
        <div className="space-y-4">
          <Card className="space-y-4">
            <div className="flex items-center gap-2 mb-4">
              <Shield size={20} className="text-conduit-accent" />
              <h3 className="text-lg font-semibold text-conduit-light">Schema</h3>
            </div>

            <div>
              <label className="block text-sm font-medium text-conduit-light mb-2">
                Task ID
              </label>
              <input
                type="text"
                value={contractSchema.task_id}
                onChange={(e) =>
                  setContractSchema({ ...contractSchema, task_id: e.target.value })
                }
                placeholder="task_id"
                className="w-full px-3 py-2 bg-conduit-dark border border-conduit-accent/30 rounded-md text-conduit-light placeholder-conduit-light/40 focus:outline-none focus:border-conduit-accent"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-conduit-light mb-2">
                Columns
              </label>
              <div className="space-y-2 max-h-96 overflow-y-auto">
                {contractSchema.columns.map((col, idx) => (
                  <div
                    key={idx}
                    className="p-3 bg-conduit-dark/50 border border-conduit-accent/20 rounded-md space-y-2"
                  >
                    <div className="grid grid-cols-2 gap-2">
                      <input
                        type="text"
                        value={col.name}
                        onChange={(e) =>
                          handleUpdateContractColumn(idx, 'name', e.target.value)
                        }
                        placeholder="Column name"
                        className="px-2 py-1 bg-conduit-dark border border-conduit-accent/30 rounded text-conduit-light placeholder-conduit-light/40 focus:outline-none focus:border-conduit-accent text-sm"
                      />
                      <select
                        value={col.type}
                        onChange={(e) =>
                          handleUpdateContractColumn(idx, 'type', e.target.value)
                        }
                        className="px-2 py-1 bg-conduit-dark border border-conduit-accent/30 rounded text-conduit-light focus:outline-none focus:border-conduit-accent text-sm"
                      >
                        <option value="string">string</option>
                        <option value="integer">integer</option>
                        <option value="float">float</option>
                        <option value="boolean">boolean</option>
                        <option value="date">date</option>
                        <option value="timestamp">timestamp</option>
                        <option value="array">array</option>
                        <option value="struct">struct</option>
                      </select>
                    </div>
                    <div className="flex items-center gap-2">
                      <input
                        type="checkbox"
                        checked={col.nullable}
                        onChange={(e) =>
                          handleUpdateContractColumn(idx, 'nullable', e.target.checked)
                        }
                        className="w-4 h-4 bg-conduit-dark border border-conduit-accent/30 rounded focus:outline-none"
                      />
                      <label className="text-sm text-conduit-light">Nullable</label>
                    </div>
                    <input
                      type="text"
                      value={col.description}
                      onChange={(e) =>
                        handleUpdateContractColumn(idx, 'description', e.target.value)
                      }
                      placeholder="Description"
                      className="w-full px-2 py-1 bg-conduit-dark border border-conduit-accent/30 rounded text-conduit-light placeholder-conduit-light/40 focus:outline-none focus:border-conduit-accent text-sm"
                    />
                    <button
                      onClick={() => handleRemoveContractColumn(idx)}
                      className="w-full px-2 py-1 text-red-400 hover:text-red-300 bg-red-500/10 hover:bg-red-500/20 rounded text-sm font-semibold transition"
                    >
                      Remove
                    </button>
                  </div>
                ))}
              </div>
              <Button
                onClick={handleAddContractColumn}
                variant="secondary"
                className="w-full mt-2"
              >
                Add Column
              </Button>
            </div>
          </Card>

          <Card className="space-y-4">
            <div className="flex items-center gap-2 mb-4">
              <Shield size={20} className="text-conduit-accent" />
              <h3 className="text-lg font-semibold text-conduit-light">Contract Rules</h3>
            </div>

            <div>
              <label className="block text-sm font-medium text-conduit-light mb-2">
                Required Columns
              </label>
              <div className="space-y-2 max-h-48 overflow-y-auto">
                {contractRequiredColumns.map((col, idx) => (
                  <div
                    key={idx}
                    className="flex gap-2 items-end p-2 bg-conduit-dark/50 border border-conduit-accent/20 rounded-md"
                  >
                    <input
                      type="text"
                      value={col.name}
                      onChange={(e) =>
                        handleUpdateRequiredColumn(idx, 'name', e.target.value)
                      }
                      placeholder="Column name"
                      className="flex-1 px-2 py-1 bg-conduit-dark border border-conduit-accent/30 rounded text-conduit-light placeholder-conduit-light/40 focus:outline-none focus:border-conduit-accent text-sm"
                    />
                    <select
                      value={col.type}
                      onChange={(e) =>
                        handleUpdateRequiredColumn(idx, 'type', e.target.value)
                      }
                      className="px-2 py-1 bg-conduit-dark border border-conduit-accent/30 rounded text-conduit-light focus:outline-none focus:border-conduit-accent text-sm"
                    >
                      <option value="">Any</option>
                      <option value="string">string</option>
                      <option value="integer">integer</option>
                      <option value="float">float</option>
                      <option value="boolean">boolean</option>
                      <option value="date">date</option>
                      <option value="timestamp">timestamp</option>
                    </select>
                    <button
                      onClick={() => handleRemoveRequiredColumn(idx)}
                      className="px-2 py-1 text-red-400 hover:text-red-300 bg-red-500/10 hover:bg-red-500/20 rounded text-sm font-semibold transition"
                    >
                      Remove
                    </button>
                  </div>
                ))}
              </div>
              <Button
                onClick={handleAddRequiredColumn}
                variant="secondary"
                className="w-full mt-2"
              >
                Add Required Column
              </Button>
            </div>

            <div>
              <label className="block text-sm font-medium text-conduit-light mb-2">
                Forbidden Columns
              </label>
              <div className="space-y-2 max-h-48 overflow-y-auto">
                {contractForbiddenColumns.map((col, idx) => (
                  <div
                    key={idx}
                    className="flex gap-2 items-center p-2 bg-conduit-dark/50 border border-conduit-accent/20 rounded-md"
                  >
                    <input
                      type="text"
                      value={col}
                      onChange={(e) => handleUpdateForbiddenColumn(idx, e.target.value)}
                      placeholder="Column name"
                      className="flex-1 px-2 py-1 bg-conduit-dark border border-conduit-accent/30 rounded text-conduit-light placeholder-conduit-light/40 focus:outline-none focus:border-conduit-accent text-sm"
                    />
                    <button
                      onClick={() => handleRemoveForbiddenColumn(idx)}
                      className="px-2 py-1 text-red-400 hover:text-red-300 bg-red-500/10 hover:bg-red-500/20 rounded text-sm font-semibold transition"
                    >
                      Remove
                    </button>
                  </div>
                ))}
              </div>
              <Button
                onClick={handleAddForbiddenColumn}
                variant="secondary"
                className="w-full mt-2"
              >
                Add Forbidden Column
              </Button>
            </div>

            <div className="grid grid-cols-2 gap-4">
              <div>
                <label className="block text-sm font-medium text-conduit-light mb-2">
                  Max Columns
                </label>
                <input
                  type="number"
                  value={contractMaxColumns}
                  onChange={(e) => setContractMaxColumns(e.target.value)}
                  placeholder="No limit"
                  className="w-full px-3 py-2 bg-conduit-dark border border-conduit-accent/30 rounded-md text-conduit-light placeholder-conduit-light/40 focus:outline-none focus:border-conduit-accent"
                />
              </div>

              <div className="flex items-end">
                <label className="flex items-center gap-2 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={contractRequireDocs}
                    onChange={(e) => setContractRequireDocs(e.target.checked)}
                    className="w-4 h-4 bg-conduit-dark border border-conduit-accent/30 rounded focus:outline-none"
                  />
                  <span className="text-sm font-medium text-conduit-light">
                    Require Docs
                  </span>
                </label>
              </div>
            </div>

            <label className="flex items-center gap-2 cursor-pointer">
              <input
                type="checkbox"
                checked={contractNoUnknownTypes}
                onChange={(e) => setContractNoUnknownTypes(e.target.checked)}
                className="w-4 h-4 bg-conduit-dark border border-conduit-accent/30 rounded focus:outline-none"
              />
              <span className="text-sm font-medium text-conduit-light">
                No Unknown Types
              </span>
            </label>
          </Card>

          <Card>
            <Button
              onClick={handleValidateContract}
              disabled={contractLoading || !contractSchema.task_id.trim()}
              className="w-full"
            >
              {contractLoading ? (
                <>
                  <Spinner size={16} className="mr-2" />
                  Validating...
                </>
              ) : (
                <>
                  <Shield size={16} className="mr-2" />
                  Validate Contract
                </>
              )}
            </Button>
          </Card>

          {contractResult && (
            <Card className="space-y-4">
              {contractResult.error ? (
                <div className="p-4 bg-red-500/20 border border-red-500/50 rounded text-red-200 font-mono text-sm">
                  {contractResult.error}
                </div>
              ) : (
                <>
                  <div
                    className={`p-4 border rounded-md ${
                      contractResult.passed
                        ? 'bg-green-500/20 border-green-500/50'
                        : 'bg-red-500/20 border-red-500/50'
                    }`}
                  >
                    <p
                      className={`text-lg font-semibold ${
                        contractResult.passed ? 'text-green-200' : 'text-red-200'
                      }`}
                    >
                      {contractResult.passed ? 'Validation Passed' : 'Validation Failed'}
                    </p>
                  </div>

                  {contractResult.violations && contractResult.violations.length > 0 && (
                    <div>
                      <h4 className="text-md font-semibold text-conduit-light mb-3">
                        Violations
                      </h4>
                      <div className="space-y-2">
                        {contractResult.violations.map((violation, idx) => (
                          <div
                            key={idx}
                            className="p-3 bg-conduit-accent/10 border border-conduit-accent/20 rounded-md"
                          >
                            <div className="flex items-start justify-between mb-2">
                              <span className="font-semibold text-conduit-light">
                                {violation.rule}
                              </span>
                              <StatusBadge
                                status={
                                  violation.severity === 'Error'
                                    ? 'error'
                                    : violation.severity === 'Warning'
                                      ? 'warning'
                                      : 'info'
                                }
                                label={violation.severity}
                                size="sm"
                              />
                            </div>
                            <p className="text-sm text-conduit-light/70">
                              {violation.message}
                            </p>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </>
              )}
            </Card>
          )}
        </div>
      )}

      {/* Tab 5: Datasets — unified internal + external lineage view */}
      {tabIndex === 4 && (
        <div className="space-y-4">
          <Card className="space-y-4">
            <div className="flex items-center justify-between mb-2">
              <div className="flex items-center gap-2">
                <Database size={20} className="text-conduit-accent" />
                <h3 className="text-lg font-semibold text-conduit-light">
                  Cross-system dataset view
                </h3>
              </div>
              <div className="flex items-center gap-3 text-xs text-conduit-light/60">
                {datasetStats && (
                  <span>
                    store: {datasetStats.eventCount} events · {datasetStats.datasetCount} datasets · {datasetStats.edgeCount} edges
                  </span>
                )}
                {planCacheStats && (
                  <span title={`last compile: ${planCacheStats.lastCompileMs}ms`}>
                    cache: {planCacheStats.hits}H / {planCacheStats.misses}M · {planCacheStats.stitchedDagCount}/{planCacheStats.cachedDagCount} DAGs
                  </span>
                )}
                <button
                  onClick={handleInvalidateCache}
                  disabled={invalidating}
                  className="px-2 py-0.5 rounded border border-conduit-accent/30 text-conduit-accent hover:bg-conduit-accent/10 disabled:opacity-50 transition"
                  title="Force a plan recompile on the next request"
                >
                  {invalidating ? 'flushing…' : 'flush cache'}
                </button>
              </div>
            </div>

            <p className="text-sm text-conduit-light/70">
              Search any dataset (by OpenLineage namespace + name) to see its
              producers, schema, and column-level upstream/downstream — fused
              across Conduit's compiled DAGs and ingested OpenLineage events
              from foreign systems.
            </p>

            <div className="grid grid-cols-1 md:grid-cols-[1fr_2fr_auto] gap-3">
              <input
                type="text"
                value={datasetNamespace}
                onChange={(e) => setDatasetNamespace(e.target.value)}
                placeholder="namespace (e.g. warehouse)"
                className="px-3 py-2 bg-conduit-dark border border-conduit-accent/30 rounded-md text-conduit-light placeholder-conduit-light/40 focus:outline-none focus:border-conduit-accent"
              />
              <input
                type="text"
                value={datasetName}
                onChange={(e) => setDatasetName(e.target.value)}
                placeholder="dataset name (e.g. staging.orders)"
                className="px-3 py-2 bg-conduit-dark border border-conduit-accent/30 rounded-md text-conduit-light placeholder-conduit-light/40 focus:outline-none focus:border-conduit-accent"
                onKeyDown={(e) => {
                  if (e.key === 'Enter') handleLoadDatasetView();
                }}
              />
              <Button onClick={handleLoadDatasetView} loading={datasetLoading}>
                <Search size={14} />
                Inspect
              </Button>
            </div>

            {datasetError && (
              <div className="p-3 bg-red-900/20 border border-red-700/50 rounded text-red-200 text-sm">
                {datasetError}
              </div>
            )}
          </Card>

          {datasetView && (
            <Card className="space-y-5">
              <div className="flex items-center justify-between border-b border-conduit-accent/20 pb-3">
                <div>
                  <h3 className="text-lg font-semibold text-conduit-light">
                    {datasetView.namespace}/{datasetView.name}
                  </h3>
                  <p className="text-xs text-conduit-light/60 mt-1">
                    schema source: <span className="font-mono">{datasetView.schema?.source}</span>
                  </p>
                </div>
              </div>

              {/* Producers */}
              <div>
                <h4 className="text-sm font-semibold text-conduit-light mb-2 flex items-center gap-2">
                  <Table2 size={14} /> Producers
                </h4>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                  <div className="p-3 bg-conduit-dark/40 border border-conduit-accent/20 rounded">
                    <div className="text-xs text-conduit-accent uppercase tracking-wide mb-1">
                      Internal (Conduit)
                    </div>
                    {datasetView.producers?.internal ? (
                      <div className="text-sm text-conduit-light font-mono">
                        {datasetView.producers.internal.dagId}.{datasetView.producers.internal.taskId}
                      </div>
                    ) : (
                      <div className="text-sm text-conduit-light/50">(none)</div>
                    )}
                  </div>
                  <div className="p-3 bg-conduit-dark/40 border border-conduit-accent/20 rounded">
                    <div className="text-xs text-conduit-accent uppercase tracking-wide mb-1">
                      External (Ingested)
                    </div>
                    {datasetView.producers?.external ? (
                      <div className="text-sm text-conduit-light">
                        <div className="font-mono">
                          {datasetView.producers.external.jobNamespace}/{datasetView.producers.external.jobName}
                        </div>
                        <div className="text-xs text-conduit-light/60 mt-1">
                          latest run: {datasetView.producers.external.runId}
                          {' · '}
                          {datasetView.producers.externalEventCount} event(s)
                        </div>
                      </div>
                    ) : (
                      <div className="text-sm text-conduit-light/50">(none)</div>
                    )}
                  </div>
                </div>
              </div>

              {/* Schema */}
              {datasetView.schema?.columns?.length > 0 && (
                <div>
                  <h4 className="text-sm font-semibold text-conduit-light mb-2 flex items-center gap-2">
                    <Columns size={14} /> Schema ({datasetView.schema.columns.length} columns)
                  </h4>
                  <div className="bg-conduit-dark/40 border border-conduit-accent/20 rounded p-2">
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="text-xs text-conduit-light/60 uppercase">
                          <th className="text-left py-1 px-2">Name</th>
                          <th className="text-left py-1 px-2">Type</th>
                        </tr>
                      </thead>
                      <tbody>
                        {datasetView.schema.columns.map((c, i) => (
                          <tr key={i} className="border-t border-conduit-accent/10">
                            <td className="py-1 px-2 font-mono text-conduit-light">{c.name}</td>
                            <td className="py-1 px-2 text-conduit-light/70">{c.dtype || '—'}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}

              {/* Upstream */}
              <div>
                <h4 className="text-sm font-semibold text-conduit-light mb-2 flex items-center gap-2">
                  <ArrowUp size={14} /> Upstream
                </h4>
                <UpstreamDownstreamPanel
                  internal={datasetView.upstream?.internal || []}
                  external={datasetView.upstream?.external || []}
                  direction="upstream"
                />
              </div>

              {/* Downstream */}
              <div>
                <h4 className="text-sm font-semibold text-conduit-light mb-2 flex items-center gap-2">
                  <ArrowDown size={14} /> Downstream
                </h4>
                <UpstreamDownstreamPanel
                  internal={datasetView.downstream?.internal || []}
                  external={datasetView.downstream?.external || []}
                  direction="downstream"
                />
              </div>

              {/* Recent events */}
              {datasetView.recentEvents?.length > 0 && (
                <div>
                  <h4 className="text-sm font-semibold text-conduit-light mb-2">
                    Recent ingested events ({datasetView.recentEvents.length})
                  </h4>
                  <div className="bg-conduit-dark/40 border border-conduit-accent/20 rounded p-2 max-h-64 overflow-y-auto">
                    {datasetView.recentEvents.map((e, i) => (
                      <div
                        key={i}
                        className="py-2 px-2 border-b border-conduit-accent/10 last:border-b-0 text-xs"
                      >
                        <div className="flex items-center justify-between">
                          <span className="font-mono text-conduit-light">
                            {e.jobNamespace}/{e.jobName}
                          </span>
                          <span className="text-conduit-light/50">
                            {new Date(e.receivedAt).toLocaleString()}
                          </span>
                        </div>
                        <div className="text-conduit-light/60 mt-1">
                          run {e.runId} · {e.eventType} ·{' '}
                          {e.inputs.length} in / {e.outputs.length} out
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </Card>
          )}
        </div>
      )}
    </div>
  );
}

/// Renders the internal+external split for upstream/downstream sections.
function UpstreamDownstreamPanel({ internal, external, direction }) {
  if (internal.length === 0 && external.length === 0) {
    return (
      <div className="p-3 bg-conduit-dark/30 border border-conduit-accent/10 rounded text-sm text-conduit-light/50">
        No {direction} dependencies recorded.
      </div>
    );
  }
  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
      <div className="p-3 bg-conduit-dark/40 border border-conduit-accent/20 rounded">
        <div className="text-xs text-conduit-accent uppercase tracking-wide mb-2">
          Internal ({internal.length})
        </div>
        {internal.length === 0 ? (
          <div className="text-sm text-conduit-light/50">(none)</div>
        ) : (
          <ul className="space-y-1">
            {internal.map((r, i) => (
              <li key={i} className="text-sm font-mono text-conduit-light">
                {r.kind === 'task'
                  ? `${r.dagId}.${r.taskId}.${r.column}`
                  : `${r.table}.${r.column}`}
              </li>
            ))}
          </ul>
        )}
      </div>
      <div className="p-3 bg-conduit-dark/40 border border-conduit-accent/20 rounded">
        <div className="text-xs text-conduit-accent uppercase tracking-wide mb-2">
          External ({external.length})
        </div>
        {external.length === 0 ? (
          <div className="text-sm text-conduit-light/50">(none)</div>
        ) : (
          <ul className="space-y-1">
            {external.map((e, i) => (
              <li key={i} className="text-sm font-mono text-conduit-light">
                {direction === 'upstream' ? (
                  <>
                    {e.sourceDataset}.{e.sourceColumn}
                    <span className="text-conduit-light/40"> → {e.targetColumn}</span>
                  </>
                ) : (
                  <>
                    <span className="text-conduit-light/40">{e.sourceColumn} → </span>
                    {e.targetDataset}.{e.targetColumn}
                  </>
                )}
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}
