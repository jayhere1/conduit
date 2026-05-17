// ─── Conduit API Client ──────────────────────────────────────────────────────
// Thin wrapper around fetch for all /api/v1 endpoints.
// WebSocket helper for live event streaming.

const BASE = '/api/v1';

// ─── Auth Token Management ───────────────────────────────────────────────────

let _authToken = null;

/** Set the API key for all subsequent requests. */
export function setAuthToken(token) {
  _authToken = token;
}

/** Get the current auth token. */
export function getAuthToken() {
  return _authToken;
}

/** Clear the auth token. */
export function clearAuthToken() {
  _authToken = null;
}

// ─── Request Helpers ─────────────────────────────────────────────────────────

async function request(path, options = {}) {
  const url = `${BASE}${path}`;
  const headers = { 'Content-Type': 'application/json', ...options.headers };

  // Inject auth token if set
  if (_authToken) {
    headers['Authorization'] = `Bearer ${_authToken}`;
  }

  const res = await fetch(url, { headers, ...options });

  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    const msg = body?.error?.message || res.statusText;
    const err = new Error(`${res.status}: ${msg}`);
    err.status = res.status;
    throw err;
  }

  return res.json();
}

function get(path) {
  return request(path);
}

function post(path, body) {
  return request(path, { method: 'POST', body: JSON.stringify(body) });
}

function del(path) {
  return request(path, { method: 'DELETE' });
}

function put(path, body) {
  return request(path, { method: 'PUT', body: JSON.stringify(body) });
}

// ─── Health & Info ───────────────────────────────────────────────────────────

export const health = () => get('/health');
export const systemInfo = () => get('/info');

// ─── DAGs ────────────────────────────────────────────────────────────────────

/**
 * Normalize DAG object field names from backend to frontend conventions.
 */
function normalizeDAG(dag) {
  return {
    id: dag.id,
    name: dag.name || dag.id,
    description: dag.description,
    schedule: dag.schedule,
    tags: dag.tags || [],
    taskCount: dag.taskCount || dag.task_count,
    sourceFile: dag.sourceFile || dag.source_file,
    maxActiveRuns: dag.maxActiveRuns || dag.max_active_runs,
    executionOrder: dag.executionOrder || dag.execution_order || [],
    lastRunStatus: dag.lastRunStatus || dag.last_run_status || null,
    lastRunAt: dag.lastRunAt || dag.last_run_at || null,
    tasks: (dag.tasks || []).map((task) => ({
      id: task.id,
      name: task.name || task.id,
      type: task.type,
      dependencies: task.dependencies || [],
      pool: task.pool,
      retries: task.retries,
      retryDelay: task.retryDelay,
      timeout: task.timeout,
      priority: task.priority,
      triggerRule: task.triggerRule,
    })),
  };
}

/**
 * Enrich DAGs with last-run data by cross-referencing recent runs.
 */
async function enrichDagsWithRuns(dags) {
  try {
    const runs = await get('/runs').then((r) => r.runs || []).catch(() => []);
    // Group runs by dag_id, pick the most recent
    const latestByDag = {};
    for (const run of runs) {
      const dagId = run.dag_id || run.dagId;
      const startedAt = run.started_at || run.startedAt;
      if (!latestByDag[dagId] || startedAt > latestByDag[dagId].startedAt) {
        latestByDag[dagId] = { status: run.status, startedAt };
      }
    }
    return dags.map((dag) => {
      const latest = latestByDag[dag.id];
      if (latest) {
        dag.lastRunStatus = latest.status;
        dag.lastRunAt = latest.startedAt;
      }
      return dag;
    });
  } catch {
    return dags;
  }
}

export const listDags = () =>
  get('/dags')
    .then((r) => (r.dags || []).map(normalizeDAG))
    .then(enrichDagsWithRuns);
export const getDag = (dagId) => get(`/dags/${dagId}`).then(normalizeDAG);
export const getDagGraph = (dagId) => get(`/dags/${dagId}/graph`);
export const compileDags = () => post('/dags/compile');

// ─── Runs ────────────────────────────────────────────────────────────────────

/**
 * Normalize run object field names from backend to frontend conventions.
 */
function normalizeRun(run) {
  return {
    id: run.id || run.run_id,
    runId: run.id || run.run_id,
    dagId: run.dagId || run.dag_id,
    status: run.status,
    startedAt: run.startedAt || run.started_at,
    endedAt: run.endedAt || run.finished_at || run.ended_at,
    taskStates: run.taskStates || run.task_states || {},
    tasks: run.tasks || [],
    triggeredBy: run.triggeredBy || run.triggered_by,
    environment: run.environment,
  };
}

/**
 * Normalize array of runs.
 */
function normalizeRuns(runs) {
  return (runs || []).map(normalizeRun);
}

export const listAllRuns = (filters = {}) => {
  const params = new URLSearchParams();
  if (filters.environment) params.set('environment', filters.environment);
  if (filters.status) params.set('status', filters.status);
  if (filters.limit != null) params.set('limit', String(filters.limit));
  const qs = params.toString();
  return get(`/runs${qs ? `?${qs}` : ''}`)
    .then((r) => normalizeRuns(r.runs || []))
    .catch(() => []);
};

export const listRuns = (dagId, filters = {}) => {
  const params = new URLSearchParams();
  if (filters.environment) params.set('environment', filters.environment);
  if (filters.status) params.set('status', filters.status);
  if (filters.limit != null) params.set('limit', String(filters.limit));
  const qs = params.toString();
  return get(`/dags/${dagId}/runs${qs ? `?${qs}` : ''}`)
    .then((r) => normalizeRuns(r.runs || []))
    .catch(() => []);
};

export const getRun = (runId) =>
  get(`/runs/${runId}`).then(normalizeRun);

export const triggerRun = (dagId, env = 'production') =>
  post(`/dags/${dagId}/runs`, { environment: env }).then(normalizeRun);

// ─── Environments ────────────────────────────────────────────────────────────

export const listEnvironments = () => get('/environments').then((r) => r.environments || []);
export const getEnvironment = (name) => get(`/environments/${name}`);
export const createEnvironment = (name, basedOn) =>
  post('/environments', { name, based_on: basedOn });
export const deleteEnvironment = (name) => del(`/environments/${name}`);
export const promoteEnvironment = (source, target) =>
  post('/environments/promote', { source, target });
export const diffEnvironments = (envA, envB) =>
  get(`/environments/${envA}/diff/${envB}`);

export const getEnvHistory = (name, includeSnapshots = false) =>
  get(`/environments/${name}/history${includeSnapshots ? '?include_snapshots=true' : ''}`);

export const getEnvHistoryVersion = (name, version) =>
  get(`/environments/${name}/history/${version}`);

export const rollbackEnvironment = (name, toVersion) =>
  post(`/environments/${name}/rollback`, toVersion != null ? { to_version: toVersion } : {});

export const updateEnvPolicy = (name, policy) =>
  put(`/environments/${name}/policy`, {
    require_source: policy.requireSource ?? null,
    min_age_secs: policy.minAgeSecs ?? null,
  });

// ─── Plan / Apply ────────────────────────────────────────────────────────────

export const generatePlan = (env = 'production') =>
  post('/plan', { environment: env });
export const applyPlan = (planId) => post('/apply', { plan_id: planId });

// ─── Events ──────────────────────────────────────────────────────────────────

export const listEvents = () => get('/events').then((r) => r.events || []);
export const getEvent = (seq) => get(`/events/${seq}`);

// ─── Lineage ─────────────────────────────────────────────────────────────────

export const extractSqlLineage = (sql, sourceTaskId) =>
  post('/lineage/sql', { sql, source_task_id: sourceTaskId });

export const traceUpstream = (taskId, columnName, edges) =>
  post('/lineage/trace/upstream', {
    target: { task_id: taskId, column_name: columnName },
    edges,
  });

export const traceDownstream = (taskId, columnName, edges) =>
  post('/lineage/trace/downstream', {
    target: { task_id: taskId, column_name: columnName },
    edges,
  });

export const lineageGraph = (edges) => post('/lineage/graph', { edges });

export const schemaDiff = (oldSchema, newSchema) =>
  post('/lineage/schema/diff', { old_schema: oldSchema, new_schema: newSchema });

export const validateContract = (schema, contract) =>
  post('/lineage/contracts/validate', { schema, contract });

// ─── Contracts ──────────────────────────────────────────────────────────────

export const listContracts = () => get('/contracts').then((r) => r.contracts || r);
export const dagContracts = (dagId) => get(`/contracts/${dagId}`);
export const taskContracts = (dagId, taskId) => get(`/contracts/${dagId}/${taskId}`);

// ─── Metrics ────────────────────────────────────────────────────────────────

export const listMetrics = () => get('/metrics').then((r) => r.metrics || r);
export const getTaskMetrics = (dagId, taskId) => get(`/metrics/${dagId}/${taskId}`);

// ─── Connections ────────────────────────────────────────────────────────────

export const listConnections = () => get('/connections').then((r) => r.connections || []);
export const getConnection = (name) => get(`/connections/${name}`).then((r) => r.connection || r);
export const testConnection = (name) => post(`/connections/${name}/test`);
export const listProviders = () => get('/connections/providers').then((r) => r.providers || []);

// ─── Cluster ─────────────────────────────────────────────────────────────────

export const getClusterStatus = () => get('/cluster/status');
export const drainWorker = (workerId) => post(`/cluster/workers/${workerId}/drain`, {});

// ─── Authentication ─────────────────────────────────────────────────────────

export const whoami = () => get('/auth/me');
export const listApiKeys = () => get('/auth/keys').then((r) => r.keys || []);
export const getApiKey = (id) => get(`/auth/keys/${id}`);
export const createApiKey = (name, role, description, expiresAt) =>
  post('/auth/keys', { name, role, description, expiresAt });
export const revokeApiKey = (id) => del(`/auth/keys/${id}`);

// ─── WebSocket ───────────────────────────────────────────────────────────────

export function connectEvents(onMessage, onError, _retryCount = 0) {
  const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  const ws = new WebSocket(`${proto}//${window.location.host}/ws/events`);

  ws.onopen = () => {
    // Reset retry count on successful connection
    ws._retryCount = 0;
  };

  ws.onmessage = (evt) => {
    try {
      const data = JSON.parse(evt.data);
      onMessage(data);
    } catch {
      onMessage({ raw: evt.data });
    }
  };

  ws.onerror = (evt) => onError?.(evt);
  ws.onclose = () => {
    // Exponential backoff: 1s, 2s, 4s, 8s, 16s, capped at 30s
    const retryCount = ws._retryCount ?? _retryCount;
    const delay = Math.min(1000 * Math.pow(2, retryCount), 30000);
    console.log(`[WS] Reconnecting in ${delay / 1000}s (attempt ${retryCount + 1})`);
    setTimeout(() => connectEvents(onMessage, onError, retryCount + 1), delay);
  };

  ws._retryCount = _retryCount;
  return ws;
}
