import { useState, useCallback } from 'react';
import { useApi } from '../hooks/useApi';
import { listConnections, testConnection, listProviders } from '../api';
import Card, { StatCard } from '../components/Card';
import StatusBadge from '../components/StatusBadge';
import Button from '../components/Button';
import Spinner from '../components/Spinner';
import PageHeader from '../components/PageHeader';
import EmptyState from '../components/EmptyState';
import clsx from 'clsx';
import {
  Plug,
  Database,
  Cloud,
  Globe,
  Radio,
  RefreshCw,
  CheckCircle,
  XCircle,
  Clock,
  Server,
  Zap,
  Search,
  Filter,
  ChevronDown,
  ChevronRight,
  Activity,
} from 'lucide-react';

const TYPE_ICONS = {
  // SQL
  postgres: Database, snowflake: Cloud, clickhouse: Database, redshift: Database,
  bigquery: Cloud, duckdb: Database, mysql: Database, sqlite: Database,
  oracle: Database, sqlserver: Database, cockroachdb: Database, timescaledb: Database,
  // Storage
  s3: Cloud, gcs: Cloud,
  // HTTP
  http: Globe, webhook: Globe,
  // Streaming
  kafka: Radio, rabbitmq: Radio, kinesis: Radio, pubsub: Radio, redis: Radio,
  // SaaS
  salesforce: Globe, hubspot: Globe, stripe: Globe, github: Globe, jira: Globe, slack: Globe,
  // Document/NoSQL
  mongodb: Database, dynamodb: Cloud, cassandra: Database, elasticsearch: Database,
  redis_kv: Database, neo4j: Database,
};

const TYPE_COLORS = {
  // SQL
  postgres: 'text-blue-400', snowflake: 'text-cyan-400', clickhouse: 'text-amber-400',
  redshift: 'text-red-400', bigquery: 'text-blue-300', duckdb: 'text-yellow-400',
  mysql: 'text-orange-400', sqlite: 'text-sky-400', oracle: 'text-red-500',
  sqlserver: 'text-blue-500', cockroachdb: 'text-green-400', timescaledb: 'text-amber-300',
  // Storage
  s3: 'text-orange-400', gcs: 'text-blue-400',
  // HTTP
  http: 'text-green-400', webhook: 'text-purple-400',
  // Streaming
  kafka: 'text-teal-400', rabbitmq: 'text-orange-300', kinesis: 'text-orange-500',
  pubsub: 'text-blue-400', redis: 'text-red-400',
  // SaaS
  salesforce: 'text-blue-400', hubspot: 'text-orange-400', stripe: 'text-purple-400',
  github: 'text-gray-300', jira: 'text-blue-500', slack: 'text-pink-400',
  // Document/NoSQL
  mongodb: 'text-green-500', dynamodb: 'text-orange-400', cassandra: 'text-cyan-400',
  elasticsearch: 'text-yellow-400', redis_kv: 'text-red-400', neo4j: 'text-blue-300',
};

const CATEGORY_LABELS = {
  sql: 'SQL Databases',
  storage: 'Object Storage',
  http: 'HTTP / Webhooks',
  stream: 'Streaming',
  saas: 'SaaS Platforms',
  document: 'Document / NoSQL',
};

const SQL_TYPES = ['postgres', 'snowflake', 'clickhouse', 'redshift', 'bigquery', 'duckdb', 'mysql', 'sqlite', 'oracle', 'sqlserver', 'cockroachdb', 'timescaledb'];
const STORAGE_TYPES = ['s3', 'gcs'];
const HTTP_TYPES = ['http', 'webhook'];
const STREAM_TYPES = ['kafka', 'rabbitmq', 'kinesis', 'pubsub', 'redis', 'redis_stream'];
const SAAS_TYPES = ['salesforce', 'sfdc', 'hubspot', 'stripe', 'github', 'jira', 'slack'];
const DOC_TYPES = ['mongodb', 'mongo', 'dynamodb', 'cassandra', 'elasticsearch', 'opensearch', 'es', 'redis_kv', 'neo4j'];

function categorize(connType) {
  const t = (connType || '').toLowerCase();
  if (SQL_TYPES.includes(t)) return 'sql';
  if (STORAGE_TYPES.includes(t)) return 'storage';
  if (HTTP_TYPES.includes(t)) return 'http';
  if (STREAM_TYPES.includes(t)) return 'stream';
  if (SAAS_TYPES.includes(t)) return 'saas';
  if (DOC_TYPES.includes(t)) return 'document';
  return 'sql';
}

function ConnectionCard({ conn, onTest, testResult, isTesting }) {
  const [expanded, setExpanded] = useState(false);
  const Icon = TYPE_ICONS[conn.connType] || Database;
  const color = TYPE_COLORS[conn.connType] || 'text-conduit-400';

  const capabilities = conn.capabilities || [];

  return (
    <Card className="h-full">
      <div className="flex flex-col h-full">
        {/* Header */}
        <div className="flex items-start justify-between mb-3">
          <div className="flex items-center gap-3 flex-1 min-w-0">
            <div className={clsx('w-10 h-10 rounded-lg flex items-center justify-center bg-conduit-800/80 border border-conduit-700/50', color)}>
              <Icon size={20} />
            </div>
            <div className="min-w-0 flex-1">
              <h3 className="text-base font-semibold text-conduit-50 truncate">{conn.name}</h3>
              <p className="text-xs text-conduit-400 font-mono">{conn.connType}</p>
            </div>
          </div>
          <StatusBadge
            status={conn.status || 'configured'}
            dot
          />
        </div>

        {/* Connection details */}
        <div className="space-y-2 mb-4 flex-1">
          {conn.host && (
            <div className="flex items-center gap-2 text-sm">
              <Server size={14} className="text-conduit-500 shrink-0" />
              <span className="text-conduit-300 truncate font-mono text-xs">{conn.host}</span>
              {conn.port && <span className="text-conduit-500 text-xs">:{conn.port}</span>}
            </div>
          )}
          {conn.database && (
            <div className="flex items-center gap-2 text-sm">
              <Database size={14} className="text-conduit-500 shrink-0" />
              <span className="text-conduit-300 text-xs">{conn.database}</span>
            </div>
          )}

          {/* Capabilities */}
          {capabilities.length > 0 && (
            <div className="flex flex-wrap gap-1.5 pt-1">
              {capabilities.map((cap) => (
                <span
                  key={cap}
                  className="px-2 py-0.5 text-[10px] font-medium uppercase tracking-wide bg-conduit-800/50 text-conduit-400 rounded border border-conduit-700/30"
                >
                  {cap}
                </span>
              ))}
            </div>
          )}
        </div>

        {/* Extra config (expandable) */}
        {conn.extra && Object.keys(conn.extra).length > 0 && (
          <div className="mb-3">
            <button
              onClick={() => setExpanded(!expanded)}
              className="flex items-center gap-1 text-xs text-conduit-500 hover:text-conduit-300 transition-colors"
            >
              {expanded ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
              Configuration ({Object.keys(conn.extra).length} params)
            </button>
            {expanded && (
              <div className="mt-2 p-2.5 bg-conduit-900/50 rounded-lg border border-conduit-700/30 space-y-1">
                {Object.entries(conn.extra).map(([key, val]) => (
                  <div key={key} className="flex items-center justify-between text-xs">
                    <span className="text-conduit-500 font-mono">{key}</span>
                    <span className="text-conduit-300 font-mono truncate ml-3 max-w-[200px]">
                      {typeof val === 'string' ? val : JSON.stringify(val)}
                    </span>
                  </div>
                ))}
              </div>
            )}
          </div>
        )}

        {/* Test result */}
        {testResult && (
          <div className={clsx(
            'mb-3 p-2.5 rounded-lg border text-xs',
            testResult.result?.success
              ? 'bg-emerald-900/20 border-emerald-700/30'
              : 'bg-red-900/20 border-red-700/30'
          )}>
            <div className="flex items-center gap-2 mb-1">
              {testResult.result?.success ? (
                <CheckCircle size={14} className="text-emerald-400" />
              ) : (
                <XCircle size={14} className="text-red-400" />
              )}
              <span className={testResult.result?.success ? 'text-emerald-300' : 'text-red-300'}>
                {testResult.result?.success ? 'Connection healthy' : 'Connection failed'}
              </span>
            </div>
            {testResult.result?.message && (
              <p className="text-conduit-400 ml-6">{testResult.result.message}</p>
            )}
            <div className="flex items-center gap-4 ml-6 mt-1">
              {testResult.result?.latencyMs > 0 && (
                <span className="text-conduit-500">
                  <Clock size={10} className="inline mr-1" />
                  {testResult.result.latencyMs}ms
                </span>
              )}
              {testResult.result?.serverVersion && (
                <span className="text-conduit-500">
                  v{testResult.result.serverVersion}
                </span>
              )}
            </div>
          </div>
        )}

        {/* Actions */}
        <div className="flex gap-2 pt-3 border-t border-conduit-700/50">
          <button
            onClick={() => onTest(conn.name)}
            disabled={isTesting}
            className={clsx(
              'flex-1 flex items-center justify-center gap-1.5 px-3 py-2 text-xs font-medium rounded-lg border transition-all',
              isTesting
                ? 'bg-conduit-800/30 text-conduit-500 border-conduit-700/30 cursor-not-allowed'
                : 'bg-conduit-800/50 hover:bg-conduit-700/50 text-conduit-300 border-conduit-700/50'
            )}
          >
            <RefreshCw size={14} className={isTesting ? 'animate-spin' : ''} />
            {isTesting ? 'Testing...' : 'Test Connection'}
          </button>
        </div>
      </div>
    </Card>
  );
}

export default function Connections() {
  const { data: connections, loading, error, refetch } = useApi(listConnections);
  const { data: providers } = useApi(listProviders);
  const [testResults, setTestResults] = useState({});
  const [testingNames, setTestingNames] = useState(new Set());
  const [searchQuery, setSearchQuery] = useState('');
  const [filterCategory, setFilterCategory] = useState('all');
  const [isTestingAll, setIsTestingAll] = useState(false);

  const handleTest = useCallback(async (name) => {
    setTestingNames((prev) => new Set([...prev, name]));
    try {
      const result = await testConnection(name);
      setTestResults((prev) => ({ ...prev, [name]: result }));
    } catch (err) {
      setTestResults((prev) => ({
        ...prev,
        [name]: { result: { success: false, message: err.message, latencyMs: 0 } },
      }));
    } finally {
      setTestingNames((prev) => {
        const next = new Set(prev);
        next.delete(name);
        return next;
      });
    }
  }, []);

  const handleTestAll = useCallback(async () => {
    if (!connections?.length) return;
    setIsTestingAll(true);
    const promises = connections.map((c) => handleTest(c.name));
    await Promise.allSettled(promises);
    setIsTestingAll(false);
  }, [connections, handleTest]);

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-screen">
        <Spinner />
      </div>
    );
  }

  const connList = connections || [];

  // Filtering
  const filtered = connList.filter((c) => {
    const matchesSearch = !searchQuery ||
      c.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
      (c.connType || '').toLowerCase().includes(searchQuery.toLowerCase()) ||
      (c.host || '').toLowerCase().includes(searchQuery.toLowerCase());
    const matchesCategory = filterCategory === 'all' || categorize(c.connType) === filterCategory;
    return matchesSearch && matchesCategory;
  });

  // Group by category
  const grouped = {};
  for (const conn of filtered) {
    const cat = categorize(conn.connType);
    if (!grouped[cat]) grouped[cat] = [];
    grouped[cat].push(conn);
  }

  // Stats
  const totalConnections = connList.length;
  const uniqueTypes = new Set(connList.map((c) => c.connType)).size;
  const testedCount = Object.keys(testResults).length;
  const healthyCount = Object.values(testResults).filter((r) => r.result?.success).length;

  return (
    <div className="min-h-screen bg-gradient-to-br from-conduit-950 via-conduit-900 to-conduit-950">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <PageHeader
          title="Connections"
          description="Manage data source and destination connections"
          action={
            <Button
              onClick={handleTestAll}
              disabled={isTestingAll || connList.length === 0}
              className="flex items-center gap-2"
            >
              <Zap className="w-4 h-4" />
              {isTestingAll ? 'Testing All...' : 'Test All Connections'}
            </Button>
          }
        />

        {error && (
          <div className="mb-6 p-4 bg-red-900/20 border border-red-700/50 rounded-lg text-red-200">
            Error loading connections: {error}
          </div>
        )}

        {/* Stats Row */}
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-4 mb-8">
          <StatCard label="Connections" value={totalConnections} icon={Plug} />
          <StatCard label="Provider Types" value={uniqueTypes} icon={Database} />
          <StatCard label="Tested" value={testedCount} sub={`of ${totalConnections}`} icon={Activity} />
          <StatCard
            label="Healthy"
            value={testedCount > 0 ? healthyCount : '--'}
            sub={testedCount > 0 ? `${Math.round((healthyCount / testedCount) * 100)}% pass rate` : 'run tests first'}
            icon={CheckCircle}
          />
        </div>

        {/* Search & Filter Bar */}
        <div className="flex flex-col sm:flex-row gap-3 mb-6">
          <div className="relative flex-1">
            <Search size={16} className="absolute left-3 top-1/2 -translate-y-1/2 text-conduit-500" />
            <input
              type="text"
              placeholder="Search connections..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="w-full pl-10 pr-4 py-2.5 bg-conduit-800/50 border border-conduit-700/50 rounded-lg text-conduit-50 placeholder-conduit-500 focus:outline-none focus:border-conduit-500 focus:ring-1 focus:ring-conduit-500 glass transition-all text-sm"
            />
          </div>
          <div className="flex gap-2 flex-wrap">
            {['all', 'sql', 'storage', 'http', 'stream', 'saas', 'document'].map((cat) => (
              <button
                key={cat}
                onClick={() => setFilterCategory(cat)}
                className={clsx(
                  'px-3 py-2 text-xs font-medium rounded-lg border transition-all whitespace-nowrap',
                  filterCategory === cat
                    ? 'bg-conduit-600/20 text-conduit-300 border-conduit-600/30'
                    : 'bg-conduit-800/30 text-conduit-500 border-conduit-700/30 hover:text-conduit-300 hover:border-conduit-600/30'
                )}
              >
                {cat === 'all' ? 'All' : CATEGORY_LABELS[cat] || cat}
              </button>
            ))}
          </div>
        </div>

        {connList.length === 0 ? (
          <EmptyState
            icon={Plug}
            title="No connections configured"
            description="Add connection configurations in your connections.yaml file to get started."
          />
        ) : filtered.length === 0 ? (
          <EmptyState
            icon={Search}
            title="No matching connections"
            description="Try adjusting your search or filter criteria."
          />
        ) : (
          <div className="space-y-8">
            {Object.entries(grouped).map(([category, conns]) => (
              <div key={category}>
                <h2 className="text-sm font-semibold text-conduit-400 uppercase tracking-wide mb-4 flex items-center gap-2">
                  <span className="w-8 h-px bg-conduit-700/50" />
                  {CATEGORY_LABELS[category] || category}
                  <span className="text-conduit-600 font-normal">({conns.length})</span>
                  <span className="flex-1 h-px bg-conduit-700/50" />
                </h2>
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                  {conns.map((conn) => (
                    <ConnectionCard
                      key={conn.name}
                      conn={conn}
                      onTest={handleTest}
                      testResult={testResults[conn.name]}
                      isTesting={testingNames.has(conn.name)}
                    />
                  ))}
                </div>
              </div>
            ))}
          </div>
        )}

        {/* Supported Providers */}
        {providers && providers.length > 0 && (
          <div className="mt-12">
            <h2 className="text-sm font-semibold text-conduit-400 uppercase tracking-wide mb-4 flex items-center gap-2">
              <span className="w-8 h-px bg-conduit-700/50" />
              Supported Providers
              <span className="flex-1 h-px bg-conduit-700/50" />
            </h2>
            {Object.entries(
              providers.reduce((acc, p) => {
                const cat = p.category || categorize(p.id);
                if (!acc[cat]) acc[cat] = [];
                acc[cat].push(p);
                return acc;
              }, {})
            ).map(([cat, catProviders]) => (
              <div key={cat} className="mb-6">
                <h3 className="text-xs font-medium text-conduit-500 uppercase tracking-wide mb-2">
                  {CATEGORY_LABELS[cat] || cat} ({catProviders.length})
                </h3>
                <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 gap-3">
                  {catProviders.map((p) => {
                    const Icon = TYPE_ICONS[p.id] || Database;
                    const color = TYPE_COLORS[p.id] || 'text-conduit-400';
                    return (
                      <div
                        key={p.id}
                        className="glass p-3 flex items-center gap-3"
                      >
                        <Icon size={18} className={color} />
                        <div className="min-w-0">
                          <p className="text-sm font-medium text-conduit-200 truncate">{p.name}</p>
                          {p.aliases && p.aliases.length > 0 && (
                            <p className="text-[10px] text-conduit-500 truncate">
                              {p.aliases.join(', ')}
                            </p>
                          )}
                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
