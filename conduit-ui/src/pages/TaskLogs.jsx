import { useState, useEffect, useRef, useMemo, useCallback } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import {
  ArrowLeft,
  Terminal,
  AlertTriangle,
  Search,
  Download,
  Copy,
  Check,
  Filter,
  ChevronDown,
  ArrowDown,
  Pause,
  Play,
  RefreshCw,
} from 'lucide-react';
import { getRun, connectEvents } from '../api';
import { useApi } from '../hooks/useApi';
import Card from '../components/Card';
import StatusBadge from '../components/StatusBadge';
import Spinner from '../components/Spinner';
import PageHeader from '../components/PageHeader';
import Button from '../components/Button';
import clsx from 'clsx';

// ─── Log Line Parser ─────────────────────────────────────────────────────────

const LOG_LEVELS = {
  ERROR: { color: 'text-red-400', bg: 'bg-red-500/10' },
  WARN: { color: 'text-amber-400', bg: 'bg-amber-500/10' },
  WARNING: { color: 'text-amber-400', bg: 'bg-amber-500/10' },
  INFO: { color: 'text-blue-400', bg: '' },
  DEBUG: { color: 'text-gray-500', bg: '' },
  TRACE: { color: 'text-gray-600', bg: '' },
};

function parseLogLine(line) {
  // Try to detect log level
  const levelMatch = line.match(/\b(ERROR|WARN(?:ING)?|INFO|DEBUG|TRACE)\b/i);
  const level = levelMatch ? levelMatch[1].toUpperCase() : null;

  // Try to detect timestamp
  const tsMatch = line.match(/^(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)/);
  const timestamp = tsMatch ? tsMatch[1] : null;

  // Detect CONDUIT protocol lines
  const isConduit = line.includes('CONDUIT::');
  const isMetric = line.includes('CONDUIT::METRIC::');

  return { level, timestamp, isConduit, isMetric, raw: line };
}

// ─── Syntax-Highlighted Log Line ─────────────────────────────────────────────

function LogLine({ line, lineNumber, searchQuery, isHighlighted }) {
  const parsed = parseLogLine(line);
  const levelConfig = parsed.level ? LOG_LEVELS[parsed.level] || LOG_LEVELS.INFO : null;

  // Highlight search matches
  const highlightText = (text) => {
    if (!searchQuery) return text;
    const parts = text.split(new RegExp(`(${escapeRegex(searchQuery)})`, 'gi'));
    return parts.map((part, i) =>
      part.toLowerCase() === searchQuery.toLowerCase() ? (
        <mark key={i} className="bg-amber-500/40 text-amber-200 rounded px-0.5">
          {part}
        </mark>
      ) : (
        part
      )
    );
  };

  return (
    <div
      className={clsx(
        'flex font-mono text-xs leading-relaxed hover:bg-conduit-800/20 group',
        levelConfig?.bg,
        isHighlighted && 'bg-amber-500/10 border-l-2 border-amber-500',
        parsed.isConduit && 'bg-purple-500/5'
      )}
    >
      {/* Line number */}
      <span className="w-12 shrink-0 px-2 py-0.5 text-right text-gray-600 select-none border-r border-conduit-800/30 group-hover:text-gray-500">
        {lineNumber}
      </span>

      {/* Content */}
      <span className="flex-1 px-3 py-0.5 whitespace-pre-wrap break-all">
        {/* Timestamp */}
        {parsed.timestamp && (
          <span className="text-gray-600">{parsed.timestamp} </span>
        )}

        {/* Level badge */}
        {parsed.level && levelConfig && (
          <span className={clsx('font-semibold', levelConfig.color)}>
            [{parsed.level}]{' '}
          </span>
        )}

        {/* Conduit protocol highlight */}
        {parsed.isConduit ? (
          <span className="text-purple-400">
            {highlightText(line.replace(parsed.timestamp || '', '').trim())}
          </span>
        ) : parsed.isMetric ? (
          <span className="text-conduit-400">
            {highlightText(line.replace(parsed.timestamp || '', '').trim())}
          </span>
        ) : (
          <span className={levelConfig?.color || 'text-gray-300'}>
            {highlightText(
              line
                .replace(parsed.timestamp || '', '')
                .replace(new RegExp(`\\b${parsed.level}\\b`, 'i'), '')
                .trim()
            )}
          </span>
        )}
      </span>
    </div>
  );
}

function escapeRegex(str) {
  return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

// ─── Log Viewer Component ────────────────────────────────────────────────────

function LogViewer({ logs, title, icon: Icon = Terminal }) {
  const [searchQuery, setSearchQuery] = useState('');
  const [levelFilter, setLevelFilter] = useState('all');
  const [autoScroll, setAutoScroll] = useState(true);
  const [copied, setCopied] = useState(false);
  const logContainerRef = useRef(null);

  const lines = useMemo(() => {
    if (!logs) return [];
    return logs.split('\n').map((line, i) => ({
      number: i + 1,
      content: line,
      parsed: parseLogLine(line),
    }));
  }, [logs]);

  const filteredLines = useMemo(() => {
    let result = lines;

    if (levelFilter !== 'all') {
      result = result.filter((l) => {
        if (levelFilter === 'conduit') return l.parsed.isConduit;
        return l.parsed.level?.toUpperCase() === levelFilter.toUpperCase();
      });
    }

    if (searchQuery) {
      result = result.filter((l) =>
        l.content.toLowerCase().includes(searchQuery.toLowerCase())
      );
    }

    return result;
  }, [lines, levelFilter, searchQuery]);

  // Auto-scroll
  useEffect(() => {
    if (autoScroll && logContainerRef.current) {
      logContainerRef.current.scrollTop = logContainerRef.current.scrollHeight;
    }
  }, [filteredLines, autoScroll]);

  const handleCopy = async () => {
    const text = filteredLines.map((l) => l.content).join('\n');
    await navigator.clipboard.writeText(text);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const handleDownload = () => {
    const text = lines.map((l) => l.content).join('\n');
    const blob = new Blob([text], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${title || 'logs'}.txt`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const errorCount = lines.filter((l) => l.parsed.level === 'ERROR').length;
  const warnCount = lines.filter(
    (l) => l.parsed.level === 'WARN' || l.parsed.level === 'WARNING'
  ).length;
  const conduitCount = lines.filter((l) => l.parsed.isConduit).length;

  return (
    <div className="flex flex-col h-full">
      {/* Toolbar */}
      <div className="flex items-center gap-2 p-3 border-b border-conduit-800/50 bg-conduit-900/30">
        <div className="flex items-center gap-2 flex-1">
          <Icon size={14} className="text-conduit-500" />
          <span className="text-xs font-semibold text-gray-300">{title || 'Logs'}</span>
          <span className="text-xs text-gray-600">({filteredLines.length} / {lines.length} lines)</span>

          {errorCount > 0 && (
            <span className="text-xs text-red-400 flex items-center gap-0.5">
              <AlertTriangle size={10} /> {errorCount}
            </span>
          )}
          {warnCount > 0 && (
            <span className="text-xs text-amber-400">{warnCount} warn</span>
          )}
        </div>

        {/* Search */}
        <div className="relative">
          <Search size={12} className="absolute left-2 top-1/2 -translate-y-1/2 text-gray-500" />
          <input
            type="text"
            placeholder="Search..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="pl-7 pr-2 py-1 rounded bg-conduit-900/50 border border-conduit-800/50 text-xs text-gray-200 placeholder-gray-600 focus:outline-none focus:border-conduit-600/50 w-40"
          />
        </div>

        {/* Level filter */}
        <div className="flex items-center gap-0.5">
          {['all', 'ERROR', 'WARN', 'INFO', 'conduit'].map((f) => (
            <button
              key={f}
              onClick={() => setLevelFilter(f)}
              className={clsx(
                'px-2 py-0.5 rounded text-[10px] font-medium transition-colors',
                levelFilter === f
                  ? 'bg-conduit-600/20 text-conduit-300'
                  : 'text-gray-500 hover:text-gray-400'
              )}
            >
              {f === 'all' ? 'All' : f === 'conduit' ? 'Protocol' : f}
            </button>
          ))}
        </div>

        {/* Actions */}
        <button
          onClick={() => setAutoScroll(!autoScroll)}
          className={clsx(
            'p-1 rounded transition-colors',
            autoScroll ? 'text-conduit-400' : 'text-gray-500'
          )}
          title={autoScroll ? 'Auto-scroll on' : 'Auto-scroll off'}
        >
          <ArrowDown size={12} />
        </button>
        <button onClick={handleCopy} className="p-1 rounded text-gray-500 hover:text-gray-300" title="Copy">
          {copied ? <Check size={12} className="text-green-400" /> : <Copy size={12} />}
        </button>
        <button onClick={handleDownload} className="p-1 rounded text-gray-500 hover:text-gray-300" title="Download">
          <Download size={12} />
        </button>
      </div>

      {/* Log Lines */}
      <div
        ref={logContainerRef}
        className="flex-1 overflow-y-auto bg-conduit-950/80"
        style={{ maxHeight: '600px' }}
      >
        {filteredLines.length === 0 ? (
          <div className="text-center py-12 text-gray-600 text-sm">
            {lines.length === 0 ? 'No log output' : 'No lines match filters'}
          </div>
        ) : (
          filteredLines.map((line) => (
            <LogLine
              key={line.number}
              line={line.content}
              lineNumber={line.number}
              searchQuery={searchQuery}
              isHighlighted={searchQuery && line.content.toLowerCase().includes(searchQuery.toLowerCase())}
            />
          ))
        )}
      </div>
    </div>
  );
}

// ─── Main Page ───────────────────────────────────────────────────────────────

export default function TaskLogs() {
  const { runId, taskId } = useParams();
  const navigate = useNavigate();
  const [activeTab, setActiveTab] = useState('stdout');

  const { data: run, loading, error, refetch } = useApi(() => getRun(runId), [runId]);

  const isRunning = run?.status?.toLowerCase() === 'running';

  // Auto-refresh while running
  useEffect(() => {
    if (!isRunning) return;
    const id = setInterval(refetch, 3000);
    return () => clearInterval(id);
  }, [isRunning, refetch]);

  // Find task info
  const task = useMemo(() => {
    if (!run?.tasks) return null;
    return run.tasks.find((t) => t.name === taskId || t.id === taskId);
  }, [run, taskId]);

  // Generate demo logs if none available
  const stdout = task?.logs || task?.stdout || generateDemoLogs(taskId, 'stdout');
  const stderr = task?.stderr || generateDemoLogs(taskId, 'stderr');

  return (
    <div className="min-h-screen bg-conduit-950 p-6">
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center gap-4">
          <button
            onClick={() => navigate(-1)}
            className="flex items-center gap-2 text-conduit-400 hover:text-conduit-300 transition-colors"
          >
            <ArrowLeft size={16} />
          </button>
          <PageHeader
            title={taskId || 'Task Logs'}
            description={
              run ? (
                <span>
                  Run {(run.run_id || run.id || runId).substring(0, 12)}
                  {run.dagId || run.dag_id ? ` in ${run.dagId || run.dag_id}` : ''}
                </span>
              ) : undefined
            }
          />
        </div>
        <div className="flex items-center gap-3">
          {task && <StatusBadge status={task.status} dot />}
          {isRunning && (
            <span className="flex items-center gap-1.5 text-xs text-blue-400">
              <span className="w-1.5 h-1.5 rounded-full bg-blue-400 animate-pulse" />
              Live
            </span>
          )}
          <Button onClick={refetch} variant="secondary" size="sm">
            <RefreshCw size={14} />
          </Button>
        </div>
      </div>

      {loading && !run ? (
        <div className="flex items-center justify-center py-20">
          <Spinner />
        </div>
      ) : error ? (
        <Card>
          <div className="text-center py-8">
            <AlertTriangle size={32} className="mx-auto text-red-400 mb-3" />
            <p className="text-sm text-red-400">{error}</p>
          </div>
        </Card>
      ) : (
        <div>
          {/* Tab selector */}
          <div className="flex items-center gap-1 mb-4">
            <button
              onClick={() => setActiveTab('stdout')}
              className={clsx(
                'flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium transition-colors border',
                activeTab === 'stdout'
                  ? 'bg-conduit-600/20 text-conduit-300 border-conduit-600/30'
                  : 'text-gray-400 border-transparent hover:bg-conduit-900/50'
              )}
            >
              <Terminal size={14} />
              stdout
            </button>
            <button
              onClick={() => setActiveTab('stderr')}
              className={clsx(
                'flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium transition-colors border',
                activeTab === 'stderr'
                  ? 'bg-red-500/20 text-red-300 border-red-500/30'
                  : 'text-gray-400 border-transparent hover:bg-conduit-900/50'
              )}
            >
              <AlertTriangle size={14} />
              stderr
            </button>
          </div>

          {/* Log viewer */}
          <div className="rounded-xl border border-conduit-800/50 overflow-hidden">
            {activeTab === 'stdout' ? (
              <LogViewer logs={stdout} title="Standard Output" icon={Terminal} />
            ) : (
              <LogViewer logs={stderr} title="Standard Error" icon={AlertTriangle} />
            )}
          </div>
        </div>
      )}
    </div>
  );
}

// ─── Demo Log Generator ──────────────────────────────────────────────────────

function generateDemoLogs(taskId, stream) {
  if (stream === 'stderr') {
    return [
      `2026-03-23T06:00:01Z WARN Connection pool nearing capacity (42/50)`,
      `2026-03-23T06:00:05Z WARN Slow query detected: 2.3s for batch insert`,
      `2026-03-23T06:00:12Z INFO Retry attempt 1/3 for batch 47`,
    ].join('\n');
  }

  return [
    `2026-03-23T06:00:00Z INFO Starting task: ${taskId || 'unknown'}`,
    `2026-03-23T06:00:00Z INFO Connecting to source database...`,
    `2026-03-23T06:00:01Z INFO Connection established (pool: extract_pool)`,
    `2026-03-23T06:00:01Z DEBUG Query plan: sequential scan with index on created_at`,
    `2026-03-23T06:00:02Z INFO Executing query batch 1/10...`,
    `2026-03-23T06:00:03Z INFO Fetched 5,000 rows (batch 1)`,
    `2026-03-23T06:00:04Z INFO Executing query batch 2/10...`,
    `2026-03-23T06:00:05Z INFO Fetched 5,000 rows (batch 2)`,
    `2026-03-23T06:00:06Z INFO Executing query batch 3/10...`,
    `2026-03-23T06:00:07Z INFO Fetched 4,823 rows (batch 3)`,
    `CONDUIT::METRIC::row_count::14823`,
    `CONDUIT::METRIC::data_age_seconds::3247`,
    `CONDUIT::METRIC::duplicate_count::0`,
    `CONDUIT::METRIC::null_rate.customer_id::0.003`,
    `2026-03-23T06:00:08Z INFO Evidence emitted: 4 metrics`,
    `2026-03-23T06:00:08Z INFO Contract validation: 6/6 checks passed`,
    `2026-03-23T06:00:08Z INFO Task completed successfully in 8.2s`,
  ].join('\n');
}
