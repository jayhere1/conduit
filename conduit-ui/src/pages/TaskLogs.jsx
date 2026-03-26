import { useState, useEffect, useRef, useMemo } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import {
  ArrowLeft,
  Terminal,
  AlertTriangle,
  Search,
  Download,
  Copy,
  Check,
  ArrowDown,
  RefreshCw,
  WrapText,
  Clock,
  Hash,
  Repeat,
  FileText,
} from 'lucide-react';
import { getRun } from '../api';
import { useApi } from '../hooks/useApi';
import { formatDuration, formatShortTime } from '../utils/time';
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
  const levelMatch = line.match(/\b(ERROR|WARN(?:ING)?|INFO|DEBUG|TRACE)\b/i);
  const level = levelMatch ? levelMatch[1].toUpperCase() : null;
  const tsMatch = line.match(/^(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)/);
  const timestamp = tsMatch ? tsMatch[1] : null;
  const isConduit = line.includes('CONDUIT::');
  const isMetric = line.includes('CONDUIT::METRIC::');
  return { level, timestamp, isConduit, isMetric, raw: line };
}

// ─── Syntax-Highlighted Log Line ─────────────────────────────────────────────

function LogLine({ line, lineNumber, searchQuery, isHighlighted, wrap }) {
  const parsed = parseLogLine(line);
  const levelConfig = parsed.level ? LOG_LEVELS[parsed.level] || LOG_LEVELS.INFO : null;

  const highlightText = (text) => {
    if (!searchQuery) return text;
    const parts = text.split(new RegExp(`(${escapeRegex(searchQuery)})`, 'gi'));
    return parts.map((part, i) =>
      part.toLowerCase() === searchQuery.toLowerCase() ? (
        <mark key={i} className="bg-amber-500/40 text-amber-200 rounded px-0.5">{part}</mark>
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
      <span className="w-12 shrink-0 px-2 py-0.5 text-right text-gray-600 select-none border-r border-conduit-800/30 group-hover:text-gray-500">
        {lineNumber}
      </span>
      <span className={clsx('flex-1 px-3 py-0.5', wrap ? 'whitespace-pre-wrap break-all' : 'whitespace-pre overflow-x-auto')}>
        {parsed.timestamp && <span className="text-gray-600">{parsed.timestamp} </span>}
        {parsed.level && levelConfig && (
          <span className={clsx('font-semibold', levelConfig.color)}>[{parsed.level}] </span>
        )}
        {parsed.isConduit ? (
          <span className="text-purple-400">{highlightText(line.replace(parsed.timestamp || '', '').trim())}</span>
        ) : parsed.isMetric ? (
          <span className="text-conduit-400">{highlightText(line.replace(parsed.timestamp || '', '').trim())}</span>
        ) : (
          <span className={levelConfig?.color || 'text-gray-300'}>
            {highlightText(line.replace(parsed.timestamp || '', '').replace(new RegExp(`\\b${parsed.level}\\b`, 'i'), '').trim())}
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
  const [wrap, setWrap] = useState(true);
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
      result = result.filter((l) => l.content.toLowerCase().includes(searchQuery.toLowerCase()));
    }
    return result;
  }, [lines, levelFilter, searchQuery]);

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
  const warnCount = lines.filter((l) => l.parsed.level === 'WARN' || l.parsed.level === 'WARNING').length;

  return (
    <div className="flex flex-col h-full">
      {/* Sticky Toolbar */}
      <div className="flex items-center gap-2 p-3 border-b border-conduit-800/50 bg-conduit-900/50 backdrop-blur-sm sticky top-0 z-10">
        <div className="flex items-center gap-2 flex-1">
          <Icon size={14} className="text-conduit-500" />
          <span className="text-xs font-semibold text-gray-300">{title || 'Logs'}</span>
          <span className="text-xs text-gray-600">({filteredLines.length} / {lines.length})</span>
          {errorCount > 0 && (
            <span className="text-xs text-red-400 flex items-center gap-0.5">
              <AlertTriangle size={10} /> {errorCount}
            </span>
          )}
          {warnCount > 0 && (
            <span className="text-xs text-amber-400">{warnCount} warn</span>
          )}
        </div>

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

        <div className="flex items-center gap-0.5">
          {['all', 'ERROR', 'WARN', 'INFO', 'conduit'].map((f) => (
            <button
              key={f}
              onClick={() => setLevelFilter(f)}
              className={clsx(
                'px-2 py-0.5 rounded text-[10px] font-medium transition-colors',
                levelFilter === f ? 'bg-conduit-600/20 text-conduit-300' : 'text-gray-500 hover:text-gray-400'
              )}
            >
              {f === 'all' ? 'All' : f === 'conduit' ? 'Protocol' : f}
            </button>
          ))}
        </div>

        <div className="flex items-center gap-0.5 border-l border-conduit-800/40 pl-2">
          <button
            onClick={() => setWrap(!wrap)}
            className={clsx('p-1 rounded transition-colors', wrap ? 'text-conduit-400' : 'text-gray-600')}
            title={wrap ? 'Word wrap on' : 'Word wrap off'}
          >
            <WrapText size={12} />
          </button>
          <button
            onClick={() => setAutoScroll(!autoScroll)}
            className={clsx('p-1 rounded transition-colors', autoScroll ? 'text-conduit-400' : 'text-gray-600')}
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
      </div>

      {/* Log Lines */}
      <div
        ref={logContainerRef}
        className="flex-1 overflow-y-auto bg-conduit-950/80"
        style={{ height: 'calc(100vh - 280px)', minHeight: '300px' }}
      >
        {filteredLines.length === 0 ? (
          <div className="flex flex-col items-center justify-center py-20 text-gray-600">
            <FileText size={32} className="mb-3 text-gray-700" />
            <p className="text-sm font-medium">{lines.length === 0 ? 'No log output available' : 'No lines match filters'}</p>
            {lines.length === 0 && (
              <p className="text-xs text-gray-700 mt-1">Logs will appear here once the task produces output</p>
            )}
          </div>
        ) : (
          filteredLines.map((line) => (
            <LogLine
              key={line.number}
              line={line.content}
              lineNumber={line.number}
              searchQuery={searchQuery}
              isHighlighted={searchQuery && line.content.toLowerCase().includes(searchQuery.toLowerCase())}
              wrap={wrap}
            />
          ))
        )}
      </div>
    </div>
  );
}

// ─── Task Metadata Bar ──────────────────────────────────────────────────────

function TaskMetadata({ task, run }) {
  if (!task) return null;

  return (
    <div className="flex items-center gap-4 px-4 py-2.5 bg-conduit-900/40 border border-conduit-800/40 rounded-lg mb-4">
      <div className="flex items-center gap-1.5 text-xs text-gray-400">
        <Terminal size={12} className="text-conduit-500" />
        <span className="font-medium text-gray-300">{task.type || 'task'}</span>
      </div>
      <div className="w-px h-4 bg-conduit-800/50" />
      <div className="flex items-center gap-1.5 text-xs text-gray-400">
        <Clock size={12} />
        <span>{task.startedAt ? formatShortTime(task.startedAt) : '—'}</span>
      </div>
      <div className="flex items-center gap-1.5 text-xs text-gray-400">
        <Hash size={12} />
        <span>{formatDuration(task.startedAt, task.endedAt) || '—'}</span>
      </div>
      {(task.attempt > 0 || task.retries > 0) && (
        <>
          <div className="w-px h-4 bg-conduit-800/50" />
          <div className="flex items-center gap-1.5 text-xs text-amber-400">
            <Repeat size={12} />
            <span>Attempt {(task.attempt || 0) + 1}{task.retries > 0 ? ` / ${task.retries + 1}` : ''}</span>
          </div>
        </>
      )}
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

  useEffect(() => {
    if (!isRunning) return;
    const id = setInterval(refetch, 3000);
    return () => clearInterval(id);
  }, [isRunning, refetch]);

  const task = useMemo(() => {
    if (!run?.tasks) return null;
    return run.tasks.find((t) => t.name === taskId || t.id === taskId);
  }, [run, taskId]);

  const stdout = task?.logs || task?.stdout || null;
  const stderr = task?.stderr || null;

  return (
    <div className="min-h-screen bg-conduit-950 p-6">
      {/* Header */}
      <div className="flex items-center justify-between mb-4">
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
        <div className="flex items-center justify-center py-20"><Spinner /></div>
      ) : error ? (
        <Card>
          <div className="text-center py-8">
            <AlertTriangle size={32} className="mx-auto text-red-400 mb-3" />
            <p className="text-sm text-red-400">{error}</p>
          </div>
        </Card>
      ) : (
        <div>
          {/* Task Metadata */}
          <TaskMetadata task={task} run={run} />

          {/* Tab selector */}
          <div className="flex items-center gap-1 mb-3">
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
