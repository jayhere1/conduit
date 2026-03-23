import { useState, useCallback, useEffect, useRef } from 'react';
import { listEvents } from '../api';
import { useApi } from '../hooks/useApi';
import { useWebSocket } from '../hooks/useWebSocket';
import Card from '../components/Card';
import StatusBadge from '../components/StatusBadge';
import Button from '../components/Button';
import Spinner from '../components/Spinner';
import PageHeader from '../components/PageHeader';
import EmptyState from '../components/EmptyState';
import {
  Radio,
  Pause,
  Play,
  Trash2,
  Filter,
  Clock,
  ChevronDown,
  ChevronRight,
  Wifi,
  WifiOff,
} from 'lucide-react';

// ─── Event Type Detection & Styling ───────────────────────────────────────

function getEventTypeCategory(eventType) {
  if (!eventType) return 'system';
  const type = eventType.toLowerCase();
  if (type.includes('task')) return 'task';
  if (type.includes('dag')) return 'dag';
  if (type.includes('error') || type.includes('failed')) return 'error';
  return 'system';
}

function getEventTypeColor(eventType) {
  const category = getEventTypeCategory(eventType);
  const colors = {
    task: 'bg-blue-500/15 text-blue-400 border-blue-500/25',
    dag: 'bg-purple-500/15 text-purple-400 border-purple-500/25',
    error: 'bg-red-500/15 text-red-400 border-red-500/25',
    system: 'bg-gray-500/15 text-gray-400 border-gray-500/25',
  };
  return colors[category] || colors.system;
}

// ─── Event Formatting ─────────────────────────────────────────────────────

function formatTimestamp(ts) {
  if (!ts) return '';
  const date = new Date(ts);
  return date.toLocaleTimeString('en-US', {
    hour12: false,
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    fractionalSecondDigits: 3,
  });
}

function getEventSummary(event) {
  const type = event.event_type || event.type || 'Unknown';

  if (event.task_id) {
    return `${type} - Task: ${event.task_id}`;
  }
  if (event.dag_id) {
    return `${type} - DAG: ${event.dag_id}`;
  }
  if (event.run_id) {
    return `${type} - Run: ${event.run_id.substring(0, 8)}`;
  }
  if (event.message) {
    return `${type} - ${event.message}`;
  }
  return type;
}

// ─── Event Row Component ──────────────────────────────────────────────────

function EventRow({ event, isExpanded, onToggleExpand, isHistorical }) {
  const eventType = event.event_type || event.type || 'Unknown';
  const timestamp = event.timestamp || event._ts;
  const summary = getEventSummary(event);
  const colorClass = getEventTypeColor(eventType);

  return (
    <div className="border border-conduit-700/30 rounded-lg overflow-hidden hover:border-conduit-700/50 transition-colors">
      <button
        onClick={onToggleExpand}
        className="w-full text-left"
      >
        <div className="px-4 py-3 bg-conduit-900/40 hover:bg-conduit-900/60 transition-colors flex items-center gap-3">
          {/* Expand/Collapse Icon */}
          <div className="flex-shrink-0">
            {isExpanded ? (
              <ChevronDown className="w-4 h-4 text-conduit-400" />
            ) : (
              <ChevronRight className="w-4 h-4 text-conduit-400" />
            )}
          </div>

          {/* Timestamp */}
          <div className="font-mono text-xs text-gray-500 flex-shrink-0 w-16">
            {formatTimestamp(timestamp)}
          </div>

          {/* Event Type Badge */}
          <div className="flex-shrink-0">
            <span
              className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium border ${colorClass}`}
            >
              {eventType}
            </span>
          </div>

          {/* Summary */}
          <div className="flex-grow truncate text-sm text-gray-300">
            {summary}
          </div>

          {/* Historical Label */}
          {isHistorical && (
            <span className="flex-shrink-0 text-xs text-gray-500 font-medium">
              Historical
            </span>
          )}
        </div>
      </button>

      {/* Expanded Payload */}
      {isExpanded && (
        <div className="px-4 py-3 bg-conduit-950/80 border-t border-conduit-700/30">
          <div className="bg-conduit-950 rounded p-3 overflow-x-auto">
            <pre className="text-xs font-mono text-gray-300 leading-relaxed whitespace-pre-wrap break-words">
              {JSON.stringify(event, null, 2)}
            </pre>
          </div>
        </div>
      )}
    </div>
  );
}

// ─── Main Events Page ─────────────────────────────────────────────────────

export default function Events() {
  // Live WebSocket events
  const { events: liveEvents, connected, clear: clearLive } = useWebSocket(200);

  // Historical events from API
  const { data: historicalEvents, loading: histLoading } = useApi(listEvents, []);

  // Local state
  const [isLive, setIsLive] = useState(true);
  const [expandedIndices, setExpandedIndices] = useState(new Set());
  const [eventTypeFilter, setEventTypeFilter] = useState('all');
  const scrollContainerRef = useRef(null);
  const prevEventCountRef = useRef(0);

  // Auto-scroll to newest event when live and new events arrive
  useEffect(() => {
    if (isLive && liveEvents.length > prevEventCountRef.current && scrollContainerRef.current) {
      setTimeout(() => {
        if (scrollContainerRef.current) {
          scrollContainerRef.current.scrollTop = 0;
        }
      }, 0);
    }
    prevEventCountRef.current = liveEvents.length;
  }, [liveEvents, isLive]);

  // Toggle expand/collapse for an event
  const handleToggleExpand = useCallback((index) => {
    setExpandedIndices((prev) => {
      const next = new Set(prev);
      if (next.has(index)) {
        next.delete(index);
      } else {
        next.add(index);
      }
      return next;
    });
  }, []);

  // Filter events by type
  const filterEventsByType = useCallback((events, filterType) => {
    if (filterType === 'all') return events;
    return events.filter((event) => {
      const eventType = event.event_type || event.type || '';
      return eventType.toLowerCase().includes(filterType.toLowerCase());
    });
  }, []);

  // Get display events based on live/pause toggle
  const displayedLiveEvents = filterEventsByType(liveEvents, eventTypeFilter);
  const displayedHistoricalEvents = filterEventsByType(historicalEvents || [], eventTypeFilter);

  // Extract unique event types for filter dropdown
  const allEventTypes = new Set();
  [...liveEvents, ...(historicalEvents || [])].forEach((event) => {
    const type = event.event_type || event.type || '';
    if (type) allEventTypes.add(type);
  });
  const eventTypeOptions = Array.from(allEventTypes).sort();

  // Handle clear
  const handleClear = () => {
    clearLive();
    setExpandedIndices(new Set());
  };

  // Handle load historical
  const handleLoadHistorical = async () => {
    // Trigger refetch by calling the API directly
    try {
      await listEvents();
    } catch (err) {
      console.error('Failed to load historical events:', err);
    }
  };

  return (
    <div className="p-6">
      {/* Page Header with Connection Status */}
      <div className="flex items-center justify-between mb-6">
        <PageHeader
          title="Events"
          actions={
            connected ? (
              <div className="flex items-center gap-2 px-3 py-1.5 rounded-lg bg-emerald-500/10 border border-emerald-500/25">
                <Wifi className="w-4 h-4 text-emerald-400" />
                <span className="text-xs text-emerald-400 font-medium">Connected</span>
              </div>
            ) : (
              <div className="flex items-center gap-2 px-3 py-1.5 rounded-lg bg-red-500/10 border border-red-500/25">
                <WifiOff className="w-4 h-4 text-red-400" />
                <span className="text-xs text-red-400 font-medium">Disconnected</span>
              </div>
            )
          }
        />
      </div>

      {/* Controls Bar */}
      <div className="glass rounded-lg p-4 mb-6">
        <div className="flex flex-wrap items-center gap-3">
          {/* Live/Pause Toggle */}
          <Button
            variant={isLive ? 'primary' : 'secondary'}
            size="md"
            icon={isLive ? Pause : Play}
            onClick={() => setIsLive(!isLive)}
          >
            {isLive ? 'Live' : 'Paused'}
          </Button>

          {/* Clear Button */}
          <Button
            variant="danger"
            size="md"
            icon={Trash2}
            onClick={handleClear}
            disabled={displayedLiveEvents.length === 0}
          >
            Clear
          </Button>

          {/* Event Type Filter */}
          <div className="relative flex items-center gap-2">
            <Filter className="w-4 h-4 text-gray-400" />
            <select
              value={eventTypeFilter}
              onChange={(e) => setEventTypeFilter(e.target.value)}
              className="bg-conduit-900/50 border border-conduit-700/50 rounded-lg px-3 py-2 text-sm text-gray-200 focus:outline-none focus:ring-2 focus:ring-conduit-500 appearance-none pr-8"
            >
              <option value="all">All Event Types</option>
              {eventTypeOptions.map((type) => (
                <option key={type} value={type}>
                  {type}
                </option>
              ))}
            </select>
            <ChevronDown className="absolute right-2.5 w-4 h-4 text-gray-400 pointer-events-none" />
          </div>

          {/* Event Count */}
          <div className="ml-auto flex items-center gap-2 text-sm text-gray-400">
            <Clock className="w-4 h-4" />
            <span className="font-medium">{displayedLiveEvents.length} events</span>
          </div>
        </div>
      </div>

      {/* Live Events Section */}
      <div className="mb-8">
        <h3 className="text-sm font-semibold text-gray-300 mb-3 flex items-center gap-2">
          <Radio className="w-4 h-4 text-conduit-400" />
          Live Events
        </h3>

        {!isLive && displayedLiveEvents.length > 0 && (
          <div className="mb-3 p-3 bg-amber-500/10 border border-amber-500/25 rounded-lg">
            <p className="text-xs text-amber-400">
              Paused - Click "Live" to resume auto-scroll
            </p>
          </div>
        )}

        {displayedLiveEvents.length === 0 ? (
          <EmptyState
            icon={Clock}
            title={liveEvents.length === 0 ? 'No events yet' : 'No events match filter'}
            description={
              liveEvents.length === 0
                ? 'Waiting for events... make sure the server is running.'
                : 'Try changing the event type filter.'
            }
          />
        ) : (
          <div
            ref={scrollContainerRef}
            className="glass rounded-lg overflow-y-auto max-h-96 space-y-2 p-4"
          >
            {displayedLiveEvents.map((event, idx) => (
              <EventRow
                key={`live-${idx}`}
                event={event}
                isExpanded={expandedIndices.has(idx)}
                onToggleExpand={() => handleToggleExpand(idx)}
                isHistorical={false}
              />
            ))}
          </div>
        )}
      </div>

      {/* Divider */}
      <div className="my-8 border-t border-conduit-700/30" />

      {/* Historical Events Section */}
      <div>
        <div className="flex items-center justify-between mb-3">
          <h3 className="text-sm font-semibold text-gray-300 flex items-center gap-2">
            <Clock className="w-4 h-4 text-conduit-500" />
            Historical Events
          </h3>
          <Button
            variant="secondary"
            size="md"
            onClick={handleLoadHistorical}
            loading={histLoading}
          >
            Load History
          </Button>
        </div>

        {histLoading ? (
          <Spinner />
        ) : displayedHistoricalEvents.length === 0 ? (
          <EmptyState
            icon={Clock}
            title={!historicalEvents ? 'No history loaded' : 'No historical events'}
            description={
              !historicalEvents
                ? 'Click "Load History" to fetch events from the API.'
                : 'Try changing the event type filter.'
            }
          />
        ) : (
          <div className="glass rounded-lg overflow-y-auto max-h-96 space-y-2 p-4">
            {displayedHistoricalEvents.map((event, idx) => (
              <EventRow
                key={`hist-${idx}`}
                event={event}
                isExpanded={expandedIndices.has(`hist-${idx}`)}
                onToggleExpand={() => handleToggleExpand(`hist-${idx}`)}
                isHistorical={true}
              />
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
