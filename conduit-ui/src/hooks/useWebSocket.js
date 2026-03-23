import { useState, useEffect, useRef } from 'react';
import { connectEvents } from '../api';

/**
 * Hook for live WebSocket event streaming.
 * Returns recent events (capped at maxEvents) and connection status.
 */
export function useWebSocket(maxEvents = 200) {
  const [events, setEvents] = useState([]);
  const [connected, setConnected] = useState(false);
  const wsRef = useRef(null);

  useEffect(() => {
    wsRef.current = connectEvents(
      (data) => {
        setConnected(true);
        setEvents((prev) => {
          const next = [{ ...data, _ts: Date.now() }, ...prev];
          return next.slice(0, maxEvents);
        });
      },
      () => setConnected(false)
    );

    return () => wsRef.current?.close();
  }, [maxEvents]);

  const clear = () => setEvents([]);

  return { events, connected, clear };
}
