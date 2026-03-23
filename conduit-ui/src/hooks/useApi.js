import { useState, useEffect, useCallback } from 'react';

/**
 * Generic hook for calling API functions with loading/error state.
 * Usage: const { data, loading, error, refetch } = useApi(api.listDags);
 */
export function useApi(apiFn, deps = []) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  const fetch = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const result = await apiFn();
      setData(result);
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }, deps);

  useEffect(() => {
    fetch();
  }, [fetch]);

  return { data, loading, error, refetch: fetch };
}

/**
 * Hook for polling an API endpoint at an interval.
 * Pass intervalMs as null to disable polling.
 */
export function usePolling(apiFn, intervalMs = 5000, deps = []) {
  const state = useApi(apiFn, deps);

  useEffect(() => {
    if (!intervalMs) return; // Don't poll if interval is null/0/false
    const id = setInterval(state.refetch, intervalMs);
    return () => clearInterval(id);
  }, [state.refetch, intervalMs]);

  return state;
}
