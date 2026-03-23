import { createContext, useContext, useState, useEffect, useCallback } from 'react';
import { setAuthToken, clearAuthToken, getAuthToken } from '../api';

const AuthContext = createContext(null);

/**
 * Auth states:
 * - "checking"   — initial probe to see if auth is enabled
 * - "not_required" — server has auth disabled, skip login
 * - "unauthenticated" — auth is enabled, no valid key yet
 * - "authenticated"  — valid key, user info loaded
 */
export function AuthProvider({ children }) {
  const [authState, setAuthState] = useState('checking');
  const [user, setUser] = useState(null);
  const [error, setError] = useState(null);

  // Probe: is auth enabled on this server?
  const probe = useCallback(async (token) => {
    try {
      // If we have a stored token, try it
      if (token) {
        setAuthToken(token);
      }

      // Hit /auth/me — if auth is disabled, it returns a synthetic admin context
      // If auth is enabled and no token, it returns 401
      const res = await fetch('/api/v1/auth/me', {
        headers: token ? { Authorization: `Bearer ${token}` } : {},
      });

      if (res.ok) {
        const data = await res.json();
        if (token) {
          setAuthToken(token);
        }
        setUser(data);
        setAuthState('authenticated');
        setError(null);
        return;
      }

      if (res.status === 401) {
        // Auth is enabled but we don't have a valid token
        clearAuthToken();
        setAuthState('unauthenticated');
        return;
      }

      // Other error — might be server down, treat as no auth required
      // so the rest of the UI can show its own errors
      setAuthState('not_required');
    } catch {
      // Network error — server might not be running
      // Let the app load and show connection errors naturally
      setAuthState('not_required');
    }
  }, []);

  // On mount, check for a stored token and probe
  useEffect(() => {
    const stored = sessionStorage.getItem('conduit_api_key');
    probe(stored || null);
  }, [probe]);

  const login = useCallback(async (apiKey) => {
    setError(null);
    try {
      setAuthToken(apiKey);
      const res = await fetch('/api/v1/auth/me', {
        headers: { Authorization: `Bearer ${apiKey}` },
      });

      if (res.ok) {
        const data = await res.json();
        sessionStorage.setItem('conduit_api_key', apiKey);
        setUser(data);
        setAuthState('authenticated');
        return { success: true };
      }

      clearAuthToken();
      if (res.status === 401) {
        const body = await res.json().catch(() => ({}));
        const msg = body?.error?.message || 'Invalid API key';
        setError(msg);
        return { success: false, error: msg };
      }

      setError('Unexpected error');
      return { success: false, error: 'Unexpected error' };
    } catch (err) {
      clearAuthToken();
      setError('Connection failed');
      return { success: false, error: 'Connection failed' };
    }
  }, []);

  const logout = useCallback(() => {
    clearAuthToken();
    sessionStorage.removeItem('conduit_api_key');
    setUser(null);
    setAuthState('unauthenticated');
    setError(null);
  }, []);

  const value = {
    authState,
    user,
    error,
    login,
    logout,
    isAuthenticated: authState === 'authenticated' || authState === 'not_required',
    isAuthRequired: authState === 'unauthenticated',
    isChecking: authState === 'checking',
  };

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth() {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error('useAuth must be used within an AuthProvider');
  return ctx;
}
