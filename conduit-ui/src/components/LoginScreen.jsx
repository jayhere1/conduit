import { useState } from 'react';
import { Activity, Key, ArrowRight, AlertCircle, Loader2 } from 'lucide-react';
import { useAuth } from './AuthProvider';

export default function LoginScreen() {
  const { login, error: authError } = useAuth();
  const [apiKey, setApiKey] = useState('');
  const [loading, setLoading] = useState(false);
  const [localError, setLocalError] = useState(null);

  const error = localError || authError;

  const handleSubmit = async (e) => {
    e.preventDefault();
    const trimmed = apiKey.trim();

    if (!trimmed) {
      setLocalError('Please enter an API key');
      return;
    }

    setLocalError(null);
    setLoading(true);

    const result = await login(trimmed);

    setLoading(false);
    if (!result.success) {
      setLocalError(result.error);
    }
  };

  return (
    <div className="min-h-screen bg-conduit-950 flex items-center justify-center p-4">
      <div className="w-full max-w-md">
        {/* Logo */}
        <div className="text-center mb-8">
          <div className="inline-flex items-center justify-center w-16 h-16 rounded-2xl bg-conduit-600 mb-4">
            <Activity size={32} className="text-white" />
          </div>
          <h1 className="text-2xl font-bold text-white tracking-tight">Conduit</h1>
          <p className="text-sm text-conduit-400 mt-1">Pipeline Orchestrator</p>
        </div>

        {/* Login card */}
        <div className="bg-gray-900 border border-gray-800 rounded-xl p-6 shadow-2xl">
          <div className="text-center mb-6">
            <h2 className="text-lg font-semibold text-white">Sign in</h2>
            <p className="text-sm text-gray-400 mt-1">
              Enter your API key to access the dashboard
            </p>
          </div>

          <form onSubmit={handleSubmit} className="space-y-4">
            <div>
              <label
                htmlFor="api-key"
                className="block text-sm font-medium text-gray-300 mb-1.5"
              >
                API Key
              </label>
              <div className="relative">
                <Key
                  size={16}
                  className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-500"
                />
                <input
                  id="api-key"
                  type="password"
                  value={apiKey}
                  onChange={(e) => {
                    setApiKey(e.target.value);
                    setLocalError(null);
                  }}
                  placeholder="cdt_..."
                  autoFocus
                  autoComplete="off"
                  spellCheck={false}
                  className="w-full bg-gray-800 border border-gray-700 rounded-lg pl-10 pr-4 py-2.5 text-white text-sm font-mono placeholder-gray-600 focus:outline-none focus:border-conduit-500 focus:ring-1 focus:ring-conduit-500/30 transition-colors"
                />
              </div>
            </div>

            {/* Error message */}
            {error && (
              <div className="flex items-center gap-2 text-red-400 text-sm bg-red-500/10 border border-red-500/20 rounded-lg px-3 py-2">
                <AlertCircle size={14} className="shrink-0" />
                <span>{error}</span>
              </div>
            )}

            <button
              type="submit"
              disabled={loading}
              className="w-full flex items-center justify-center gap-2 bg-conduit-600 hover:bg-conduit-500 disabled:bg-conduit-600/50 disabled:cursor-not-allowed text-white font-medium py-2.5 px-4 rounded-lg text-sm transition-colors"
            >
              {loading ? (
                <>
                  <Loader2 size={16} className="animate-spin" />
                  Authenticating...
                </>
              ) : (
                <>
                  Sign in
                  <ArrowRight size={16} />
                </>
              )}
            </button>
          </form>

          <div className="mt-6 pt-4 border-t border-gray-800">
            <p className="text-xs text-gray-500 text-center">
              Get your API key from your Conduit administrator or generate one
              with <code className="text-gray-400">conduit serve --auth-enabled</code>
            </p>
          </div>
        </div>

        {/* Version */}
        <p className="text-center text-xs text-gray-600 mt-6 font-mono">v0.1.0</p>
      </div>
    </div>
  );
}
