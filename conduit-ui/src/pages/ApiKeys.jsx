import { useState, useEffect, useCallback } from 'react';
import { Key, Plus, Trash2, Copy, Clock, Shield, AlertCircle, CheckCircle } from 'lucide-react';
import * as api from '../api';

const ROLES = [
  { value: 'viewer', label: 'Viewer', desc: 'Read-only access to all data' },
  { value: 'operator', label: 'Operator', desc: 'Read/write: trigger runs, manage envs, plan/apply' },
  { value: 'admin', label: 'Admin', desc: 'Full access including API key management' },
];

function roleBadge(role) {
  const colors = {
    admin: 'bg-red-500/20 text-red-300 border-red-500/30',
    operator: 'bg-amber-500/20 text-amber-300 border-amber-500/30',
    viewer: 'bg-blue-500/20 text-blue-300 border-blue-500/30',
  };
  return (
    <span className={`px-2 py-0.5 text-xs font-medium rounded border ${colors[role] || colors.viewer}`}>
      {role}
    </span>
  );
}

function timeAgo(dateStr) {
  if (!dateStr) return 'Never';
  const diff = Date.now() - new Date(dateStr).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return 'Just now';
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

export default function ApiKeys() {
  const [keys, setKeys] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [showCreate, setShowCreate] = useState(false);
  const [newKey, setNewKey] = useState(null);
  const [copied, setCopied] = useState(false);

  // Create form state
  const [name, setName] = useState('');
  const [role, setRole] = useState('viewer');
  const [description, setDescription] = useState('');

  const fetchKeys = useCallback(async () => {
    try {
      setLoading(true);
      const data = await api.listApiKeys();
      setKeys(data);
      setError(null);
    } catch (err) {
      if (err.status === 401) {
        setError('Authentication required. Set an API key to manage keys.');
      } else if (err.status === 403) {
        setError('Admin role required to manage API keys.');
      } else {
        setError(err.message);
      }
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { fetchKeys(); }, [fetchKeys]);

  const handleCreate = async (e) => {
    e.preventDefault();
    try {
      const result = await api.createApiKey(name, role, description || undefined);
      setNewKey(result);
      setShowCreate(false);
      setName('');
      setRole('viewer');
      setDescription('');
      fetchKeys();
    } catch (err) {
      setError(err.message);
    }
  };

  const handleRevoke = async (id, keyName) => {
    if (!confirm(`Revoke API key "${keyName}"? This cannot be undone.`)) return;
    try {
      await api.revokeApiKey(id);
      fetchKeys();
    } catch (err) {
      setError(err.message);
    }
  };

  const copyKey = async (text) => {
    await navigator.clipboard.writeText(text);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const activeKeys = keys.filter((k) => !k.revoked);
  const revokedKeys = keys.filter((k) => k.revoked);

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white flex items-center gap-2">
            <Key size={24} /> API Keys
          </h1>
          <p className="text-gray-400 mt-1">
            Manage authentication keys for the Conduit API
          </p>
        </div>
        <button
          onClick={() => setShowCreate(true)}
          className="flex items-center gap-2 px-4 py-2 bg-conduit-600 hover:bg-conduit-500 text-white rounded-lg text-sm font-medium transition-colors"
        >
          <Plus size={16} /> Create Key
        </button>
      </div>

      {/* Newly created key banner */}
      {newKey && (
        <div className="bg-green-500/10 border border-green-500/30 rounded-lg p-4">
          <div className="flex items-start gap-3">
            <CheckCircle size={20} className="text-green-400 mt-0.5 shrink-0" />
            <div className="flex-1">
              <p className="text-green-300 font-medium">API key created successfully</p>
              <p className="text-green-400/70 text-sm mt-1">
                Copy this key now — it will not be shown again.
              </p>
              <div className="mt-3 flex items-center gap-2">
                <code className="flex-1 bg-gray-900 text-green-300 px-3 py-2 rounded font-mono text-sm">
                  {newKey.key}
                </code>
                <button
                  onClick={() => copyKey(newKey.key)}
                  className="px-3 py-2 bg-gray-700 hover:bg-gray-600 text-white rounded text-sm flex items-center gap-1"
                >
                  <Copy size={14} />
                  {copied ? 'Copied!' : 'Copy'}
                </button>
              </div>
              <button
                onClick={() => setNewKey(null)}
                className="mt-2 text-sm text-green-400/60 hover:text-green-400"
              >
                Dismiss
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Error */}
      {error && (
        <div className="bg-red-500/10 border border-red-500/30 rounded-lg p-4 flex items-center gap-3">
          <AlertCircle size={18} className="text-red-400" />
          <p className="text-red-300 text-sm">{error}</p>
        </div>
      )}

      {/* Create form modal */}
      {showCreate && (
        <div className="bg-gray-800 border border-gray-700 rounded-lg p-6">
          <h2 className="text-lg font-semibold text-white mb-4">Create API Key</h2>
          <form onSubmit={handleCreate} className="space-y-4">
            <div>
              <label className="block text-sm font-medium text-gray-300 mb-1">Name</label>
              <input
                type="text"
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="e.g. ci-pipeline, dashboard-readonly"
                className="w-full bg-gray-900 border border-gray-600 rounded-lg px-3 py-2 text-white text-sm focus:outline-none focus:border-conduit-500"
                required
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-300 mb-1">Role</label>
              <div className="space-y-2">
                {ROLES.map((r) => (
                  <label
                    key={r.value}
                    className={`flex items-start gap-3 p-3 rounded-lg border cursor-pointer transition-colors ${
                      role === r.value
                        ? 'border-conduit-500 bg-conduit-600/10'
                        : 'border-gray-700 hover:border-gray-600'
                    }`}
                  >
                    <input
                      type="radio"
                      name="role"
                      value={r.value}
                      checked={role === r.value}
                      onChange={(e) => setRole(e.target.value)}
                      className="mt-0.5"
                    />
                    <div>
                      <span className="text-white text-sm font-medium">{r.label}</span>
                      <p className="text-gray-400 text-xs mt-0.5">{r.desc}</p>
                    </div>
                  </label>
                ))}
              </div>
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-300 mb-1">
                Description <span className="text-gray-500">(optional)</span>
              </label>
              <input
                type="text"
                value={description}
                onChange={(e) => setDescription(e.target.value)}
                placeholder="What is this key used for?"
                className="w-full bg-gray-900 border border-gray-600 rounded-lg px-3 py-2 text-white text-sm focus:outline-none focus:border-conduit-500"
              />
            </div>
            <div className="flex justify-end gap-3 pt-2">
              <button
                type="button"
                onClick={() => setShowCreate(false)}
                className="px-4 py-2 text-gray-400 hover:text-white text-sm"
              >
                Cancel
              </button>
              <button
                type="submit"
                className="px-4 py-2 bg-conduit-600 hover:bg-conduit-500 text-white rounded-lg text-sm font-medium"
              >
                Create Key
              </button>
            </div>
          </form>
        </div>
      )}

      {/* Active keys table */}
      {loading ? (
        <div className="text-gray-400 text-center py-12">Loading keys...</div>
      ) : (
        <>
          <div className="bg-gray-800 border border-gray-700 rounded-lg overflow-hidden">
            <div className="px-4 py-3 border-b border-gray-700">
              <h2 className="text-sm font-semibold text-white">
                Active Keys ({activeKeys.length})
              </h2>
            </div>
            {activeKeys.length === 0 ? (
              <div className="px-4 py-8 text-center text-gray-500 text-sm">
                No active API keys. Create one to get started.
              </div>
            ) : (
              <table className="w-full">
                <thead>
                  <tr className="text-xs text-gray-400 uppercase tracking-wider">
                    <th className="text-left px-4 py-2">Name</th>
                    <th className="text-left px-4 py-2">Prefix</th>
                    <th className="text-left px-4 py-2">Role</th>
                    <th className="text-left px-4 py-2">Created</th>
                    <th className="text-left px-4 py-2">Last Used</th>
                    <th className="text-left px-4 py-2">Expires</th>
                    <th className="text-right px-4 py-2">Actions</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-700/50">
                  {activeKeys.map((k) => (
                    <tr key={k.id} className="hover:bg-gray-700/30">
                      <td className="px-4 py-3">
                        <div className="text-white text-sm font-medium">{k.name}</div>
                        {k.description && (
                          <div className="text-gray-500 text-xs mt-0.5">{k.description}</div>
                        )}
                      </td>
                      <td className="px-4 py-3">
                        <code className="text-gray-400 text-xs bg-gray-900 px-2 py-0.5 rounded">
                          {k.prefix}...
                        </code>
                      </td>
                      <td className="px-4 py-3">{roleBadge(k.role)}</td>
                      <td className="px-4 py-3 text-gray-400 text-sm">{timeAgo(k.createdAt)}</td>
                      <td className="px-4 py-3 text-gray-400 text-sm">
                        <span className="flex items-center gap-1">
                          <Clock size={12} /> {timeAgo(k.lastUsedAt)}
                        </span>
                      </td>
                      <td className="px-4 py-3 text-gray-400 text-sm">
                        {k.expiresAt
                          ? new Date(k.expiresAt).toLocaleDateString()
                          : 'Never'}
                      </td>
                      <td className="px-4 py-3 text-right">
                        <button
                          onClick={() => handleRevoke(k.id, k.name)}
                          className="p-1.5 text-gray-500 hover:text-red-400 transition-colors"
                          title="Revoke key"
                        >
                          <Trash2 size={14} />
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* Revoked keys */}
          {revokedKeys.length > 0 && (
            <div className="bg-gray-800/50 border border-gray-700/50 rounded-lg overflow-hidden">
              <div className="px-4 py-3 border-b border-gray-700/50">
                <h2 className="text-sm font-semibold text-gray-400">
                  Revoked Keys ({revokedKeys.length})
                </h2>
              </div>
              <table className="w-full">
                <tbody className="divide-y divide-gray-700/30">
                  {revokedKeys.map((k) => (
                    <tr key={k.id} className="opacity-50">
                      <td className="px-4 py-2 text-gray-500 text-sm">{k.name}</td>
                      <td className="px-4 py-2">
                        <code className="text-gray-600 text-xs">{k.prefix}...</code>
                      </td>
                      <td className="px-4 py-2">{roleBadge(k.role)}</td>
                      <td className="px-4 py-2 text-gray-600 text-sm">Revoked</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </>
      )}

      {/* Info panel */}
      <div className="bg-gray-800/50 border border-gray-700/50 rounded-lg p-4">
        <div className="flex items-start gap-3">
          <Shield size={18} className="text-conduit-400 mt-0.5" />
          <div className="text-sm text-gray-400">
            <p className="text-gray-300 font-medium mb-1">Authentication</p>
            <p>
              Pass your API key in the <code className="text-conduit-300">Authorization</code> header:
            </p>
            <code className="block mt-2 bg-gray-900 px-3 py-2 rounded text-xs text-gray-300">
              Authorization: Bearer cdt_your_key_here
            </code>
          </div>
        </div>
      </div>
    </div>
  );
}
