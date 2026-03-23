import { useState, useEffect } from 'react';
import {
  X,
  Play,
  Calendar,
  Settings,
  Plus,
  Trash2,
  Loader,
  CheckCircle,
  AlertTriangle,
  Layers,
} from 'lucide-react';
import { triggerRun, listEnvironments } from '../api';
import { useApi } from '../hooks/useApi';
import clsx from 'clsx';

// ─── Modal Backdrop ──────────────────────────────────────────────────────────

function ModalBackdrop({ children, onClose }) {
  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm"
      onClick={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      {children}
    </div>
  );
}

// ─── Trigger Run Modal ───────────────────────────────────────────────────────

export default function TriggerRunModal({ dagId, dagName, onClose, onTriggered }) {
  const [environment, setEnvironment] = useState('production');
  const [logicalDate, setLogicalDate] = useState('');
  const [configEntries, setConfigEntries] = useState([]);
  const [submitting, setSubmitting] = useState(false);
  const [result, setResult] = useState(null);
  const [error, setError] = useState(null);

  // Fetch available environments
  const { data: environments } = useApi(listEnvironments);

  // Set default logical date to today
  useEffect(() => {
    const today = new Date().toISOString().split('T')[0];
    setLogicalDate(today);
  }, []);

  const addConfigEntry = () => {
    setConfigEntries([...configEntries, { key: '', value: '' }]);
  };

  const removeConfigEntry = (index) => {
    setConfigEntries(configEntries.filter((_, i) => i !== index));
  };

  const updateConfigEntry = (index, field, value) => {
    const updated = [...configEntries];
    updated[index] = { ...updated[index], [field]: value };
    setConfigEntries(updated);
  };

  const handleSubmit = async () => {
    setSubmitting(true);
    setError(null);

    try {
      // Build config object from entries
      const config = {};
      configEntries.forEach(({ key, value }) => {
        if (key.trim()) config[key.trim()] = value;
      });

      const response = await triggerRun(dagId, environment);
      setResult(response);
      onTriggered?.(response);
    } catch (e) {
      setError(e.message || 'Failed to trigger run');
    } finally {
      setSubmitting(false);
    }
  };

  // Success state
  if (result) {
    return (
      <ModalBackdrop onClose={onClose}>
        <div className="w-full max-w-md bg-conduit-900 border border-conduit-700/50 rounded-2xl shadow-2xl p-6">
          <div className="text-center">
            <div className="w-12 h-12 rounded-full bg-green-500/20 border border-green-500/30 mx-auto flex items-center justify-center mb-4">
              <CheckCircle size={24} className="text-green-400" />
            </div>
            <h3 className="text-lg font-semibold text-white mb-2">Run Triggered</h3>
            <p className="text-sm text-gray-400 mb-4">
              {result.message || `Run ${(result.runId || result.run_id)?.substring(0, 12)} has been queued`}
            </p>
            <div className="flex items-center justify-center gap-2 text-xs text-gray-500 mb-6">
              <span className="font-mono bg-conduit-800/50 px-2 py-1 rounded">
                {(result.runId || result.run_id)?.substring(0, 16)}
              </span>
            </div>
            <div className="flex gap-3 justify-center">
              <button
                onClick={onClose}
                className="px-4 py-2 rounded-lg text-sm text-gray-400 hover:text-white transition-colors"
              >
                Close
              </button>
              {(result.runId || result.run_id) && (
                <a
                  href={`/runs/${result.runId || result.run_id}/live`}
                  className="px-4 py-2 rounded-lg text-sm bg-conduit-600/20 border border-conduit-600/30 text-conduit-300 hover:bg-conduit-600/30 transition-colors"
                >
                  View Live Execution
                </a>
              )}
            </div>
          </div>
        </div>
      </ModalBackdrop>
    );
  }

  return (
    <ModalBackdrop onClose={onClose}>
      <div className="w-full max-w-lg bg-conduit-900 border border-conduit-700/50 rounded-2xl shadow-2xl">
        {/* Header */}
        <div className="flex items-center justify-between p-5 border-b border-conduit-800/50">
          <div className="flex items-center gap-3">
            <div className="w-9 h-9 rounded-lg bg-conduit-600/20 border border-conduit-600/30 flex items-center justify-center">
              <Play size={18} className="text-conduit-400" />
            </div>
            <div>
              <h2 className="text-base font-semibold text-white">Trigger Run</h2>
              <p className="text-xs text-gray-500">{dagName || dagId}</p>
            </div>
          </div>
          <button
            onClick={onClose}
            className="p-1.5 rounded-lg hover:bg-conduit-800/50 text-gray-500 hover:text-gray-300 transition-colors"
          >
            <X size={16} />
          </button>
        </div>

        {/* Body */}
        <div className="p-5 space-y-5">
          {/* Environment */}
          <div>
            <label className="flex items-center gap-2 text-xs font-semibold text-gray-400 mb-2">
              <Layers size={12} />
              Environment
            </label>
            <select
              value={environment}
              onChange={(e) => setEnvironment(e.target.value)}
              className="w-full px-3 py-2 rounded-lg bg-conduit-950/50 border border-conduit-800/50 text-sm text-gray-200 focus:outline-none focus:border-conduit-600/50 appearance-none"
            >
              <option value="production">production</option>
              {environments?.map((env) => (
                <option key={env.name || env} value={env.name || env}>
                  {env.name || env}
                </option>
              ))}
            </select>
          </div>

          {/* Logical Date */}
          <div>
            <label className="flex items-center gap-2 text-xs font-semibold text-gray-400 mb-2">
              <Calendar size={12} />
              Logical Date
            </label>
            <input
              type="date"
              value={logicalDate}
              onChange={(e) => setLogicalDate(e.target.value)}
              className="w-full px-3 py-2 rounded-lg bg-conduit-950/50 border border-conduit-800/50 text-sm text-gray-200 focus:outline-none focus:border-conduit-600/50"
            />
            <p className="text-[10px] text-gray-600 mt-1">
              The data interval date this run processes ({'{{ ds }}'} in templates)
            </p>
          </div>

          {/* Configuration Overrides */}
          <div>
            <label className="flex items-center justify-between mb-2">
              <span className="flex items-center gap-2 text-xs font-semibold text-gray-400">
                <Settings size={12} />
                Configuration Overrides
              </span>
              <button
                onClick={addConfigEntry}
                className="flex items-center gap-1 text-[10px] text-conduit-400 hover:text-conduit-300 transition-colors"
              >
                <Plus size={10} /> Add
              </button>
            </label>

            {configEntries.length === 0 ? (
              <p className="text-xs text-gray-600 italic">
                No overrides. Click "Add" to pass custom config.
              </p>
            ) : (
              <div className="space-y-2">
                {configEntries.map((entry, idx) => (
                  <div key={idx} className="flex items-center gap-2">
                    <input
                      type="text"
                      placeholder="key"
                      value={entry.key}
                      onChange={(e) => updateConfigEntry(idx, 'key', e.target.value)}
                      className="flex-1 px-2.5 py-1.5 rounded-lg bg-conduit-950/50 border border-conduit-800/50 text-xs text-gray-200 placeholder-gray-600 focus:outline-none focus:border-conduit-600/50 font-mono"
                    />
                    <span className="text-gray-600 text-xs">=</span>
                    <input
                      type="text"
                      placeholder="value"
                      value={entry.value}
                      onChange={(e) => updateConfigEntry(idx, 'value', e.target.value)}
                      className="flex-1 px-2.5 py-1.5 rounded-lg bg-conduit-950/50 border border-conduit-800/50 text-xs text-gray-200 placeholder-gray-600 focus:outline-none focus:border-conduit-600/50 font-mono"
                    />
                    <button
                      onClick={() => removeConfigEntry(idx)}
                      className="p-1 rounded text-gray-600 hover:text-red-400 transition-colors"
                    >
                      <Trash2 size={12} />
                    </button>
                  </div>
                ))}
              </div>
            )}
          </div>

          {/* Error */}
          {error && (
            <div className="flex items-center gap-2 p-3 rounded-lg bg-red-500/10 border border-red-500/30">
              <AlertTriangle size={14} className="text-red-400 shrink-0" />
              <p className="text-xs text-red-400">{error}</p>
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="flex items-center justify-end gap-3 p-5 border-t border-conduit-800/50">
          <button
            onClick={onClose}
            disabled={submitting}
            className="px-4 py-2 rounded-lg text-sm text-gray-400 hover:text-white transition-colors"
          >
            Cancel
          </button>
          <button
            onClick={handleSubmit}
            disabled={submitting}
            className={clsx(
              'flex items-center gap-2 px-5 py-2 rounded-lg text-sm font-medium transition-all',
              submitting
                ? 'bg-conduit-700/30 text-gray-500 cursor-not-allowed'
                : 'bg-conduit-600 text-white hover:bg-conduit-500 shadow-lg shadow-conduit-600/20'
            )}
          >
            {submitting ? (
              <>
                <Loader size={14} className="animate-spin" />
                Triggering...
              </>
            ) : (
              <>
                <Play size={14} />
                Trigger Run
              </>
            )}
          </button>
        </div>
      </div>
    </ModalBackdrop>
  );
}
