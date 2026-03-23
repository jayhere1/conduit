// ─── Shared Time Formatting Utilities ─────────────────────────────────────────

/**
 * Format an ISO timestamp as a relative string (e.g. "5m ago", "2h 15m ago").
 */
export function formatRelativeTime(timestamp) {
  if (!timestamp) return '';

  const date = new Date(timestamp);
  const now = new Date();
  const secondsAgo = Math.floor((now - date) / 1000);

  if (secondsAgo < 0) return 'just now';
  if (secondsAgo < 60) return `${secondsAgo}s ago`;
  const minutesAgo = Math.floor(secondsAgo / 60);
  if (minutesAgo < 60) return `${minutesAgo}m ago`;
  const hoursAgo = Math.floor(minutesAgo / 60);
  if (hoursAgo < 24) return `${hoursAgo}h ${minutesAgo % 60}m ago`;
  const daysAgo = Math.floor(hoursAgo / 24);
  return `${daysAgo}d ago`;
}

/**
 * Format an ISO timestamp as a locale time string (e.g. "02:15 PM").
 */
export function formatAbsoluteTime(timestamp) {
  if (!timestamp) return '';
  return new Date(timestamp).toLocaleString();
}

/**
 * Format the duration between two timestamps (or from start to now).
 */
export function formatDuration(startedAt, endedAt) {
  if (!startedAt) return '';

  const start = new Date(startedAt);
  const end = endedAt ? new Date(endedAt) : new Date();
  const seconds = Math.floor((end - start) / 1000);

  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ${seconds % 60}s`;
  const hours = Math.floor(minutes / 60);
  return `${hours}h ${minutes % 60}m`;
}

/**
 * Format milliseconds as a human-readable duration.
 */
export function formatMs(ms) {
  if (!ms) return '0s';
  const seconds = Math.floor(ms / 1000);
  const minutes = Math.floor(seconds / 60);
  const hours = Math.floor(minutes / 60);

  if (hours > 0) return `${hours}h ${minutes % 60}m`;
  if (minutes > 0) return `${minutes}m ${seconds % 60}s`;
  return `${seconds}s`;
}

/**
 * Format an ISO timestamp as a short time (e.g. "02:15 PM").
 */
export function formatShortTime(dateString) {
  if (!dateString) return 'N/A';
  return new Date(dateString).toLocaleTimeString('en-US', {
    hour: '2-digit',
    minute: '2-digit',
  });
}
