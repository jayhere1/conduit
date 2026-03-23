// ─── Cron Expression to Human-Readable String Utility ──────────────────────

// Convert a cron expression to a human-readable schedule string.
// Examples: "0 6 * * *" -> "Daily at 6:00 AM", "0 8 * * 1" -> "Weekly on Monday at 8:00 AM"
export function humanCron(expression) {
  if (!expression || expression === '@manual') {
    return 'Manual';
  }

  // Handle common shortcuts
  if (expression.startsWith('@')) {
    const shortcuts = {
      '@yearly': 'Yearly on January 1st at midnight',
      '@annually': 'Yearly on January 1st at midnight',
      '@monthly': 'Monthly on the 1st at midnight',
      '@weekly': 'Weekly on Sunday at midnight',
      '@daily': 'Daily at midnight',
      '@hourly': 'Every hour',
      '@reboot': 'On system reboot',
    };
    return shortcuts[expression] || expression;
  }

  const parts = expression.trim().split(/\s+/);
  if (parts.length !== 5) {
    return expression; // Fallback for malformed cron
  }

  const [minute, hour, dayOfMonth, month, dayOfWeek] = parts;

  // Helper: format hour to 12-hour time
  const formatTime = (h, m) => {
    const hourNum = parseInt(h, 10);
    const minNum = parseInt(m, 10);
    if (hourNum === 0 && minNum === 0) return 'midnight';
    if (hourNum === 12 && minNum === 0) return 'noon';
    const period = hourNum >= 12 ? 'PM' : 'AM';
    const displayHour = hourNum === 0 ? 12 : hourNum > 12 ? hourNum - 12 : hourNum;
    const displayMin = minNum.toString().padStart(2, '0');
    return `${displayHour}:${displayMin} ${period}`;
  };

  // Helper: get day name
  const getDayName = (dayNum) => {
    const days = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
    return days[parseInt(dayNum, 10)] || dayNum;
  };

  // ─── Check for every N minutes ───────────────────────────────
  if (minute.startsWith('*/') && hour === '*' && dayOfMonth === '*' && month === '*' && dayOfWeek === '*') {
    const interval = parseInt(minute.split('/')[1], 10);
    return `Every ${interval} minute${interval > 1 ? 's' : ''}`;
  }

  // ─── Check for every N hours ────────────────────────────────
  if (minute === '0' && hour.startsWith('*/') && dayOfMonth === '*' && month === '*' && dayOfWeek === '*') {
    const interval = parseInt(hour.split('/')[1], 10);
    return `Every ${interval} hour${interval > 1 ? 's' : ''}`;
  }

  // ─── Check for specific times ────────────────────────────────
  if (minute !== '*' && hour !== '*' && minute !== '*/') {
    const timeStr = formatTime(hour, minute);

    // Daily
    if (dayOfMonth === '*' && month === '*' && dayOfWeek === '*') {
      return `Daily at ${timeStr}`;
    }

    // Weekdays
    if (dayOfMonth === '*' && month === '*' && dayOfWeek === '1-5') {
      return `Weekdays at ${timeStr}`;
    }

    // Specific day of week
    if (dayOfMonth === '*' && month === '*' && dayOfWeek !== '*') {
      const days = dayOfWeek.split(',');
      if (days.length === 1) {
        const dayName = getDayName(dayOfWeek);
        return `Weekly on ${dayName} at ${timeStr}`;
      }
      const dayNames = days.map(getDayName).join(', ');
      return `${dayNames} at ${timeStr}`;
    }

    // Monthly on specific day
    if (dayOfMonth !== '*' && month === '*' && dayOfWeek === '*') {
      const dayNum = parseInt(dayOfMonth, 10);
      const suffix = getDayOfMonthSuffix(dayNum);
      return `Monthly on the ${dayNum}${suffix} at ${timeStr}`;
    }

    // Specific date (month and day)
    if (dayOfMonth !== '*' && month !== '*' && dayOfWeek === '*') {
      const monthNum = parseInt(month, 10);
      const dayNum = parseInt(dayOfMonth, 10);
      const monthName = getMonthName(monthNum);
      const suffix = getDayOfMonthSuffix(dayNum);
      return `${monthName} ${dayNum}${suffix} at ${timeStr}`;
    }
  }

  // ─── Hourly patterns ────────────────────────────────────────
  if (minute !== '*' && hour === '*' && dayOfMonth === '*' && month === '*' && dayOfWeek === '*') {
    const minNum = parseInt(minute, 10);
    return `At minute ${minNum} of every hour`;
  }

  // Fallback: return the raw expression
  return expression;
}

/**
 * Get the suffix for day of month (st, nd, rd, th).
 */
function getDayOfMonthSuffix(day) {
  if (day > 3 && day < 21) return 'th';
  switch (day % 10) {
    case 1:
      return 'st';
    case 2:
      return 'nd';
    case 3:
      return 'rd';
    default:
      return 'th';
  }
}

/**
 * Get the month name.
 */
function getMonthName(monthNum) {
  const months = [
    '', 'January', 'February', 'March', 'April', 'May', 'June',
    'July', 'August', 'September', 'October', 'November', 'December',
  ];
  return months[monthNum] || monthNum;
}
