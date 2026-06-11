/**
 * Web Worker for Log Processing
 * Handles ANSI parsing and text processing off the main thread
 * This keeps the UI responsive even with massive log volumes
 */

// ANSI color code mapping
const ANSI_COLORS = {
  '0': { reset: true },
  '1': { bold: true },
  '2': { dim: true },
  '3': { italic: true },
  '4': { underline: true },
  '30': { color: '#1F2937' },
  '31': { color: '#DC2626' },
  '32': { color: '#10B981' },
  '33': { color: '#F59E0B' },
  '34': { color: '#3B82F6' },
  '35': { color: '#8B5CF6' },
  '36': { color: '#06B6D4' },
  '37': { color: '#E5E7EB' },
  '90': { color: '#6B7280' },
  '91': { color: '#EF4444' },
  '92': { color: '#22C55E' },
  '93': { color: '#FCD34D' },
  '94': { color: '#60A5FA' },
  '95': { color: '#A78BFA' },
  '96': { color: '#22D3EE' },
  '97': { color: '#F3F4F6' }
};

/**
 * Parse ANSI escape codes and convert to HTML spans with inline styles
 */
function parseAnsiCodes(text) {
  const ansiRegex = /\x1b\[([0-9;]*)m/g;
  let result = '';
  let lastIndex = 0;
  let currentStyles = [];
  
  let match;
  while ((match = ansiRegex.exec(text)) !== null) {
    // Add text before this escape code
    if (match.index > lastIndex) {
      const textBefore = text.substring(lastIndex, match.index);
      if (currentStyles.length > 0) {
        const styleStr = currentStyles.join('; ');
        result += `<span style="${styleStr}">${escapeHtml(textBefore)}</span>`;
      } else {
        result += escapeHtml(textBefore);
      }
    }
    
    // Parse the escape code
    const codes = match[1].split(';').filter(c => c);
    
    if (codes.length === 0 || codes[0] === '0') {
      // Reset all styles
      currentStyles = [];
    } else {
      // Apply new styles
      for (const code of codes) {
        const style = ANSI_COLORS[code];
        if (style) {
          if (style.reset) {
            currentStyles = [];
          } else {
            if (style.color) currentStyles.push(`color: ${style.color}`);
            if (style.bold) currentStyles.push('font-weight: bold');
            if (style.dim) currentStyles.push('opacity: 0.6');
            if (style.italic) currentStyles.push('font-style: italic');
            if (style.underline) currentStyles.push('text-decoration: underline');
          }
        }
      }
    }
    
    lastIndex = ansiRegex.lastIndex;
  }
  
  // Add remaining text
  if (lastIndex < text.length) {
    const remainingText = text.substring(lastIndex);
    if (currentStyles.length > 0) {
      const styleStr = currentStyles.join('; ');
      result += `<span style="${styleStr}">${escapeHtml(remainingText)}</span>`;
    } else {
      result += escapeHtml(remainingText);
    }
  }
  
  return result || escapeHtml(text);
}

/**
 * Escape HTML entities
 */
function escapeHtml(text) {
  return text
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
}

/**
 * Detect duplicate messages to reduce noise
 */
let lastMessage = '';
let duplicateCount = 0;

function isDuplicate(message) {
  if (message === lastMessage) {
    duplicateCount++;
    return true;
  }
  lastMessage = message;
  duplicateCount = 0;
  return false;
}

/**
 * Process a batch of log messages
 */
function processBatch(messages) {
  const processed = [];
  
  for (const msg of messages) {
    const { timestamp, level, message } = msg;
    
    // Skip duplicate consecutive messages
    if (isDuplicate(message)) {
      // Only report every 100th duplicate
      if (duplicateCount % 100 === 0) {
        processed.push({
          timestamp,
          level: 'WARNING',
          html: `<span style="color: #F59E0B;">⚠️ Previous message repeated ${duplicateCount} times (showing every 100th)</span>`
        });
      }
      continue;
    }
    
    // Reset duplicate counter notification
    if (duplicateCount > 0 && duplicateCount < 100) {
      // Don't spam for small duplicate counts
      duplicateCount = 0;
    }
    
    // Parse ANSI codes
    const parsedMessage = parseAnsiCodes(message);
    
    processed.push({
      timestamp,
      level,
      html: parsedMessage
    });
  }
  
  return processed;
}

// Listen for messages from main thread
self.addEventListener('message', (event) => {
  const { type, data } = event.data;
  
  if (type === 'PROCESS_BATCH') {
    // Process the batch of messages
    const processed = processBatch(data);
    
    // Send back processed results
    self.postMessage({
      type: 'BATCH_PROCESSED',
      data: processed
    });
  } else if (type === 'RESET') {
    // Reset duplicate detection
    lastMessage = '';
    duplicateCount = 0;
    
    self.postMessage({
      type: 'RESET_COMPLETE'
    });
  }
});

// Send ready signal
self.postMessage({ type: 'READY' });
