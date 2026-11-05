import { formatBytes } from '@/utils/formatBytes';
import { formatNumber, formatPercent } from '@/utils/formatNumber';
import { formatDate } from '@/utils/formatDate';

describe('formatters', () => {
  it('formats bytes', () => {
    expect(formatBytes(0)).toBe('0 B');
    expect(formatBytes(1024)).toBe('1.0 KB');
    expect(formatBytes(1024 * 1024)).toBe('1.0 MB');
  });

  it('formats numbers', () => {
    expect(formatNumber(1234567)).toBe('1,234,567');
  });

  it('formats percentages', () => {
    expect(formatPercent(0.123)).toBe('12.3%');
  });

  it('formats dates', () => {
    expect(formatDate('2026-01-15T10:00:00Z')).toMatch(/^2026-01-15/);
  });
});
