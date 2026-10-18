import { describe, expect, it } from 'vitest';
import { safeArticleUrl } from './articleLinks';

describe('explicit external navigation', () => {
  it.each(['https://example.test/path?x=1#section', 'http://example.test/', 'HTTPS://EXAMPLE.test/a%20b', 'https://example.test/naïve'])('preserves allowed URL text: %s', (value) => {
    expect(safeArticleUrl(value)).toBe(value);
  });
  it.each([null, '', 'javascript:alert(1)', 'data:text/html,hello', 'file:///etc/passwd', 'ftp://example.test', '//example.test',
    'example.test', '/relative', 'https:', 'https://', 'http:example.test', 'https:///example.test', 'https://[bad', 'https://example.test:bad',
    ' https://example.test', 'https://example.test\n', 'https://examp\u0000le.test', 'https://example.test\\@evil.test', 'https://user:password@example.test'])('rejects unsafe or malformed URL: %s', (value) => {
    expect(safeArticleUrl(value)).toBeNull();
  });
});
