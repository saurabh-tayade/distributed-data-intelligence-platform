/** Validate for explicit user navigation only. Return original text; never guess a scheme or fetch resources. */
export function safeArticleUrl(value: string | null): string | null {
  if (value === null || !/^https?:\/\/[^/?#]+/i.test(value) || /[\s\\]/.test(value)
      || [...value].some((character) => character.charCodeAt(0) < 33 || character.charCodeAt(0) === 127)) return null;
  try {
    const parsed = new URL(value);
    if (!['http:', 'https:'].includes(parsed.protocol) || !parsed.hostname || parsed.username || parsed.password) return null;
    return value;
  } catch { return null; }
}
