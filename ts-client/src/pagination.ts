/**
 * Encode a cursor from an array of values to an opaque base64 string
 */
export function encodeCursor(values: unknown[]): string {
  const json = JSON.stringify(values);
  return btoa(json);
}

/**
 * Decode an opaque cursor string back to an array of values
 */
export function decodeCursor(cursor: string): unknown[] {
  try {
    const json = atob(cursor);
    return JSON.parse(json);
  } catch (e) {
    throw new Error(`Invalid cursor: ${e}`);
  }
}
