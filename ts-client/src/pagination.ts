/**
 * Encode a cursor from an array of values to an opaque base64 string
 */
export function encodeCursor(values: unknown[]): string {
  const json = JSON.stringify(values);
  const bytes = new TextEncoder().encode(json);
  return btoa(Array.from(bytes, (byte) => String.fromCharCode(byte)).join(""));
}

/**
 * Decode an opaque cursor string back to an array of values
 */
export function decodeCursor(cursor: string): unknown[] {
  try {
    const bytes = Uint8Array.from(atob(cursor), (char) => char.charCodeAt(0));
    const json = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
    const values: unknown = JSON.parse(json);
    if (!Array.isArray(values)) {
      throw new Error("cursor must decode to a JSON array");
    }
    return values;
  } catch (e) {
    throw new Error(`Invalid cursor: ${e}`);
  }
}
