import encodeBase32 from "base32-encode";

export function getOrDefault<K, V>(map: Map<K, V>, key: K, defaultValue: V): V {
  if (!map.has(key)) {
    map.set(key, defaultValue);
  }
  return map.get(key)!;
}

/** Get the sha256 hash of the given data. */
export async function sha256Base32(data: Uint8Array): Promise<string> {
  return encodeBase32(
    new Uint8Array(await crypto.subtle.digest("SHA-256", new Uint8Array(data))),
    "Crockford"
  );
}
