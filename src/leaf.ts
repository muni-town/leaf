import decodeBase32 from "base32-decode";
import encodeBase32 from "base32-encode";

export type ComponentId = string;
export type ComponentNames = { [name: string]: ComponentId };

export class EntityId {
  bytes: Uint8Array;

  /** Create a new Entity ID.
   *
   * If `id` is not specified a random ID will be generated.
   *
   * @argument id 32 bytes encoded as a Crockford base32 string.
   */
  constructor(id?: string) {
    if (id) {
      const data = new Uint8Array(decodeBase32(id, "Crockford"));
      if (data.length != 32)
        throw new Error(
          `Invalid byte length for Entity ID ( ${data.length} ), expected 32.`
        );
      this.bytes = data;
    } else {
      // For now we just pretend we're generating like a keypair and returning the public key, since
      // that's what Keyhive will need later.
      const bytes = new Uint8Array(32);
      crypto.getRandomValues(bytes);
      this.bytes = bytes;
    }
  }

  /** Get the entity ID as a Crockford base32 string. */
  toString(): string {
    return encodeBase32(this.bytes, "Crockford").toLowerCase();
  }
}

export class Entity {}
