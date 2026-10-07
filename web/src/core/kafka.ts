// The producer uses confluent-kafka (librdkafka), whose default "consistent_random" partitioner
// maps a non-empty key to crc32(key) % partitions. Checked against 700 messages from the running
// broker's three wiki.edits.raw partitions (see docs/verification.md).
const TABLE = (() => {
  const table = new Uint32Array(256);
  for (let n = 0; n < 256; n += 1) {
    let c = n;
    for (let k = 0; k < 8; k += 1) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    table[n] = c >>> 0;
  }
  return table;
})();

export function crc32(text: string): number {
  let crc = 0xffffffff;
  for (const byte of new TextEncoder().encode(text)) crc = TABLE[(crc ^ byte) & 0xff] ^ (crc >>> 8);
  return (crc ^ 0xffffffff) >>> 0;
}

export const RAW_PARTITIONS = 3;

export function kafkaKey(edit: { wiki: string; pageId: number }): string {
  return `${edit.wiki}:${edit.pageId}`;
}

export function partitionFor(key: string, partitions = RAW_PARTITIONS): number {
  return crc32(key) % partitions;
}
