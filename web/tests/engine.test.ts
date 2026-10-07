import { describe, expect, it } from "vitest";
import type { Edit } from "../src/core/contract";
import { classify, DEFAULT_THRESHOLDS, PatrolEngine } from "../src/core/engine";
import { editWar, lateEdit } from "../src/core/hardcases";
import { DEFAULT_WEIGHTS, scoreEdit } from "../src/core/score";

let n = 0;
function edit(overrides: Partial<Edit> = {}): Edit {
  n += 1;
  return {
    eventId: `e-${n}`,
    wiki: "enwiki",
    pageId: 42,
    revisionId: 1000 + n,
    timestamp: 1_800_000_000,
    user: "Alice",
    title: "Example",
    pageUrl: "https://en.wikipedia.org/wiki/Example",
    serverUrl: "https://en.wikipedia.org",
    bot: false,
    comment: "",
    oldLength: 100,
    newLength: 200,
    schemaUri: "/mediawiki/recentchange/1.0.0",
    ...overrides,
  };
}

describe("microbatch engine", () => {
  it("dedupes redelivered event ids within and across batches", () => {
    const engine = new PatrolEngine();
    const a = edit();
    engine.offer(a);
    engine.offer(a);
    expect(engine.commit()).toMatchObject({ arrived: 2, accepted: 1, duplicates: 1 });
    expect(engine.lastOutcomes).toEqual(["accepted", "duplicate"]);
    engine.offer(a);
    expect(engine.commit()).toMatchObject({ accepted: 0, duplicates: 1 });
    expect(engine.silver).toHaveLength(1);
  });

  it("drops records older than the watermark from previous batches, like Spark", () => {
    const engine = new PatrolEngine();
    const now = 1_800_000_000;
    // The watermark is not yet set inside the first batch, so an old record is accepted there.
    engine.offer(edit({ timestamp: now }));
    engine.offer(edit({ timestamp: now - 3600 }));
    expect(engine.commit()).toMatchObject({ accepted: 2, late: 0, watermark: null });
    engine.offer(edit({ timestamp: now - 3600 }));
    engine.offer(edit({ timestamp: now - 599 }));
    expect(engine.commit()).toMatchObject({ accepted: 1, late: 1, watermark: now - 600 });
  });

  it("forgets dedupe state below the watermark, bounding memory", () => {
    const engine = new PatrolEngine();
    const old = edit({ timestamp: 1_800_000_000 });
    engine.offer(old);
    engine.commit();
    engine.offer(edit({ timestamp: 1_800_000_000 + 1200 }));
    engine.commit();
    // Redelivery after state eviction is late, not silently re-accepted.
    engine.offer(old);
    expect(engine.commit()).toMatchObject({ late: 1, accepted: 0, duplicates: 0 });
  });

  it("raises an edit-war alert for a staged edit war and keys it by page/minute/kind", () => {
    const engine = new PatrolEngine();
    const now = 1_800_000_000;
    editWar("enwiki", now).forEach((e) => engine.offer(e));
    engine.commit();
    const alerts = [...engine.alerts.values()];
    expect(alerts).toHaveLength(1);
    expect(alerts[0]).toMatchObject({ kind: "edit_war", edits: 4, editors: 2, revertHints: 3, synthetic: true });
    // A retried batch for the same minute updates rather than duplicates the alert.
    engine.offer(edit({ ...alerts[0], timestamp: alerts[0].lastEventTs, comment: "rv" }));
    engine.commit();
    expect(engine.alerts.size).toBe(1);
  });

  it("drops a staged late edit once the stream has a watermark", () => {
    const engine = new PatrolEngine();
    const now = 1_800_000_000;
    engine.offer(edit({ timestamp: now }));
    engine.commit();
    lateEdit("enwiki", now).forEach((e) => engine.offer(e));
    expect(engine.commit()).toMatchObject({ late: 1, accepted: 0 });
  });

  it("only counts edits inside the five-minute lookback", () => {
    const engine = new PatrolEngine();
    const now = 1_800_000_000;
    for (let i = 0; i < 8; i += 1) engine.offer(edit({ user: `U${i % 3}`, timestamp: now - 400 }));
    engine.commit();
    expect(engine.alerts.size).toBe(1);
    engine.offer(edit({ timestamp: now }));
    engine.commit();
    // The newer batch's window excludes the earlier burst, so no second-minute alert.
    expect([...engine.alerts.values()].filter((a) => a.lastEventTs === now)).toHaveLength(0);
  });

  it("reclassifies active windows when thresholds change", () => {
    const engine = new PatrolEngine();
    for (let i = 0; i < 5; i += 1) engine.offer(edit({ user: `U${i}` }));
    engine.commit();
    expect(engine.alerts.size).toBe(0);
    engine.thresholds = { ...DEFAULT_THRESHOLDS, editBurstEdits: 5 };
    engine.reclassify();
    expect([...engine.alerts.values()][0].kind).toBe("edit_burst");
  });
});

describe("rules", () => {
  it("prefers edit war over bot burst over edit burst, as the Spark CASE does", () => {
    expect(classify({ edits: 20, editors: 2, revertHints: 3, botEdits: 20 }, DEFAULT_THRESHOLDS)).toBe("edit_war");
    expect(classify({ edits: 20, editors: 2, revertHints: 0, botEdits: 12 }, DEFAULT_THRESHOLDS)).toBe("bot_burst");
    expect(classify({ edits: 8, editors: 3, revertHints: 0, botEdits: 0 }, DEFAULT_THRESHOLDS)).toBe("edit_burst");
    expect(classify({ edits: 7, editors: 3, revertHints: 2, botEdits: 0 }, DEFAULT_THRESHOLDS)).toBeNull();
  });

  it("explains the score as parts that sum to it", () => {
    const scored = scoreEdit(edit({ user: "~2026-11111-22", comment: "rvv", oldLength: 0, newLength: 1250 }));
    expect(scored.parts.map((p) => p.points)).toEqual([20, 25, 20, 12]);
    expect(scored.score).toBe(77);
    expect(scoreEdit(edit({ bot: true }), { ...DEFAULT_WEIGHTS, bot: -30 }).score).toBe(0);
  });
});

describe("kafka partitioning", () => {
  it("matches librdkafka consistent_random (crc32 % partitions) on keys from the broker", async () => {
    const { crc32, partitionFor } = await import("../src/core/kafka");
    expect(crc32("123456789")).toBe(0xcbf43926);
    // Observed on the running broker (kafka-console-consumer --property print.partition=true).
    const observed: [string, number][] = [
      ["enwiki:2077495274", 0],
      ["enwiki:2077495281", 0],
      ["enwiki:2077495286", 0],
      ["enwiki:2077495275", 1],
      ["enwiki:2077495276", 1],
      ["enwiki:2077495277", 1],
      ["enwiki:2077495284", 2],
      ["enwiki:2077495287", 2],
      ["enwiki:2077495289", 2],
    ];
    for (const [key, partition] of observed) expect(partitionFor(key)).toBe(partition);
  });
});
