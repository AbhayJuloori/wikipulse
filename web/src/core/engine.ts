// A browser-sized model of the Spark silver + alerts queries in src/wikipulse/stream.py:
// arrivals are buffered and committed as 10-second microbatches, deduplicated by event id
// within an event-time watermark, and alert rules are recomputed for touched pages over a
// five-minute event-time lookback. Spark/Iceberg remain the system of record; this engine
// exists so the same semantics can be watched and poked at in a static page.
import type { Edit } from "./contract";
import { revertMatch } from "./score";

export interface Thresholds {
  editWarReverts: number;
  editWarEditors: number;
  botBurstEdits: number;
  botBurstMaxEditors: number;
  editBurstEdits: number;
  editBurstEditors: number;
}

export const DEFAULT_THRESHOLDS: Thresholds = {
  editWarReverts: 3,
  editWarEditors: 2,
  botBurstEdits: 12,
  botBurstMaxEditors: 2,
  editBurstEdits: 8,
  editBurstEditors: 3,
};

export type AlertKind = "edit_war" | "bot_burst" | "edit_burst";

export interface Alert {
  alertId: string;
  bucketTs: number;
  lastEventTs: number;
  kind: AlertKind;
  severity: "high" | "medium";
  wiki: string;
  pageId: number;
  title: string;
  pageUrl: string;
  edits: number;
  editors: number;
  revertHints: number;
  botEdits: number;
  explanation: string;
  synthetic: boolean;
}

export interface BatchStat {
  batchId: number;
  committedAt: number;
  arrived: number;
  accepted: number;
  duplicates: number;
  late: number;
  /** Event-time watermark (unix seconds) applied to this batch, null before any data. */
  watermark: number | null;
  alertsUpserted: number;
}

export interface EngineOptions {
  watermarkSeconds: number;
  lookbackSeconds: number;
  /** Accepted edits kept in memory for the queue and alert history. */
  retainSeconds: number;
  maxRetained: number;
  maxBatches: number;
}

export const DEFAULT_OPTIONS: EngineOptions = {
  watermarkSeconds: 600,
  lookbackSeconds: 300,
  retainSeconds: 900,
  maxRetained: 5000,
  maxBatches: 30,
};

export function classify(
  stats: { edits: number; editors: number; revertHints: number; botEdits: number },
  t: Thresholds,
): AlertKind | null {
  if (stats.revertHints >= t.editWarReverts && stats.editors >= t.editWarEditors) return "edit_war";
  if (stats.botEdits >= t.botBurstEdits && stats.editors <= t.botBurstMaxEditors) return "bot_burst";
  if (stats.edits >= t.editBurstEdits && stats.editors >= t.editBurstEditors) return "edit_burst";
  return null;
}

const pageKey = (edit: Pick<Edit, "wiki" | "pageId">) => `${edit.wiki}:${edit.pageId}`;

export class PatrolEngine {
  readonly options: EngineOptions;
  thresholds: Thresholds;
  private pending: Edit[] = [];
  private seen = new Map<string, number>();
  private maxEventTs: number | null = null;
  private nextBatchId = 0;
  /** Accepted ("silver") edits, oldest first. */
  silver: Edit[] = [];
  alerts = new Map<string, Alert>();
  batches: BatchStat[] = [];
  lastCommitted: Edit[] = [];
  totals = { arrived: 0, accepted: 0, duplicates: 0, late: 0 };

  constructor(thresholds: Thresholds = DEFAULT_THRESHOLDS, options: Partial<EngineOptions> = {}) {
    this.thresholds = { ...thresholds };
    this.options = { ...DEFAULT_OPTIONS, ...options };
  }

  offer(edit: Edit): void {
    this.pending.push(edit);
  }

  get pendingCount(): number {
    return this.pending.length;
  }

  get watermark(): number | null {
    return this.maxEventTs === null ? null : this.maxEventTs - this.options.watermarkSeconds;
  }

  /** Commit one microbatch. As in Spark, the watermark used is the one from earlier batches. */
  commit(now: number = Date.now()): BatchStat {
    const arrivals = this.pending;
    this.pending = [];
    const watermark = this.watermark;
    const accepted: Edit[] = [];
    let duplicates = 0;
    let late = 0;
    for (const edit of arrivals) {
      if (watermark !== null && edit.timestamp < watermark) {
        late += 1;
      } else if (this.seen.has(edit.eventId)) {
        duplicates += 1;
      } else {
        this.seen.set(edit.eventId, edit.timestamp);
        accepted.push(edit);
      }
    }
    for (const edit of accepted) {
      if (this.maxEventTs === null || edit.timestamp > this.maxEventTs) this.maxEventTs = edit.timestamp;
    }
    const nextWatermark = this.watermark;
    if (nextWatermark !== null) {
      for (const [id, ts] of this.seen) if (ts < nextWatermark) this.seen.delete(id);
    }
    this.silver.push(...accepted);
    this.silver.sort((a, b) => a.timestamp - b.timestamp);
    const alertsUpserted = accepted.length ? this.refreshAlerts(accepted) : 0;
    this.evict();

    const stat: BatchStat = {
      batchId: this.nextBatchId++,
      committedAt: now,
      arrived: arrivals.length,
      accepted: accepted.length,
      duplicates,
      late,
      watermark,
      alertsUpserted,
    };
    this.lastCommitted = accepted;
    this.batches = [...this.batches, stat].slice(-this.options.maxBatches);
    this.totals = {
      arrived: this.totals.arrived + arrivals.length,
      accepted: this.totals.accepted + accepted.length,
      duplicates: this.totals.duplicates + duplicates,
      late: this.totals.late + late,
    };
    return stat;
  }

  /** Re-evaluate every active alert window, e.g. after thresholds change. */
  reclassify(): void {
    this.alerts.clear();
    const byPage = new Map<string, Edit[]>();
    for (const edit of this.silver) {
      const key = pageKey(edit);
      byPage.set(key, [...(byPage.get(key) ?? []), edit]);
    }
    for (const edits of byPage.values()) {
      const latest = edits[edits.length - 1].timestamp;
      this.upsertAlert(edits, latest);
    }
  }

  private refreshAlerts(batch: Edit[]): number {
    const latest = Math.max(...batch.map((edit) => edit.timestamp));
    const lower = latest - this.options.lookbackSeconds;
    const touched = new Set(batch.map(pageKey));
    const history = new Map<string, Edit[]>();
    for (const edit of this.silver) {
      const key = pageKey(edit);
      if (!touched.has(key) || edit.timestamp < lower || edit.timestamp > latest) continue;
      history.set(key, [...(history.get(key) ?? []), edit]);
    }
    let upserted = 0;
    for (const edits of history.values()) if (this.upsertAlert(edits, latest)) upserted += 1;
    return upserted;
  }

  private upsertAlert(windowEdits: Edit[], latest: number): boolean {
    const lower = latest - this.options.lookbackSeconds;
    const edits = windowEdits.filter((edit) => edit.timestamp >= lower && edit.timestamp <= latest);
    if (!edits.length) return false;
    const stats = {
      edits: edits.length,
      editors: new Set(edits.map((edit) => edit.user)).size,
      revertHints: edits.filter((edit) => revertMatch(edit.comment) !== null).length,
      botEdits: edits.filter((edit) => edit.bot).length,
    };
    const kind = classify(stats, this.thresholds);
    if (!kind) return false;
    const newest = edits.reduce((a, b) => (b.timestamp >= a.timestamp ? b : a));
    const bucketTs = Math.floor(latest / 60) * 60;
    const alertId = `${newest.wiki}:${newest.pageId}:${bucketTs}:${kind}`;
    this.alerts.set(alertId, {
      alertId,
      bucketTs,
      lastEventTs: latest,
      kind,
      severity: kind === "edit_war" ? "high" : "medium",
      wiki: newest.wiki,
      pageId: newest.pageId,
      title: newest.title,
      pageUrl: newest.pageUrl,
      ...stats,
      explanation: `${stats.edits} edits from ${stats.editors} editors in 5 minutes; ${stats.revertHints} revert hints.`,
      synthetic: edits.some((edit) => edit.synthetic),
    });
    return true;
  }

  private evict(): void {
    if (this.maxEventTs === null) return;
    const floor = this.maxEventTs - this.options.retainSeconds;
    let start = 0;
    while (start < this.silver.length && this.silver[start].timestamp < floor) start += 1;
    const overflow = Math.max(0, this.silver.length - start - this.options.maxRetained);
    if (start + overflow) this.silver = this.silver.slice(start + overflow);
    for (const [id, alert] of this.alerts) if (alert.lastEventTs < floor) this.alerts.delete(id);
  }
}
