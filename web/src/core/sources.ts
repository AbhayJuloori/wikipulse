import { InvalidEvent, parseEdit, type Edit } from "./contract";

export const STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange";

export type SourceStatus = "connecting" | "live" | "reconnecting" | "ended" | "error";

export interface SourceCounters {
  /** All SSE messages, including other wikis, log entries and canaries. */
  messages: number;
  edits: number;
  /** Edits outside the selected wiki filter. */
  filtered: number;
  /** Malformed edit records; the Kafka producer routes these to wiki.edits.dlq. */
  invalid: number;
}

export interface EditSource {
  stop(): void;
}

interface Handlers {
  onEdit(edit: Edit): void;
  onStatus(status: SourceStatus, detail?: string): void;
  onCounters(counters: SourceCounters): void;
}

function counterSink(handlers: Handlers) {
  const counters: SourceCounters = { messages: 0, edits: 0, filtered: 0, invalid: 0 };
  return {
    counters,
    handle(payload: string | Record<string, unknown>, wiki: string) {
      counters.messages += 1;
      try {
        const edit = parseEdit(payload);
        if (!edit) return;
        counters.edits += 1;
        if (wiki !== "all" && edit.wiki !== wiki) counters.filtered += 1;
        else handlers.onEdit(edit);
      } catch (error) {
        if (error instanceof InvalidEvent) counters.invalid += 1;
        else throw error;
      }
    },
  };
}

/**
 * Connects the visitor's browser straight to Wikimedia EventStreams (CORS-enabled SSE).
 * EventSource resends Last-Event-ID on reconnect, the same cursor the Kafka producer persists.
 */
export function liveSource(wiki: string, handlers: Handlers): EditSource {
  const sink = counterSink(handlers);
  const source = new EventSource(STREAM_URL);
  handlers.onStatus("connecting");
  source.onopen = () => handlers.onStatus("live");
  source.onerror = () =>
    handlers.onStatus(source.readyState === EventSource.CLOSED ? "error" : "reconnecting");
  source.onmessage = (message) => sink.handle(message.data, wiki);
  const timer = window.setInterval(() => handlers.onCounters({ ...sink.counters }), 1000);
  return {
    stop() {
      source.close();
      window.clearInterval(timer);
    },
  };
}

/**
 * Replays a recorded capture from the Kafka topic. Event time keeps the original spacing on a
 * virtual clock that starts now, so a faster replay runs ahead of wall time instead of
 * compressing five-minute windows (which would inflate the burst rules).
 */
export function replaySource(url: string, speed: number, handlers: Handlers): EditSource {
  const sink = counterSink(handlers);
  let stopped = false;
  let timer = 0;
  handlers.onStatus("connecting");
  fetch(url)
    .then((response) => {
      if (!response.ok) throw new Error(`capture unavailable (${response.status})`);
      return response.text();
    })
    .then((text) => {
      if (stopped) return;
      const events = text
        .split("\n")
        .filter(Boolean)
        .map((line) => JSON.parse(line) as Record<string, unknown>)
        .sort((a, b) => Number(a.timestamp) - Number(b.timestamp));
      if (!events.length) throw new Error("capture is empty");
      const first = Number(events[0].timestamp);
      const span = Number(events[events.length - 1].timestamp) - first + 1;
      let index = 0;
      let lap = 0;
      let startedAt = Date.now();
      let lapBase = Math.floor(startedAt / 1000);
      handlers.onStatus("live", `${events.length.toLocaleString("en-US")} recorded events`);
      timer = window.setInterval(() => {
        const elapsed = ((Date.now() - startedAt) / 1000) * speed;
        while (index < events.length && Number(events[index].timestamp) - first <= elapsed) {
          const event = events[index];
          const offset = Number(event.timestamp) - first;
          const meta = (event.meta ?? {}) as Record<string, unknown>;
          sink.handle(
            {
              ...event,
              // Each lap gets fresh ids so a looped capture is new data, not a duplicate.
              meta: { ...meta, id: lap ? `${meta.id}~lap${lap}` : meta.id },
              timestamp: lapBase + offset,
            },
            "all",
          );
          index += 1;
        }
        if (index >= events.length && elapsed >= span) {
          index = 0;
          lap += 1;
          startedAt = Date.now();
          lapBase += span;
        }
        handlers.onCounters({ ...sink.counters });
      }, 250);
    })
    .catch((error: Error) => handlers.onStatus("error", error.message));
  return {
    stop() {
      stopped = true;
      window.clearInterval(timer);
    },
  };
}

/** Snapshot served by src/wikipulse/api.py when the Docker stack is running. */
export interface PipelineSnapshot {
  generated_at: string | null;
  batch_id: number | null;
  batch_rows: number;
  queue: Array<Record<string, unknown>>;
  alerts: Array<Record<string, unknown>>;
  throughput: Array<{ at: string; rows: number }>;
  metrics?: {
    batch_id?: number;
    watermark?: string | null;
    input_rows_per_second?: number;
    processed_rows_per_second?: number;
    batch_duration_ms?: number;
    late_rows_dropped?: number;
  };
}

export async function pipelineAvailable(): Promise<boolean> {
  try {
    const response = await fetch("api/health", { cache: "no-store" });
    return response.ok && (response.headers.get("content-type") ?? "").includes("json");
  } catch {
    return false;
  }
}

export async function fetchPipeline(): Promise<PipelineSnapshot> {
  const response = await fetch("api/dashboard", { cache: "no-store" });
  if (!response.ok) throw new Error(`pipeline API returned ${response.status}`);
  return (await response.json()) as PipelineSnapshot;
}

/** Map an Iceberg silver row back onto the shared contract so it can be rescored locally. */
export function silverRowToEdit(row: Record<string, unknown>): Edit {
  const pageUrl = String(row.page_url ?? "");
  return {
    eventId: String(row.event_id),
    wiki: String(row.wiki),
    pageId: Number(row.page_id),
    revisionId: Number(row.revision_id),
    timestamp: Math.floor(Date.parse(String(row.event_ts)) / 1000),
    user: String(row.username),
    title: String(row.title),
    pageUrl,
    serverUrl: "",
    bot: Boolean(row.bot),
    comment: String(row.comment ?? ""),
    oldLength: row.old_length == null ? null : Number(row.old_length),
    newLength: row.new_length == null ? null : Number(row.new_length),
    schemaUri: String(row.schema_uri ?? "unknown"),
  };
}
