import { fmt } from "./format";

export interface AnatomyCounts {
  messages: number;
  edits: number;
  matching: number;
  invalid: number;
  batches: number;
  accepted: number;
  duplicates: number;
  late: number;
  retained: number;
  alerts: number;
}

/** This session's own numbers, stage by stage, next to what the Docker pipeline does there. */
export function Anatomy({ c, wikiLabel }: { c: AnatomyCounts; wikiLabel: string }) {
  const stages = [
    {
      n: c.messages,
      label: "messages on the wire",
      here: "Every SSE event from Wikimedia, all wikis and types",
      there: "Producer reads the same feed; cursor = Last-Event-ID",
    },
    {
      n: c.edits,
      label: "edit events",
      here: `Parsed by the shared contract; ${fmt(c.invalid)} malformed`,
      there: "Malformed edits go to wiki.edits.dlq with the reason",
    },
    {
      n: c.matching,
      label: `on ${wikiLabel}`,
      here: "Filtered before buffering",
      there: "WIKI_FILTER, then keyed wiki:page into 3 partitions",
    },
    {
      n: c.batches,
      label: "microbatches committed",
      here: "One commit every 10 seconds",
      there: "Spark trigger 10 s, up to 2,000 offsets per batch",
    },
    {
      n: c.accepted,
      label: "rows accepted",
      here: `${fmt(c.duplicates)} duplicates and ${fmt(c.late)} late rows rejected`,
      there: "Silver: watermark dedupe + MERGE on event_id",
    },
    {
      n: c.alerts,
      label: "active alerts",
      here: `Over ${fmt(c.retained)} edits held in memory`,
      there: "Iceberg alerts table, keyed page/minute/kind",
    },
  ];
  return (
    <ol className="anatomy">
      {stages.map((stage) => (
        <li key={stage.label}>
          <span className="anatomy-n">{fmt(stage.n)}</span>
          <span className="anatomy-label">{stage.label}</span>
          <span className="anatomy-here">{stage.here}</span>
          <span className="anatomy-there">{stage.there}</span>
        </li>
      ))}
    </ol>
  );
}
