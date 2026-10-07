const STAGES = [
  { name: "Wikimedia EventStreams", detail: "Public SSE feed of every edit" },
  { name: "Python producer", detail: "Acked writes, Last-Event-ID cursor, DLQ" },
  { name: "Kafka (KRaft)", detail: "wiki.edits.raw · keyed by wiki:page" },
  { name: "Spark Structured Streaming", detail: "10s microbatches · watermark dedupe · alert rules" },
  { name: "Apache Iceberg", detail: "bronze · silver · alerts tables, keyed MERGEs" },
  { name: "FastAPI + this UI", detail: "Snapshot exported after each commit" },
];

const MAPPING = [
  ["Ingest", "EventSource straight from your browser; auto-resumes with Last-Event-ID", "Producer persists the SSE cursor only after Kafka acknowledges the write"],
  ["Batching", "Arrivals buffered, committed every 10 s", "Spark trigger processingTime = 10 seconds, maxOffsetsPerTrigger = 2000"],
  ["Dedupe", "Event-id set bounded by a 10-minute event-time watermark", "dropDuplicatesWithinWatermark + Iceberg MERGE on event_id"],
  ["Late data", "Edits older than the watermark are counted and dropped", "Dropped from silver, kept in bronze for audit or backfill"],
  ["Scoring & alerts", "Same formula and thresholds, ported to TypeScript and parity-tested", "Spark SQL expressions + per-page 5-minute lookback"],
  ["Storage", "In memory, last 15 minutes", "Iceberg tables with time travel and schema evolution"],
];

const EVIDENCE = [
  {
    stat: "8 → 4",
    label: "bronze rows → silver rows",
    detail: "Four fixture edits delivered twice through Kafka. Bronze audits all eight deliveries; silver and alerts converge on four unique edits and one edit-war alert.",
  },
  {
    stat: "1 → 0",
    label: "late record → silver rows",
    detail: "An edit one hour behind the stream landed in bronze, was excluded from silver, and Spark reported late_rows_dropped = 1 for that batch.",
  },
  {
    stat: "+1 column",
    label: "live schema change",
    detail: "A nullable analyst_note column was added to the Iceberg alerts table mid-run; existing readers and named-column MERGEs kept working.",
  },
];

export function HowItWorks() {
  return (
    <section className="how" aria-labelledby="how-title">
      <div className="eyebrow">How it works</div>
      <h2 id="how-title">The browser demo runs the pipeline's rules; the pipeline is the system of record.</h2>
      <ol className="flow">
        {STAGES.map((stage) => (
          <li key={stage.name}>
            <b>{stage.name}</b>
            <span>{stage.detail}</span>
          </li>
        ))}
      </ol>

      <div className="mapping">
        <table>
          <thead>
            <tr>
              <th scope="col">Concern</th>
              <th scope="col">In this page</th>
              <th scope="col">In the Docker pipeline</th>
            </tr>
          </thead>
          <tbody>
            {MAPPING.map(([concern, browser, spark]) => (
              <tr key={concern}>
                <th scope="row">{concern}</th>
                <td>{browser}</td>
                <td>{spark}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <h3>Verified on the real stack</h3>
      <p className="panel-note">From a recorded run of the Docker pipeline (Kafka 4.0, Spark 4.0.4, Iceberg 1.10.2) on 7 Oct 2026. See docs/verification.md.</p>
      <div className="evidence">
        {EVIDENCE.map((item) => (
          <div key={item.label} className="card">
            <div className="value">{item.stat}</div>
            <div className="eyebrow">{item.label}</div>
            <p>{item.detail}</p>
          </div>
        ))}
      </div>

      <h3>Limits, stated plainly</h3>
      <ul className="limits">
        <li>The score is an interpretable ordering aid. No labelled outcomes are used yet, so there are no precision or recall claims.</li>
        <li>Revert hints are regex matches on edit summaries: language-limited, and a summary can say “revert” for legitimate reasons.</li>
        <li>Burst rules are page-local and fixed-threshold. Busy, legitimate pages (breaking news) trigger them.</li>
        <li>Iceberg layers commit independently, so bronze, silver and alerts are eventually consistent rather than one atomic transaction.</li>
      </ul>
    </section>
  );
}
