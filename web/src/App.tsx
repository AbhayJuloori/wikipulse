import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { diffUrl, safeWikiUrl, type Edit } from "./core/contract";
import { DEFAULT_THRESHOLDS, PatrolEngine, type Alert, type Thresholds } from "./core/engine";
import { editWar, lateEdit } from "./core/hardcases";
import { DEFAULT_WEIGHTS, scoreEdit, type Scored, type Weights } from "./core/score";
import {
  fetchPipeline,
  liveSource,
  pipelineAvailable,
  replaySource,
  silverRowToEdit,
  type EditSource,
  type PipelineSnapshot,
  type SourceCounters,
  type SourceStatus,
} from "./core/sources";
import { BatchChart, type BatchBar } from "./ui/BatchChart";
import { ago, fmt, kindLabel } from "./ui/format";
import { HowItWorks } from "./ui/HowItWorks";

type Mode = "live" | "replay" | "pipeline";

const TRIGGER_MS = 10_000;
const QUEUE_SIZE = 25;
const CAPTURE_URL = `${import.meta.env.BASE_URL}sample/enwiki-capture.jsonl`;
const WIKIS = [
  { value: "enwiki", label: "English Wikipedia" },
  { value: "dewiki", label: "German Wikipedia" },
  { value: "frwiki", label: "French Wikipedia" },
  { value: "eswiki", label: "Spanish Wikipedia" },
  { value: "jawiki", label: "Japanese Wikipedia" },
  { value: "all", label: "All wikis (high volume)" },
];

interface QueueItem {
  edit: Edit;
  scored: Scored;
}

interface AlertItem extends Omit<Alert, "bucketTs" | "pageId" | "severity" | "edits" | "editors" | "revertHints" | "botEdits"> {}

export function App() {
  const [mode, setMode] = useState<Mode>("live");
  const [wiki, setWiki] = useState("enwiki");
  const [speed, setSpeed] = useState(1);
  const [paused, setPaused] = useState(false);
  const [status, setStatus] = useState<{ state: SourceStatus; detail?: string }>({ state: "connecting" });
  const [counters, setCounters] = useState<SourceCounters>({ messages: 0, edits: 0, filtered: 0, invalid: 0 });
  const [hasPipeline, setHasPipeline] = useState(false);
  const [snapshot, setSnapshot] = useState<PipelineSnapshot | null>(null);
  const [weights, setWeights] = useState<Weights>(DEFAULT_WEIGHTS);
  const [thresholds, setThresholds] = useState<Thresholds>(DEFAULT_THRESHOLDS);
  const [hideBots, setHideBots] = useState(false);
  const [expanded, setExpanded] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [, setTick] = useState(0);
  const [nextCommitAt, setNextCommitAt] = useState(Date.now() + TRIGGER_MS);
  const engineRef = useRef(new PatrolEngine());
  const pausedRef = useRef(paused);
  pausedRef.current = paused;
  const rerender = useCallback(() => setTick((n) => n + 1), []);

  // Prefer the real pipeline when this page is served by the local FastAPI stack.
  useEffect(() => {
    pipelineAvailable().then((available) => {
      setHasPipeline(available);
      if (available) setMode("pipeline");
    });
  }, []);

  // Source lifecycle: a fresh engine per source so modes never mix data.
  useEffect(() => {
    if (mode === "pipeline") return;
    engineRef.current = new PatrolEngine(thresholds);
    setCounters({ messages: 0, edits: 0, filtered: 0, invalid: 0 });
    setExpanded(null);
    rerender();
    const handlers = {
      onEdit: (edit: Edit) => {
        if (!pausedRef.current) engineRef.current.offer(edit);
      },
      onStatus: (state: SourceStatus, detail?: string) => setStatus({ state, detail }),
      onCounters: setCounters,
    };
    const source: EditSource =
      mode === "live" ? liveSource(wiki, handlers) : replaySource(CAPTURE_URL, speed, handlers);
    return () => source.stop();
  }, [mode, wiki, speed]);

  // Microbatch trigger, like Spark's processingTime="10 seconds".
  useEffect(() => {
    if (mode === "pipeline") return;
    const commit = () => {
      if (!pausedRef.current) engineRef.current.commit();
      setNextCommitAt(Date.now() + TRIGGER_MS);
      rerender();
    };
    setNextCommitAt(Date.now() + TRIGGER_MS);
    const trigger = window.setInterval(commit, TRIGGER_MS);
    const second = window.setInterval(rerender, 1000);
    return () => {
      window.clearInterval(trigger);
      window.clearInterval(second);
    };
  }, [mode, wiki, speed, rerender]);

  useEffect(() => {
    if (mode !== "pipeline") return;
    let alive = true;
    const poll = () =>
      fetchPipeline()
        .then((data) => {
          if (!alive) return;
          setSnapshot(data);
          const fresh = data.generated_at && Date.now() - Date.parse(data.generated_at) < 90_000;
          setStatus({ state: fresh ? "live" : "reconnecting", detail: fresh ? undefined : "waiting for a fresh Spark batch" });
        })
        .catch((error: Error) => alive && setStatus({ state: "error", detail: error.message }));
    setStatus({ state: "connecting" });
    poll();
    const timer = window.setInterval(poll, 5000);
    return () => {
      alive = false;
      window.clearInterval(timer);
    };
  }, [mode]);

  useEffect(() => {
    if (!notice) return;
    const timer = window.setTimeout(() => setNotice(null), 9000);
    return () => window.clearTimeout(timer);
  }, [notice]);

  const engine = engineRef.current;
  const pipeline = mode === "pipeline";
  const nowSeconds = Date.now() / 1000;

  const pipelineEdits = useMemo(() => (snapshot?.queue ?? []).map(silverRowToEdit), [snapshot]);
  const sourceEdits = pipeline ? pipelineEdits : engine.silver;
  const latestEvent = sourceEdits.length ? sourceEdits[sourceEdits.length - 1].timestamp : 0;
  const latestTs = pipeline ? Math.max(0, ...pipelineEdits.map((e) => e.timestamp)) : latestEvent;
  // Replay runs on a virtual event clock that can be ahead of wall time.
  const clock = Math.max(nowSeconds, latestTs);

  const queue: QueueItem[] = useMemo(() => {
    const windowStart = latestTs - 600;
    return sourceEdits
      .filter((edit) => edit.timestamp >= windowStart && !(hideBots && edit.bot))
      .map((edit) => ({ edit, scored: scoreEdit(edit, weights) }))
      .sort((a, b) => b.scored.score - a.scored.score || b.edit.timestamp - a.edit.timestamp)
      .slice(0, QUEUE_SIZE);
    // engine.silver is mutated in place by commits; batches.length tracks those commits.
  }, [sourceEdits, sourceEdits.length, engine.batches, weights, hideBots, latestTs]);

  const alerts: AlertItem[] = pipeline
    ? (snapshot?.alerts ?? []).map((row) => ({
        alertId: String(row.alert_id),
        kind: String(row.kind) as Alert["kind"],
        wiki: String(row.wiki),
        title: String(row.title),
        pageUrl: "",
        lastEventTs: Date.parse(String(row.last_event_ts)) / 1000,
        explanation: String(row.explanation),
        synthetic: false,
      }))
    : [...engine.alerts.values()].sort((a, b) => b.lastEventTs - a.lastEventTs);

  const bars: BatchBar[] = pipeline
    ? (snapshot?.throughput ?? []).map((point, i) => ({
        id: (snapshot?.batch_id ?? 0) - (snapshot!.throughput.length - 1 - i),
        at: Date.parse(point.at),
        accepted: point.rows,
        duplicates: 0,
        late: 0,
      }))
    : engine.batches.map((b) => ({
        id: b.batchId,
        at: b.committedAt,
        accepted: b.accepted,
        duplicates: b.duplicates,
        late: b.late,
      }));

  const recent = engine.batches.slice(-6);
  const perMinute = pipeline
    ? Math.round((snapshot?.metrics?.input_rows_per_second ?? 0) * 60)
    : recent.length
      ? Math.round((recent.reduce((s, b) => s + b.accepted, 0) / recent.length) * 6)
      : 0;
  const ready = pipeline ? Boolean(snapshot?.generated_at) : engine.batches.some((b) => b.accepted > 0);
  const secondsToCommit = Math.max(0, Math.ceil((nextCommitAt - Date.now()) / 1000));

  const updateThresholds = (next: Thresholds) => {
    setThresholds(next);
    engine.thresholds = next;
    engine.reclassify();
    rerender();
  };

  const inject = (kind: "war" | "duplicate" | "late") => {
    const eventNow = Math.max(Math.floor(Date.now() / 1000), engine.silver.at(-1)?.timestamp ?? 0);
    if (kind === "war") {
      editWar(wiki, eventNow).forEach((e) => engine.offer(e));
      setNotice("Queued 4 simulated edits on one page: two editors, three revert summaries. Watch Alerts after the next commit.");
    } else if (kind === "duplicate") {
      const redelivered = engine.lastCommitted;
      redelivered.forEach((e) => engine.offer(e));
      setNotice(`Re-delivered ${fmt(redelivered.length)} edits from batch #${engine.batches.at(-1)?.batchId ?? 0} with the same event ids, as a producer replay after a crash would. Expect amber duplicate bars and no new queue rows.`);
    } else {
      lateEdit(wiki, eventNow).forEach((e) => engine.offer(e));
      setNotice("Queued an edit stamped one hour behind the stream. It is older than the 10-minute watermark, so the next batch drops it (purple bar).");
    }
    rerender();
  };

  const statusText = {
    connecting: pipeline ? "Connecting to local pipeline" : "Connecting to Wikimedia",
    live: pipeline ? "Live · Kafka → Spark → Iceberg" : mode === "live" ? "Live · Wikimedia EventStreams" : "Replaying recorded capture",
    reconnecting: "Reconnecting",
    ended: "Ended",
    error: "Source unavailable",
  }[status.state];

  return (
    <>
      <header className="topbar">
        <div className="brand">
          Wiki<span>Pulse</span>
        </div>
        <div className="status" role="status">
          <span className={`dot ${status.state === "live" && !paused ? "live" : ""}`} />
          {paused ? "Paused" : statusText}
          {status.detail && <em> · {status.detail}</em>}
        </div>
      </header>

      <main>
        <section className="hero">
          <div>
            <div className="eyebrow">Live Wikipedia edit patrol</div>
            <h1>Which edits should a patroller check first?</h1>
            <p>
              Every edit to Wikipedia is published as a public event. WikiPulse ranks them for human review and flags pages
              that look like edit wars or bursts. {
                {
                  pipeline: "This view reads the Spark + Iceberg pipeline running locally.",
                  live: "This page connects your browser to the live stream and applies the same rules as the Spark pipeline.",
                  replay: "This replays a 42-minute capture from the pipeline's Kafka topic through the same rules as the Spark job.",
                }[mode]
              }
            </p>
          </div>
        </section>

        <section className="controls" aria-label="Data source">
          <div className="segmented" role="tablist">
            <button role="tab" aria-selected={mode === "live"} onClick={() => setMode("live")}>
              Live stream
            </button>
            <button role="tab" aria-selected={mode === "replay"} onClick={() => setMode("replay")}>
              Recorded capture
            </button>
            {hasPipeline && (
              <button role="tab" aria-selected={pipeline} onClick={() => setMode("pipeline")}>
                Local pipeline
              </button>
            )}
          </div>
          {mode === "live" && (
            <label className="field">
              Wiki
              <select value={wiki} onChange={(e) => setWiki(e.target.value)}>
                {WIKIS.map((w) => (
                  <option key={w.value} value={w.value}>
                    {w.label}
                  </option>
                ))}
              </select>
            </label>
          )}
          {mode === "replay" && (
            <label className="field">
              Speed
              <select value={speed} onChange={(e) => setSpeed(Number(e.target.value))}>
                {[1, 3, 10].map((s) => (
                  <option key={s} value={s}>
                    {s}×
                  </option>
                ))}
              </select>
            </label>
          )}
          {!pipeline && (
            <>
              <button className="ghost" onClick={() => setPaused((p) => !p)}>
                {paused ? "Resume" : "Pause"}
              </button>
              <span className="trigger">
                {paused ? "Commits paused" : `Next microbatch in ${secondsToCommit}s · ${fmt(engine.pendingCount)} arrivals buffered`}
              </span>
            </>
          )}
        </section>

        <section className="cards">
          <Kpi label="Accepted edits / min" value={ready ? fmt(perMinute) : "—"} detail={pipeline ? "Spark input rate" : "Average of last 6 microbatches"} />
          <Kpi label="Top review score" value={queue[0] ? String(queue[0].scored.score) : "—"} detail="Transparent 0–100 ordering aid" />
          <Kpi label="Active alerts" value={ready ? fmt(alerts.length) : "—"} detail="Edit war, edit burst, bot burst" />
          {pipeline ? (
            <Kpi label="Late rows dropped" value={fmt(snapshot?.metrics?.late_rows_dropped ?? 0)} detail="Latest Spark batch" />
          ) : (
            <Kpi
              label="Duplicates · late dropped"
              value={`${fmt(engine.totals.duplicates)} · ${fmt(engine.totals.late)}`}
              detail={`${fmt(counters.messages)} stream messages · ${fmt(counters.invalid)} malformed`}
            />
          )}
        </section>

        {notice && (
          <div className="notice" role="status">
            {notice}
          </div>
        )}

        <section className="grid">
          <div className="panel">
            <div className="panel-head">
              <h2>Review queue</h2>
              <label className="toggle">
                <input type="checkbox" checked={hideBots} onChange={(e) => setHideBots(e.target.checked)} /> Hide bots
              </label>
            </div>
            <p className="panel-note">Highest score first among edits from the last 10 minutes of stream time. Select an edit to see why it ranks there.</p>
            {queue.length ? (
              <ol className="queue">
                {queue.map(({ edit, scored }) => {
                  const open = expanded === edit.eventId;
                  const page = safeWikiUrl(edit.pageUrl);
                  const diff = safeWikiUrl(diffUrl(edit));
                  return (
                    <li key={edit.eventId} className={open ? "open" : ""}>
                      <button className="row" aria-expanded={open} onClick={() => setExpanded(open ? null : edit.eventId)}>
                        <span className={`score ${scored.score >= 45 ? "high" : ""}`}>{scored.score}</span>
                        <span className="row-main">
                          <span className="title">
                            {edit.title}
                            {edit.synthetic && <span className="sim">Simulated</span>}
                          </span>
                          <span className="meta">
                            <span>{edit.wiki}</span>
                            <span>{edit.user}</span>
                            <span>{ago(edit.timestamp, clock)}</span>
                            {edit.bot && <span>bot</span>}
                          </span>
                        </span>
                      </button>
                      {open && (
                        <div className="why">
                          <ul>
                            {scored.parts.map((part) => (
                              <li key={part.key}>
                                <span>{part.label}</span>
                                <b className={part.points < 0 ? "neg" : ""}>
                                  {part.points > 0 ? "+" : ""}
                                  {part.points}
                                </b>
                              </li>
                            ))}
                          </ul>
                          {edit.comment && <p className="comment">“{edit.comment}”</p>}
                          <div className="links">
                            {diff && (
                              <a href={diff} target="_blank" rel="noopener noreferrer">
                                View diff ↗
                              </a>
                            )}
                            {page && (
                              <a href={page} target="_blank" rel="noopener noreferrer">
                                Open page ↗
                              </a>
                            )}
                          </div>
                        </div>
                      )}
                    </li>
                  );
                })}
              </ol>
            ) : (
              <div className="empty">
                {status.state === "error"
                  ? `Could not reach the source${status.detail ? ` (${status.detail})` : ""}. Try “Recorded capture”.`
                  : pipeline
                    ? "Waiting for the first committed Spark batch."
                    : "Collecting edits. The first microbatch commits within 10 seconds."}
              </div>
            )}
          </div>

          <div className="side">
            <div className="panel">
              <div className="panel-head">
                <h2>Alerts</h2>
                <span>5-minute event-time lookback</span>
              </div>
              {alerts.length ? (
                <ul className="alerts">
                  {alerts.slice(0, 8).map((alert) => {
                    const link = safeWikiUrl(alert.pageUrl);
                    return (
                      <li key={alert.alertId}>
                        <div className="alert-top">
                          {link ? (
                            <a href={link} target="_blank" rel="noopener noreferrer">
                              {alert.title}
                            </a>
                          ) : (
                            <strong>{alert.title}</strong>
                          )}
                          <span className={`tag ${alert.kind === "edit_war" ? "war" : ""}`}>{kindLabel(alert.kind)}</span>
                        </div>
                        <div className="meta">
                          {alert.synthetic && <span className="sim">Simulated</span>}
                          <span>{alert.wiki}</span>
                          <span>{alert.explanation}</span>
                          <span>{ago(alert.lastEventTs, clock)}</span>
                        </div>
                      </li>
                    );
                  })}
                </ul>
              ) : (
                <div className="empty">
                  No page has crossed a rule yet. Quiet is normal{pipeline ? "; `make replay` sends a fixture edit war through Kafka." : "; try “Stage an edit war” below."}
                </div>
              )}
            </div>

            <div className="panel">
              <div className="panel-head">
                <h2>Microbatches</h2>
                <span>{pipeline ? "Rows per Spark batch" : "Rows per 10-second commit"}</span>
              </div>
              <BatchChart bars={bars} showDropped={!pipeline} />
            </div>

            {!pipeline && (
              <div className="panel">
                <div className="panel-head">
                  <h2>Try a hard case</h2>
                  <span>Synthetic, clearly labelled</span>
                </div>
                <div className="cases">
                  <button onClick={() => inject("war")} disabled={!ready}>
                    <b>Stage an edit war</b>
                    <span>Four reverting edits by two users on one page</span>
                  </button>
                  <button onClick={() => inject("duplicate")} disabled={!engine.lastCommitted.length}>
                    <b>Re-deliver the last batch</b>
                    <span>Same event ids again: dedupe should absorb them</span>
                  </button>
                  <button onClick={() => inject("late")} disabled={!ready}>
                    <b>Send a late edit</b>
                    <span>One hour behind the stream: past the watermark</span>
                  </button>
                </div>
              </div>
            )}
          </div>
        </section>

        <Tuning
          weights={weights}
          thresholds={thresholds}
          onWeights={setWeights}
          onThresholds={updateThresholds}
          thresholdsDisabled={pipeline}
        />

        <HowItWorks />
      </main>
      <footer>
        Edit data © Wikimedia contributors, via the public{" "}
        <a href="https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams" target="_blank" rel="noopener noreferrer">
          EventStreams
        </a>{" "}
        API. Scores and alerts are heuristic triage aids, not vandalism verdicts. ·{" "}
        <a href="https://github.com/AbhayJuloori/wikipulse" target="_blank" rel="noopener noreferrer">
          Source on GitHub
        </a>
      </footer>
    </>
  );
}

function Kpi({ label, value, detail }: { label: string; value: string; detail: string }) {
  return (
    <div className="card">
      <div className="eyebrow">{label}</div>
      <div className="value">{value}</div>
      <div className="detail">{detail}</div>
    </div>
  );
}

function Slider(props: { label: string; value: number; min: number; max: number; onChange: (v: number) => void; disabled?: boolean }) {
  return (
    <label className="slider">
      <span>
        {props.label} <b>{props.value > 0 && props.min < 0 ? `+${props.value}` : props.value}</b>
      </span>
      <input
        type="range"
        min={props.min}
        max={props.max}
        value={props.value}
        disabled={props.disabled}
        onChange={(e) => props.onChange(Number(e.target.value))}
      />
    </label>
  );
}

function Tuning(props: {
  weights: Weights;
  thresholds: Thresholds;
  onWeights: (w: Weights) => void;
  onThresholds: (t: Thresholds) => void;
  thresholdsDisabled: boolean;
}) {
  const { weights: w, thresholds: t } = props;
  const changed = JSON.stringify(w) !== JSON.stringify(DEFAULT_WEIGHTS) || JSON.stringify(t) !== JSON.stringify(DEFAULT_THRESHOLDS);
  return (
    <section className="panel tuning">
      <div className="panel-head">
        <h2>Tune the rules</h2>
        <button className="ghost small" disabled={!changed} onClick={() => {
          props.onWeights(DEFAULT_WEIGHTS);
          props.onThresholds(DEFAULT_THRESHOLDS);
        }}>
          Reset to pipeline defaults
        </button>
      </div>
      <div className="tuning-grid">
        <div>
          <h3>Review score</h3>
          <p className="panel-note">The queue re-ranks instantly. Defaults match the Spark job.</p>
          <Slider label="Logged-out editor" value={w.anonymous} min={0} max={50} onChange={(v) => props.onWeights({ ...w, anonymous: v })} />
          <Slider label="Revert hint in summary" value={w.revertHint} min={0} max={50} onChange={(v) => props.onWeights({ ...w, revertHint: v })} />
          <Slider label="Size change cap" value={w.sizeCap} min={0} max={50} onChange={(v) => props.onWeights({ ...w, sizeCap: v })} />
          <Slider label="Bot flag" value={w.bot} min={-30} max={0} onChange={(v) => props.onWeights({ ...w, bot: v })} />
        </div>
        <div>
          <h3>Alert rules</h3>
          <p className="panel-note">
            {props.thresholdsDisabled ? "Alert thresholds run inside Spark in pipeline mode." : "Active alerts are recomputed over the current windows."}
          </p>
          <Slider label="Edit war: revert hints ≥" value={t.editWarReverts} min={1} max={8} disabled={props.thresholdsDisabled} onChange={(v) => props.onThresholds({ ...t, editWarReverts: v })} />
          <Slider label="Edit burst: edits ≥" value={t.editBurstEdits} min={3} max={20} disabled={props.thresholdsDisabled} onChange={(v) => props.onThresholds({ ...t, editBurstEdits: v })} />
          <Slider label="Edit burst: editors ≥" value={t.editBurstEditors} min={1} max={8} disabled={props.thresholdsDisabled} onChange={(v) => props.onThresholds({ ...t, editBurstEditors: v })} />
          <Slider label="Bot burst: bot edits ≥" value={t.botBurstEdits} min={3} max={30} disabled={props.thresholdsDisabled} onChange={(v) => props.onThresholds({ ...t, botBurstEdits: v })} />
        </div>
      </div>
    </section>
  );
}
