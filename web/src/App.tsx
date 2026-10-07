import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { Edit } from "./core/contract";
import { DEFAULT_THRESHOLDS, PatrolEngine, type Thresholds } from "./core/engine";
import { editWar, lateEdit } from "./core/hardcases";
import { anonymousKind, DEFAULT_WEIGHTS, revertMatch, scoreEdit, type Weights } from "./core/score";
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
import { Anatomy } from "./ui/Anatomy";
import { clockTime, fmt } from "./ui/format";
import { HowItWorks } from "./ui/HowItWorks";
import { Incidents, type IncidentItem } from "./ui/Incidents";
import { Ledger, type EditContext, type LedgerItem } from "./ui/Ledger";
import { RuleDesk } from "./ui/RuleDesk";
import { Seismograph, type CommitMark, type TapeItem } from "./ui/Seismograph";

type Mode = "live" | "replay" | "pipeline";
type Theme = "day" | "night";

const TRIGGER_MS = 10_000;
const QUEUE_SIZE = 20;
const TAPE_MS = 120_000;
const CAPTURE_URL = `${import.meta.env.BASE_URL}sample/enwiki-capture.jsonl`;
const WIKIS = [
  { value: "enwiki", label: "English Wikipedia" },
  { value: "dewiki", label: "German Wikipedia" },
  { value: "frwiki", label: "French Wikipedia" },
  { value: "eswiki", label: "Spanish Wikipedia" },
  { value: "jawiki", label: "Japanese Wikipedia" },
  { value: "all", label: "Every wiki (busy)" },
];
const EMPTY_COUNTERS: SourceCounters = { messages: 0, edits: 0, filtered: 0, invalid: 0 };

function readTheme(): Theme {
  try {
    const saved = localStorage.getItem("wikipulse-theme");
    if (saved === "day" || saved === "night") return saved;
  } catch {
    /* storage unavailable: fall through to the system preference */
  }
  return window.matchMedia("(prefers-color-scheme: dark)").matches ? "night" : "day";
}

export function App() {
  const [mode, setMode] = useState<Mode>("live");
  const [wiki, setWiki] = useState("enwiki");
  const [speed, setSpeed] = useState(1);
  const [paused, setPaused] = useState(false);
  const [theme, setTheme] = useState<Theme>(readTheme);
  const [status, setStatus] = useState<{ state: SourceStatus; detail?: string }>({ state: "connecting" });
  const [counters, setCounters] = useState<SourceCounters>(EMPTY_COUNTERS);
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
  const tapeRef = useRef<TapeItem[]>([]);
  const pendingRef = useRef<TapeItem[]>([]);
  const commitsRef = useRef<CommitMark[]>([]);
  const seqRef = useRef(0);
  const pausedRef = useRef(paused);
  pausedRef.current = paused;
  const rerender = useCallback(() => setTick((n) => n + 1), []);

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    try {
      localStorage.setItem("wikipulse-theme", theme);
    } catch {
      /* the preference is a convenience only */
    }
  }, [theme]);

  useEffect(() => {
    pipelineAvailable().then((available) => {
      setHasPipeline(available);
      if (available) setMode("pipeline");
    });
  }, []);

  const arrive = useCallback((edit: Edit) => {
    engineRef.current.offer(edit);
    const item: TapeItem = { seq: seqRef.current++, edit, arrivedAt: Date.now(), outcome: "pending" };
    tapeRef.current.push(item);
    pendingRef.current.push(item);
  }, []);

  // A fresh engine per source, so live, replay and pipeline data never mix.
  useEffect(() => {
    if (mode === "pipeline") return;
    engineRef.current = new PatrolEngine(thresholds);
    tapeRef.current = [];
    pendingRef.current = [];
    commitsRef.current = [];
    setCounters(EMPTY_COUNTERS);
    setExpanded(null);
    rerender();
    const handlers = {
      onEdit: (edit: Edit) => {
        if (!pausedRef.current) arrive(edit);
      },
      onStatus: (state: SourceStatus, detail?: string) => setStatus({ state, detail }),
      onCounters: setCounters,
    };
    const source: EditSource = mode === "live" ? liveSource(wiki, handlers) : replaySource(CAPTURE_URL, speed, handlers);
    return () => source.stop();
  }, [mode, wiki, speed]);

  // The microbatch trigger, like Spark's processingTime = "10 seconds".
  useEffect(() => {
    if (mode === "pipeline") return;
    const commit = () => {
      if (!pausedRef.current) {
        const engine = engineRef.current;
        const stat = engine.commit();
        pendingRef.current.forEach((item, i) => (item.outcome = engine.lastOutcomes[i] ?? "accepted"));
        pendingRef.current = [];
        commitsRef.current = [...commitsRef.current, { id: stat.batchId, at: stat.committedAt }].slice(-40);
        const floor = Date.now() - TAPE_MS;
        tapeRef.current = tapeRef.current.filter((item) => item.arrivedAt >= floor);
      }
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
    const timer = window.setTimeout(() => setNotice(null), 12_000);
    return () => window.clearTimeout(timer);
  }, [notice]);

  const engine = engineRef.current;
  const pipeline = mode === "pipeline";
  const nowSeconds = Date.now() / 1000;
  const pipelineEdits = useMemo(() => (snapshot?.queue ?? []).map(silverRowToEdit), [snapshot]);
  const edits = pipeline ? pipelineEdits : engine.silver;
  const latestTs = edits.reduce((max, e) => Math.max(max, e.timestamp), 0);
  // Replay runs on a virtual event clock that can run ahead of wall time.
  const clock = Math.max(nowSeconds, latestTs);
  const commitCount = engine.batches.length;

  const queue: LedgerItem[] = useMemo(
    () =>
      edits
        .filter((edit) => edit.timestamp >= latestTs - 600 && !(hideBots && edit.bot))
        .map((edit) => ({ edit, scored: scoreEdit(edit, weights) }))
        .sort((a, b) => b.scored.score - a.scored.score || b.edit.timestamp - a.edit.timestamp)
        .slice(0, QUEUE_SIZE),
    // engine.silver is mutated in place by commits; commitCount tracks those commits.
    [edits, edits.length, commitCount, weights, hideBots, latestTs],
  );

  const recent = useMemo(
    () => edits.filter((e) => e.timestamp >= latestTs - 300),
    [edits, edits.length, commitCount, latestTs],
  );
  const pulse = useMemo(() => {
    const n = recent.length || 1;
    // Rate over the observed span (at least 10 s), so a young session is not under-counted.
    const spanMinutes = Math.max(10, latestTs - (recent[0]?.timestamp ?? latestTs)) / 60;
    return {
      perMinute: pipeline
        ? Math.round((snapshot?.metrics?.input_rows_per_second ?? 0) * 60)
        : Math.round(recent.length / spanMinutes),
      loggedOut: Math.round((100 * recent.filter((e) => anonymousKind(e.user)).length) / n),
      reverts: Math.round((100 * recent.filter((e) => revertMatch(e.comment)).length) / n),
      bots: Math.round((100 * recent.filter((e) => e.bot).length) / n),
    };
  }, [recent, pipeline, snapshot, latestTs]);

  const hotPages = useMemo(() => {
    const pages = new Map<string, { title: string; edits: number; editors: Set<string> }>();
    for (const e of recent) {
      const key = `${e.wiki}:${e.pageId}`;
      const page = pages.get(key) ?? { title: e.title, edits: 0, editors: new Set<string>() };
      page.edits += 1;
      page.editors.add(e.user);
      pages.set(key, page);
    }
    return [...pages.entries()]
      .map(([key, p]) => ({ key, title: p.title, edits: p.edits, editors: p.editors.size }))
      .filter((p) => p.edits > 1)
      .sort((a, b) => b.edits - a.edits || b.editors - a.editors)
      .slice(0, 6);
  }, [recent]);

  const context = (edit: Edit): EditContext => {
    const page = edits.filter((e) => e.wiki === edit.wiki && e.pageId === edit.pageId && e.timestamp >= latestTs - 300);
    return {
      editorEdits: edits.filter((e) => e.user === edit.user && e.timestamp >= latestTs - 600).length,
      pageEdits: page.length,
      pageEditors: new Set(page.map((e) => e.user)).size,
    };
  };

  const incidents: IncidentItem[] = pipeline
    ? (snapshot?.alerts ?? []).slice(0, 8).map((row) => ({
        alertId: String(row.alert_id),
        kind: String(row.kind) as IncidentItem["kind"],
        wiki: String(row.wiki),
        title: String(row.title),
        pageUrl: "",
        lastEventTs: Date.parse(String(row.last_event_ts)) / 1000,
        explanation: String(row.explanation),
        synthetic: false,
        edits: null,
      }))
    : [...engine.alerts.values()]
        .sort((a, b) => b.lastEventTs - a.lastEventTs)
        .slice(0, 8)
        .map((a) => ({ ...a, edits: engine.pageWindow(a.wiki, a.pageId, a.lastEventTs - 300, a.lastEventTs) }));

  const tape = (): TapeItem[] =>
    pipeline
      ? pipelineEdits.map((edit, i) => ({ seq: i, edit, arrivedAt: edit.timestamp * 1000, outcome: "accepted" as const }))
      : tapeRef.current;
  const commits = (): CommitMark[] =>
    pipeline
      ? (snapshot?.throughput ?? []).map((p, i, all) => ({
          id: (snapshot?.batch_id ?? 0) - (all.length - 1 - i),
          at: Date.parse(p.at),
        }))
      : commitsRef.current;

  const ready = pipeline ? Boolean(snapshot?.generated_at) : engine.totals.accepted > 0;
  const lastBatches = [...engine.batches].reverse().slice(0, 7);

  const updateThresholds = (next: Thresholds) => {
    setThresholds(next);
    engine.thresholds = next;
    engine.reclassify();
    rerender();
  };

  const inject = (kind: "war" | "duplicate" | "late") => {
    const eventNow = Math.max(Math.floor(Date.now() / 1000), latestTs);
    if (kind === "war") {
      editWar(wiki, eventNow).forEach(arrive);
      setNotice(
        "Four simulated edits queued on one page: two editors, three revert summaries. They commit with the next microbatch and should open an edit-war incident.",
      );
    } else if (kind === "duplicate") {
      const again = engine.lastCommitted;
      again.forEach(arrive);
      setNotice(
        `Batch #${engine.batches.at(-1)?.batchId ?? 0} re-sent: ${fmt(again.length)} edits with the same event ids, as a producer replay after a crash would send them. Expect rings on the duplicate lane and no new ledger rows.`,
      );
    } else {
      lateEdit(wiki, eventNow).forEach(arrive);
      setNotice(
        "One simulated edit stamped an hour behind the stream. The watermark trails the newest event by ten minutes, so the next commit drops it onto the late lane.",
      );
    }
    rerender();
  };

  const wikiLabel = mode === "replay" ? "English Wikipedia" : (WIKIS.find((w) => w.value === wiki)?.label ?? wiki);
  const statusLine = paused
    ? "Paused"
    : {
        connecting: pipeline ? "Connecting to the local pipeline" : "Connecting to Wikimedia",
        live: pipeline
          ? "Live from Kafka → Spark → Iceberg"
          : mode === "live"
            ? "Live from Wikimedia EventStreams"
            : `Replaying a recorded capture${speed > 1 ? ` at ${speed}×` : ""}`,
        reconnecting: "Reconnecting",
        ended: "Ended",
        error: "Source unavailable",
      }[status.state];
  const date = new Date(clock * 1000);

  return (
    <div className="page">
      <header className="masthead">
        <div className="mast-top">
          <span className="mast-label">Live Wikipedia edit patrol</span>
          <span className={`wire-status ${status.state === "live" && !paused ? "on" : ""}`}>
            <i aria-hidden="true" />
            {statusLine}
            {status.detail && <em> · {status.detail}</em>}
          </span>
          <button className="theme" onClick={() => setTheme(theme === "day" ? "night" : "day")}>
            {theme === "day" ? "Night desk" : "Day desk"}
          </button>
        </div>
        <h1 className="nameplate">
          Wiki<span>Pulse</span>
        </h1>
        <div className="dateline">
          <span>
            {date.toLocaleDateString("en-GB", { weekday: "long", day: "numeric", month: "long", year: "numeric", timeZone: "UTC" })}
          </span>
          <span>{clockTime(clock)}</span>
          <span>
            {pipeline ? `Spark batch #${snapshot?.batch_id ?? "—"}` : `Microbatch #${engine.batches.at(-1)?.batchId ?? "—"}`}
          </span>
        </div>
        <nav className="desk-nav" aria-label="Data source">
          <div className="sources" role="tablist">
            <button role="tab" aria-selected={mode === "live"} onClick={() => setMode("live")}>
              Live wire
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
          <div className="nav-controls">
            {mode === "live" && (
              <select aria-label="Wiki" value={wiki} onChange={(e) => setWiki(e.target.value)}>
                {WIKIS.map((w) => (
                  <option key={w.value} value={w.value}>
                    {w.label}
                  </option>
                ))}
              </select>
            )}
            {mode === "replay" && (
              <select aria-label="Replay speed" value={speed} onChange={(e) => setSpeed(Number(e.target.value))}>
                {[1, 3, 10].map((s) => (
                  <option key={s} value={s}>
                    {s}× speed
                  </option>
                ))}
              </select>
            )}
            {!pipeline && (
              <button className="text-button" onClick={() => setPaused((p) => !p)}>
                {paused ? "Resume" : "Pause"}
              </button>
            )}
          </div>
        </nav>
      </header>

      <main>
        <section className="lede">
          <h2>Which edits should a patroller check first?</h2>
          <p className="dek">
            {
              {
                live: "Your browser is reading Wikipedia's public change feed right now. Each edit is scored, batched every ten seconds and checked for edit wars, with the same rules as the WikiPulse Spark pipeline.",
                replay: "A 42-minute capture from the pipeline's Kafka topic, replayed through the same rules. Useful when the live feed is quiet or blocked.",
                pipeline: "Reading the Spark + Iceberg pipeline running on this machine. Scores are recomputed here so the rule desk still works.",
              }[mode]
            }
          </p>
          <dl className="pulse" aria-label="Last five minutes of stream time">
            <div>
              <dt>Edits / min</dt>
              <dd>{ready ? fmt(pulse.perMinute) : "—"}</dd>
            </div>
            <div>
              <dt>Logged out</dt>
              <dd>{ready ? `${pulse.loggedOut}%` : "—"}</dd>
            </div>
            <div>
              <dt>Revert-like</dt>
              <dd>{ready ? `${pulse.reverts}%` : "—"}</dd>
            </div>
            <div>
              <dt>Bots</dt>
              <dd>{ready ? `${pulse.bots}%` : "—"}</dd>
            </div>
            <div>
              <dt>Incidents</dt>
              <dd className={incidents.length ? "hot" : ""}>{ready ? fmt(incidents.length) : "—"}</dd>
            </div>
          </dl>
        </section>

        <section className="wire-section" aria-labelledby="wire-title">
          <header className="section-head">
            <span className="kicker">The wire</span>
            <h3 id="wire-title">Every arrival in the last 90 seconds</h3>
          </header>
          <Seismograph
            tape={tape}
            commits={commits}
            weights={weights}
            nextCommitAt={pipeline ? null : nextCommitAt}
            paused={paused}
            caption={
              pipeline
                ? "Plotted by event time from Spark's committed snapshot."
                : "Ticks appear on arrival and settle when a commit accepts them."
            }
          />
          {!pipeline && (
            <div className="wire-tools">
              <div className="commit-log">
                <h4>Commit log</h4>
                {lastBatches.length ? (
                  <ol>
                    {lastBatches.map((b) => (
                      <li key={b.batchId}>
                        <span>#{String(b.batchId).padStart(3, "0")}</span>
                        <span>{clockTime(b.committedAt / 1000)}</span>
                        <span>
                          <b>{b.accepted}</b> in
                        </span>
                        <span className={b.duplicates ? "dup-n" : "zero"}>{b.duplicates} dup</span>
                        <span className={b.late ? "late-n" : "zero"}>{b.late} late</span>
                        <span className="wm">wm {b.watermark === null ? "unset" : clockTime(b.watermark)}</span>
                      </li>
                    ))}
                  </ol>
                ) : (
                  <p className="empty">The first commit lands within ten seconds.</p>
                )}
              </div>
              <div className="stress">
                <h4>Stress the pipeline</h4>
                <p>Each button injects labelled synthetic events into the same engine.</p>
                <button onClick={() => inject("war")} disabled={!ready}>
                  <b>Stage an edit war</b>
                  <span>4 edits, 2 editors, 3 revert summaries</span>
                </button>
                <button onClick={() => inject("duplicate")} disabled={!engine.lastCommitted.length}>
                  <b>Re-send the last batch</b>
                  <span>Same ids again; dedupe should absorb them</span>
                </button>
                <button onClick={() => inject("late")} disabled={!ready}>
                  <b>Deliver an edit an hour late</b>
                  <span>Older than the 10-minute watermark</span>
                </button>
                {notice && (
                  <p className="notice" role="status">
                    {notice}
                  </p>
                )}
              </div>
            </div>
          )}
        </section>

        <div className="columns">
          <section className="col-main" aria-labelledby="ledger-title">
            <header className="section-head with-tools">
              <div>
                <span className="kicker">Review ledger</span>
                <h3 id="ledger-title">Top {QUEUE_SIZE} edits from the last ten minutes</h3>
              </div>
              <label className="check">
                <input type="checkbox" checked={hideBots} onChange={(e) => setHideBots(e.target.checked)} /> Hide bots
              </label>
            </header>
            <div className="receipt-key" aria-hidden="true">
              <span>
                <i className="seg-base" /> base
              </span>
              <span>
                <i className="seg-anon" /> logged out
              </span>
              <span>
                <i className="seg-revert" /> revert-like summary
              </span>
              <span>
                <i className="seg-size" /> bytes changed
              </span>
              <span>
                <i className="seg-bot" /> bot penalty
              </span>
            </div>
            <Ledger
              items={queue}
              clock={clock}
              expanded={expanded}
              onToggle={(id) => setExpanded(expanded === id ? null : id)}
              context={context}
              emptyText={
                status.state === "error"
                  ? `The source could not be reached${status.detail ? ` (${status.detail})` : ""}. Try the recorded capture.`
                  : pipeline
                    ? "Waiting for the first committed Spark batch."
                    : "Collecting edits. The first microbatch commits within ten seconds."
              }
            />
          </section>

          <aside className="col-side">
            <section aria-labelledby="incidents-title">
              <header className="section-head">
                <span className="kicker">Incidents</span>
                <h3 id="incidents-title">Pages crossing a rule</h3>
              </header>
              <Incidents
                items={incidents}
                clock={clock}
                emptyText={
                  pipeline
                    ? "Nothing has crossed a rule. `make replay` sends a fixture edit war through Kafka."
                    : "Nothing has crossed a rule yet, which is the normal state. Stage an edit war above to see one."
                }
              />
            </section>
            <section aria-labelledby="hot-title">
              <header className="section-head">
                <span className="kicker">Busiest pages</span>
                <h3 id="hot-title">Most edited in the last 5 minutes</h3>
              </header>
              {hotPages.length ? (
                <ol className="hot">
                  {hotPages.map((page) => (
                    <li key={page.key}>
                      <span className="hot-title">{page.title}</span>
                      <span className="hot-bar">
                        <i style={{ width: `${(page.edits / hotPages[0].edits) * 100}%` }} />
                      </span>
                      <span className="hot-n">
                        {page.edits} edits · {page.editors} {page.editors === 1 ? "editor" : "editors"}
                      </span>
                    </li>
                  ))}
                </ol>
              ) : (
                <p className="empty">No page has more than one edit in the window yet.</p>
              )}
            </section>
          </aside>
        </div>

        <RuleDesk
          weights={weights}
          thresholds={thresholds}
          onWeights={setWeights}
          onThresholds={updateThresholds}
          thresholdsDisabled={pipeline}
        />

        {!pipeline && (
          <section className="anatomy-section" aria-labelledby="anatomy-title">
            <header className="section-head">
              <span className="kicker">Anatomy of this session</span>
              <h3 id="anatomy-title">Where every message went, and where the real pipeline does the same work</h3>
            </header>
            <Anatomy
              wikiLabel={wikiLabel}
              c={{
                messages: counters.messages,
                edits: counters.edits,
                matching: counters.edits - counters.filtered,
                invalid: counters.invalid,
                batches: commitCount,
                accepted: engine.totals.accepted,
                duplicates: engine.totals.duplicates,
                late: engine.totals.late,
                retained: engine.silver.length,
                alerts: engine.alerts.size,
              }}
            />
          </section>
        )}

        <HowItWorks />
      </main>

      <footer className="colophon">
        <span>
          Edits © Wikipedia contributors, CC BY-SA, via{" "}
          <a href="https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams" target="_blank" rel="noopener noreferrer">
            Wikimedia EventStreams
          </a>
          .
        </span>
        <span>Scores and incidents are triage aids, not verdicts on any editor.</span>
        <a href="https://github.com/AbhayJuloori/wikipulse" target="_blank" rel="noopener noreferrer">
          Source and pipeline on GitHub ↗
        </a>
      </footer>
    </div>
  );
}
