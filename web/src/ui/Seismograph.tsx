import { useEffect, useRef, useState } from "react";
import type { Edit } from "../core/contract";
import type { Outcome } from "../core/engine";
import { scoreEdit, type Weights } from "../core/score";
import { clockTime } from "./format";

export interface TapeItem {
  seq: number;
  edit: Edit;
  /** Wall-clock arrival in ms (pipeline mode: event time). */
  arrivedAt: number;
  outcome: Outcome | "pending";
}

export interface CommitMark {
  id: number;
  at: number;
}

interface Props {
  tape: () => TapeItem[];
  commits: () => CommitMark[];
  weights: Weights;
  nextCommitAt: number | null;
  paused: boolean;
  windowSeconds?: number;
  caption: string;
}

const HEIGHT = 230;
const TOP = 26;
const BASE = 150;
const DUP_Y = 176;
const LATE_Y = 204;
const PRIORITY = 45;

/**
 * A scrolling strip of every arrival: tick height is the review score, ticks turn solid when
 * a microbatch commits them, and rejected arrivals fall below the baseline by reason.
 */
export function Seismograph({ tape, commits, weights, nextCommitAt, paused, windowSeconds = 90, caption }: Props) {
  const wrap = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(900);
  const [now, setNow] = useState(Date.now());
  const [hover, setHover] = useState<{ x: number; item: TapeItem } | null>(null);

  useEffect(() => {
    const el = wrap.current;
    if (!el) return;
    const observer = new ResizeObserver(([entry]) => setWidth(Math.max(280, entry.contentRect.width)));
    observer.observe(el);
    return () => observer.disconnect();
  }, []);

  useEffect(() => {
    const reduce = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    if (paused) return;
    let frame = 0;
    let last = 0;
    const loop = (t: number) => {
      if (t - last > (reduce ? 1000 : 50)) {
        setNow(Date.now());
        last = t;
      }
      frame = requestAnimationFrame(loop);
    };
    frame = requestAnimationFrame(loop);
    return () => cancelAnimationFrame(frame);
  }, [paused]);

  const span = windowSeconds * 1000;
  const start = now - span;
  const x = (ms: number) => ((ms - start) / span) * width;
  const items = tape().filter((item) => item.arrivedAt >= start - 1000);
  const marks = commits().filter((c) => c.at >= start - 1000);
  const lastCommit = marks.length ? marks[marks.length - 1].at : null;
  const scored = items.map((item) => ({ item, score: scoreEdit(item.edit, weights).score }));

  const onMove = (event: React.MouseEvent<SVGSVGElement>) => {
    const rect = event.currentTarget.getBoundingClientRect();
    const px = event.clientX - rect.left;
    let best: { x: number; item: TapeItem } | null = null;
    let distance = 8;
    for (const { item } of scored) {
      const d = Math.abs(x(item.arrivedAt) - px);
      if (d < distance) {
        distance = d;
        best = { x: x(item.arrivedAt), item };
      }
    }
    setHover(best);
  };

  const gridSeconds = [15, 30, 45, 60, 75, 90].filter((s) => s <= windowSeconds);
  const hoverScore = hover ? scoreEdit(hover.item.edit, weights) : null;

  return (
    <figure className="seismo" ref={wrap}>
      <svg
        width={width}
        height={HEIGHT}
        role="img"
        aria-label={`Arrivals over the last ${windowSeconds} seconds: ${items.length} edits, ${marks.length} microbatch commits`}
        onMouseMove={onMove}
        onMouseLeave={() => setHover(null)}
      >
        {/* Buffered region: arrivals since the last commit, waiting for the next trigger. */}
        {lastCommit !== null && nextCommitAt !== null && (
          <g>
            <rect className="buffer" x={x(lastCommit)} y={TOP} width={Math.max(0, width - x(lastCommit))} height={BASE - TOP} />
            <text className="axis-label" x={width - 6} y={TOP + 12} textAnchor="end">
              buffer · commit in {Math.max(0, Math.round((nextCommitAt - now) / 1000))}s
            </text>
          </g>
        )}
        {gridSeconds.map((s) => (
          <g key={s}>
            <line className="grid" x1={x(now - s * 1000)} x2={x(now - s * 1000)} y1={TOP} y2={HEIGHT - 4} />
            <text className="axis-label" x={x(now - s * 1000) + 4} y={HEIGHT - 6}>
              −{s}s
            </text>
          </g>
        ))}
        <line className="baseline" x1={0} x2={width} y1={BASE} y2={BASE} />
        <text className="lane-label" x={4} y={DUP_Y + 4}>
          duplicate
        </text>
        <text className="lane-label" x={4} y={LATE_Y + 4}>
          late
        </text>
        {marks.map((mark) => (
          <g key={mark.id} className="commit">
            <line x1={x(mark.at)} x2={x(mark.at)} y1={TOP - 8} y2={BASE} />
            <text x={x(mark.at)} y={TOP - 12} textAnchor="middle">
              #{mark.id}
            </text>
          </g>
        ))}
        {scored.map(({ item, score }) => {
          const px = x(item.arrivedAt);
          if (item.outcome === "duplicate") {
            return <circle key={item.seq} className="dup" cx={px} cy={DUP_Y} r={4} />;
          }
          if (item.outcome === "late") {
            return (
              <path key={item.seq} className="late" d={`M${px - 4} ${LATE_Y - 4}l8 8m0 -8l-8 8`} />
            );
          }
          const h = Math.max(3, (score / 100) * (BASE - TOP - 6));
          const cls = item.outcome === "pending" ? "tick pending" : score >= PRIORITY ? "tick priority" : "tick";
          return (
            <g key={item.seq}>
              <line className={cls} x1={px} x2={px} y1={BASE} y2={BASE - h} />
              {score >= PRIORITY && item.outcome === "accepted" && <circle className="cap" cx={px} cy={BASE - h} r={2.5} />}
              {item.edit.synthetic && <circle className="sim-dot" cx={px} cy={BASE + 8} r={2} />}
            </g>
          );
        })}
        {hover && <line className="crosshair" x1={hover.x} x2={hover.x} y1={TOP} y2={HEIGHT - 16} />}
      </svg>
      {hover && hoverScore && (
        <div className="seismo-tip" style={{ left: Math.min(Math.max(hover.x, 120), width - 120) }}>
          <b>{hover.item.edit.title}</b>
          <span>
            {hover.item.outcome === "pending" ? "buffered" : hover.item.outcome} · score {hoverScore.score}
          </span>
          <span>
            {hover.item.edit.user} · {clockTime(hover.item.arrivedAt / 1000)}
          </span>
        </div>
      )}
      <figcaption>
        <span className="key">
          <i className="k-tick" /> committed edit, height = score
        </span>
        <span className="key">
          <i className="k-priority" /> score ≥ {PRIORITY}
        </span>
        <span className="key">
          <i className="k-pending" /> buffered, not yet committed
        </span>
        <span className="key">
          <i className="k-dup" /> duplicate absorbed
        </span>
        <span className="key">
          <i className="k-late" /> late, below watermark
        </span>
        <span className="key">
          <i className="k-commit" /> microbatch commit
        </span>
        <span className="caption-note">{caption}</span>
      </figcaption>
    </figure>
  );
}
