import { useState } from "react";
import { clockTime, fmt } from "./format";

export interface BatchBar {
  id: number;
  at: number;
  accepted: number;
  duplicates: number;
  late: number;
}

interface Series {
  key: "accepted" | "duplicates" | "late";
  label: string;
  color: string;
}

const SERIES: Series[] = [
  { key: "accepted", label: "Accepted", color: "var(--series-accepted)" },
  { key: "duplicates", label: "Duplicate", color: "var(--series-duplicate)" },
  { key: "late", label: "Late (dropped)", color: "var(--series-late)" },
];

const SLOTS = 30;

export function BatchChart({ bars, showDropped }: { bars: BatchBar[]; showDropped: boolean }) {
  const [hover, setHover] = useState<number | null>(null);
  const series = showDropped ? SERIES : SERIES.slice(0, 1);
  const total = (bar: BatchBar) => series.reduce((sum, s) => sum + bar[s.key], 0);
  const max = Math.max(1, ...bars.map(total));
  const padded: (BatchBar | null)[] = [...Array(Math.max(0, SLOTS - bars.length)).fill(null), ...bars.slice(-SLOTS)];
  const active = hover === null ? null : padded[hover];

  return (
    <div className="chart-wrap">
      {series.length > 1 && (
        <div className="legend" aria-hidden="true">
          {series.map((s) => (
            <span key={s.key}>
              <i style={{ background: s.color }} />
              {s.label}
            </span>
          ))}
        </div>
      )}
      <div className="chart" role="img" aria-label={`Rows per microbatch for the last ${bars.length} batches`} onMouseLeave={() => setHover(null)}>
        {padded.map((bar, i) => (
          <div key={bar ? bar.id : `empty-${i}`} className="slot" onMouseEnter={() => setHover(bar ? i : null)}>
            {bar && (
              <div className="stack" style={{ height: `${Math.max(total(bar) ? 4 : 0, (total(bar) / max) * 100)}%` }}>
                {series
                  .filter((s) => bar[s.key] > 0)
                  .reverse()
                  .map((s) => (
                    <div key={s.key} style={{ flexGrow: bar[s.key], background: s.color }} />
                  ))}
              </div>
            )}
          </div>
        ))}
        {active && hover !== null && (
          <div className="tooltip" style={{ left: `${((hover + 0.5) / SLOTS) * 100}%` }}>
            <strong>Batch #{active.id}</strong>
            <span>{clockTime(active.at / 1000)}</span>
            {series.map((s) => (
              <span key={s.key}>
                <i style={{ background: s.color }} />
                {s.label}: {fmt(active[s.key])}
              </span>
            ))}
          </div>
        )}
      </div>
      <details className="table-view">
        <summary>Show as table</summary>
        <table>
          <thead>
            <tr>
              <th>Batch</th>
              {series.map((s) => (
                <th key={s.key}>{s.label}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {[...bars].reverse().map((bar) => (
              <tr key={bar.id}>
                <td>#{bar.id}</td>
                {series.map((s) => (
                  <td key={s.key}>{fmt(bar[s.key])}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </details>
    </div>
  );
}
