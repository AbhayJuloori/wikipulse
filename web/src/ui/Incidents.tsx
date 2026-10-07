import { safeWikiUrl, type Edit } from "../core/contract";
import type { AlertKind } from "../core/engine";
import { revertMatch } from "../core/score";
import { ago, kindLabel } from "./format";

export interface IncidentItem {
  alertId: string;
  kind: AlertKind;
  wiki: string;
  title: string;
  pageUrl: string;
  lastEventTs: number;
  explanation: string;
  synthetic: boolean;
  /** Edits in the alert's five-minute window, when the engine has them. */
  edits: Edit[] | null;
}

const LOOKBACK = 300;
const LANES = 4;

/** One lane per editor over the five-minute window: edit wars read as alternating dots. */
function Swimlane({ edits, end }: { edits: Edit[]; end: number }) {
  const counts = new Map<string, number>();
  for (const edit of edits) counts.set(edit.user, (counts.get(edit.user) ?? 0) + 1);
  const editors = [...counts.entries()].sort((a, b) => b[1] - a[1]).map(([user]) => user);
  const shown = editors.slice(0, LANES);
  const lanes = shown.length + (editors.length > LANES ? 1 : 0);
  const width = 300;
  const label = 92;
  const rowH = 16;
  const x = (ts: number) => label + ((ts - (end - LOOKBACK)) / LOOKBACK) * (width - label - 8);
  const lane = (user: string) => {
    const i = shown.indexOf(user);
    return i === -1 ? shown.length : i;
  };
  return (
    <svg className="swimlane" viewBox={`0 0 ${width} ${lanes * rowH + 14}`} role="img" aria-label={`${edits.length} edits by ${editors.length} editors in the five-minute window`}>
      {[...shown, ...(editors.length > LANES ? [`+${editors.length - LANES} others`] : [])].map((user, i) => (
        <g key={user}>
          <line className="lane" x1={label} x2={width - 8} y1={i * rowH + 8} y2={i * rowH + 8} />
          <text className="lane-name" x={label - 6} y={i * rowH + 11} textAnchor="end">
            {user.length > 13 ? `${user.slice(0, 12)}…` : user}
          </text>
        </g>
      ))}
      {edits.map((edit) => {
        const cy = lane(edit.user) * rowH + 8;
        const cx = x(edit.timestamp);
        if (edit.bot) return <rect key={edit.eventId} className="mark bot" x={cx - 3} y={cy - 3} width={6} height={6} />;
        return <circle key={edit.eventId} className={`mark ${revertMatch(edit.comment) ? "revert" : ""}`} cx={cx} cy={cy} r={3.5} />;
      })}
      <text className="lane-axis" x={label} y={lanes * rowH + 12}>
        −5 min
      </text>
      <text className="lane-axis" x={width - 8} y={lanes * rowH + 12} textAnchor="end">
        latest
      </text>
    </svg>
  );
}

export function Incidents({ items, clock, emptyText }: { items: IncidentItem[]; clock: number; emptyText: string }) {
  if (!items.length) return <p className="empty">{emptyText}</p>;
  return (
    <ul className="incidents">
      {items.map((item) => {
        const link = safeWikiUrl(item.pageUrl);
        return (
          <li key={item.alertId} className={`incident ${item.kind}`}>
            <div className="incident-head">
              <span className="stamp">{kindLabel(item.kind)}</span>
              {item.synthetic && <span className="stamp sim">simulated</span>}
              <span className="incident-age">{ago(item.lastEventTs, clock)}</span>
            </div>
            <h4>
              {link ? (
                <a href={link} target="_blank" rel="noopener noreferrer">
                  {item.title}
                </a>
              ) : (
                item.title
              )}
            </h4>
            <p className="incident-why">{item.explanation}</p>
            {item.edits && item.edits.length > 0 && <Swimlane edits={item.edits} end={item.lastEventTs} />}
          </li>
        );
      })}
    </ul>
  );
}
