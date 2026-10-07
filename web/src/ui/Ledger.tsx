import { diffUrl, safeWikiUrl, type Edit } from "../core/contract";
import { kafkaKey, partitionFor } from "../core/kafka";
import { anonymousKind, revertMatch, type Scored, type ScorePart } from "../core/score";
import { ago, fmt } from "./format";

export interface LedgerItem {
  edit: Edit;
  scored: Scored;
}

export interface EditContext {
  editorEdits: number;
  pageEdits: number;
  pageEditors: number;
}

interface Props {
  items: LedgerItem[];
  clock: number;
  expanded: string | null;
  onToggle: (id: string) => void;
  context: (edit: Edit) => EditContext;
  emptyText: string;
}

const PART_CLASS: Record<ScorePart["key"], string> = {
  base: "seg-base",
  anonymous: "seg-anon",
  revertHint: "seg-revert",
  size: "seg-size",
  bot: "seg-bot",
  sizePer100: "seg-size",
  sizeCap: "seg-size",
};

/** The score drawn as its additive parts, so the ranking explains itself at a glance. */
export function Receipt({ scored }: { scored: Scored }) {
  const positive = scored.parts.filter((p) => p.points > 0);
  const penalty = -scored.parts.filter((p) => p.points < 0).reduce((s, p) => s + p.points, 0);
  const gross = positive.reduce((s, p) => s + p.points, 0);
  const scale = (n: number) => `${Math.min(100, n)}%`;
  return (
    <span className="receipt" aria-hidden="true">
      <span className="receipt-bar" style={{ width: scale(gross) }}>
        {positive.map((part) => (
          <span key={part.key} className={PART_CLASS[part.key]} style={{ flexGrow: part.points }} />
        ))}
      </span>
      {penalty > 0 && (
        <span className="receipt-penalty" style={{ left: scale(Math.max(0, gross - penalty)), width: scale(Math.min(penalty, gross)) }} />
      )}
    </span>
  );
}

function signed(n: number): string {
  return n > 0 ? `+${fmt(n)}` : n < 0 ? `−${fmt(-n)}` : "±0";
}

export function Ledger({ items, clock, expanded, onToggle, context, emptyText }: Props) {
  if (!items.length) return <p className="empty">{emptyText}</p>;
  return (
    <ol className="ledger">
      {items.map(({ edit, scored }, index) => {
        const open = expanded === edit.eventId;
        const delta = (edit.newLength ?? 0) - (edit.oldLength ?? 0);
        const anon = anonymousKind(edit.user);
        const revert = revertMatch(edit.comment);
        return (
          <li key={edit.eventId} className={open ? "open" : ""}>
            <button className="ledger-row" aria-expanded={open} onClick={() => onToggle(edit.eventId)}>
              <span className="rank">{String(index + 1).padStart(2, "0")}</span>
              <span className="score-cell">
                <span className={`score-num ${scored.score >= 45 ? "hot" : ""}`}>{scored.score}</span>
                <Receipt scored={scored} />
              </span>
              <span className="entry">
                <span className="entry-title">
                  {edit.title}
                  {edit.synthetic && <span className="stamp sim">simulated</span>}
                </span>
                <span className="entry-meta">
                  <span>{edit.user}</span>
                  <span>{edit.wiki}</span>
                  <span>{ago(edit.timestamp, clock)}</span>
                  {anon && <span className="flag">{anon === "ip" ? "IP" : "TEMP"}</span>}
                  {revert && <span className="flag flag-revert">REVERT?</span>}
                  {edit.bot && <span className="flag">BOT</span>}
                </span>
              </span>
              <span className={`delta ${Math.abs(delta) >= 1000 ? "big" : ""}`}>{signed(delta)}</span>
            </button>
            {open && <Inspector edit={edit} scored={scored} ctx={context(edit)} />}
          </li>
        );
      })}
    </ol>
  );
}

function Inspector({ edit, scored, ctx }: { edit: Edit; scored: Scored; ctx: EditContext }) {
  const key = kafkaKey(edit);
  const diff = safeWikiUrl(diffUrl(edit));
  const page = safeWikiUrl(edit.pageUrl);
  return (
    <div className="inspector">
      <section>
        <h4>Why it ranks here</h4>
        <table className="tally">
          <tbody>
            {scored.parts.map((part) => (
              <tr key={part.key}>
                <td>
                  <i className={`swatch ${PART_CLASS[part.key]}`} />
                  {part.label}
                </td>
                <td>{part.points > 0 ? `+${part.points}` : `−${-part.points}`}</td>
              </tr>
            ))}
            <tr className="total">
              <td>Review score{scored.score === 100 ? " (capped)" : ""}</td>
              <td>{scored.score}</td>
            </tr>
          </tbody>
        </table>
      </section>
      <section>
        <h4>Context in the window</h4>
        <dl className="facts">
          <dt>This editor</dt>
          <dd>{fmt(ctx.editorEdits)} edit{ctx.editorEdits === 1 ? "" : "s"} in the last 10 min</dd>
          <dt>This page</dt>
          <dd>
            {fmt(ctx.pageEdits)} edit{ctx.pageEdits === 1 ? "" : "s"} by {fmt(ctx.pageEditors)} editor{ctx.pageEditors === 1 ? "" : "s"} in 5 min
          </dd>
          <dt>Summary</dt>
          <dd className="quote">{edit.comment ? `“${edit.comment}”` : <em>no edit summary</em>}</dd>
        </dl>
      </section>
      <section>
        <h4>In the pipeline</h4>
        <dl className="facts mono">
          <dt>Kafka key</dt>
          <dd>
            {key} → partition {partitionFor(key)}
          </dd>
          <dt>Dedupe id</dt>
          <dd>{edit.eventId}</dd>
          <dt>Revision</dt>
          <dd>{edit.revisionId}</dd>
          <dt>Schema</dt>
          <dd>{edit.schemaUri}</dd>
        </dl>
        <div className="links">
          {diff && (
            <a href={diff} target="_blank" rel="noopener noreferrer">
              Read the diff ↗
            </a>
          )}
          {page && (
            <a href={page} target="_blank" rel="noopener noreferrer">
              Open the article ↗
            </a>
          )}
        </div>
      </section>
    </div>
  );
}
