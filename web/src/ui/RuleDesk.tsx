import { DEFAULT_THRESHOLDS, type Thresholds } from "../core/engine";
import { DEFAULT_WEIGHTS, type Weights } from "../core/score";

interface DialProps {
  label: string;
  value: number;
  base: number;
  min: number;
  max: number;
  unit?: string;
  disabled?: boolean;
  onChange: (value: number) => void;
}

function Dial({ label, value, base, min, max, unit = "", disabled, onChange }: DialProps) {
  const changed = value !== base;
  return (
    <label className={`dial ${changed ? "changed" : ""}`}>
      <span className="dial-label">{label}</span>
      <input type="range" min={min} max={max} value={value} disabled={disabled} onChange={(e) => onChange(Number(e.target.value))} />
      <span className="dial-value">
        {value > 0 && min < 0 ? "+" : ""}
        {value}
        {unit}
        {changed && <s>{base}</s>}
      </span>
    </label>
  );
}

export function RuleDesk(props: {
  weights: Weights;
  thresholds: Thresholds;
  onWeights: (w: Weights) => void;
  onThresholds: (t: Thresholds) => void;
  thresholdsDisabled: boolean;
}) {
  const { weights: w, thresholds: t } = props;
  const changed =
    JSON.stringify(w) !== JSON.stringify(DEFAULT_WEIGHTS) || JSON.stringify(t) !== JSON.stringify(DEFAULT_THRESHOLDS);
  return (
    <section className="rule-desk" aria-labelledby="desk-title">
      <header className="section-head with-tools">
        <div>
          <span className="kicker">Rule desk</span>
          <h3 id="desk-title">Change the rules and watch the ledger re-rank</h3>
        </div>
        <button
          className="text-button"
          disabled={!changed}
          onClick={() => {
            props.onWeights(DEFAULT_WEIGHTS);
            props.onThresholds(DEFAULT_THRESHOLDS);
          }}
        >
          Restore pipeline defaults
        </button>
      </header>
      <div className="desk-grid">
        <div>
          <h4>Review score</h4>
          <p className="formula">
            score = {w.base} + <b className="c-anon">{w.anonymous}</b>·logged-out + <b className="c-revert">{w.revertHint}</b>·revert-like +
            min(bytes/100, <b className="c-size">{w.sizeCap}</b>) {w.bot < 0 ? "−" : "+"} {Math.abs(w.bot)}·bot
          </p>
          <Dial label="Logged-out editor" value={w.anonymous} base={DEFAULT_WEIGHTS.anonymous} min={0} max={50} onChange={(v) => props.onWeights({ ...w, anonymous: v })} />
          <Dial label="Revert-like summary" value={w.revertHint} base={DEFAULT_WEIGHTS.revertHint} min={0} max={50} onChange={(v) => props.onWeights({ ...w, revertHint: v })} />
          <Dial label="Bytes-changed cap" value={w.sizeCap} base={DEFAULT_WEIGHTS.sizeCap} min={0} max={50} onChange={(v) => props.onWeights({ ...w, sizeCap: v })} />
          <Dial label="Bot flag" value={w.bot} base={DEFAULT_WEIGHTS.bot} min={-30} max={0} onChange={(v) => props.onWeights({ ...w, bot: v })} />
        </div>
        <div>
          <h4>Incident rules, per page over 5 minutes</h4>
          <p className="formula">
            {props.thresholdsDisabled
              ? "In pipeline mode these thresholds run inside Spark; they are shown read-only."
              : "Changing a threshold re-evaluates every open window immediately."}
          </p>
          <Dial label="Edit war: revert-like edits ≥" value={t.editWarReverts} base={DEFAULT_THRESHOLDS.editWarReverts} min={1} max={8} disabled={props.thresholdsDisabled} onChange={(v) => props.onThresholds({ ...t, editWarReverts: v })} />
          <Dial label="Edit burst: edits ≥" value={t.editBurstEdits} base={DEFAULT_THRESHOLDS.editBurstEdits} min={3} max={20} disabled={props.thresholdsDisabled} onChange={(v) => props.onThresholds({ ...t, editBurstEdits: v })} />
          <Dial label="Edit burst: editors ≥" value={t.editBurstEditors} base={DEFAULT_THRESHOLDS.editBurstEditors} min={1} max={8} disabled={props.thresholdsDisabled} onChange={(v) => props.onThresholds({ ...t, editBurstEditors: v })} />
          <Dial label="Bot burst: bot edits ≥" value={t.botBurstEdits} base={DEFAULT_THRESHOLDS.botBurstEdits} min={3} max={30} disabled={props.thresholdsDisabled} onChange={(v) => props.onThresholds({ ...t, botBurstEdits: v })} />
        </div>
      </div>
    </section>
  );
}
