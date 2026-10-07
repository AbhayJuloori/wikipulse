// Mirrors risk_score / is_revert_comment / is_anonymous in src/wikipulse/events.py and the
// Spark expression in src/wikipulse/stream.py. Defaults must stay identical (parity tested).
import type { Edit } from "./contract";

export interface Weights {
  base: number;
  anonymous: number;
  revertHint: number;
  /** Points per 100 bytes changed, before the cap. */
  sizePer100: number;
  sizeCap: number;
  bot: number;
}

export const DEFAULT_WEIGHTS: Weights = {
  base: 20,
  anonymous: 25,
  revertHint: 20,
  sizePer100: 1,
  sizeCap: 25,
  bot: -10,
};

const REVERT = /\b(revert(?:ed|ing)?|undid|undo|rvv?|revertido|rückgängig)\b/i;
const ANONYMOUS = /^(?:(?:\d{1,3}\.){3}\d{1,3}|[0-9a-fA-F]*:[0-9a-fA-F:]+|~\d{4}-\d+-\d+)$/;

export function revertMatch(comment: string): string | null {
  return REVERT.exec(comment)?.[0] ?? null;
}

export function anonymousKind(user: string): "ip" | "temporary account" | null {
  if (!ANONYMOUS.test(user)) return null;
  return user.startsWith("~") ? "temporary account" : "ip";
}

export function sizeDelta(edit: Edit): number {
  return Math.abs((edit.newLength ?? 0) - (edit.oldLength ?? 0));
}

export interface ScorePart {
  key: keyof Weights | "size";
  label: string;
  points: number;
}

export interface Scored {
  score: number;
  parts: ScorePart[];
  revertHint: boolean;
  anonymous: boolean;
}

export function scoreEdit(edit: Edit, weights: Weights = DEFAULT_WEIGHTS): Scored {
  const parts: ScorePart[] = [{ key: "base", label: "Base", points: weights.base }];
  const anon = anonymousKind(edit.user);
  if (anon) parts.push({ key: "anonymous", label: `Logged out (${anon})`, points: weights.anonymous });
  const revert = revertMatch(edit.comment);
  if (revert) {
    parts.push({ key: "revertHint", label: `Revert hint “${revert}”`, points: weights.revertHint });
  }
  const delta = sizeDelta(edit);
  const sizePoints = Math.min(Math.floor(delta / 100) * weights.sizePer100, weights.sizeCap);
  if (sizePoints) {
    parts.push({ key: "size", label: `${delta.toLocaleString("en-US")} bytes changed`, points: sizePoints });
  }
  if (edit.bot) parts.push({ key: "bot", label: "Bot flag", points: weights.bot });
  const raw = parts.reduce((sum, part) => sum + part.points, 0);
  return {
    score: Math.max(0, Math.min(100, raw)),
    parts,
    revertHint: revert !== null,
    anonymous: anon !== null,
  };
}
