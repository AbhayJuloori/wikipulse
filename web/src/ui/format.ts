export function ago(eventSeconds: number, clockSeconds: number): string {
  const seconds = Math.max(0, Math.round(clockSeconds - eventSeconds));
  if (seconds < 60) return `${seconds}s ago`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m ago`;
  return `${Math.round(seconds / 3600)}h ago`;
}

export function clockTime(seconds: number): string {
  return new Date(seconds * 1000).toISOString().slice(11, 19) + " UTC";
}

export const fmt = (n: number) => n.toLocaleString("en-US");

export function kindLabel(kind: string): string {
  return kind.replaceAll("_", " ");
}
