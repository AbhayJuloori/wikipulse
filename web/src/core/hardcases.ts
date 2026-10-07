// Synthetic events for the "try a hard case" controls. They are flagged `synthetic`, rendered
// with a SIMULATED badge, never link to Wikipedia, and go through the same engine as real edits.
import type { Edit } from "./contract";

let counter = 0;

function syntheticEdit(overrides: Partial<Edit> & Pick<Edit, "timestamp">): Edit {
  counter += 1;
  return {
    eventId: `demo-${Date.now()}-${counter}`,
    wiki: "enwiki",
    pageId: 900_000_001,
    revisionId: 900_000_000 + counter,
    user: "DemoEditorA",
    title: "WikiPulse demo page",
    pageUrl: "",
    serverUrl: "",
    bot: false,
    comment: "",
    oldLength: 1200,
    newLength: 1260,
    schemaUri: "/mediawiki/recentchange/1.0.0",
    synthetic: true,
    ...overrides,
  };
}

/** Two editors reverting each other four times within a minute: crosses the edit-war rule. */
export function editWar(wiki: string, eventNow: number): Edit[] {
  const pageId = 900_000_000 + Math.floor(Math.random() * 1000);
  const comments = ["Rewrite lead section", "Undid revision by DemoEditorA", "Revert: sourced", "rv unexplained removal"];
  return comments.map((comment, i) =>
    syntheticEdit({
      wiki: wiki === "all" ? "enwiki" : wiki,
      pageId,
      timestamp: eventNow - 40 + i * 10,
      user: i % 2 ? "DemoEditorB" : "DemoEditorA",
      comment,
      oldLength: i % 2 ? 4800 : 1200,
      newLength: i % 2 ? 1200 : 4800,
    }),
  );
}

/** An edit whose event time is an hour behind the stream: older than the 10-minute watermark. */
export function lateEdit(wiki: string, eventNow: number): Edit[] {
  return [
    syntheticEdit({
      wiki: wiki === "all" ? "enwiki" : wiki,
      timestamp: eventNow - 3600,
      user: "~2026-00000-00",
      comment: "Delayed delivery after a producer outage",
    }),
  ];
}
