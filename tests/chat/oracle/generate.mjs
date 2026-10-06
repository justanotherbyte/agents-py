// Runs upstream's own chat modules over cases.json and writes oracle.json.
//
//   node tests/chat/oracle/generate.mjs <path to agents/packages/agents/src/chat>
//
// Node strips the TypeScript types; the modules are copied to a temp dir so
// their extensionless relative imports can be given ".ts".
import { mkdtempSync, readFileSync, writeFileSync, copyFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const source = process.argv[2];
const modules = [
  "message-builder",
  "stream-accumulator",
  "message-reconciler",
  "tool-state",
  "repair-transcript"
];
const dir = mkdtempSync(join(tmpdir(), "chat-oracle-"));
for (const name of modules) {
  const text = readFileSync(join(source, `${name}.ts`), "utf8").replace(
    /from "\.\/([a-z-]+)"/g,
    'from "./$1.ts"'
  );
  writeFileSync(join(dir, `${name}.ts`), text);
}
const builder = await import(join(dir, "message-builder.ts"));
const accumulator = await import(join(dir, "stream-accumulator.ts"));
const reconciler = await import(join(dir, "message-reconciler.ts"));
const toolState = await import(join(dir, "tool-state.ts"));
const repair = await import(join(dir, "repair-transcript.ts"));

// JSON.stringify drops undefined, as storage does; this is what's compared.
const plain = (value) => JSON.parse(JSON.stringify(value ?? null));
const cases = JSON.parse(readFileSync(join(here, "cases.json"), "utf8"));
const out = { builder: {}, accumulator: {}, reconciler: {}, toolState: {}, repair: {} };

for (const c of cases.builder) {
  const parts = structuredClone(c.parts ?? []);
  const replay = [];
  const handled = [];
  for (const chunk of c.chunks) {
    replay.push(builder.isReplayChunk(parts, chunk));
    handled.push(builder.applyChunkToParts(parts, chunk));
  }
  out.builder[c.name] = plain({ parts, replay, handled });
}

for (const c of cases.accumulator) {
  const acc = new accumulator.StreamAccumulator(structuredClone(c.options));
  const results = c.chunks.map((chunk) => acc.applyChunk(chunk));
  const result = { results, message: acc.toMessage() };
  if (c.mergeInto) result.merged = acc.mergeInto(structuredClone(c.mergeInto));
  out.accumulator[c.name] = plain(result);
}

for (const c of cases.reconciler) {
  out.reconciler[c.name] = plain({
    reconciled: reconciler.reconcileMessages(structuredClone(c.incoming), structuredClone(c.server)),
    toolMergeIds: c.incoming.map((m) => reconciler.resolveToolMergeId(m, c.server).id),
    orphan: c.orphan
      ? reconciler.reconcileOrphanPartial(c.orphan.existing, c.orphan.incoming)
      : null
  });
}

const updates = {
  result: (u) => toolState.toolResultUpdate(u.toolCallId, u.output, u.overrideState, u.errorText),
  crossMessage: (u) =>
    toolState.crossMessageToolResultUpdate(u.toolCallId, u.updateType, u.output, u.errorText, u.preliminary),
  approval: (u) => toolState.toolApprovalUpdate(u.toolCallId, u.approved),
  paused: (u) => toolState.pausedExecutionUpdate(u.toolCallId, u.executionId, u.output)
};
for (const c of cases.toolState) {
  const applied = toolState.applyToolUpdate(structuredClone(c.parts), updates[c.update.kind](c.update));
  out.toolState[c.name] = plain({
    applied,
    awaits: c.parts.map((p) => toolState.partAwaitsClientInteraction(p, new Set(c.clientTools ?? []))),
    incompleteBatch: toolState.hasIncompleteToolBatch([{ role: "assistant", parts: c.parts }])
  });
}

for (const c of cases.repair) {
  const result = repair.repairInterruptedToolParts(structuredClone(c.messages), {
    repairPart: (part) => ({ ...part, state: "output-error", errorText: "interrupted" }),
    repairApprovalResponded: c.repairApprovalResponded ?? false
  });
  out.repair[c.name] = plain(result);
}

writeFileSync(join(here, "oracle.json"), JSON.stringify(out, null, 1) + "\n");
console.log("cases:", Object.fromEntries(Object.entries(out).map(([k, v]) => [k, Object.keys(v).length])));
