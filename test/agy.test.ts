import { expect, test } from "bun:test";
import { chmodSync, mkdirSync, mkdtempSync, readFileSync, realpathSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { classify, failoverAccount } from "../src/accounts";
import { AgyModel } from "../src/brain";
import { type Cfg, DATA } from "../src/core";
import { guestCommand } from "../src/guest";
import { agyUserEvent, cliCommand, extractAgyStreamText, runAgent, startModel } from "../src/runner";
import { settingsView } from "../src/server";

test("cliCommand builds agy stream-json command with permissions, dirs, model, and effort", () => {
  const dir = realpathSync(mkdtempSync(join(tmpdir(), "noop-agy-dir-")));
  try {
    const cmd = cliCommand("agy", {
      resources: { work_dirs: [dir] },
      clis: { agy: { model: "gemini-4-hydrogen-high", effort: "high" } },
    } as unknown as Cfg);
    expect(cmd.slice(0, 7)).toEqual([
      "agy", "-p=", "--input-format", "stream-json", "--output-format", "stream-json", "--disable-slash-commands",
    ]);
    expect(cmd).toContain("--dangerously-skip-permissions");
    expect(cmd).not.toContain("--mode");
    expect(cmd[cmd.indexOf("--model") + 1]).toBe("gemini-4-hydrogen-high");
    expect(cmd[cmd.indexOf("--effort") + 1]).toBe("high");
    const added = cmd.filter((_, i) => cmd[i - 1] === "--add-dir");
    expect(added).toContain(dir);

    const autoMode = cliCommand("agy", { clis: { agy: { permission_mode: "auto" } } } as unknown as Cfg);
    expect(autoMode).not.toContain("--dangerously-skip-permissions");
    expect(autoMode[autoMode.indexOf("--mode") + 1]).toBe("auto");
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("startModel reads --model for agy, else falls back to agy settings.json", () => {
  expect(startModel("agy", ["agy", "-p=", "--model", "gemini-4-carbon-medium"])).toBe("gemini-4-carbon-medium");
  const agyHome = mkdtempSync(join(tmpdir(), "noop-agy-home-"));
  try {
    writeFileSync(join(agyHome, "settings.json"), JSON.stringify({ model: "Gemini 4 Hydrogen (High)" }));
    expect(startModel("agy", ["agy", "-p="], undefined, agyHome)).toBe("Gemini 4 Hydrogen (High)");
  } finally {
    rmSync(agyHome, { recursive: true, force: true });
  }
});

test("agyUserEvent and extractAgyStreamText encode and decode stream-json turns", () => {
  const line = agyUserEvent("hello agy");
  expect(line.endsWith("\n")).toBe(true);
  expect(JSON.parse(line)).toEqual({ event: "user", message: { role: "user", content: "hello agy" } });

  const stream = [
    JSON.stringify({ event: "init", conversation_id: "c1", init: { model: "gemini-3.8-flash-low" } }),
    JSON.stringify({ event: "step_update", step_update: { step_index: 2, state: "ACTIVE", step_type: "tool", tool_name: "run_command", tool_info: { parameters: { CommandLine: "echo hi" } } } }),
    JSON.stringify({ event: "step_update", step_update: { step_index: 2, state: "DONE", step_type: "tool", tool_name: "run_command" } }),
    JSON.stringify({ event: "step_update", step_update: { step_index: 3, state: "DONE", step_type: "agent_response", text_delta: "done\n" } }),
    JSON.stringify({ event: "result", result: { status: "SUCCESS", response: '```json\n{"status":"done","summary":"ok"}\n```' } }),
  ].join("\n");
  expect(extractAgyStreamText(stream)).toContain('"status":"done"');

  const errStream = JSON.stringify({ event: "result", result: { status: "ERROR", error: "401 unauthorized" } });
  expect(extractAgyStreamText(errStream)).toBe("401 unauthorized");
});

test("runAgent drives agy stream-json stdin/stdout end-to-end", async () => {
  const tmp = mkdtempSync(join(tmpdir(), "noop-agy-run-"));
  try {
    const script = join(tmp, "fake-agy.ts");
    const bin = join(tmp, "agy");
    writeFileSync(script, `
import { readFileSync } from "node:fs";
const input = readFileSync(0, "utf8").trim();
const msg = JSON.parse(input);
if (msg.event !== "user" || !msg.message?.content) process.exit(2);
console.log(JSON.stringify({ event: "init", conversation_id: "c1", init: { model: "gemini-4-hydrogen-high" } }));
console.log(JSON.stringify({ event: "step_update", step_update: { step_index: 1, state: "ACTIVE", step_type: "tool", tool_name: "run_command", tool_info: { parameters: { CommandLine: "bun test" } } } }));
console.log(JSON.stringify({ event: "step_update", step_update: { step_index: 1, state: "DONE", step_type: "tool", tool_name: "run_command" } }));
console.log(JSON.stringify({ event: "step_update", step_update: { step_index: 2, state: "DONE", step_type: "agent_response", text_delta: "partial " } }));
console.log(JSON.stringify({ event: "result", result: { status: "SUCCESS", response: "\\x60\\x60\\x60json\\n" + JSON.stringify({ status: "done", summary: "ran with agy" }) + "\\n\\x60\\x60\\x60" } }));
`);
    writeFileSync(bin, `#!/bin/sh\nexec ${process.execPath} ${script} "$@"\n`);
    chmodSync(bin, 0o755);

    const log = join(DATA, "logs", "runs", "T-AGY1-test.log");
    mkdirSync(join(tmp, "work"), { recursive: true });
    const cfg = { clis: { agy: { command: [bin, "-p=", "--input-format", "stream-json", "--output-format", "stream-json"] } } } as unknown as Cfg;
    const out = await runAgent("agy", "please run tests", join(tmp, "work"), cfg, log);
    expect(out).toContain('"summary":"ran with agy"');
    expect(readFileSync(log, "utf8")).toContain("gemini-4-hydrogen-high");

    // Empty response on ERROR result + AGY_ERROR stderr must preserve the actual error and not parse as PERMISSION_DENIED result
    const errScript = join(tmp, "fake-agy-err.ts");
    const errBin = join(tmp, "agy-err");
    writeFileSync(errScript, `
console.log(JSON.stringify({ event: "result", result: { status: "ERROR", response: "", error: "Your current account is not eligible in your location." } }));
console.error('AGY_ERROR: {"short_error":"PERMISSION_DENIED (code 403)","status":"PERMISSION_DENIED","error_code":403}');
process.exit(3);
`);
    writeFileSync(errBin, `#!/bin/sh\nexec ${process.execPath} ${errScript} "$@"\n`);
    chmodSync(errBin, 0o755);
    const { parseResult } = await import("../src/runner");
    const errOut = await runAgent("agy", "test err", join(tmp, "work"), { clis: { agy: { command: [errBin, "-p=", "--input-format", "stream-json", "--output-format", "stream-json"] } } } as unknown as Cfg, log);
    expect(errOut).toContain("not eligible in your location");
    const res = parseResult(errOut);
    expect(res.status).toBe("failed");
    expect(res.summary).toContain("not eligible in your location");
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
});

test("AgyModel supports warm multi-turn stream-json conversations", async () => {
  const tmp = mkdtempSync(join(tmpdir(), "noop-agy-brain-"));
  try {
    const script = join(tmp, "fake-agy-brain.ts");
    const bin = join(tmp, "agy");
    writeFileSync(script, `
const dec = new TextDecoder();
let buf = "";
let turns = 0;
console.log(JSON.stringify({ event: "init", init: { model: "gemini-3.8-flash-low" } }));
for await (const chunk of Bun.stdin.stream()) {
  buf += dec.decode(chunk);
  while (buf.includes("\\n")) {
    const i = buf.indexOf("\\n");
    const line = buf.slice(0, i); buf = buf.slice(i + 1);
    if (!line.trim()) continue;
    const m = JSON.parse(line);
    turns++;
    console.log(JSON.stringify({ event: "result", result: { status: "SUCCESS", response: JSON.stringify({ turn: turns, hasSystem: m.message.content.includes("<system>"), content: m.message.content }) } }));
  }
}
`);
    writeFileSync(bin, `#!/bin/sh\nexec ${process.execPath} ${script} "$@"\n`);
    chmodSync(bin, 0o755);

    const model = new AgyModel("gemini-3.8-flash-low", bin, 10_000);
    const conv = await model.open("You are Noop.");
    try {
      const r1 = JSON.parse(await conv.send("first"));
      expect(r1.turn).toBe(1);
      expect(r1.hasSystem).toBe(true);
      expect(r1.content).toContain("first");

      const r2 = JSON.parse(await conv.send("second"));
      expect(r2.turn).toBe(2);
      expect(r2.hasSystem).toBe(false);
      expect(r2.content).toBe("second");
    } finally {
      conv.close();
      // close warmed follow-up process if spawned
      (model as any).next?.kill();
    }
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
});

test("accounts failover, classify, guestCommand, and settingsView support agy", () => {
  const cfg = { accounts: [{ cli: "agy", name: "default" }, { cli: "claude", name: "zz-c" }] } as unknown as Cfg;
  expect(failoverAccount(cfg, { cli: "agy", name: "default", dir: "" }, ["agy"], (c) => c === "agy" || c === "claude")?.cli).toBe("claude");
  expect(failoverAccount(cfg, { cli: "agy", name: "default", dir: "" }, ["agy"], (c) => c === "agy")).toBeNull();
  expect(classify("RESOURCE_EXHAUSTED: quota exceeded")).toBe("quota");
  expect(classify("not authenticated, please sign in")).toBe("auth");
  expect(guestCommand("agy")).toContain("--sandbox");
  expect(settingsView().options.clis).toContainEqual(["agy"]);
  expect(settingsView().options.clis).toContainEqual(["agy", "claude"]);
});
