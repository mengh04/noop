import { expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { startSelfHostServer } from "../cloud/src/selfhost";
import { b32decode, hotp, stepAt } from "../cloud/src/totp";

test("self-hosted hub serves HTTP, SQLite files/blobs, 2FA/login, and WebSocket streams", async () => {
  const dir = mkdtempSync(join(tmpdir(), "noop-selfhost-"));
  const dbPath = join(dir, "cloud.sqlite");
  const token = "test-machine-token-1234567890";
  const h = await startSelfHostServer({ port: 0, host: "127.0.0.1", dbPath, token, lang: "en" });
  const auth = { Authorization: `Bearer ${token}` };

  try {
    // 1. Static asset index & unauthenticated status check
    const indexRes = await fetch(`${h.url}/`);
    expect(indexRes.status).toBe(200);
    expect(indexRes.headers.get("content-type")).toContain("text/html");

    const unauth = await fetch(`${h.url}/api/status`);
    expect(unauth.status).toBe(401);

    // 2. Host state & status round-trip
    const putState = await fetch(`${h.url}/api/host/state`, {
      method: "PUT",
      headers: { ...auth, "Content-Type": "application/json" },
      body: JSON.stringify({ status: { hosts: [{ name: "laptop", online: true }] } }),
    });
    expect(putState.status).toBe(200);

    const statusRes = await fetch(`${h.url}/api/status`, { headers: auth });
    expect(statusRes.status).toBe(200);
    const statusJson = await statusRes.json() as any;
    expect(statusJson.hosts?.[0]?.name).toBe("laptop");
    expect(statusJson.hub?.online).toBe(true);

    // 3. Files CAS commit + conflict + changes
    const b64Hello = Buffer.from("hello selfhost").toString("base64");
    const commit1 = await fetch(`${h.url}/api/files/commit`, {
      method: "POST",
      headers: { ...auth, "Content-Type": "application/json", "x-noop-host": "laptop" },
      body: JSON.stringify({
        msg: "init",
        changes: [{ path: "tasks/open/T-0001.md", base_version: 0, content: b64Hello }],
      }),
    });
    expect(commit1.status).toBe(200);
    const c1 = await commit1.json() as any;
    expect(c1.rev).toBeGreaterThan(0);

    const conflict = await fetch(`${h.url}/api/files/commit`, {
      method: "POST",
      headers: { ...auth, "Content-Type": "application/json" },
      body: JSON.stringify({
        msg: "stale",
        changes: [{ path: "tasks/open/T-0001.md", base_version: 0, content: Buffer.from("overwrite").toString("base64") }],
      }),
    });
    expect(conflict.status).toBe(409);

    const changes = await fetch(`${h.url}/api/files/changes?since=0`, { headers: auth });
    expect(changes.status).toBe(200);
    const chJson = await changes.json() as any;
    expect(chJson.files.some((c: any) => c.path === "tasks/open/T-0001.md" && c.content === b64Hello)).toBe(true);

    // 4. Binary image upload & /media/:id (ArrayBuffer <-> Uint8Array BLOB conversion)
    const pngBytes = new Uint8Array([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 1, 2, 3, 4]);
    const fd = new FormData();
    fd.append("file", new File([pngBytes], "shot.png", { type: "image/png" }));
    const upRes = await fetch(`${h.url}/api/uploads`, { method: "POST", headers: auth, body: fd });
    expect(upRes.status).toBe(200);
    const { url: mediaPath } = await upRes.json() as { url: string };
    expect(mediaPath.startsWith("/media/")).toBe(true);

    const mediaRes = await fetch(`${h.url}${mediaPath}`, { headers: auth });
    expect(mediaRes.status).toBe(200);
    expect(new Uint8Array(await mediaRes.arrayBuffer())).toEqual(pngBytes);

    // 5. WebSockets: owner ?v=2 receives $status push; host socket receives pong & inbox nudge
    const ownerWs = new WebSocket(`${h.url.replace(/^http/, "ws")}/api/stream?v=2&token=${encodeURIComponent(token)}`);
    let hostWs: WebSocket | undefined;

    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("WebSocket test timed out")), 4000);
      let gotPong = false, gotInbox = false, gotOwnerStatus = false;
      const maybeDone = () => {
        if (gotPong && gotInbox && gotOwnerStatus) {
          clearTimeout(timer);
          resolve();
        }
      };

      ownerWs.onmessage = (e) => {
        const s = String(e.data);
        if (s.includes("$status")) gotOwnerStatus = true;
        maybeDone();
      };
      ownerWs.onerror = reject;
      ownerWs.onopen = () => {
        hostWs = new WebSocket(`${h.url.replace(/^http/, "ws")}/api/host/stream`, { headers: auth } as any);
        hostWs.onopen = () => {
          hostWs!.send("ping");
          void fetch(`${h.url}/api/messages`, {
            method: "POST",
            headers: { ...auth, "Content-Type": "application/json" },
            body: JSON.stringify({ text: "hello from test", attachments: [mediaPath] }),
          });
        };
        hostWs.onmessage = (e) => {
          const s = String(e.data);
          if (s === "pong") gotPong = true;
          if (s.includes('"inbox"')) gotInbox = true;
          maybeDone();
        };
        hostWs.onerror = reject;
      };
    });
    hostWs?.close();
    ownerWs.close();

    // Drain owner message from host inbox
    const inboxRes = await fetch(`${h.url}/api/host/inbox?after=0`, { headers: auth });
    const inboxJson = await inboxRes.json() as { items: Array<{ seq: number; type: string; event?: { text: string } }> };
    expect(inboxJson.items.length).toBeGreaterThanOrEqual(1);
    expect(inboxJson.items[0].type).toBe("message");
    expect(inboxJson.items[0].event?.text).toBe("hello from test");

    // 6. 2FA setup + CLI code login flow
    const tfaSetup = await fetch(`${h.url}/api/host/2fa/setup`, { method: "POST", headers: auth });
    expect(tfaSetup.status).toBe(200);
    const { secret } = await tfaSetup.json() as { secret: string; uri: string };
    const totpCode = await hotp(b32decode(secret), stepAt());
    const tfaConfirm = await fetch(`${h.url}/api/host/2fa/confirm`, {
      method: "POST",
      headers: { ...auth, "Content-Type": "application/json" },
      body: JSON.stringify({ totp: totpCode }),
    });
    expect(tfaConfirm.status).toBe(200);

    const startAuth = await fetch(`${h.url}/api/auth/start`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ email: "owner@example.com" }),
    });
    expect(startAuth.status).toBe(200);
    const cliCodeRes = await fetch(`${h.url}/api/host/login-code`, { headers: auth });
    expect(cliCodeRes.status).toBe(200);
    const { code } = await cliCodeRes.json() as { code: string };

    // Confirm with +1 step (within ±1 window, not yet consumed).
    const nextTotp = await hotp(b32decode(secret), stepAt() + 1);
    const verifyRes = await fetch(`${h.url}/api/auth/verify`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ email: "owner@example.com", code, totp: nextTotp }),
    });
    expect(verifyRes.status).toBe(200);
    const { session } = await verifyRes.json() as { session: string };
    expect(session).toMatch(/^[0-9a-f]{64}$/);
  } finally {
    h.stop(true);
    rmSync(dir, { recursive: true, force: true });
  }
});
