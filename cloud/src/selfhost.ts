// Self-hosted Bun runtime for cloud/src/index.ts (no Cloudflare Workers required).
// Uses bun:sqlite for Durable Object SQLite storage and Bun.serve for HTTP + WebSocket + static assets.
import { Database } from "bun:sqlite";
import { AsyncLocalStorage } from "node:async_hooks";
import { existsSync, mkdirSync, readFileSync, statSync } from "node:fs";
import { connect as netConnect } from "node:net";
import { homedir } from "node:os";
import { dirname, extname, join, resolve } from "node:path";
import { connect as tlsConnect } from "node:tls";

const REPO_ROOT = resolve(import.meta.dir, "../..");

export interface SelfHostOptions {
  port?: number;
  host?: string;
  dbPath?: string;
  assetsDir?: string;
  token?: string;
  ownerEmail?: string;
  smtp?: string;
  vapidPublic?: string;
  vapidPrivate?: string;
  movedTo?: string;
  lang?: string;
  allowIps?: string;
}

export interface SelfHostHandle {
  server: ReturnType<typeof Bun.serve>;
  db: Database;
  port: number;
  url: string;
  stop: (closeDb?: boolean) => void;
}

class BridgeWebSocket {
  readyState = 1; // WebSocket.OPEN
  _tags: string[] = [];
  _real: any = null;
  _queue: Array<string | Uint8Array | ArrayBuffer> = [];
  _onClose?: () => void;

  send(data: string | Uint8Array | ArrayBuffer) {
    if (this.readyState !== 1) return;
    if (this._real) {
      try { this._real.send(data); } catch {}
    } else {
      this._queue.push(data);
    }
  }

  close(code?: number, reason?: string) {
    if (this.readyState === 3) return;
    this.readyState = 3;
    this._onClose?.();
    if (this._real) {
      try { this._real.close(code, reason); } catch {}
    }
  }

  _attach(realWs: any) {
    this._real = realWs;
    if (this.readyState !== 1) {
      try { realWs.close(); } catch {}
      return;
    }
    for (const msg of this._queue) {
      try { realWs.send(msg); } catch {}
    }
    this._queue.length = 0;
  }
}

function wrapTcpSocket(socket: any, hostname: string) {
  let controller: ReadableStreamDefaultController<Uint8Array> | null = null;
  let closed = false;

  const readable = new ReadableStream<Uint8Array>({
    start(c) {
      controller = c;
      socket.on("data", (chunk: Uint8Array) => {
        try { c.enqueue(new Uint8Array(chunk)); } catch {}
      });
      socket.on("end", () => {
        if (!closed) { closed = true; try { c.close(); } catch {} }
      });
      socket.on("error", (err: Error) => {
        if (!closed) { closed = true; try { c.error(err); } catch {} }
      });
    },
    cancel() {
      try { socket.destroy(); } catch {}
    },
  });

  const writable = new WritableStream<Uint8Array>({
    write(chunk) {
      return new Promise((resolveWrite, rejectWrite) => {
        socket.write(chunk, (err?: Error | null) => err ? rejectWrite(err) : resolveWrite());
      });
    },
    close() {
      socket.end();
    },
    abort(reason) {
      socket.destroy(reason instanceof Error ? reason : undefined);
    },
  });

  return {
    readable,
    writable,
    close: async () => { try { socket.destroy(); } catch {} },
    startTls: () => {
      // Detach our data listeners before handing the raw socket to tls.connect.
      socket.removeAllListeners("data");
      socket.removeAllListeners("end");
      socket.removeAllListeners("error");
      const tlsSock = tlsConnect({ socket, servername: hostname });
      return wrapTcpSocket(tlsSock, hostname);
    },
  };
}

let shimsRegistered = false;
let loadedWorkerModule: Promise<any> | null = null;
async function loadWorkerModule(): Promise<any> {
  if (loadedWorkerModule) return loadedWorkerModule;
  loadedWorkerModule = (async () => {
    ensureCloudflareShims();
    const res = await Bun.build({
      entrypoints: [resolve(import.meta.dir, "index.ts")],
      target: "bun",
      plugins: [{
        name: "noop-selfhost-cloudflare-build-shims",
        setup(build) {
          build.onResolve({ filter: /^cloudflare:(workers|email|sockets)$/ }, (args) => ({
            path: args.path,
            namespace: "cf-selfhost",
          }));
          build.onLoad({ filter: /.*/, namespace: "cf-selfhost" }, (args) => {
            if (args.path === "cloudflare:workers") {
              return {
                loader: "js",
                contents: `export class DurableObject { constructor(ctx, env) { this.ctx = ctx; this.env = env; } }`,
              };
            }
            if (args.path === "cloudflare:email") {
              return {
                loader: "js",
                contents: `export class EmailMessage { constructor(from, to, raw) { this.from = from; this.to = to; this.raw = raw; } }`,
              };
            }
            return {
              loader: "js",
              contents: `export function connect(...args) { return globalThis.__noopCfConnect(...args); }`,
            };
          });
        },
      }],
    });
    if (!res.success) {
      throw new Error(`selfhost build failed: ${res.logs.map((l) => l.message).join("; ")}`);
    }
    const js = await res.outputs[0].text();
    const dataUrl = `data:text/javascript;base64,${Buffer.from(js).toString("base64")}`;
    return await import(dataUrl);
  })();
  return loadedWorkerModule;
}

function ensureCloudflareShims() {
  if (shimsRegistered) return;
  shimsRegistered = true;

  (globalThis as any).__noopCfConnect = (
    address: { hostname: string; port: number } | string,
    opts?: { secureTransport?: string },
  ) => {
    const target = typeof address === "string"
      ? { hostname: address.split(":")[0], port: Number(address.split(":")[1] || 25) }
      : address;
    const raw = opts?.secureTransport === "on"
      ? tlsConnect({ host: target.hostname, port: target.port, servername: target.hostname })
      : netConnect({ host: target.hostname, port: target.port });
    return wrapTcpSocket(raw, target.hostname);
  };

  (globalThis as any).WebSocketRequestResponsePair = class WebSocketRequestResponsePair {
    constructor(public request: string, public response: string) {}
  };
  (globalThis as any).WebSocketPair = class WebSocketPair {
    0: BridgeWebSocket;
    1: BridgeWebSocket;
    constructor() {
      this[0] = new BridgeWebSocket();
      this[1] = new BridgeWebSocket();
    }
  };
}

function hasMultipleStatements(sql: string): boolean {
  let count = 0;
  let inSingle = false;
  let inDouble = false;
  let inLineComment = false;
  let inBlockComment = false;
  let hasNonWs = false;

  for (let i = 0; i < sql.length; i++) {
    const ch = sql[i];
    const next = sql[i + 1];
    if (inLineComment) {
      if (ch === "\n") inLineComment = false;
      continue;
    }
    if (inBlockComment) {
      if (ch === "*" && next === "/") { inBlockComment = false; i++; }
      continue;
    }
    if (inSingle) {
      if (ch === "'") {
        if (next === "'") i++;
        else inSingle = false;
      }
      continue;
    }
    if (inDouble) {
      if (ch === '"') {
        if (next === '"') i++;
        else inDouble = false;
      }
      continue;
    }
    if (ch === "-" && next === "-") { inLineComment = true; i++; continue; }
    if (ch === "/" && next === "*") { inBlockComment = true; i++; continue; }
    if (ch === "'") { inSingle = true; hasNonWs = true; continue; }
    if (ch === '"') { inDouble = true; hasNonWs = true; continue; }
    if (ch === ";") {
      if (hasNonWs) count++;
      hasNonWs = false;
      if (count > 1) return true;
      continue;
    }
    if (!/\s/.test(ch)) hasNonWs = true;
  }
  if (hasNonWs && count >= 1) return true;
  return false;
}

const normalizeBind = (v: unknown) => (v instanceof ArrayBuffer ? new Uint8Array(v) : v);
const normalizeRow = (row: Record<string, unknown>) => {
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(row)) {
    out[k] = v instanceof Uint8Array
      ? v.buffer.slice(v.byteOffset, v.byteOffset + v.byteLength)
      : v;
  }
  return out;
};

function makeSqlStorage(db: Database) {
  return {
    exec(q: string, ...args: unknown[]) {
      const binds = args.map(normalizeBind);
      if (binds.length === 0 && hasMultipleStatements(q)) {
        db.exec(q);
        return {
          toArray: () => [],
          one: () => { throw new Error("expected a row, got 0"); },
          rowsRead: 0,
          rowsWritten: 0,
        };
      }
      const stmt = db.query(q);
      if (stmt.columnNames.length > 0) {
        const rows = (stmt.all(...(binds as any[])) as Record<string, unknown>[]).map(normalizeRow);
        return {
          toArray: () => rows,
          one: () => {
            if (rows.length !== 1) throw new Error(`expected 1 row, got ${rows.length}`);
            return rows[0];
          },
          rowsRead: rows.length,
          rowsWritten: 0,
        };
      }
      const info = stmt.run(...(binds as any[]));
      return {
        toArray: () => [],
        one: () => { throw new Error("expected 1 row, got 0"); },
        rowsRead: 0,
        rowsWritten: Number(info.changes ?? 0),
      };
    },
    get databaseSize() {
      const r = db.query("SELECT page_count * page_size AS sz FROM pragma_page_count(), pragma_page_size()").get() as { sz?: number } | null;
      return Number(r?.sz ?? 0);
    },
  };
}

const MIME: Record<string, string> = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".mjs": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".wasm": "application/wasm",
  ".json": "application/json; charset=utf-8",
  ".webmanifest": "application/manifest+json; charset=utf-8",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".webp": "image/webp",
  ".gif": "image/gif",
  ".svg": "image/svg+xml",
  ".ico": "image/x-icon",
  ".txt": "text/plain; charset=utf-8",
  ".br": "application/octet-stream",
  ".gz": "application/octet-stream",
};

function resolveAssetsDir(explicit?: string): string {
  if (explicit) return resolve(explicit);
  const built = join(REPO_ROOT, "app", "dist", "web", "noop");
  if (existsSync(join(built, "index.html"))) return built;
  return join(REPO_ROOT, "web");
}

function makeAssetsFetcher(assetsDir: string) {
  const root = resolve(assetsDir);
  return {
    async fetch(req: Request): Promise<Response> {
      const url = new URL(req.url);
      let rel = "/";
      try { rel = decodeURIComponent(url.pathname); } catch { return new Response("Bad Request", { status: 400 }); }
      const clean = rel.replace(/^\/+/, "");
      const candidate = resolve(root, clean || "index.html");
      if (candidate !== root && !candidate.startsWith(root + "/")) return new Response("Forbidden", { status: 403 });

      const serveFile = (path: string, contentType?: string) => {
        const st = statSync(path);
        const type = contentType ?? MIME[extname(path).toLowerCase()] ?? "application/octet-stream";
        const etag = `"${st.size}-${Math.floor(st.mtimeMs)}"`;
        if (req.headers.get("if-none-match") === etag) {
          return new Response(null, { status: 304, headers: { etag } });
        }
        return new Response(req.method === "HEAD" ? null : Bun.file(path), {
          status: 200,
          headers: { "content-type": type, "content-length": String(st.size), etag },
        });
      };

      if (existsSync(candidate) && statSync(candidate).isFile()) {
        return serveFile(candidate);
      }
      if (existsSync(candidate) && statSync(candidate).isDirectory()) {
        const idx = join(candidate, "index.html");
        if (existsSync(idx) && statSync(idx).isFile()) return serveFile(idx, MIME[".html"]);
      }

      // Single-page app fallback for clean routes only (never swallow missing .js/.wasm/.br/.gz files).
      const leaf = clean.split("/").pop() ?? "";
      if (!leaf.includes(".")) {
        const idx = join(root, "index.html");
        if (existsSync(idx) && statSync(idx).isFile()) return serveFile(idx, MIME[".html"]);
      }
      return new Response("Not Found", { status: 404 });
    },
  };
}

function readTokenFromConfig(): string {
  const f = join(homedir(), ".noop", "noop.toml");
  if (!existsSync(f)) return "";
  try {
    const parsed = Bun.TOML.parse(readFileSync(f, "utf8")) as { server?: { token?: string } };
    return String(parsed.server?.token ?? "");
  } catch {
    return "";
  }
}

export async function startSelfHostServer(opts: SelfHostOptions = {}): Promise<SelfHostHandle> {
  const { default: worker, Hub } = await loadWorkerModule();

  const port = opts.port ?? Number(process.env.HUB_PORT || process.env.PORT || 8788);
  const host = opts.host ?? process.env.HUB_HOST ?? process.env.HOST ?? "0.0.0.0";
  const dbPath = opts.dbPath ?? process.env.HUB_DB ?? join(homedir(), ".noop", "cloud.sqlite");
  if (dbPath !== ":memory:") mkdirSync(dirname(resolve(dbPath)), { recursive: true });

  const db = new Database(dbPath);
  db.exec("PRAGMA journal_mode = WAL; PRAGMA busy_timeout = 5000;");

  const sockets = new Set<BridgeWebSocket>();
  const als = new AsyncLocalStorage<{ accepted?: BridgeWebSocket }>();
  let autoResp: { request: string; response: string } | null = null;

  const sql = makeSqlStorage(db);
  const ctx = {
    storage: {
      sql,
      transactionSync<T>(fn: () => T): T {
        return db.transaction(fn)();
      },
    },
    setWebSocketAutoResponse(pair: { request: string; response: string }) {
      autoResp = pair;
    },
    acceptWebSocket(ws: BridgeWebSocket, tags: string[] = []) {
      ws._tags = [...tags];
      ws._onClose = () => { sockets.delete(ws); };
      sockets.add(ws);
      const slot = als.getStore();
      if (slot) slot.accepted = ws;
    },
    getWebSockets(tag?: string) {
      return [...sockets].filter((ws) => !tag || ws._tags.includes(tag));
    },
    getTags(ws: BridgeWebSocket) {
      return ws._tags;
    },
    waitUntil(p: Promise<unknown>) {
      Promise.resolve(p).catch((err) => console.error("selfhost waitUntil:", err));
    },
  };

  let hubInstance: any;
  const env: Record<string, any> = {
    HUB_TOKEN: opts.token ?? process.env.HUB_TOKEN ?? readTokenFromConfig(),
    HUB_OWNER_EMAIL: opts.ownerEmail ?? process.env.HUB_OWNER_EMAIL,
    HUB_SMTP: opts.smtp ?? process.env.HUB_SMTP,
    HUB_VAPID_PUBLIC: opts.vapidPublic ?? process.env.HUB_VAPID_PUBLIC,
    HUB_VAPID_PRIVATE: opts.vapidPrivate ?? process.env.HUB_VAPID_PRIVATE,
    HUB_MOVED_TO: opts.movedTo ?? process.env.HUB_MOVED_TO,
    HUB_LANG: opts.lang ?? process.env.HUB_LANG,
    HUB_ALLOW_IPS: opts.allowIps ?? process.env.HUB_ALLOW_IPS,
    ASSETS: makeAssetsFetcher(resolveAssetsDir(opts.assetsDir ?? process.env.HUB_ASSETS_DIR)),
    HUB: {
      idFromName: (name: string) => name,
      get: () => hubInstance,
    },
  };

  hubInstance = new Hub(ctx as any, env as any);
  hubInstance.ctx = ctx;
  hubInstance.env = env;

  const server = Bun.serve<{ bridge: BridgeWebSocket }>({
    hostname: host,
    port,
    async fetch(req, srv) {
      let fwdReq = req;
      if (!req.headers.has("cf-connecting-ip")) {
        const headers = new Headers(req.headers);
        const peer = req.headers.get("x-real-ip")
          ?? req.headers.get("x-forwarded-for")?.split(",")[0]?.trim()
          ?? srv.requestIP(req)?.address
          ?? "";
        if (peer) headers.set("cf-connecting-ip", peer);
        fwdReq = new Request(req, { headers });
      }
      const slot: { accepted?: BridgeWebSocket } = {};
      const res = await als.run(slot, () => worker.fetch(fwdReq, env as any));
      if (res.status === 101 && slot.accepted) {
        if (srv.upgrade(req, { data: { bridge: slot.accepted } })) return undefined as unknown as Response;
        slot.accepted.close();
        return new Response("WebSocket upgrade failed", { status: 400 });
      }
      return res;
    },
    websocket: {
      open(ws) {
        ws.data.bridge._attach(ws);
      },
      message(ws, msg) {
        if (typeof msg === "string" && autoResp && msg === autoResp.request) {
          try { ws.send(autoResp.response); } catch {}
          return;
        }
        hubInstance.webSocketMessage?.(ws.data.bridge, msg);
      },
      close(ws) {
        const bridge = ws.data.bridge;
        bridge.readyState = 3;
        sockets.delete(bridge);
        hubInstance.webSocketClose?.(bridge);
      },
    },
  });

  const shownHost = host === "0.0.0.0" || host === "::" ? "127.0.0.1" : host;
  return {
    server,
    db,
    port: server.port ?? port,
    url: `http://${shownHost}:${server.port ?? port}`,
    stop(closeDb = true) {
      server.stop(true);
      if (closeDb) try { db.close(); } catch {}
    },
  };
}

export function parseSelfHostArgs(args: string[]): SelfHostOptions {
  const opts: SelfHostOptions = {};
  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    if ((a === "--port" || a === "-p") && args[i + 1]) opts.port = Number(args[++i]);
    else if ((a === "--host" || a === "-H") && args[i + 1]) opts.host = args[++i];
    else if (a === "--db" && args[i + 1]) opts.dbPath = args[++i];
    else if (a === "--assets" && args[i + 1]) opts.assetsDir = args[++i];
    else if (a === "--token" && args[i + 1]) opts.token = args[++i];
    else if (a === "--lang" && args[i + 1]) opts.lang = args[++i];
    else if (a === "--owner-email" && args[i + 1]) opts.ownerEmail = args[++i];
    else if (a === "--allow-ips" && args[i + 1]) opts.allowIps = args[++i];
  }
  return opts;
}

if (import.meta.main) {
  const opts = parseSelfHostArgs(process.argv.slice(2));
  const h = await startSelfHostServer(opts);
  const assetsDir = resolveAssetsDir(opts.assetsDir ?? process.env.HUB_ASSETS_DIR);
  const dbLabel = opts.dbPath ?? process.env.HUB_DB ?? join(homedir(), ".noop", "cloud.sqlite");
  console.log(`noop self-hosted cloud listening on ${h.url} (db: ${dbLabel}, assets: ${assetsDir})`);
  const shutdown = () => { h.stop(true); process.exit(0); };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}
