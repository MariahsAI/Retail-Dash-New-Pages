/**
 * MX1 Trade — OKX → browser relay.
 *
 * The browser must never hold OKX credentials. This process signs in once,
 * holds the private stream, and fans out normalized frames to session clients.
 *
 * Private:  account, positions, orders   (needs HMAC login)
 * Public:   mark-price per open instrument (drives continuous uPnL motion)
 *
 * env: OKX_KEY, OKX_SECRET, OKX_PASSPHRASE, PORT
 */

import crypto from "node:crypto";
import { WebSocket, WebSocketServer } from "ws";

const PRIVATE = "wss://ws.okx.com:8443/ws/v5/private";
const PUBLIC = "wss://ws.okx.com:8443/ws/v5/public";

const clients = new Set();
const broadcast = (msg) => {
  const s = JSON.stringify(msg);
  clients.forEach((c) => c.readyState === 1 && c.send(s));
};

/* ---------------------------------------------------------------- state */

let lastAccount = null;
const positions = new Map(); // instId:posSide -> position
let publicWs = null;
let subscribed = new Set();

/* --------------------------------------------------------- private feed */

function sign(ts) {
  return crypto
    .createHmac("sha256", process.env.OKX_SECRET)
    .update(ts + "GET" + "/users/self/verify")
    .digest("base64");
}

function connectPrivate() {
  const ws = new WebSocket(PRIVATE);
  let ping;

  ws.on("open", () => {
    const ts = (Date.now() / 1000).toFixed(0);
    ws.send(
      JSON.stringify({
        op: "login",
        args: [
          {
            apiKey: process.env.OKX_KEY,
            passphrase: process.env.OKX_PASSPHRASE,
            timestamp: ts,
            sign: sign(ts),
          },
        ],
      })
    );
    ping = setInterval(() => ws.readyState === 1 && ws.send("ping"), 20_000);
  });

  ws.on("message", (raw) => {
    const text = raw.toString();
    if (text === "pong") return;
    const msg = JSON.parse(text);

    if (msg.event === "login") {
      return ws.send(
        JSON.stringify({
          op: "subscribe",
          args: [
            { channel: "account" },
            { channel: "positions", instType: "ANY" },
            { channel: "orders", instType: "ANY" },
          ],
        })
      );
    }
    if (msg.event === "error") return console.error("[okx]", msg.code, msg.msg);
    if (!msg.arg || !msg.data) return;

    const ch = msg.arg.channel;

    if (ch === "account") {
      const a = msg.data[0];
      lastAccount = {
        type: "account",
        totalEq: a.totalEq,
        upl: a.details?.reduce((s, d) => s + Number(d.upl || 0), 0) ?? 0,
        ts: Number(a.uTime),
      };
      broadcast(lastAccount);
    }

    if (ch === "positions") {
      positions.clear();
      msg.data
        .filter((p) => Number(p.pos) !== 0)
        .forEach((p) =>
          positions.set(`${p.instId}:${p.posSide}`, {
            instId: p.instId,
            posSide: p.posSide,
            pos: p.pos,
            avgPx: p.avgPx,
            ctVal: p.ctVal || 1,
            markPx: p.markPx,
            lever: Number(p.lever || 1),
          })
        );
      broadcast({ type: "positions", data: [...positions.values()] });
      syncMarkSubs();
    }

    if (ch === "orders") {
      const fills = msg.data.filter((o) => o.state === "filled");
      if (fills.length) {
        const pnl = fills.reduce((s, o) => s + Number(o.pnl || 0), 0);
        broadcast({ type: "fill", pnl, orders: fills.map((o) => o.ordId) });
      }
    }
  });

  const retry = () => {
    clearInterval(ping);
    setTimeout(connectPrivate, 2000);
  };
  ws.on("close", retry);
  ws.on("error", () => ws.close());
}

/* ----------------------------------------------------- mark price stream */

function syncMarkSubs() {
  const want = new Set([...positions.values()].map((p) => p.instId));
  if (!publicWs || publicWs.readyState !== 1) return;

  const add = [...want].filter((i) => !subscribed.has(i));
  const drop = [...subscribed].filter((i) => !want.has(i));

  if (add.length)
    publicWs.send(
      JSON.stringify({
        op: "subscribe",
        args: add.map((instId) => ({ channel: "mark-price", instId })),
      })
    );
  if (drop.length)
    publicWs.send(
      JSON.stringify({
        op: "unsubscribe",
        args: drop.map((instId) => ({ channel: "mark-price", instId })),
      })
    );

  subscribed = want;
}

function connectPublic() {
  const ws = new WebSocket(PUBLIC);
  publicWs = ws;
  let ping;

  ws.on("open", () => {
    subscribed = new Set();
    syncMarkSubs();
    ping = setInterval(() => ws.readyState === 1 && ws.send("ping"), 20_000);
  });

  ws.on("message", (raw) => {
    const text = raw.toString();
    if (text === "pong") return;
    const msg = JSON.parse(text);
    if (msg.arg?.channel !== "mark-price" || !msg.data) return;
    const d = msg.data[0];
    broadcast({ type: "mark", instId: d.instId, markPx: d.markPx });
  });

  const retry = () => {
    clearInterval(ping);
    setTimeout(connectPublic, 2000);
  };
  ws.on("close", retry);
  ws.on("error", () => ws.close());
}

/* ---------------------------------------------------------- fan-out server */

const wss = new WebSocketServer({ port: Number(process.env.PORT) || 8787 });

wss.on("connection", (client) => {
  clients.add(client);

  // Prime the new client so its first paint has real numbers.
  client.send(JSON.stringify({ type: "hello", anchorEq: Number(lastAccount?.totalEq ?? 0) }));
  if (lastAccount) client.send(JSON.stringify(lastAccount));
  if (positions.size)
    client.send(JSON.stringify({ type: "positions", data: [...positions.values()] }));

  client.on("message", (m) => m.toString() === "ping" && client.send("pong"));
  client.on("close", () => clients.delete(client));
});

connectPrivate();
connectPublic();

console.log(`relay listening on :${process.env.PORT || 8787}`);
