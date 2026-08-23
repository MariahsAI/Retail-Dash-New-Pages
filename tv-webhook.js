/**
 * MX1 Trade — TradingView alert receiver.
 *
 * TradingView alerts POST here; we normalize, retain the latest per
 * instrument+timeframe, and fan out over the same socket the portfolio
 * already listens on.
 *
 * Add to okx-relay.js:
 *   import { mountTradingView } from "./tv-webhook.js";
 *   const tv = mountTradingView(broadcast);
 *   // in wss.on("connection"): client.send(JSON.stringify(tv.snapshot()));
 *
 * env: TV_WEBHOOK_SECRET, TV_PORT
 *
 * NOTE: TradingView sends alerts from a fixed set of IPs. Allowlisting them
 * at the firewall is the real defense — the shared secret below is a second
 * layer, not the only one. Webhook alerts require a paid TradingView plan.
 */

import http from "node:http";
import crypto from "node:crypto";

const TV_IPS = [
  "52.89.214.238",
  "34.212.75.30",
  "54.218.53.128",
  "52.32.178.7",
];

/* Map a raw alert into a verdict + a -1..1 consensus score. */
function normalize(raw) {
  const action = String(raw.action ?? raw.verdict ?? raw.signal ?? "").trim().toUpperCase();

  // Explicit score wins; otherwise derive one from the action word.
  const fromAction = {
    "STRONG BUY": 0.8, "BUY": 0.5, "LONG": 0.5,
    "NEUTRAL": 0, "HOLD": 0, "FLAT": 0,
    "SELL": -0.5, "SHORT": -0.5, "STRONG SELL": -0.8,
  };

  const score = raw.score !== undefined
    ? Math.max(-1, Math.min(1, Number(raw.score)))
    : (fromAction[action] ?? 0);

  const verdict = action || (
    score > 0.5 ? "STRONG BUY" : score > 0.15 ? "BUY" :
    score < -0.5 ? "STRONG SELL" : score < -0.15 ? "SELL" : "NEUTRAL"
  );

  return {
    instId: String(raw.instId ?? raw.ticker ?? raw.symbol ?? "UNKNOWN").toUpperCase(),
    timeframe: String(raw.timeframe ?? raw.interval ?? ""),
    verdict,
    score,
    source: String(raw.source ?? raw.strategy ?? "TradingView"),
    price: raw.price !== undefined ? Number(raw.price) : undefined,
    ts: Date.now(),
  };
}

export function mountTradingView(broadcast) {
  const latest = new Map();   // "instId:timeframe" -> signal
  const seen = new Set();     // replay guard on alert id

  const server = http.createServer((req, res) => {
    if (req.method !== "POST" || !req.url.startsWith("/tv-webhook")) {
      res.writeHead(404).end();
      return;
    }

    const ip = (req.headers["x-forwarded-for"] || req.socket.remoteAddress || "")
      .split(",")[0].trim().replace(/^::ffff:/, "");

    if (process.env.TV_ENFORCE_IP === "1" && !TV_IPS.includes(ip)) {
      console.warn("[tv] rejected ip", ip);
      res.writeHead(403).end();
      return;
    }

    let body = "";
    req.on("data", c => {
      body += c;
      if (body.length > 16_384) req.destroy();   // alerts are tiny; cap the surface
    });

    req.on("end", () => {
      let payload;
      try {
        payload = JSON.parse(body);
      } catch {
        res.writeHead(400).end("bad json");
        return;
      }

      // Shared secret, compared in constant time.
      const want = process.env.TV_WEBHOOK_SECRET || "";
      const got = String(payload.secret ?? req.headers["x-tv-secret"] ?? "");
      const ok = want.length === got.length &&
        crypto.timingSafeEqual(Buffer.from(want), Buffer.from(got));
      if (!want || !ok) {
        console.warn("[tv] bad secret from", ip);
        res.writeHead(401).end();
        return;
      }

      // TradingView retries on non-2xx — drop duplicates.
      if (payload.id) {
        if (seen.has(payload.id)) { res.writeHead(200).end("dup"); return; }
        seen.add(payload.id);
        if (seen.size > 5000) seen.clear();
      }

      const sig = normalize(payload);
      delete sig.secret;
      latest.set(sig.instId + ":" + sig.timeframe, sig);

      broadcast({ type: "signal", ...sig });
      console.log("[tv]", sig.instId, sig.timeframe, sig.verdict, sig.score);

      res.writeHead(200).end("ok");
    });
  });

  server.listen(Number(process.env.TV_PORT) || 8788, () =>
    console.log(`tv webhook on :${process.env.TV_PORT || 8788}/tv-webhook`)
  );

  return {
    snapshot: () => ({ type: "signals", data: [...latest.values()] }),
    latest,
  };
}
