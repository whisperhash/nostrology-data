// The sidecar's HTTP surface - node:http, no framework (every dependency in
// this container is a dependency in the thing that holds the money). Four
// routes, all JSON:
//
//   POST /redeem                {token, client_ip?}   -> RedeemResult
//   POST /redemptions/outcome   {id, delivered, note?} -> {ok}
//   GET  /health                                       -> liveness (no mint, no store)
//   GET  /metrics                                      -> Prometheus text
//
// Reachability is the security model: this listener binds inside the compose
// netns and is NEVER published to a host port; only the MCP container (compose
// default net) and - for /metrics - the deployment's metrics scraper
// can reach it. `client_ip` is the ORIGINAL caller's address as seen by the
// MCP server's transport, forwarded for the per-IP outbound rate limit; the
// TCP peer here is always just the MCP container.

import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import { loadConfig } from './config.ts';
import { log } from './log.ts';
import { exposeMetrics } from './metrics.ts';
import { Redeemer } from './redeem.ts';
import { storeFor } from './store.ts';
import { setGlobalRequestOptions } from '@cashu/cashu-ts';

// A cashuB token for a 21-sat payment is a few hundred bytes; the MCP server
// already caps the tool argument at 4 KiB. 16 KiB here is defense in depth,
// not a second contract.
const MAX_BODY_BYTES = 16 * 1024;

function readBody(req: IncomingMessage): Promise<string | null> {
  return new Promise((resolve) => {
    const chunks: Buffer[] = [];
    let size = 0;
    req.on('data', (chunk: Buffer) => {
      size += chunk.length;
      if (size > MAX_BODY_BYTES) {
        // Stop consuming and let the handler WRITE the 413 before the socket
        // dies - destroying here raced the response and the client usually
        // saw a bare reset instead of the JSON (review nit). The handler
        // closes the connection after responding.
        req.pause();
        resolve(null);
        return;
      }
      chunks.push(chunk);
    });
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    req.on('error', () => resolve(null));
  });
}

function json(res: ServerResponse, status: number, body: unknown): void {
  const text = JSON.stringify(body);
  res.writeHead(status, { 'content-type': 'application/json' });
  res.end(text);
}

/** 413 path: answer, then drop the connection - the request body is still
 * streaming (readBody paused it) and nothing further on this socket can be
 * trusted to frame correctly. */
function jsonAndClose(req: IncomingMessage, res: ServerResponse, status: number, body: unknown): void {
  const text = JSON.stringify(body);
  res.writeHead(status, { 'content-type': 'application/json', connection: 'close' });
  res.end(text, () => req.destroy());
}

export function makeHandler(redeemer: Redeemer) {
  return async (req: IncomingMessage, res: ServerResponse): Promise<void> => {
    try {
      const url = req.url ?? '';
      if (req.method === 'GET' && url === '/health') {
        json(res, 200, { status: 'ok' });
        return;
      }
      if (req.method === 'GET' && url === '/metrics') {
        res.writeHead(200, { 'content-type': 'text/plain; version=0.0.4' });
        res.end(exposeMetrics());
        return;
      }
      if (req.method === 'POST' && url === '/redeem') {
        const body = await readBody(req);
        if (body === null) {
          jsonAndClose(req, res, 413, { outcome: 'structural', reason: 'not_a_token', message: 'request body too large.' });
          return;
        }
        let parsed: { token?: unknown; client_ip?: unknown };
        try {
          parsed = JSON.parse(body);
        } catch {
          json(res, 400, { outcome: 'structural', reason: 'not_a_token', message: 'body must be JSON.' });
          return;
        }
        if (typeof parsed.token !== 'string' || parsed.token.length === 0) {
          json(res, 400, { outcome: 'structural', reason: 'not_a_token', message: 'body needs a string `token`.' });
          return;
        }
        const clientIp = typeof parsed.client_ip === 'string' ? parsed.client_ip : null;
        const result = await redeemer.redeem(parsed.token, clientIp);
        // Transport status is 200 for every RESOLVED outcome - the outcome
        // field is the contract, as on the MCP side, where every payment outcome
        // travels in the tool result. Only unresolved/store states get 5xx so a dumb client
        // retry-loop cannot mistake them for a settled answer.
        const status = result.outcome === 'unresolved' || result.outcome === 'store_unavailable' ? 503 : 200;
        json(res, status, result);
        return;
      }
      if (req.method === 'POST' && url === '/redemptions/outcome') {
        const body = await readBody(req);
        if (body === null) {
          jsonAndClose(req, res, 413, { ok: false });
          return;
        }
        let parsed: { id?: unknown; delivered?: unknown; note?: unknown };
        try {
          parsed = JSON.parse(body);
        } catch {
          json(res, 400, { ok: false });
          return;
        }
        if (typeof parsed.id !== 'string' || !/^[0-9a-f]{64}$/.test(parsed.id) || typeof parsed.delivered !== 'boolean') {
          json(res, 400, { ok: false });
          return;
        }
        const note = typeof parsed.note === 'string' ? parsed.note.slice(0, 256) : null;
        const ok = await redeemer.recordOutcome(parsed.id, parsed.delivered, note);
        json(res, ok ? 200 : 503, { ok });
        return;
      }
      json(res, 404, { error: 'not found' });
    } catch (e) {
      // Nothing below this handler catches; a stray exception must become a
      // clean 500, not a connection reset - and its text must NOT go to the
      // client (it could carry request fragments). Log the class only.
      log.error('unhandled error in request handler', { error: String((e as Error).name ?? 'Error') });
      json(res, 500, { error: 'internal' });
    }
  };
}

export function main(): void {
  const config = loadConfig();
  // Per-request fetch timeout for every mint call; without it a hung
  // connection consumes the entire NUT-19 retry window (the option's own doc).
  setGlobalRequestOptions({ requestTimeout: config.mintTimeoutMs });
  const redeemer = new Redeemer(config, storeFor(config));
  const server = createServer((req, res) => {
    void makeHandler(redeemer)(req, res);
  });
  server.listen(config.port, () => {
    log.info('cashu-redeemer listening', {
      port: config.port,
      price_sats: config.priceSats,
      unit: config.unit,
      mints: config.mints.join(','),
      store: config.store,
    });
  });
}

// ESM entrypoint check: run only when executed directly, not when imported by
// the test suite.
if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}
