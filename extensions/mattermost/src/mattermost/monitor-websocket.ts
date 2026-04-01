import { safeParseJsonWithSchema, safeParseWithSchema } from "openclaw/plugin-sdk/extension-shared";
import { z } from "openclaw/plugin-sdk/zod";
import WebSocket from "ws";
import { MattermostPostSchema, type MattermostPost } from "./client.js";
import { rawDataToString } from "./monitor-helpers.js";
import type { ChannelAccountSnapshot, RuntimeEnv } from "./runtime-api.js";

export type MattermostEventPayload = {
  event?: string;
  data?: {
    post?: string | MattermostPost;
    reaction?: string | Record<string, unknown>;
    channel_id?: string;
    channel_name?: string;
    channel_display_name?: string;
    channel_type?: string;
    sender_name?: string;
    team_id?: string;
  };
  broadcast?: {
    channel_id?: string;
    team_id?: string;
    user_id?: string;
  };
};

export type MattermostWebSocketLike = {
  on(event: "open", listener: () => void): void;
  on(event: "message", listener: (data: WebSocket.RawData) => void | Promise<void>): void;
  on(event: "close", listener: (code: number, reason: Buffer) => void): void;
  on(event: "error", listener: (err: unknown) => void): void;
  send(data: string): void;
  close(): void;
  terminate(): void;
};

export type MattermostWebSocketFactory = (url: string) => MattermostWebSocketLike;
const MattermostEventPayloadSchema = z.object({
  event: z.string().optional(),
  data: z
    .object({
      post: z.union([z.string(), MattermostPostSchema]).optional(),
      reaction: z.union([z.string(), z.record(z.string(), z.unknown())]).optional(),
      channel_id: z.string().optional(),
      channel_name: z.string().optional(),
      channel_display_name: z.string().optional(),
      channel_type: z.string().optional(),
      sender_name: z.string().optional(),
      team_id: z.string().optional(),
    })
    .optional(),
  broadcast: z
    .object({
      channel_id: z.string().optional(),
      team_id: z.string().optional(),
      user_id: z.string().optional(),
    })
    .optional(),
}) as z.ZodType<MattermostEventPayload>;

function parseMattermostEventPayload(raw: string): MattermostEventPayload | null {
  return safeParseJsonWithSchema(MattermostEventPayloadSchema, raw);
}

function parseMattermostPost(value: unknown): MattermostPost | null {
  if (typeof value === "string") {
    return safeParseJsonWithSchema(MattermostPostSchema, value);
  }
  return safeParseWithSchema(MattermostPostSchema, value);
}

export class WebSocketClosedBeforeOpenError extends Error {
  constructor(
    public readonly code: number,
    public readonly reason?: string,
  ) {
    super(`websocket closed before open (code ${code})`);
    this.name = "WebSocketClosedBeforeOpenError";
  }
}

type CreateMattermostConnectOnceOpts = {
  wsUrl: string;
  botToken: string;
  abortSignal?: AbortSignal;
  statusSink?: (patch: Partial<ChannelAccountSnapshot>) => void;
  runtime: RuntimeEnv;
  nextSeq: () => number;
  onPosted: (post: MattermostPost, payload: MattermostEventPayload) => Promise<void>;
  onReaction?: (payload: MattermostEventPayload) => Promise<void>;
  webSocketFactory?: MattermostWebSocketFactory;
  /**
   * Called periodically to check whether the bot account has been modified
   * (e.g. disabled then re-enabled) since the WebSocket was opened.
   * Returns the bot's current `update_at` timestamp.  When it differs from
   * the value recorded at connect time, the connection is terminated so the
   * reconnect loop can establish a fresh one.
   */
  getBotUpdateAt?: () => Promise<number>;
  healthCheckIntervalMs?: number;
};

export type KeepaliveOptions = {
  /** Interval between ping frames in milliseconds (default: 30000). */
  pingIntervalMs?: number;
  /** Time to wait for a pong response before terminating (default: 10000). */
  pongTimeoutMs?: number;
};

/**
 * Wraps a raw WebSocket with protocol-level ping/pong keepalive.
 *
 * After `open`, sends a ping frame every `pingIntervalMs`.  If a pong is not
 * received within `pongTimeoutMs` the socket is terminated, which surfaces as
 * a normal `close` event to the upper layer and triggers the reconnect loop.
 */
function wrapWithKeepalive(ws: WebSocket, opts: KeepaliveOptions = {}): MattermostWebSocketLike {
  const pingIntervalMs = opts.pingIntervalMs ?? 30_000;
  const pongTimeoutMs = opts.pongTimeoutMs ?? 10_000;
  let pingTimer: ReturnType<typeof setInterval> | undefined;
  let pongTimer: ReturnType<typeof setTimeout> | undefined;

  const clearKeepaliveTimers = () => {
    if (pingTimer !== undefined) {
      clearInterval(pingTimer);
      pingTimer = undefined;
    }
    if (pongTimer !== undefined) {
      clearTimeout(pongTimer);
      pongTimer = undefined;
    }
  };

  ws.on("open", () => {
    pingTimer = setInterval(() => {
      if (pongTimer !== undefined) {
        return; // previous ping still waiting for pong
      }
      ws.ping();
      pongTimer = setTimeout(() => {
        pongTimer = undefined;
        ws.terminate();
      }, pongTimeoutMs);
    }, pingIntervalMs);
  });

  ws.on("pong", () => {
    if (pongTimer !== undefined) {
      clearTimeout(pongTimer);
      pongTimer = undefined;
    }
  });

  ws.on("close", () => {
    clearKeepaliveTimers();
  });

  // Wrap terminate/close to clean up timers when called by upper layer
  const origTerminate = ws.terminate.bind(ws);
  const origClose = ws.close.bind(ws);
  const wrapped = ws as unknown as MattermostWebSocketLike;
  (wrapped as { terminate: () => void }).terminate = () => {
    clearKeepaliveTimers();
    origTerminate();
  };
  (wrapped as { close: () => void }).close = () => {
    clearKeepaliveTimers();
    origClose();
  };

  return wrapped;
}

export const DEFAULT_KEEPALIVE_PING_INTERVAL_MS = 30_000;
export const DEFAULT_KEEPALIVE_PONG_TIMEOUT_MS = 10_000;

export const defaultMattermostWebSocketFactory: MattermostWebSocketFactory = (url) =>
  wrapWithKeepalive(new WebSocket(url));

export function parsePostedPayload(
  payload: MattermostEventPayload,
): { payload: MattermostEventPayload; post: MattermostPost } | null {
  if (payload.event !== "posted") {
    return null;
  }
  const postData = payload.data?.post;
  if (!postData) {
    return null;
  }
  const post = parseMattermostPost(postData);
  if (!post) {
    return null;
  }
  return { payload, post };
}

export function parsePostedEvent(
  data: WebSocket.RawData,
): { payload: MattermostEventPayload; post: MattermostPost } | null {
  const raw = rawDataToString(data);
  const payload = parseMattermostEventPayload(raw);
  if (!payload) {
    return null;
  }
  return parsePostedPayload(payload);
}

export function createMattermostConnectOnce(
  opts: CreateMattermostConnectOnceOpts,
): () => Promise<void> {
  const webSocketFactory = opts.webSocketFactory ?? defaultMattermostWebSocketFactory;
  const healthCheckIntervalMs = opts.healthCheckIntervalMs ?? 30_000;
  return async () => {
    const ws = webSocketFactory(opts.wsUrl);
    const onAbort = () => ws.terminate();
    opts.abortSignal?.addEventListener("abort", onAbort, { once: true });
    const getBotUpdateAt = opts.getBotUpdateAt;

    try {
      return await new Promise<void>((resolve, reject) => {
        let opened = false;
        let settled = false;
        let healthCheckEnabled = getBotUpdateAt != null;
        let healthCheckInFlight = false;
        let healthCheckTimer: ReturnType<typeof setTimeout> | undefined;
        let initialUpdateAt: number | undefined;

        const clearTimers = () => {
          if (healthCheckTimer !== undefined) {
            clearTimeout(healthCheckTimer);
            healthCheckTimer = undefined;
          }
        };

        const stopHealthChecks = () => {
          healthCheckEnabled = false;
          clearTimers();
        };

        const scheduleHealthCheck = () => {
          if (!getBotUpdateAt || !healthCheckEnabled || settled || healthCheckInFlight) {
            return;
          }
          healthCheckTimer = setTimeout(() => {
            healthCheckTimer = undefined;
            void runHealthCheck();
          }, healthCheckIntervalMs);
        };

        const runHealthCheck = async () => {
          if (!getBotUpdateAt || !healthCheckEnabled || settled || healthCheckInFlight) {
            return;
          }
          healthCheckInFlight = true;
          try {
            const current = await getBotUpdateAt();
            if (!healthCheckEnabled || settled) {
              return;
            }
            if (initialUpdateAt === undefined) {
              initialUpdateAt = current;
              return;
            }
            if (current !== initialUpdateAt) {
              opts.runtime.log?.(
                `mattermost: bot account updated (update_at changed: ${initialUpdateAt} → ${current}) — reconnecting`,
              );
              stopHealthChecks();
              ws.terminate();
            }
          } catch (err) {
            if (!healthCheckEnabled || settled) {
              return;
            }
            const label =
              initialUpdateAt === undefined
                ? "mattermost: failed to get initial update_at"
                : "mattermost: health check error";
            opts.runtime.error?.(`${label}: ${String(err)}`);
          } finally {
            healthCheckInFlight = false;
            scheduleHealthCheck();
          }
        };

        const resolveOnce = () => {
          if (settled) {
            return;
          }
          settled = true;
          stopHealthChecks();
          resolve();
        };
        const rejectOnce = (error: Error) => {
          if (settled) {
            return;
          }
          settled = true;
          stopHealthChecks();
          reject(error);
        };

        ws.on("open", () => {
          opened = true;
          opts.statusSink?.({
            connected: true,
            lastConnectedAt: Date.now(),
            lastError: null,
          });
          ws.send(
            JSON.stringify({
              seq: opts.nextSeq(),
              action: "authentication_challenge",
              data: { token: opts.botToken },
            }),
          );

          // Periodically check if the bot account was modified (e.g. disable/enable).
          // After such a cycle the WebSocket silently stops delivering events even
          // though the connection itself stays alive.  Comparing update_at detects
          // this reliably regardless of how quickly the cycle happens.
          if (getBotUpdateAt) {
            // Use a recursive timeout so only one REST poll can be in flight at a time.
            void runHealthCheck();
          }
        });

        ws.on("message", async (data) => {
          const raw = rawDataToString(data);
          const payload = parseMattermostEventPayload(raw);
          if (!payload) {
            return;
          }

          if (payload.event === "reaction_added" || payload.event === "reaction_removed") {
            if (!opts.onReaction) {
              return;
            }
            try {
              await opts.onReaction(payload);
            } catch (err) {
              opts.runtime.error?.(`mattermost reaction handler failed: ${String(err)}`);
            }
            return;
          }

          if (payload.event !== "posted") {
            return;
          }
          const parsed = parsePostedPayload(payload);
          if (!parsed) {
            return;
          }
          try {
            await opts.onPosted(parsed.post, parsed.payload);
          } catch (err) {
            opts.runtime.error?.(`mattermost handler failed: ${String(err)}`);
          }
        });

        ws.on("close", (code, reason) => {
          stopHealthChecks();
          const message = reasonToString(reason);
          opts.statusSink?.({
            connected: false,
            lastDisconnect: {
              at: Date.now(),
              status: code,
              error: message || undefined,
            },
          });
          if (opened) {
            resolveOnce();
            return;
          }
          rejectOnce(new WebSocketClosedBeforeOpenError(code, message || undefined));
        });

        ws.on("error", (err) => {
          opts.runtime.error?.(`mattermost websocket error: ${String(err)}`);
          opts.statusSink?.({
            lastError: String(err),
          });
          try {
            ws.close();
          } catch {}
        });
      });
    } finally {
      opts.abortSignal?.removeEventListener("abort", onAbort);
    }
  };
}

function reasonToString(reason: Buffer | string | undefined): string {
  if (!reason) {
    return "";
  }
  if (typeof reason === "string") {
    return reason;
  }
  return reason.length > 0 ? reason.toString("utf8") : "";
}
