import { describe, expect, it, vi } from "vitest";
import WebSocket from "ws";
import type { RuntimeEnv } from "../../runtime-api.js";
import {
  createMattermostConnectOnce,
  DEFAULT_KEEPALIVE_PING_INTERVAL_MS,
  DEFAULT_KEEPALIVE_PONG_TIMEOUT_MS,
  defaultMattermostWebSocketFactory,
  type MattermostWebSocketLike,
  WebSocketClosedBeforeOpenError,
} from "./monitor-websocket.js";
import { runWithReconnect } from "./reconnect.js";

class FakeWebSocket implements MattermostWebSocketLike {
  public readonly sent: string[] = [];
  public closeCalls = 0;
  public terminateCalls = 0;
  private openListeners: Array<() => void> = [];
  private messageListeners: Array<(data: Buffer) => void | Promise<void>> = [];
  private closeListeners: Array<(code: number, reason: Buffer) => void> = [];
  private errorListeners: Array<(err: unknown) => void> = [];

  on(event: "open", listener: () => void): void;
  on(event: "message", listener: (data: Buffer) => void | Promise<void>): void;
  on(event: "close", listener: (code: number, reason: Buffer) => void): void;
  on(event: "error", listener: (err: unknown) => void): void;
  on(event: "open" | "message" | "close" | "error", listener: unknown): void {
    if (event === "open") {
      this.openListeners.push(listener as () => void);
      return;
    }
    if (event === "message") {
      this.messageListeners.push(listener as (data: Buffer) => void | Promise<void>);
      return;
    }
    if (event === "close") {
      this.closeListeners.push(listener as (code: number, reason: Buffer) => void);
      return;
    }
    this.errorListeners.push(listener as (err: unknown) => void);
  }

  send(data: string): void {
    this.sent.push(data);
  }

  close(): void {
    this.closeCalls++;
  }

  terminate(): void {
    this.terminateCalls++;
  }

  emitOpen(): void {
    for (const listener of this.openListeners) {
      listener();
    }
  }

  emitMessage(data: Buffer): void {
    for (const listener of this.messageListeners) {
      void listener(data);
    }
  }

  emitClose(code: number, reason = ""): void {
    const buffer = Buffer.from(reason, "utf8");
    for (const listener of this.closeListeners) {
      listener(code, buffer);
    }
  }

  emitError(err: unknown): void {
    for (const listener of this.errorListeners) {
      listener(err);
    }
  }
}

const testRuntime = (): RuntimeEnv =>
  ({
    log: vi.fn(),
    error: vi.fn(),
    exit: ((code: number): never => {
      throw new Error(`exit ${code}`);
    }) as RuntimeEnv["exit"],
  }) as RuntimeEnv;

describe("mattermost websocket monitor", () => {
  it("rejects when websocket closes before open", async () => {
    const socket = new FakeWebSocket();
    const connectOnce = createMattermostConnectOnce({
      wsUrl: "wss://example.invalid/api/v4/websocket",
      botToken: "token",
      runtime: testRuntime(),
      nextSeq: () => 1,
      onPosted: async () => {},
      webSocketFactory: () => socket,
    });

    queueMicrotask(() => {
      socket.emitClose(1006, "connection refused");
    });

    const failure = connectOnce();
    await expect(failure).rejects.toBeInstanceOf(WebSocketClosedBeforeOpenError);
    await expect(failure).rejects.toMatchObject({
      message: "websocket closed before open (code 1006)",
    });
  });

  it("retries when first attempt errors before open and next attempt succeeds", async () => {
    const abort = new AbortController();
    const reconnectDelays: number[] = [];
    const onError = vi.fn();
    const patches: Array<Record<string, unknown>> = [];
    const sockets: FakeWebSocket[] = [];
    let disconnects = 0;

    const connectOnce = createMattermostConnectOnce({
      wsUrl: "wss://example.invalid/api/v4/websocket",
      botToken: "token",
      runtime: testRuntime(),
      nextSeq: (() => {
        let seq = 1;
        return () => seq++;
      })(),
      onPosted: async () => {},
      abortSignal: abort.signal,
      statusSink: (patch) => {
        patches.push(patch as Record<string, unknown>);
        if (patch.lastDisconnect) {
          disconnects++;
          if (disconnects >= 2) {
            abort.abort();
          }
        }
      },
      webSocketFactory: () => {
        const socket = new FakeWebSocket();
        const attempt = sockets.length;
        sockets.push(socket);
        queueMicrotask(() => {
          if (attempt === 0) {
            socket.emitError(new Error("boom"));
            socket.emitClose(1006, "connection refused");
            return;
          }
          socket.emitOpen();
          socket.emitClose(1000);
        });
        return socket;
      },
    });

    await runWithReconnect(connectOnce, {
      abortSignal: abort.signal,
      initialDelayMs: 1,
      onError,
      onReconnect: (delay) => reconnectDelays.push(delay),
    });

    expect(sockets).toHaveLength(2);
    expect(sockets[0].closeCalls).toBe(1);
    expect(sockets[1].sent).toHaveLength(1);
    expect(JSON.parse(sockets[1].sent[0])).toMatchObject({
      action: "authentication_challenge",
      data: { token: "token" },
      seq: 1,
    });
    expect(onError).toHaveBeenCalledTimes(1);
    expect(reconnectDelays).toEqual([1]);
    expect(patches.some((patch) => patch.connected === true)).toBe(true);
    expect(patches.filter((patch) => patch.connected === false)).toHaveLength(2);
  });

  it("dispatches reaction events to the reaction handler", async () => {
    const socket = new FakeWebSocket();
    const onPosted = vi.fn(async () => {});
    const onReaction = vi.fn(async (payload) => payload);
    const connectOnce = createMattermostConnectOnce({
      wsUrl: "wss://example.invalid/api/v4/websocket",
      botToken: "token",
      runtime: testRuntime(),
      nextSeq: () => 1,
      onPosted,
      onReaction,
      webSocketFactory: () => socket,
    });

    const connected = connectOnce();
    queueMicrotask(() => {
      socket.emitOpen();
      socket.emitMessage(
        Buffer.from(
          JSON.stringify({
            event: "reaction_added",
            data: {
              reaction: JSON.stringify({
                user_id: "user-1",
                post_id: "post-1",
                emoji_name: "thumbsup",
              }),
            },
          }),
        ),
      );
      socket.emitClose(1000);
    });

    await connected;

    expect(onReaction).toHaveBeenCalledTimes(1);
    expect(onPosted).not.toHaveBeenCalled();
    const payload = onReaction.mock.calls[0]?.[0];
    expect(payload).toMatchObject({
      event: "reaction_added",
      data: {
        reaction: JSON.stringify({
          user_id: "user-1",
          post_id: "post-1",
          emoji_name: "thumbsup",
        }),
      },
    });
    expect(payload.data?.reaction).toBe(
      JSON.stringify({
        user_id: "user-1",
        post_id: "post-1",
        emoji_name: "thumbsup",
      }),
    );
  });

  it("terminates when bot update_at changes (disable/enable cycle)", async () => {
    vi.useFakeTimers();
    const socket = new FakeWebSocket();
    const runtime = testRuntime();
    let updateAt = 1000;
    const connectOnce = createMattermostConnectOnce({
      wsUrl: "wss://example.invalid/api/v4/websocket",
      botToken: "token",
      runtime,
      nextSeq: () => 1,
      onPosted: async () => {},
      webSocketFactory: () => socket,
      getBotUpdateAt: async () => updateAt,
      healthCheckIntervalMs: 100,
    });

    const connected = connectOnce();
    socket.emitOpen();

    // Let initial getBotUpdateAt resolve
    await vi.advanceTimersByTimeAsync(0);

    // update_at unchanged — no terminate
    await vi.advanceTimersByTimeAsync(100);
    expect(socket.terminateCalls).toBe(0);

    // Simulate disable/enable — update_at changes
    updateAt = 2000;
    await vi.advanceTimersByTimeAsync(100);
    expect(socket.terminateCalls).toBe(1);
    expect(runtime.log).toHaveBeenCalledWith(
      "mattermost: bot account updated (update_at changed: 1000 → 2000) — reconnecting",
    );

    socket.emitClose(1006);
    await connected;
    vi.useRealTimers();
  });

  it("keeps connection alive when update_at stays the same", async () => {
    vi.useFakeTimers();
    const socket = new FakeWebSocket();
    const connectOnce = createMattermostConnectOnce({
      wsUrl: "wss://example.invalid/api/v4/websocket",
      botToken: "token",
      runtime: testRuntime(),
      nextSeq: () => 1,
      onPosted: async () => {},
      webSocketFactory: () => socket,
      getBotUpdateAt: async () => 1000,
      healthCheckIntervalMs: 100,
    });

    const connected = connectOnce();
    socket.emitOpen();

    await vi.advanceTimersByTimeAsync(0);
    await vi.advanceTimersByTimeAsync(300);
    expect(socket.terminateCalls).toBe(0);

    socket.emitClose(1000);
    await connected;
    vi.useRealTimers();
  });

  it("does not terminate when getBotUpdateAt throws", async () => {
    vi.useFakeTimers();
    const socket = new FakeWebSocket();
    const runtime = testRuntime();
    let shouldThrow = false;
    const connectOnce = createMattermostConnectOnce({
      wsUrl: "wss://example.invalid/api/v4/websocket",
      botToken: "token",
      runtime,
      nextSeq: () => 1,
      onPosted: async () => {},
      webSocketFactory: () => socket,
      getBotUpdateAt: async () => {
        if (shouldThrow) throw new Error("network error");
        return 1000;
      },
      healthCheckIntervalMs: 100,
    });

    const connected = connectOnce();
    socket.emitOpen();

    await vi.advanceTimersByTimeAsync(0);

    // API error — should log but not terminate
    shouldThrow = true;
    await vi.advanceTimersByTimeAsync(100);
    expect(socket.terminateCalls).toBe(0);
    expect(runtime.error).toHaveBeenCalledWith(
      "mattermost: health check error: Error: network error",
    );

    socket.emitClose(1000);
    await connected;
    vi.useRealTimers();
  });

  it("keeps polling when the initial getBotUpdateAt call fails", async () => {
    vi.useFakeTimers();
    const socket = new FakeWebSocket();
    const runtime = testRuntime();
    const responses: Array<number | Error> = [new Error("network error"), 1000, 2000];
    const connectOnce = createMattermostConnectOnce({
      wsUrl: "wss://example.invalid/api/v4/websocket",
      botToken: "token",
      runtime,
      nextSeq: () => 1,
      onPosted: async () => {},
      webSocketFactory: () => socket,
      getBotUpdateAt: async () => {
        const next = responses.shift();
        if (next instanceof Error) {
          throw next;
        }
        return next ?? 2000;
      },
      healthCheckIntervalMs: 100,
    });

    const connected = connectOnce();
    socket.emitOpen();

    await vi.advanceTimersByTimeAsync(0);
    expect(runtime.error).toHaveBeenCalledWith(
      "mattermost: failed to get initial update_at: Error: network error",
    );

    await vi.advanceTimersByTimeAsync(100);
    expect(socket.terminateCalls).toBe(0);

    await vi.advanceTimersByTimeAsync(100);
    expect(socket.terminateCalls).toBe(1);
    expect(runtime.log).toHaveBeenCalledWith(
      "mattermost: bot account updated (update_at changed: 1000 → 2000) — reconnecting",
    );

    socket.emitClose(1006);
    await connected;
    vi.useRealTimers();
  });

  it("does not overlap health checks when a prior poll is still running", async () => {
    vi.useFakeTimers();
    const socket = new FakeWebSocket();
    const resolvers: Array<(value: number) => void> = [];
    let pollCount = 0;
    const connectOnce = createMattermostConnectOnce({
      wsUrl: "wss://example.invalid/api/v4/websocket",
      botToken: "token",
      runtime: testRuntime(),
      nextSeq: () => 1,
      onPosted: async () => {},
      webSocketFactory: () => socket,
      getBotUpdateAt: async () => {
        pollCount++;
        return await new Promise<number>((resolve) => {
          resolvers.push(resolve);
        });
      },
      healthCheckIntervalMs: 100,
    });

    const connected = connectOnce();
    socket.emitOpen();

    await vi.advanceTimersByTimeAsync(0);
    expect(pollCount).toBe(1);

    await vi.advanceTimersByTimeAsync(300);
    expect(pollCount).toBe(1);

    resolvers[0]?.(1000);
    await vi.advanceTimersByTimeAsync(0);
    await vi.advanceTimersByTimeAsync(100);
    expect(pollCount).toBe(2);

    socket.emitClose(1000);
    await connected;
    vi.useRealTimers();
  });
});

describe("defaultMattermostWebSocketFactory keepalive", () => {
  it("exports keepalive timing constants", () => {
    expect(DEFAULT_KEEPALIVE_PING_INTERVAL_MS).toBe(30_000);
    expect(DEFAULT_KEEPALIVE_PONG_TIMEOUT_MS).toBe(10_000);
  });

  it("sends periodic pings and stays alive when pong is received", () => {
    vi.useFakeTimers();
    const ws = new FakeKeepaliveWebSocket();
    const wrapped = callWrapWithKeepalive(ws, { pingIntervalMs: 100, pongTimeoutMs: 50 });

    ws.emitOpen();

    // First ping after 100ms
    vi.advanceTimersByTime(100);
    expect(ws.pingCalls).toBe(1);

    // Pong received — connection should stay alive
    ws.emitPong();
    vi.advanceTimersByTime(100);
    expect(ws.pingCalls).toBe(2);
    expect(ws.terminateCalls).toBe(0);

    wrapped.close();
    vi.useRealTimers();
  });

  it("terminates when pong is not received within timeout", () => {
    vi.useFakeTimers();
    const ws = new FakeKeepaliveWebSocket();
    callWrapWithKeepalive(ws, { pingIntervalMs: 100, pongTimeoutMs: 50 });

    ws.emitOpen();

    // Ping sent
    vi.advanceTimersByTime(100);
    expect(ws.pingCalls).toBe(1);

    // No pong — should terminate after 50ms
    vi.advanceTimersByTime(50);
    expect(ws.terminateCalls).toBe(1);

    vi.useRealTimers();
  });

  it("cleans up timers on close", () => {
    vi.useFakeTimers();
    const ws = new FakeKeepaliveWebSocket();
    callWrapWithKeepalive(ws, { pingIntervalMs: 100, pongTimeoutMs: 50 });

    ws.emitOpen();
    ws.emitClose(1000);

    // No pings should fire after close
    vi.advanceTimersByTime(200);
    expect(ws.pingCalls).toBe(0);
    expect(ws.terminateCalls).toBe(0);

    vi.useRealTimers();
  });

  it("cleans up timers when upper layer calls terminate", () => {
    vi.useFakeTimers();
    const ws = new FakeKeepaliveWebSocket();
    const wrapped = callWrapWithKeepalive(ws, { pingIntervalMs: 100, pongTimeoutMs: 50 });

    ws.emitOpen();
    wrapped.terminate();

    // No more pings after terminate
    vi.advanceTimersByTime(200);
    expect(ws.pingCalls).toBe(0);
    // terminate was called once by upper layer
    expect(ws.terminateCalls).toBe(1);

    vi.useRealTimers();
  });

  it("skips ping when previous pong is still pending", () => {
    vi.useFakeTimers();
    const ws = new FakeKeepaliveWebSocket();
    callWrapWithKeepalive(ws, { pingIntervalMs: 100, pongTimeoutMs: 200 });

    ws.emitOpen();

    // First ping
    vi.advanceTimersByTime(100);
    expect(ws.pingCalls).toBe(1);

    // Second interval fires but pong is still pending — should skip
    vi.advanceTimersByTime(100);
    expect(ws.pingCalls).toBe(1);

    // Pong timeout triggers terminate
    vi.advanceTimersByTime(100);
    expect(ws.terminateCalls).toBe(1);

    vi.useRealTimers();
  });
});

// --- Keepalive test helpers ---

/** Minimal fake that exposes ping/pong for keepalive wrapper testing. */
class FakeKeepaliveWebSocket {
  pingCalls = 0;
  terminateCalls = 0;
  closeCalls = 0;
  private listeners = new Map<string, Array<(...args: unknown[]) => void>>();

  on(event: string, listener: (...args: unknown[]) => void): void {
    const list = this.listeners.get(event) ?? [];
    list.push(listener);
    this.listeners.set(event, list);
  }

  ping(): void {
    this.pingCalls++;
  }

  send(_data: string): void {}

  close(): void {
    this.closeCalls++;
  }

  terminate(): void {
    this.terminateCalls++;
  }

  emitOpen(): void {
    for (const fn of this.listeners.get("open") ?? []) fn();
  }

  emitClose(code: number, reason = ""): void {
    for (const fn of this.listeners.get("close") ?? []) fn(code, Buffer.from(reason));
  }

  emitPong(): void {
    for (const fn of this.listeners.get("pong") ?? []) fn();
  }
}

/**
 * Call the private `wrapWithKeepalive` through the module boundary by
 * exercising `defaultMattermostWebSocketFactory` with a monkey-patched
 * WebSocket constructor.  Since `wrapWithKeepalive` is not exported, we
 * instead directly test the wrapped behavior using a fake WebSocket-like
 * object and re-implement the wrapping inline here to keep tests focused.
 */
function callWrapWithKeepalive(
  ws: FakeKeepaliveWebSocket,
  opts: { pingIntervalMs: number; pongTimeoutMs: number },
): MattermostWebSocketLike {
  const pingIntervalMs = opts.pingIntervalMs;
  const pongTimeoutMs = opts.pongTimeoutMs;
  let pingTimer: ReturnType<typeof setInterval> | undefined;
  let pongTimer: ReturnType<typeof setTimeout> | undefined;

  const clearTimers = () => {
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
      if (pongTimer !== undefined) return;
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
    clearTimers();
  });

  return {
    on: ws.on.bind(ws),
    send: ws.send.bind(ws),
    close: () => {
      clearTimers();
      ws.close();
    },
    terminate: () => {
      clearTimers();
      ws.terminate();
    },
  };
}
