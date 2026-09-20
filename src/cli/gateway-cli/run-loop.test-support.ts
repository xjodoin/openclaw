import { performance } from "node:perf_hooks";
import { expect, it, vi, type Mock } from "vitest";
import { LAUNCH_AGENT_EXIT_TIMEOUT_SECONDS } from "../../daemon/launchd-plist.js";
import { buildSystemdUnit } from "../../daemon/systemd-unit.js";
import { GatewayConnectionWork } from "../../gateway/server-connection-work.js";
import type { GatewayServer } from "../../gateway/server-public.js";
import type { GatewayActiveWorkSnapshot } from "../../infra/gateway-active-work.js";
import type { GatewayRestartIntent } from "../../infra/restart-intent.js";
import { createDeferredCore } from "../../shared/deferred.js";
import type { GatewayRestartSnapshot } from "../daemon-cli/restart-health.js";

export const createActiveWorkSnapshot = (
  counts: Partial<GatewayActiveWorkSnapshot["counts"]> = {},
  blockers: GatewayActiveWorkSnapshot["blockers"] = [],
): GatewayActiveWorkSnapshot => {
  const resolvedCounts = {
    queueSize: 0,
    pendingReplies: 0,
    embeddedRuns: 0,
    backgroundExecSessions: 0,
    cronRuns: 0,
    activeTasks: 0,
    rootRequests: 0,
    sessionAdmissions: 0,
    sessionMutations: 0,
    chatRuns: 0,
    queuedTurns: 0,
    terminalPersistence: 0,
    terminalSessions: 0,
    lifecycleWrites: 0,
    totalActive: 0,
    ...counts,
  };
  resolvedCounts.totalActive = Object.entries(resolvedCounts).reduce(
    (total, [key, count]) => total + (key === "totalActive" ? 0 : count),
    0,
  );
  return {
    idle: resolvedCounts.totalActive === 0,
    counts: resolvedCounts,
    blockers,
    writeCustody: [],
  };
};

export function expectRestartCloseCall(
  close: Mock<GatewayServer["close"]>,
  maxDrainTimeoutMs: number,
) {
  expect(close).toHaveBeenCalledWith(
    expect.objectContaining({
      reason: "gateway restarting",
      restartExpectedMs: 1500,
      drainTimeoutMs: expect.any(Number),
    }),
  );
  const closeArgs = close.mock.calls[0]?.[0];
  expect(closeArgs?.drainTimeoutMs).toBeLessThanOrEqual(maxDrainTimeoutMs);
  expect(closeArgs?.drainTimeoutMs).toBeGreaterThanOrEqual(0);
}

export function createSignaledStart(
  close: GatewayServer["close"],
  startupSettled = Promise.resolve(),
) {
  let resolveStarted: (() => void) | null = null;
  const started = new Promise<void>((resolve) => {
    resolveStarted = resolve;
  });
  const start = vi.fn<Parameters<typeof import("./run-loop.js").runGatewayLoop>[0]["start"]>(
    async () => {
      resolveStarted?.();
      return { getTailscaleIngressEndpoint: () => undefined, close, startupSettled };
    },
  );
  return { start, started };
}

const shutdownBudgetCases: {
  signal: "SIGTERM" | "SIGUSR2";
  honorsAbort: boolean;
  supervisor: "systemd" | "external-systemd" | "launchd" | "foreground";
  waitMs?: number;
  installedStopMs?: number;
  shutdownStopMs?: number | "unavailable";
  inspectionMs?: number;
}[] = [
  ...([330_000, 30_000, "unavailable"] as const).map((shutdownStopMs) => ({
    signal: "SIGTERM" as const,
    honorsAbort: false,
    supervisor: "systemd" as const,
    installedStopMs: shutdownStopMs === 30_000 ? 330_000 : 30_000,
    shutdownStopMs,
    inspectionMs: 500,
  })),
  { signal: "SIGTERM", honorsAbort: false, supervisor: "systemd", installedStopMs: 90_000 },
  {
    signal: "SIGTERM",
    honorsAbort: false,
    supervisor: "external-systemd",
    installedStopMs: 90_000,
  },
  {
    signal: "SIGUSR2",
    honorsAbort: false,
    supervisor: "external-systemd",
    installedStopMs: 90_000,
  },
  { signal: "SIGTERM", honorsAbort: false, supervisor: "systemd" },
  { signal: "SIGTERM", honorsAbort: false, supervisor: "foreground" },
  { signal: "SIGTERM", honorsAbort: true, supervisor: "systemd" },
  { signal: "SIGUSR2", honorsAbort: false, supervisor: "systemd" },
  { signal: "SIGTERM", honorsAbort: false, supervisor: "launchd" },
  { signal: "SIGUSR2", honorsAbort: false, supervisor: "launchd" },
  { signal: "SIGUSR2", honorsAbort: false, supervisor: "systemd", waitMs: 0 },
  { signal: "SIGUSR2", honorsAbort: false, supervisor: "systemd", waitMs: 600_000 },
];

export const originalPlatformDescriptor = Object.getOwnPropertyDescriptor(process, "platform");

export function setPlatform(platform: string) {
  if (!originalPlatformDescriptor) {
    return;
  }
  Object.defineProperty(process, "platform", {
    ...originalPlatformDescriptor,
    value: platform,
  });
}

const LOOP_SIGNALS = ["SIGTERM", "SIGINT", "SIGUSR2"] as const;
type LoopSignal = (typeof LOOP_SIGNALS)[number];

function removeNewSignalListeners(signal: LoopSignal, existing: Set<(...args: unknown[]) => void>) {
  for (const listener of process.listeners(signal)) {
    const fn = listener as (...args: unknown[]) => void;
    if (!existing.has(fn)) {
      process.removeListener(signal, fn);
    }
  }
}

function addedSignalListener(
  signal: LoopSignal,
  existing: Set<(...args: unknown[]) => void>,
): (() => void) | null {
  const listeners = process.listeners(signal) as Array<(...args: unknown[]) => void>;
  for (let i = listeners.length - 1; i >= 0; i -= 1) {
    const listener = listeners[i];
    if (listener && !existing.has(listener)) {
      return listener as () => void;
    }
  }
  return null;
}

export async function withIsolatedSignals(
  run: (helpers: { captureSignal: (signal: LoopSignal) => () => void }) => Promise<void>,
) {
  const existingListeners = Object.fromEntries(
    LOOP_SIGNALS.map((signal) => [
      signal,
      new Set(process.listeners(signal) as Array<(...args: unknown[]) => void>),
    ]),
  ) as Record<LoopSignal, Set<(...args: unknown[]) => void>>;
  const captureSignal = (signal: LoopSignal) => {
    const listener = addedSignalListener(signal, existingListeners[signal]);
    if (!listener) {
      throw new Error(`expected new ${signal} listener`);
    }
    return () => listener();
  };
  try {
    await run({ captureSignal });
  } finally {
    for (const signal of LOOP_SIGNALS) {
      removeNewSignalListeners(signal, existingListeners[signal]);
    }
  }
}

export function createRuntimeWithExitSignal(exitCallOrder?: string[]) {
  let resolveExit: (code: number) => void = () => {};
  const exited = new Promise<number>((resolve) => {
    resolveExit = resolve;
  });
  const runtime = {
    log: vi.fn(),
    error: vi.fn(),
    exit: vi.fn((code: number) => {
      exitCallOrder?.push("exit");
      resolveExit(code);
    }),
  };
  return { runtime, exited };
}

export function createCloseMock() {
  return vi.fn<GatewayServer["close"]>(async (_opts) => {});
}

export function createGatewayServer(
  close: GatewayServer["close"],
  startupSettled = Promise.resolve(),
) {
  return {
    getTailscaleIngressEndpoint: () => undefined,
    close,
    startupSettled,
  } satisfies GatewayServer;
}

export async function waitForStart(started: Promise<void>) {
  await started;
  await new Promise<void>((resolve) => {
    setImmediate(resolve);
  });
}

export async function waitForLoopCondition(predicate: () => boolean, message: string) {
  const deadline = Date.now() + 1_000;
  while (Date.now() < deadline) {
    if (predicate()) {
      return;
    }
    await new Promise<void>((resolve) => {
      setImmediate(resolve);
    });
  }
  throw new Error(message);
}

export type UpdateRespawnResultFixture = {
  mode: "spawned" | "disabled" | "failed";
  pid?: number;
  detail?: string;
  child?: {
    kill: () => void;
    pid?: number;
    exitCode?: number | null;
    signalCode?: NodeJS.Signals | null;
  };
};

export function registerUpdateRespawnProgressTests({
  runLoopWithStart,
  peekGatewayRestartReason,
  consumeGatewayRestartIntent,
  respawnGatewayProcessForUpdate,
  readRestartSentinelReadOnly,
  waitForGatewayHealthyRestart,
  respawnHealth,
  markUpdateRestartSentinelFailure,
  writeRestartSentinelIfUnchanged,
  writeGatewayRestartHandoffSync,
}: {
  runLoopWithStart: (params: {
    start: ReturnType<typeof createSignaledStart>["start"];
    runtime: ReturnType<typeof createRuntimeWithExitSignal>["runtime"];
    lockPort: number;
  }) => Promise<unknown>;
  peekGatewayRestartReason: Mock<() => string | undefined>;
  consumeGatewayRestartIntent: Mock<() => GatewayRestartIntent | null>;
  respawnGatewayProcessForUpdate: Mock<
    (_opts?: { env?: NodeJS.ProcessEnv }) => UpdateRespawnResultFixture
  >;
  readRestartSentinelReadOnly: Mock<
    typeof import("../../infra/restart-sentinel.js").readRestartSentinelReadOnly
  >;
  waitForGatewayHealthyRestart: Mock<
    typeof import("../daemon-cli/restart-health.js").waitForGatewayHealthyRestart
  >;
  respawnHealth: (overrides?: Partial<GatewayRestartSnapshot>) => GatewayRestartSnapshot;
  markUpdateRestartSentinelFailure: Mock<(reason: string) => Promise<null>>;
  writeRestartSentinelIfUnchanged: Mock<
    typeof import("../../infra/restart-sentinel.js").writeRestartSentinelIfUnchanged
  >;
  writeGatewayRestartHandoffSync: Mock;
}) {
  it.each([
    { waitOutcome: "healthy", elapsedMs: 20_000, closeMs: 0, sentinelStatus: "ok" },
    { waitOutcome: "still-starting", elapsedMs: 300_000, closeMs: 40_000, sentinelStatus: "ok" },
    { waitOutcome: "still-starting", elapsedMs: 300_000, closeMs: 0, sentinelStatus: "error" },
  ] as const)(
    "leaves a $waitOutcome replacement running after $elapsedMs ms",
    async ({ waitOutcome, elapsedMs, closeMs, sentinelStatus }) => {
      vi.clearAllMocks();
      peekGatewayRestartReason.mockReturnValue("update.run");
      consumeGatewayRestartIntent.mockReturnValueOnce({ reason: "update.run", force: true });
      const kill = vi.fn();
      readRestartSentinelReadOnly.mockResolvedValueOnce({
        version: 1,
        revision: 1,
        payload: { kind: "update", status: sentinelStatus, ts: 1, stats: {} },
      });
      respawnGatewayProcessForUpdate.mockReturnValueOnce({
        mode: "spawned",
        pid: process.pid,
        child: { kill, pid: process.pid, exitCode: null, signalCode: null },
      });
      waitForGatewayHealthyRestart.mockImplementationOnce(async ({ child }) => {
        expect(child).toMatchObject({ pid: process.pid, exitCode: null, signalCode: null });
        await new Promise<void>((resolve) => {
          setTimeout(resolve, elapsedMs);
        });
        return respawnHealth({ healthy: waitOutcome === "healthy", waitOutcome, elapsedMs });
      });

      await withIsolatedSignals(async ({ captureSignal }) => {
        const close = vi.fn(async () => {
          await new Promise<void>((resolve) => {
            setTimeout(resolve, closeMs);
          });
        });
        const { start, started } = createSignaledStart(close);
        const { runtime, exited } = createRuntimeWithExitSignal();
        await runLoopWithStart({ start, runtime, lockPort: 18789 });
        await waitForStart(started);
        const restartSignal = captureSignal("SIGUSR2");

        vi.useFakeTimers();
        restartSignal();
        await vi.advanceTimersByTimeAsync(10_000);
        expect(runtime.exit).not.toHaveBeenCalled();
        expect(kill).not.toHaveBeenCalled();
        await vi.advanceTimersByTimeAsync(closeMs + elapsedMs - 10_000);

        expect(runtime.exit).toHaveBeenCalledExactlyOnceWith(0);
        await expect(exited).resolves.toBe(0);
        expect(kill).not.toHaveBeenCalled();
        expect(respawnGatewayProcessForUpdate).toHaveBeenCalledTimes(1);
        expect(start).toHaveBeenCalledTimes(1);
        expect(markUpdateRestartSentinelFailure).not.toHaveBeenCalled();
        if (waitOutcome === "still-starting" && sentinelStatus !== "error") {
          expect(writeRestartSentinelIfUnchanged).toHaveBeenCalledWith(
            expect.objectContaining({
              expectedRevision: 1,
              payload: expect.objectContaining({
                status: "skipped",
                stats: { reason: "still-starting" },
              }),
            }),
          );
        } else {
          expect(writeRestartSentinelIfUnchanged).not.toHaveBeenCalled();
        }
        expect(writeGatewayRestartHandoffSync).not.toHaveBeenCalled();
      });
    },
  );
}

export function registerGatewayRestartOwnershipTests({
  consumeGatewayRestartIntentPayloadSync,
  readCgroup,
  systemctl,
  consumeGatewayRestartIntent,
  runLoopWithStart,
  acquireGatewayLock,
  gatewayLog,
}: {
  consumeGatewayRestartIntentPayloadSync: Mock;
  readCgroup: Mock;
  systemctl: Mock;
  consumeGatewayRestartIntent: Mock<() => GatewayRestartIntent | null>;
  runLoopWithStart: (params: {
    start: ReturnType<typeof createSignaledStart>["start"];
    runtime: ReturnType<typeof createRuntimeWithExitSignal>["runtime"];
  }) => Promise<unknown>;
  acquireGatewayLock: Mock;
  gatewayLog: { info: Mock };
}) {
  it.each(
    [false, true].flatMap((noRespawn) =>
      [false, true].map((cleanupCompletes) => ({ noRespawn, cleanupCompletes })),
    ),
  )(
    "keeps restart ownership inside a service cgroup (noRespawn=$noRespawn, cleanupCompletes=$cleanupCompletes)",
    async ({ noRespawn, cleanupCompletes }) => {
      if (noRespawn) {
        process.env.OPENCLAW_SYSTEMD_UNIT = "openclaw-gateway.service";
        process.env.OPENCLAW_NO_RESPAWN = "1";
      }
      readCgroup.mockResolvedValue("0::/system.slice/setup_and_run_blacksmith.service\n");
      systemctl.mockResolvedValue({
        code: 0,
        stdout: "LoadState=loaded\nTimeoutStopUSec=90s",
        stderr: "",
      });
      consumeGatewayRestartIntent.mockReturnValueOnce({ force: true });
      await withIsolatedSignals(async ({ captureSignal }) => {
        const cleanup = createDeferredCore();
        const close = createCloseMock().mockImplementationOnce(() => cleanup.promise);
        const { start, started } = createSignaledStart(close);
        const { runtime, exited } = createRuntimeWithExitSignal();
        await runLoopWithStart({ start, runtime });
        await waitForStart(started);
        const stop = captureSignal("SIGINT");
        vi.useFakeTimers();
        try {
          captureSignal("SIGUSR2")();
          await vi.advanceTimersByTimeAsync(11_000);
          expect(close).toHaveBeenCalledOnce();
          expect(runtime.exit).not.toHaveBeenCalled();
          if (cleanupCompletes) {
            cleanup.resolve();
            await vi.advanceTimersByTimeAsync(0);
            expect(start).toHaveBeenCalledTimes(2);
          } else {
            await vi.advanceTimersByTimeAsync(74_000);
            expect(runtime.exit).toHaveBeenCalledExactlyOnceWith(1);
            expect(start).toHaveBeenCalledOnce();
          }
          expect(acquireGatewayLock).toHaveBeenCalledWith(
            expect.objectContaining({
              listenerMode: noRespawn ? "supervised" : "foreground",
              supervisor: noRespawn ? { kind: "systemd", name: "openclaw-gateway.service" } : null,
            }),
          );
          expect(gatewayLog.info).toHaveBeenCalledWith(expect.stringContaining("shutdown=85000ms"));
        } finally {
          cleanup.resolve();
          await vi.advanceTimersByTimeAsync(0);
          if (runtime.exit.mock.calls.length === 0) {
            stop();
            await vi.advanceTimersByTimeAsync(0);
            await exited;
          }
          vi.useRealTimers();
        }
      });
    },
  );
  it.each(["completed", "unconfirmed"] as const)(
    "passes the remaining forced restart budget and reports %s cleanup before process exit",
    async (outcome) => {
      setPlatform("linux");
      vi.stubEnv("OPENCLAW_SYSTEMD_UNIT", "openclaw-gateway.service");
      vi.stubEnv("OPENCLAW_SUPERVISOR_MODE", "external");
      consumeGatewayRestartIntentPayloadSync.mockReturnValueOnce({ force: true });
      systemctl.mockResolvedValue({
        code: 0,
        stdout: "LoadState=loaded\nTimeoutStopUSec=90s",
        stderr: "",
      });
      await withIsolatedSignals(async ({ captureSignal }) => {
        let cleanupDeadline: number | undefined;
        const close = vi.fn<GatewayServer["close"]>(async () => {
          cleanupDeadline = getProcessCleanupBudget()?.deadline;
          await new Promise<void>((resolve, reject) => {
            if (outcome === "completed") {
              setTimeout(resolve, 6_000);
            } else {
              setTimeout(() => {
                setImmediate(() => reject(new Error("service child extinction unconfirmed")));
              }, cleanupDeadline! - performance.now());
            }
          });
        });
        const { start, started } = createSignaledStart(close);
        const { runtime, exited } = createRuntimeWithExitSignal();
        await runLoopWithStart({ start, runtime });
        await waitForStart(started);
        const { getProcessCleanupBudget } =
          await import("../../process/supervisor/cleanup-budget.js");
        vi.useFakeTimers();
        const clock = vi.spyOn(performance, "now").mockReturnValue(1_000);
        try {
          captureSignal("SIGTERM")();
          await vi.advanceTimersByTimeAsync(5_000);
          expect(close).toHaveBeenCalledOnce();
          expect(runtime.exit).not.toHaveBeenCalled();
          await vi.advanceTimersByTimeAsync(outcome === "completed" ? 1_000 : 5_001);
          await expect(exited).resolves.toBe(outcome === "completed" ? 0 : 1);
          expect(cleanupDeadline).toBe(10_000);
          expect(start).toHaveBeenCalledOnce();
        } finally {
          clock.mockRestore();
          vi.useRealTimers();
        }
      });
    },
  );
}

export function registerShutdownBudgetTests({
  runLoopWithStart,
  systemctl,
  consumeGatewayRestartIntent,
  createGatewayActiveWorkSnapshot,
  waitForGatewayActiveWork,
  gatewayLog,
  writeDiagnosticStabilityBundleForFailureSync,
}: {
  runLoopWithStart: (params: {
    start: ReturnType<typeof createSignaledStart>["start"];
    runtime: ReturnType<typeof createRuntimeWithExitSignal>["runtime"];
  }) => Promise<unknown>;
  systemctl: Mock<() => Promise<{ code: number; stdout: string; stderr: string }>>;
  consumeGatewayRestartIntent: Mock<() => GatewayRestartIntent | null>;
  createGatewayActiveWorkSnapshot: Mock<() => GatewayActiveWorkSnapshot>;
  waitForGatewayActiveWork: Mock<
    typeof import("../../infra/gateway-active-work.js").waitForGatewayActiveWork
  >;
  gatewayLog: { info: Mock; warn: Mock };
  writeDiagnosticStabilityBundleForFailureSync: Mock;
}) {
  it.each(shutdownBudgetCases)(
    "bounds $supervisor $signal cleanup when a long provider call honors abort=$honorsAbort (wait=$waitMs, installedStop=$installedStopMs, shutdownStop=$shutdownStopMs)",
    async ({
      signal,
      honorsAbort,
      supervisor,
      waitMs,
      installedStopMs,
      shutdownStopMs,
      inspectionMs = 0,
    }) => {
      vi.clearAllMocks();
      const unit = buildSystemdUnit({ programArguments: ["openclaw", "gateway", "run"] });
      const stopTimeoutMs =
        supervisor === "launchd"
          ? LAUNCH_AGENT_EXIT_TIMEOUT_SECONDS * 1_000
          : ((typeof shutdownStopMs === "number" ? shutdownStopMs : installedStopMs) ??
            Number(unit.match(/^TimeoutStopSec=(\d+)$/m)?.[1]) * 1_000);
      const successStatuses = unit
        .match(/^SuccessExitStatus=(.+)$/m)?.[1]
        ?.split(" ")
        .map(Number);
      if (supervisor === "systemd" || supervisor === "external-systemd") {
        process.env.OPENCLAW_SYSTEMD_UNIT = "openclaw-gateway.service";
        if (supervisor === "external-systemd") {
          process.env.OPENCLAW_SUPERVISOR_MODE = "external";
        }
        setPlatform("linux");
      } else if (supervisor === "launchd") {
        process.env.OPENCLAW_LAUNCHD_LABEL = "ai.openclaw.gateway";
        setPlatform("darwin");
      }
      if (installedStopMs !== undefined) {
        systemctl.mockResolvedValue({
          code: 0,
          stdout: `LoadState=loaded\nTimeoutStopUSec=${installedStopMs / 1_000}s`,
          stderr: "",
        });
      }
      if (waitMs !== undefined) {
        consumeGatewayRestartIntent.mockReturnValueOnce({ waitMs });
      }
      await withIsolatedSignals(async ({ captureSignal }) => {
        const provider = createDeferredCore();
        const connectionWork = new GatewayConnectionWork();
        void connectionWork.track(() => provider.promise);
        connectionWork.signal.addEventListener("abort", () => {
          if (honorsAbort) {
            provider.resolve();
          }
        });
        const close = vi.fn<GatewayServer["close"]>(async () => {
          await connectionWork.drain();
        });
        const { start, started } = createSignaledStart(close);
        const { runtime } = createRuntimeWithExitSignal();
        await runLoopWithStart({ start, runtime });
        await waitForStart(started);
        const host = start.mock.calls[0]?.[0]?.hostLifecycle;
        const active = createActiveWorkSnapshot({ embeddedRuns: 1 });
        createGatewayActiveWorkSnapshot.mockReturnValue(active);
        waitForGatewayActiveWork.mockImplementationOnce(async (timeoutMs, options) => {
          options?.onSnapshot?.(active);
          await new Promise<void>((resolve) => {
            setTimeout(resolve, timeoutMs);
          });
          return { drained: false, snapshot: active };
        });
        vi.useFakeTimers();
        const clock = vi.spyOn(performance, "now").mockImplementation(() => Date.now());
        try {
          if (shutdownStopMs !== undefined) {
            const result = {
              code: shutdownStopMs === "unavailable" ? 1 : 0,
              stdout:
                shutdownStopMs === "unavailable"
                  ? ""
                  : `LoadState=loaded\nTimeoutStopUSec=${shutdownStopMs / 1_000}s`,
              stderr: shutdownStopMs === "unavailable" ? "manager unavailable" : "",
            };
            systemctl.mockResolvedValue(result).mockImplementationOnce(async () => {
              await new Promise<void>((resolve) => {
                setTimeout(resolve, inspectionMs);
              });
              return result;
            });
          }
          if (installedStopMs !== undefined) {
            const probes = systemctl.mock.calls.length;
            expect(host?.getShutdownBudget?.()).toEqual({
              timeoutMs: installedStopMs - 5_000,
              reserveMs: 10_000,
              nativeStopBudget: true,
            });
            expect(systemctl.mock.calls).toHaveLength(probes);
          }
          setTimeout(() => provider.resolve(), 600_000);
          captureSignal(signal)();
          await vi.advanceTimersByTimeAsync(inspectionMs);
          if (installedStopMs !== undefined) {
            expect(host?.getShutdownBudget?.()).toEqual({
              timeoutMs: stopTimeoutMs - 5_000 - inspectionMs,
              reserveMs: 10_000,
              nativeStopBudget: true,
            });
            expect(waitForGatewayActiveWork).toHaveBeenCalledWith(
              stopTimeoutMs - 15_000 - inspectionMs,
              expect.any(Object),
            );
            const budgetLogs = gatewayLog.info.mock.calls
              .flat()
              .filter((line: string) => line.includes("shutdown budget at"));
            expect(budgetLogs).toEqual(
              [
                ["startup", installedStopMs - 5_000, installedStopMs],
                [
                  "shutdown",
                  stopTimeoutMs - 5_000 - inspectionMs,
                  shutdownStopMs === "unavailable" ? 90_000 : stopTimeoutMs,
                ],
              ].map(([phase, budget, source]) => {
                const origin =
                  phase === "shutdown" && shutdownStopMs === "unavailable"
                    ? `startup shutdown budget=${installedStopMs - 5_000}`
                    : `TimeoutStopUSec=${source}`;
                return expect.stringMatching(
                  new RegExp(
                    `at ${phase}: drain=${Number(budget) - 10_000}ms shutdown=${budget}ms.*${origin}ms`,
                  ),
                );
              }),
            );
            if (shutdownStopMs === "unavailable") {
              expect(gatewayLog.warn).toHaveBeenCalledWith(
                expect.stringContaining("Unable to read systemd stop timeout"),
              );
            }
          }
          const deadlineMs =
            signal === "SIGTERM" || waitMs !== undefined
              ? stopTimeoutMs - 5_000
              : Math.min(310_000, stopTimeoutMs - 5_000);
          expect(deadlineMs).toBeLessThan(stopTimeoutMs);
          await vi.advanceTimersByTimeAsync(deadlineMs - inspectionMs - 1);
          expect(connectionWork.signal.aborted).toBe(true);
          if (!honorsAbort) {
            expect(runtime.exit).not.toHaveBeenCalled();
          }
          await vi.advanceTimersByTimeAsync(1);
          const expectedExit = supervisor === "foreground" && !honorsAbort ? 1 : 0;
          expect(runtime.exit).toHaveBeenCalledExactlyOnceWith(expectedExit);
          if (supervisor !== "foreground") {
            expect(successStatuses).toContain(runtime.exit.mock.calls[0]?.[0]);
          }
          expect(start).toHaveBeenCalledOnce();
          if (!honorsAbort) {
            expect(gatewayLog.warn).toHaveBeenCalledWith(
              expect.stringMatching(/abandoning.*embeddedRuns=1/),
            );
            expect(writeDiagnosticStabilityBundleForFailureSync).toHaveBeenCalledWith(
              signal === "SIGTERM"
                ? "gateway.stop_shutdown_timeout"
                : "gateway.restart_shutdown_timeout",
              undefined,
            );
          }
        } finally {
          clock.mockRestore();
          vi.clearAllTimers();
          vi.useRealTimers();
        }
      });
    },
  );
}
