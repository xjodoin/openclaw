import { resolveGlobalSingleton } from "../shared/global-singleton.js";

export type LifecycleWriteCustodyPhase = "migration" | "backup" | "coordinator-write";

const owners = resolveGlobalSingleton(
  Symbol.for("openclaw.lifecycleWriteCustody"),
  () => new Map<LifecycleWriteCustodyPhase, { count: number }>(),
);

/** Records an existing owner's lifetime; this observation grants no write authority. */
export function beginLifecycleWriteCustody(phase: LifecycleWriteCustodyPhase): () => void {
  const owner = owners.get(phase) ?? { count: 0 };
  owner.count++;
  owners.set(phase, owner);
  let released = false;
  return () => {
    if (released) {
      return;
    }
    released = true;
    if (--owner.count === 0) {
      owners.delete(phase);
    }
  };
}

export function readLifecycleWriteCustody(): Array<{
  phase: LifecycleWriteCustodyPhase;
  count: number;
}> {
  return [...owners]
    .toSorted(([left], [right]) => left.localeCompare(right))
    .map(([phase, { count }]) => ({ phase, count }));
}
