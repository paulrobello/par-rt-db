import { useEffect, useRef, useState } from "react";
import { useAdmin } from "./admin";
import { MAX_SAMPLES, type Sample } from "./metrics-series";
import type { MetricsSnapshot } from "./types";

/**
 * Accumulates the /admin/stream gauge snapshots (one per second) into a rolling
 * ring buffer of the most recent `maxSamples` samples, each stamped with its
 * client receive-time. Only the `/metrics` page mounts this, localizing the 1 Hz
 * re-render churn. `AdminProvider` is untouched.
 *
 * StrictMode double-invokes the effect on mount; the `lastRef` guard dedupes by
 * snapshot-ref identity so a snapshot is never recorded twice.
 */
export function useMetricsHistory(maxSamples = MAX_SAMPLES): { samples: Sample[] } {
  const { metrics } = useAdmin();
  const bufRef = useRef<Sample[]>([]);
  const lastRef = useRef<MetricsSnapshot | null>(null);
  const [samples, setSamples] = useState<Sample[]>([]);

  useEffect(() => {
    if (!metrics || metrics === lastRef.current) return;
    lastRef.current = metrics;
    const buf = bufRef.current;
    buf.push({ t: Date.now(), snap: metrics });
    while (buf.length > maxSamples) buf.shift();
    setSamples([...buf]);
  }, [metrics, maxSamples]);

  return { samples };
}
