import { useEffect, useState } from 'react';
import { fetchControl, ManagementError } from '../managementApi';
import type { ControlResponse } from '../managementApi';

export function useControl(runId: string, controlId: string) {
  const [refresh, setRefresh] = useState(0);
  const [result, setResult] = useState<{ key: string; data?: ControlResponse; error?: ManagementError } | null>(null);
  const key = JSON.stringify([runId, controlId, refresh]);
  useEffect(() => {
    const controller = new AbortController(); let current = true;
    void fetchControl(runId, controlId, controller.signal).then((data) => {
      if (current && !controller.signal.aborted) setResult({ key, data });
    }).catch((error: unknown) => {
      if (current && !controller.signal.aborted) setResult({ key, error: error instanceof ManagementError ? error : new ManagementError('network') });
    });
    return () => { current = false; controller.abort(); };
  }, [runId, controlId, key]);
  const current = result?.key === key ? result : null;
  return { current, refresh: () => { if (current) setRefresh((old) => old + 1); } };
}
