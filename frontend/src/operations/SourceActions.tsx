import { useEffect, useState } from 'react';
import type { SourceResponse } from '../managementApi';
import { SourceEditor } from './SourceEditor';
import { reviewSourceCommand, SOURCE_CHANGED, useSourceIntent } from './sourceIntent';

export function SourceActions({ source }: { source: SourceResponse }) {
  const [baseline, setBaseline] = useState<SourceResponse | null>(null);
  const [error, setError] = useState('');
  const { intent, unavailable } = useSourceIntent();
  useEffect(() => {
    const reset = () => setBaseline(null);
    window.addEventListener(SOURCE_CHANGED, reset);
    return () => window.removeEventListener(SOURCE_CHANGED, reset);
  }, []);
  return <details className="panel source-actions"><summary>Source management actions</summary>
    <p>Source admission and configuration only. Disabling admission does not cancel active processing.</p>
    <div className="source-action-buttons"><button type="button" className="secondary-button" disabled={!!intent || unavailable} onClick={() => setBaseline(source)}>Create revision</button>
      <button type="button" className="secondary-button" disabled={!!intent || unavailable} onClick={() => {
        try {
          reviewSourceCommand({ action: 'enabled', identity: { productId: source.data.productId, sourceId: source.data.sourceId },
            body: JSON.stringify({ enabled: !source.data.enabled }), ifMatch: source.etag });
          setError(''); setBaseline(null);
        } catch { setError('The request could not be retained safely in this tab. No mutation was submitted.'); }
      }}>{source.data.enabled ? 'Disable admission' : 'Enable admission'}</button></div>
    {error && <p role="alert">{error}</p>}
    {baseline && <SourceEditor key={baseline.etag} baseline={baseline} onCancel={() => setBaseline(null)} />}
  </details>;
}
