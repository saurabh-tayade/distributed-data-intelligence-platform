import { useState } from 'react';
import { dateFilterError, EMPTY_DATES } from '../api';
import type { DateFilters as Dates } from '../api';

export function DateFilters({ applied, onApply }: { applied: Dates; onApply: (dates: Dates) => void }) {
  const [draft, setDraft] = useState(applied);
  const [error, setError] = useState<string | null>(null);
  return <form className="date-filters" noValidate onSubmit={(event) => {
    event.preventDefault();
    const invalid = dateFilterError(draft);
    setError(invalid);
    if (!invalid) onApply({ ...draft });
  }} aria-label="UTC ingestion date filters">
    <div className="date-filter-controls">
      <div><label htmlFor="from">From date (UTC)</label><input id="from" type="text" placeholder="YYYY-MM-DD" inputMode="numeric"
        value={draft.from} onChange={(event) => setDraft({ ...draft, from: event.target.value })}
        aria-invalid={!!error} aria-describedby="date-help date-error" /></div>
      <div><label htmlFor="to">To date (UTC)</label><input id="to" type="text" placeholder="YYYY-MM-DD" inputMode="numeric"
        value={draft.to} onChange={(event) => setDraft({ ...draft, to: event.target.value })}
        aria-invalid={!!error} aria-describedby="date-help date-error" /></div>
      <button className="secondary-button" type="submit">Apply dates</button>
      <button className="text-button" type="button" onClick={() => { setDraft(EMPTY_DATES); setError(null); onApply(EMPTY_DATES); }}>Clear dates</button>
    </div>
    <p id="date-help">Inclusive UTC ingestion dates, in YYYY-MM-DD format. Either bound can be left blank.</p>
    <p id="date-error" role={error ? 'alert' : undefined}>{error}</p>
    <p className="applied-dates">Applied range: <strong>{applied.from || 'No start bound'}</strong> to <strong>{applied.to || 'No end bound'}</strong>. Dates do not filter the article catalog.</p>
  </form>;
}
