import { isFilterableSource, PUBLISHER_LIMIT } from '../api';

interface Props {
  sourceId: string;
  sources: string[];
  bounded: boolean;
  onChange: (sourceId: string) => void;
}

export function PublisherFilter({ sourceId, sources, bounded, onChange }: Props) {
  return <section className="filter-bar" aria-label="Publisher filters">
    <div className="filter-control">
      <label htmlFor="publisher">Publisher scope</label>
      <select id="publisher" value={sourceId} onChange={(event) => onChange(event.target.value)} aria-describedby="filter-help">
        <option value="">All returned publishers</option>
        {sources.map((source) => <option key={source} value={source} disabled={!isFilterableSource(source)}>{source}</option>)}
      </select>
      <button className="text-button" type="button" onClick={() => onChange('')} disabled={!sourceId}>Reset filter</button>
    </div>
    <p id="filter-help">{bounded ? `Selector includes the first ${PUBLISHER_LIMIT} returned publishers.` : 'Choose a publisher to request its exact metrics.'}
      <span>{PUBLISHER_LIMIT} rows per request <span aria-hidden="true">·</span> No automatic refresh</span></p>
  </section>;
}
