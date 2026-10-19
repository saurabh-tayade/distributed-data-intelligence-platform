import type { Publisher } from '../api';
import { formatCount } from '../metrics';

import { Timestamp } from './Timestamp';

export function PublisherTable({ rows }: { rows: Publisher[] }) {
  return <section className="panel table-panel" id="publisher-details" aria-labelledby="details-title">
    <div className="panel-heading"><div><span className="eyebrow">EXPLORE THE DETAILS</span><h2 id="details-title">Publisher breakdown</h2></div>
      <span className="row-badge">{formatCount(rows.length)} {rows.length === 1 ? 'publisher' : 'publishers'} returned</span></div>
    {/* eslint-disable-next-line jsx-a11y/no-noninteractive-tabindex -- Keyboard users must be able to scroll the overflow region. */}
    <div className="table-scroll" role="region" aria-label="Publisher analytics table; scroll horizontally on small screens" tabIndex={0}>
      <table>
        <caption className="visually-hidden">Exact publisher metrics for the displayed publication. Observation timestamps are in UTC with microsecond precision.</caption>
        <thead><tr><th scope="col">Publisher ID</th><th scope="col" className="numeric">Articles</th><th scope="col" className="numeric">Observations</th>
          <th scope="col" className="numeric">Without publication date</th><th scope="col">First observed (UTC)</th><th scope="col">Last observed (UTC)</th></tr></thead>
        <tbody>{rows.map((row) => <tr key={row.sourceId}>
          <th scope="row"><span className="source-cell"><span className="source-dot" aria-hidden="true" />{row.sourceId}</span></th>
          <td className="numeric">{formatCount(row.articleCount)}</td><td className="numeric observation-cell">{formatCount(row.observationCount)}</td>
          <td className="numeric">{formatCount(row.articlesWithoutPublishedAt)}</td>
          <td><Timestamp value={row.firstObservedAt} /></td><td><Timestamp value={row.lastObservedAt} /></td>
        </tr>)}</tbody>
      </table>
    </div>
    <div className="table-footnote"><span>Counts are exact. Timestamps preserve UTC microseconds.</span><span>Missing timestamps are not warnings.</span></div>
  </section>;
}
