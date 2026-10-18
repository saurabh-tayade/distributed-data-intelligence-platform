import type { Publisher } from '../api';
import { formatCount, summarize } from '../metrics';

export function SummaryCards({ rows }: { rows: Publisher[] }) {
  const totals = summarize(rows);
  const cards = [
    { label: 'Publishers represented', value: totals.publishers, note: 'Distinct publishers in these results', accent: false },
    { label: 'Articles', value: totals.articles, note: 'Catalog articles in this scope', accent: false },
    { label: 'Observations', value: totals.observations, note: 'Accepted content observations', accent: true },
    { label: 'Without publication date', value: totals.withoutDate, note: 'Articles with no publication timestamp', accent: false },
  ];
  return <section className="summary-section" aria-labelledby="summary-title">
    <div className="section-overline"><h2 id="summary-title">Returned publisher scope</h2><span>Totals for the displayed results</span></div>
    <dl className="summary-grid">{cards.map((card, index) => <div key={card.label} className={`summary-card ${card.accent ? 'accent-card' : ''}`}>
      <dt>{card.label}<span className="card-index" aria-hidden="true">0{index + 1}</span></dt>
      <dd className={`summary-value ${formatCount(card.value).length > 12 ? 'large-count' : ''}`}>{formatCount(card.value)}</dd>
      <dd className="card-description">{card.note}</dd>
    </div>)}</dl>
  </section>;
}
