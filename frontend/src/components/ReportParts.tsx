import type { ReactNode } from 'react';
import { formatCount } from '../metrics';

export function BoundedNotice({ returned, limit }: { returned: number; limit: number }) {
  return returned === limit ? <p className="scope-notice" role="note"><strong>Bounded results.</strong> The {limit}-row limit was reached.
    Additional records may exist. Charts and totals describe only the returned scope.</p> : null;
}
export function ReportSummary({ title, cards }: { title: string; cards: { label: string; value: bigint | number; note: string }[] }) {
  return <section className="summary-section" aria-label={title}>
    <div className="section-overline"><h2>{title}</h2><span>Totals for returned rows only</span></div>
    <dl className="summary-grid report-summary">{cards.map((card, i) => <div className={`summary-card ${i === 1 ? 'accent-card' : ''}`} key={card.label}>
      <dt>{card.label}</dt><dd className={`summary-value ${formatCount(card.value).length > 12 ? 'large-count' : ''}`}>{formatCount(card.value)}</dd>
      <dd className="card-description">{card.note}</dd>
    </div>)}</dl>
  </section>;
}
export function TableScroll({ label, children }: { label: string; children: ReactNode }) {
  // A focusable overflow region supports keyboard horizontal scrolling on narrow screens.
  // eslint-disable-next-line jsx-a11y/no-noninteractive-tabindex
  return <div className="table-scroll" role="region" aria-label={label} tabIndex={0}>{children}</div>;
}
export function ChartFallback({ target }: { target: string }) {
  return <div className="chart-fallback" role="note"><strong>Exact values exceed the chart’s safe range</strong>
    <p>The chart is hidden to avoid rounding. All returned counts remain exact in the table and summary.</p>
    <a href={`#${target}`}>Read the exact values</a></div>;
}
