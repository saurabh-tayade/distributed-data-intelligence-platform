import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import type { Publisher } from '../api';
import { chartRows, formatCount } from '../metrics';

const CHART_LIMIT = 12;

export function PublisherComparisonChart({ rows }: { rows: Publisher[] }) {
  const data = chartRows(rows);
  return <section className="panel chart-panel" aria-labelledby="comparison-title">
    <div className="panel-heading"><div><span className="eyebrow">COMPARE PUBLISHERS</span>
      <h2 id="comparison-title">Content & observations</h2></div><a className="small-link" href="#publisher-details">View exact values <span aria-hidden="true">↗</span></a></div>
    <p className="panel-description">Article counts alongside accepted content observations.</p>
    {data === null ? <div className="chart-fallback" role="note"><strong>Exact values exceed the chart’s safe range</strong>
      <p>Some counts exceed 9,007,199,254,740,991. The chart is hidden to avoid rounding. All counts remain exact in the summary and publisher table.</p>
      <a href="#publisher-details">Read the exact publisher counts</a></div> : <>
      <div className="chart-legend" aria-label="Chart legend"><span><i className="legend-articles" />Articles</span><span><i className="legend-observations" />Observations</span></div>
      <div className="chart-container" style={{ height: Math.max(240, Math.min(data.length, CHART_LIMIT) * 72 + 50) }}>
        <ResponsiveContainer width="100%" height="100%" minWidth={0}>
          <BarChart data={data.slice(0, CHART_LIMIT)} layout="vertical" margin={{ top: 8, right: 28, bottom: 8, left: 0 }} accessibilityLayer>
            <CartesianGrid horizontal={false} stroke="#e4ebe9" strokeDasharray="3 4" />
            <XAxis type="number" allowDecimals={false} tick={{ fill: '#586e75', fontSize: 11 }} axisLine={false} tickLine={false} />
            <YAxis type="category" dataKey="sourceId" width={130} tick={{ fill: '#304b54', fontSize: 11 }} axisLine={false} tickLine={false}
              tickFormatter={(value: string) => value.length > 18 ? `${value.slice(0, 16)}…` : value} />
            <Tooltip cursor={{ fill: '#f1f5f3' }} formatter={(value) => typeof value === 'number' ? formatCount(value) : 'Not available'}
              contentStyle={{ borderRadius: 8, border: '1px solid #d5dfdc', fontSize: 12 }} />
            <Bar dataKey="articles" name="Articles" fill="#208a7a" radius={[0, 3, 3, 0]} maxBarSize={14} isAnimationActive={false} />
            <Bar dataKey="observations" name="Observations" fill="#203e4c" radius={[0, 3, 3, 0]} maxBarSize={14} isAnimationActive={false} />
          </BarChart>
        </ResponsiveContainer>
      </div>
      <p className="chart-caption">{data.length > CHART_LIMIT ? `Showing the first ${CHART_LIMIT} of ${formatCount(data.length)} returned publishers. ` : ''}
        Source ID order. Exact counts and timestamps are available in the table below.</p>
    </>}
  </section>;
}
