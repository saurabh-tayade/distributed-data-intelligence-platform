import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import type { QualityResponse } from '../api';
import { formatCount, observationTotals } from '../metrics';
import { BoundedNotice, ReportSummary, TableScroll } from '../components/ReportParts';
import { ReportEmptyState } from '../components/States';

export default function DataQuality({ response }: { response: QualityResponse }) {
  const totals = observationTotals(response.data);
  // Rates already have a finite floating-point API contract; no integral counters are converted here.
  const plotRates = response.data.every((row) => Number.isFinite(row.warningCodesPerObservation * 2));
  return <>
    <p className="visually-hidden" role="status">{response.meta.returned} quality rows returned.</p>
    <BoundedNotice {...response.meta} />
    <ReportSummary title="Returned quality scope" cards={[
      { label: 'Publishers represented', value: response.data.length, note: 'Sources in this filtered response' },
      { label: 'Observations', value: totals.observations, note: 'Accepted content observations' },
      { label: 'Warning codes', value: totals.warnings, note: 'Codes, not warning-bearing records' },
    ]} />
    <p className="metric-explanation">A rate of 2 means two warning codes per accepted observation. This is not a percentage and can exceed 1.0.
      Missing publication timestamps do not automatically imply warnings. Source rates are not summed or averaged.</p>
    {response.data.length === 0 ? <ReportEmptyState label="quality rows" /> : <>
      <section className="panel chart-panel standalone-panel" aria-labelledby="quality-chart-title">
        <div className="panel-heading"><div><span className="eyebrow">COMPARE SOURCES</span><h2 id="quality-chart-title">Warning codes per observation</h2></div>
          <a className="small-link" href="#quality-details">View quality values</a></div>
        <p className="panel-description">The API’s per-source warning-code total divided by accepted observations in the requested date scope.</p>
        {plotRates ? <>
          <div className="chart-legend"><span><i className="legend-articles" />Codes per observation</span></div>
          <div className="chart-container" style={{ height: Math.max(240, Math.min(response.data.length, 12) * 48 + 50) }}>
            <ResponsiveContainer width="100%" height="100%" minWidth={0}>
              <BarChart data={response.data.slice(0, 12)} layout="vertical" margin={{ top: 8, right: 28, bottom: 8, left: 0 }} accessibilityLayer>
                <CartesianGrid horizontal={false} stroke="#e4ebe9" strokeDasharray="3 4" />
                <XAxis type="number" tick={{ fontSize: 11, fill: '#586e75' }} axisLine={false} tickLine={false} />
                <YAxis type="category" dataKey="sourceId" width={130} tick={{ fontSize: 11, fill: '#304b54' }} axisLine={false} tickLine={false}
                  tickFormatter={(value: string) => value.length > 18 ? `${value.slice(0, 16)}…` : value} />
                <Tooltip formatter={(value) => String(value)} />
                <Bar dataKey="warningCodesPerObservation" name="Codes per observation" fill="#208a7a" radius={[0, 3, 3, 0]} maxBarSize={18} isAnimationActive={false} />
              </BarChart>
            </ResponsiveContainer>
          </div>
          <p className="chart-caption">{response.data.length > 12 ? `Showing the first 12 of ${response.data.length} returned publishers. ` : ''}
            Source ID order. A zero rate represents no warning codes in that returned source scope.</p>
        </> : <p className="chart-fallback">Rates exceed the chart’s plotting range. Read the supplied numeric rates in the table below.</p>}
      </section>
      <section className="panel table-panel" id="quality-details" aria-labelledby="quality-table-title">
        <div className="panel-heading"><div><span className="eyebrow">EXACT COUNTS / SUPPLIED RATES</span><h2 id="quality-table-title">Quality breakdown</h2></div><span className="row-badge">{response.meta.returned} sources returned</span></div>
        <TableScroll label="Quality analytics table; scroll horizontally on small screens"><table>
          <caption className="visually-hidden">Quality metrics for returned sources. Rates are codes per accepted observation, not percentages.</caption>
          <thead><tr><th scope="col">Publisher ID</th><th scope="col" className="numeric">Observations</th><th scope="col" className="numeric">Warning codes</th>
            <th scope="col" className="numeric">Warning codes / observation</th></tr></thead>
          <tbody>{response.data.map((row) => <tr key={row.sourceId}><th scope="row"><span className="source-cell">{row.sourceId}</span></th>
            <td className="numeric">{formatCount(row.observationCount)}</td><td className="numeric">{formatCount(row.qualityWarningCount)}</td>
            <td className="numeric">{String(row.warningCodesPerObservation)}</td></tr>)}</tbody>
        </table></TableScroll>
        <p className="table-footnote">Warning totals include codes on all accepted observations in the date scope. Article details show codes on the selected observation only.</p>
      </section>
    </>}
  </>;
}
