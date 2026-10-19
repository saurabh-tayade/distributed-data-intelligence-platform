import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import type { DailyResponse } from '../api';
import { dailyChartRows, dailyObservationTotals, formatCount, observationTotals } from '../metrics';
import { BoundedNotice, ChartFallback, ReportSummary, TableScroll } from '../components/ReportParts';
import { ReportEmptyState } from '../components/States';
import { Timestamp } from '../components/Timestamp';

export default function DailyActivity({ response }: { response: DailyResponse }) {
  const totals = observationTotals(response.data);
  const chart = dailyChartRows(response.data);
  return <>
    <p className="visually-hidden" role="status">{response.meta.returned} daily rows returned.</p>
    <BoundedNotice {...response.meta} />
    <ReportSummary title="Returned daily scope" cards={[
      { label: 'Source/day groups', value: response.data.length, note: 'Groups actually returned by the API' },
      { label: 'Observations', value: totals.observations, note: 'Accepted content observations' },
      { label: 'Warning codes', value: totals.warnings, note: 'Codes across accepted observations' },
    ]} />
    {response.data.length === 0 ? <ReportEmptyState label="daily rows" /> : <>
      <section className="panel chart-panel standalone-panel" aria-labelledby="daily-chart-title">
        <div className="panel-heading"><div><span className="eyebrow">INGESTION ACTIVITY</span><h2 id="daily-chart-title">Observations by date</h2></div>
          <a className="small-link" href="#daily-details">View exact daily values</a></div>
        <p className="panel-description">Observations summed across returned source/day groups, by UTC ingestion date. These are not article publication dates.</p>
        <ul className="visually-hidden" aria-label="Returned observation totals by UTC date">
          {dailyObservationTotals(response.data).map((row) => <li key={row.date}>{row.date}: {formatCount(row.observations)} accepted observations</li>)}
        </ul>
        {chart === null ? <ChartFallback target="daily-details" /> : <>
          <div className="chart-legend"><span><i className="legend-articles" />Accepted observations</span></div>
          <div className="chart-container" style={{ height: 280 }}><ResponsiveContainer width="100%" height="100%" minWidth={0}>
            <BarChart data={chart.slice(0, 30)} margin={{ top: 12, right: 20, bottom: 12, left: 0 }} accessibilityLayer>
              <CartesianGrid vertical={false} stroke="#e4ebe9" strokeDasharray="3 4" />
              <XAxis type="category" dataKey="date" tick={{ fontSize: 11, fill: '#586e75' }} axisLine={false} tickLine={false} />
              <YAxis allowDecimals={false} tick={{ fontSize: 11, fill: '#586e75' }} axisLine={false} tickLine={false} />
              <Tooltip formatter={(value) => typeof value === 'number' ? formatCount(value) : 'Not available'} />
              <Bar dataKey="observations" name="Accepted observations" fill="#208a7a" maxBarSize={68} radius={[4, 4, 0, 0]} isAnimationActive={false} />
            </BarChart>
          </ResponsiveContainer></div>
          <p className="chart-caption">{chart.length === 1 ? 'One UTC ingestion date is represented. ' : ''}
            {chart.length > 30 ? `Showing the first 30 of ${chart.length} returned dates. ` : ''}Only returned dates appear; missing dates are not filled with zeroes.</p>
        </>}
      </section>
      <section className="panel table-panel" id="daily-details" aria-labelledby="daily-table-title">
        <div className="panel-heading"><div><span className="eyebrow">SOURCE / DAY GRAIN</span><h2 id="daily-table-title">Daily breakdown</h2></div><span className="row-badge">{response.meta.returned} rows returned</span></div>
        <TableScroll label="Daily analytics table; scroll horizontally on small screens"><table>
          <caption className="visually-hidden">Exact daily metrics ordered by source ID and UTC ingestion date.</caption>
          <thead><tr><th scope="col">Publisher ID</th><th scope="col">Ingestion date (UTC)</th><th scope="col" className="numeric">Observations</th>
            <th scope="col" className="numeric">Distinct articles in group</th><th scope="col" className="numeric">Distinct ingestions in group</th>
            <th scope="col" className="numeric">Warning codes</th><th scope="col">Latest ingested (UTC)</th></tr></thead>
          <tbody>{response.data.map((row) => <tr key={`${row.sourceId}/${row.ingestionDate}`}>
            <th scope="row"><span className="source-cell">{row.sourceId}</span></th><td><time dateTime={row.ingestionDate}>{row.ingestionDate}</time></td>
            <td className="numeric">{formatCount(row.observationCount)}</td><td className="numeric">{formatCount(row.distinctArticleCount)}</td>
            <td className="numeric">{formatCount(row.distinctIngestionCount)}</td><td className="numeric">{formatCount(row.qualityWarningCount)}</td>
            <td><Timestamp value={row.latestIngestedAt} /></td>
          </tr>)}</tbody>
        </table></TableScroll>
        <p className="table-footnote">Distinct article and ingestion counts belong to each source/day group. They are not summed into global distinct totals. Observations are not views, clicks, or engagement.</p>
      </section>
    </>}
  </>;
}
