export function Timestamp({ value }: { value: string | null }) {
  if (value === null) return <span className="missing-value">Not available</span>;
  return <time dateTime={value}><span>{value.slice(0, 10)}</span><span className="time-detail">{value.slice(11, -1)} UTC</span></time>;
}
