export function PublicationIndicator({ publication }: { publication: string | null }) {
  return <section className="publication-panel" aria-label="Gold publication">
    <div className="publication-label"><span className="gold-mark" aria-hidden="true" />
      <span>GOLD PUBLICATION<span className="publication-note">Pinned for this session</span></span>
    </div>
    <div className="publication-value">
      {publication ? <code aria-label="Publication fingerprint">{publication}</code> : <span>Awaiting a verified publication</span>}
      <span className="publication-description">{publication ? 'Snapshot identity · SHA-256' : 'The API must verify a publication before results are shown.'}</span>
    </div>
    <span className={`publication-badge ${publication ? '' : 'pending'}`}>
      <span aria-hidden="true">{publication ? '✓' : '○'}</span> {publication ? 'Identity pinned' : 'Awaiting data'}
    </span>
  </section>;
}
