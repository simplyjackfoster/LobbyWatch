import { useEffect, useMemo, useState } from 'react'

import { fetchDataStatus } from '../api'

function formatTimestamp(value) {
  if (!value) return 'unknown'
  const normalized = String(value).replace('Z', '')
  const parsed = new Date(normalized)
  if (Number.isNaN(parsed.getTime())) return value
  return parsed.toLocaleString()
}

export default function AboutData() {
  const [status, setStatus] = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    let active = true
    fetchDataStatus()
      .then((data) => {
        if (active) setStatus(data || null)
      })
      .catch(() => {
        if (active) setStatus(null)
      })
      .finally(() => {
        if (active) setLoading(false)
      })
    return () => {
      active = false
    }
  }, [])

  const statusRows = useMemo(
    () => [
      ['Last updated', loading ? 'checking...' : formatTimestamp(status?.last_exported_at)],
      ['Last ingest run', loading ? 'checking...' : formatTimestamp(status?.last_ingest_at)],
      ['LDA coverage through', loading ? 'checking...' : status?.lda_coverage_through || 'unknown'],
      ['Congress coverage through', loading ? 'checking...' : status?.congress_coverage_through || 'unknown'],
    ],
    [loading, status],
  )

  return (
    <section className="about-data-page">
      <article className="about-data-card">
        <h2>What is LobbyWatch?</h2>
        <p>
          LobbyWatch is a public-interest index of federal lobbying, campaign contribution, and congressional activity data,
          joined into one searchable graph so anyone can trace influence paths across organizations, legislators, and policy issues.
        </p>

        <h2>Data Sources</h2>

        <div className="about-data-source-block">
          <h3>Senate LDA</h3>
          <p>
            LobbyWatch pulls federal lobbying registrations, filing periods, spend amounts, lobbyists, and issue metadata from Senate LDA disclosures.
          </p>
          <a href="https://lda.senate.gov/api/" target="_blank" rel="noreferrer">LDA API Documentation</a>
        </div>

        <div className="about-data-source-block">
          <h3>FEC</h3>
          <p>
            LobbyWatch pulls committee-linked contribution records used to map contributor organizations to recipient legislators and spending cycles.
          </p>
          <a href="https://api.open.fec.gov/developers/" target="_blank" rel="noreferrer">FEC API Documentation</a>
        </div>

        <div className="about-data-source-block">
          <h3>Congress.gov</h3>
          <p>
            LobbyWatch pulls legislator identity data, committee relationships, and vote-linked legislative context from Congress.gov data services.
          </p>
          <a href="https://api.congress.gov/" target="_blank" rel="noreferrer">Congress.gov API Documentation</a>
        </div>

        <h2>Coverage &amp; Freshness</h2>
        <div className="about-data-status-grid">
          {statusRows.map(([label, value]) => (
            <div key={label} className="about-data-status-row">
              <span>{label}</span>
              <strong>{value}</strong>
            </div>
          ))}
        </div>

        <h2>Methodology Notes</h2>
        <ul>
          <li>Organizations are deduplicated with normalized name matching and conflict-safe upserts.</li>
          <li>Lobbying amount values are filing-reported LDA spend totals for the filing period.</li>
          <li>FEC contribution data can lag by roughly 30 days from filing to API availability.</li>
          <li>Some older LDA filings do not include complete spend values and may appear as zero or null.</li>
        </ul>
      </article>
    </section>
  )
}
