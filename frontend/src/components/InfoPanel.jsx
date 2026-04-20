import { useEffect, useMemo, useState } from 'react'

import { fetchEntitySummary } from '../api'

function formatCurrency(value) {
  const amount = Number(value || 0)
  return `$${amount.toLocaleString()}`
}

function formatCount(value) {
  return Number(value || 0).toLocaleString()
}

function resolveEntity(node) {
  if (!node || !node.id) return null
  const [kind, rawId] = String(node.id).split('-')
  if (kind === 'org') return { entityType: 'organization', entityId: rawId }
  if (kind === 'leg') return { entityType: 'legislator', entityId: node.bioguide_id || rawId }
  if (kind === 'com') return { entityType: 'committee', entityId: rawId }
  return null
}

function resolveType(node) {
  if (!node) return 'Organization'
  if (node.type === 'organization') return 'Organization'
  if (node.type === 'legislator') return 'Legislator'
  if (node.type === 'committee') return 'Committee'
  if (node.type === 'lobbyist') return 'Lobbyist'
  return 'Organization'
}

function buildStats(type, summary, yearRangeLabel) {
  if (!summary) {
    return [
      { label: 'TOTAL LOBBYING SPEND', value: '$0', money: true },
      { label: 'LEGISLATORS FUNDED', value: '0' },
      { label: `FILINGS (${yearRangeLabel})`, value: '0' },
      { label: 'TOP ISSUES', value: 'N/A' },
    ]
  }

  if (type === 'Legislator') {
    return [
      { label: 'TOTAL CONTRIBUTIONS RECEIVED', value: formatCurrency(summary.total_contributions_received), money: true },
      { label: 'COMMITTEE ASSIGNMENTS', value: formatCount(summary.committees?.length || 0) },
      { label: `FILINGS (${yearRangeLabel})`, value: formatCount(summary.top_contributing_orgs?.length || 0) },
      { label: 'TOP ISSUES', value: (summary.top_issue_codes || []).join(' · ') || 'N/A' },
    ]
  }

  if (type === 'Committee') {
    return [
      { label: 'ACTIVE MEMBERS', value: formatCount(summary.member_count) },
      { label: 'ACTIVE LOBBYING ORGS', value: formatCount(summary.active_lobbying_orgs) },
      { label: `FILINGS (${yearRangeLabel})`, value: formatCount(summary.members?.length || 0) },
      { label: 'TOP ISSUES', value: (summary.top_issue_codes || []).join(' · ') || 'N/A' },
    ]
  }

  return [
    { label: 'TOTAL LOBBYING SPEND', value: formatCurrency(summary.total_lobbying_spend), money: true },
    { label: 'LEGISLATORS FUNDED', value: formatCount(summary.top_recipient_legislators?.length || 0) },
    { label: `FILINGS (${yearRangeLabel})`, value: formatCount(summary.filing_count) },
    { label: 'TOP ISSUES', value: (summary.top_issue_codes || []).join(' · ') || 'N/A' },
  ]
}

function buildConnections(type, summary) {
  if (!summary) return []

  if (type === 'Committee') {
    return (summary.members || []).slice(0, 3).map((member) => ({
      name: member.name,
      detail: `${member.role || 'Member'} · ${member.party || 'Nonpartisan'}`,
    }))
  }

  if (type === 'Legislator') {
    return (summary.top_contributing_orgs || []).slice(0, 3).map((org) => ({
      name: org.name,
      detail: `${formatCurrency(org.total_contributed)} contributed`,
      money: true,
    }))
  }

  const lobbyFirms = (summary.top_lobbyists || []).slice(0, 2).map((firm) => ({
    name: firm.name,
    detail: `Lobbying firm · ${formatCount(firm.filings)} filings`,
  }))

  const legislators = (summary.top_recipient_legislators || []).slice(0, 2).map((recipient) => ({
    name: recipient.name,
    detail: `${formatCurrency(recipient.total_received)} contributed`,
    money: true,
  }))

  return [...lobbyFirms, ...legislators]
}

function PanelSkeleton() {
  return (
    <div className="panel-skeleton" aria-hidden="true">
      <div className="skeleton-bar-static wide" />
      <div className="skeleton-bar-static medium" />
      <div className="skeleton-bar-static short" />
    </div>
  )
}

export default function InfoPanel({ node, onExpand, filters }) {
  const [summary, setSummary] = useState(null)
  const [loading, setLoading] = useState(false)
  const resolved = useMemo(() => resolveEntity(node), [node])
  const typeLabel = resolveType(node)
  const yearRangeLabel = `${filters?.year_min || 2023}–${filters?.year_max || 2025}`

  useEffect(() => {
    if (!resolved) {
      setSummary(null)
      return
    }

    setLoading(true)
    fetchEntitySummary(resolved.entityType, resolved.entityId)
      .then((data) => setSummary(data))
      .catch(() => setSummary(null))
      .finally(() => setLoading(false))
  }, [resolved])

  const stats = buildStats(typeLabel, summary, yearRangeLabel)
  const connections = buildConnections(typeLabel, summary)
  const coveredPositions = Array.isArray(node?.covered_positions) ? node.covered_positions.filter(Boolean) : []
  const hasCoveredPositions = Boolean(node?.has_covered_position && coveredPositions.length > 0)
  const hasConviction = Boolean(node?.has_conviction)
  const convictionDisclosure = node?.conviction_disclosure

  return (
    <aside className={`info-panel ${node ? 'open' : ''}`} aria-label="Entity details panel">
      {!node && (
        <div className="info-panel-empty">
          <p>Select an entity in the graph to view the reporting sidebar.</p>
        </div>
      )}

      {node && (
        <div className="info-panel-content">
          <div className="info-panel-rule-strong" />
          <h3>{node.label}</h3>
          <p className="info-panel-type">{typeLabel}</p>
          <hr className="rule" />

          {loading ? (
            <PanelSkeleton />
          ) : (
            <>
              <div className="info-stat-list">
                {stats.map((stat) => (
                  <div key={stat.label} className="info-stat-item">
                    <span>{stat.label}</span>
                    <strong className={stat.money ? 'money-value' : ''}>{stat.value}</strong>
                  </div>
                ))}
              </div>

              <hr className="rule" />

              <section className="info-connections">
                <h4>CONNECTIONS</h4>
                {connections.length > 0 ? (
                  <ul>
                    {connections.map((connection) => (
                      <li key={`${connection.name}-${connection.detail}`}>
                        <p className="connection-title">{connection.name}</p>
                        <p className={`connection-detail ${connection.money ? 'money-value' : ''}`}>
                          {connection.detail}
                        </p>
                      </li>
                    ))}
                  </ul>
                ) : (
                  <p className="connection-detail">No linked records in this scope.</p>
                )}
              </section>

              {typeLabel === 'Lobbyist' && hasCoveredPositions && (
                <>
                  <hr className="rule" />
                  <section className="info-prior-positions">
                    <h4>PRIOR GOVERNMENT POSITIONS</h4>
                    <ul>
                      {coveredPositions.map((position, idx) => (
                        <li key={`${position}-${idx}`}>{position}</li>
                      ))}
                    </ul>
                  </section>
                </>
              )}

              {typeLabel === 'Lobbyist' && hasConviction && (
                <>
                  <hr className="rule" />
                  <section className="info-conviction-warning">
                    <h4>CONVICTION DISCLOSURE</h4>
                    <p>{convictionDisclosure || 'Lobbyist conviction disclosure flagged in filing metadata.'}</p>
                  </section>
                </>
              )}
            </>
          )}

          <hr className="rule" />
          <button type="button" className="panel-network-btn" onClick={() => onExpand?.(node)}>
            [ VIEW FULL NETWORK ]
          </button>
          <div className="info-panel-rule-strong" />
        </div>
      )}
    </aside>
  )
}
