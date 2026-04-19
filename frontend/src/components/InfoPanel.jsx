import { useEffect, useState } from 'react'

import { fetchEntitySummary } from '../api'

function Skeleton() {
  return (
    <div className="summary-skeleton">
      <div className="skeleton-bar" />
      <div className="skeleton-bar" />
      <div className="skeleton-bar" />
    </div>
  )
}

function resolveEntity(node) {
  if (!node || !node.id) return null
  const [kind, rawId] = String(node.id).split('-')
  if (kind === 'org') return { entityType: 'organization', entityId: rawId }
  if (kind === 'leg') return { entityType: 'legislator', entityId: node.bioguide_id || rawId }
  if (kind === 'com') return { entityType: 'committee', entityId: rawId }
  return null
}

export default function InfoPanel({ node, onExpand }) {
  const [summary, setSummary] = useState(null)
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    const resolved = resolveEntity(node)
    if (!resolved) {
      setSummary(null)
      return
    }

    setLoading(true)
    fetchEntitySummary(resolved.entityType, resolved.entityId)
      .then((data) => setSummary(data))
      .catch(() => setSummary({ error: 'failed' }))
      .finally(() => setLoading(false))
  }, [node])

  if (!node) {
    return (
      <aside className="info-panel">
        <h3>Node Details</h3>
        <p>Select a node to view details.</p>
      </aside>
    )
  }

  return (
    <aside className="info-panel">
      <h3>{node.label}</h3>
      <p><strong>Type:</strong> {node.type}</p>
      {loading && <Skeleton />}
      {!loading && summary && !summary.error && (
        <div className="summary-body">
          {summary.total_lobbying_spend !== undefined && (
            <p><strong>Total Lobbying Spend:</strong> ${Number(summary.total_lobbying_spend).toLocaleString()}</p>
          )}
          {summary.total_contributions !== undefined && (
            <p><strong>Total Contributions:</strong> ${Number(summary.total_contributions).toLocaleString()}</p>
          )}
          {summary.total_contributions_received !== undefined && (
            <p><strong>Total Received:</strong> ${Number(summary.total_contributions_received).toLocaleString()}</p>
          )}
          {summary.filing_count !== undefined && <p><strong>Filings:</strong> {summary.filing_count}</p>}
          {summary.member_count !== undefined && <p><strong>Members:</strong> {summary.member_count}</p>}
          {Array.isArray(summary.top_issue_codes) && summary.top_issue_codes.length > 0 && (
            <p><strong>Top Issues:</strong> {summary.top_issue_codes.join(', ')}</p>
          )}
        </div>
      )}
      <button className="expand-btn" onClick={() => onExpand?.(node)}>Expand</button>
    </aside>
  )
}
