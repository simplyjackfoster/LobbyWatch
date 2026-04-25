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
  if (!node) return 'Entity'
  if (node.type === 'organization') return 'Organization'
  if (node.type === 'legislator') return 'Legislator'
  if (node.type === 'committee') return 'Committee'
  if (node.type === 'lobbyist') return 'Lobbyist'
  return 'Entity'
}

function getNodeById(graph, id) {
  return (graph?.nodes || []).find((n) => n.id === id) || null
}

function dedupeValues(values) {
  const seen = new Set()
  const out = []
  values.forEach((value) => {
    const raw = String(value || '').trim()
    if (!raw || seen.has(raw)) return
    seen.add(raw)
    out.push(raw)
  })
  return out
}

function buildCongressBillUrl(billId, congress) {
  const normalized = String(billId || '')
    .toLowerCase()
    .replace(/\./g, '')
    .replace(/\s+/g, '')
    .replace(/_/g, '-')
  const match = normalized.match(/^(hconres|hjres|hres|hr|sconres|sjres|sres|s)-?(\d+)$/)
  if (!match || !congress) return null

  const [, billType, number] = match
  const pathByType = {
    hr: 'house-bill',
    hres: 'house-resolution',
    hjres: 'house-joint-resolution',
    hconres: 'house-concurrent-resolution',
    s: 'senate-bill',
    sres: 'senate-resolution',
    sjres: 'senate-joint-resolution',
    sconres: 'senate-concurrent-resolution',
  }
  const path = pathByType[billType]
  if (!path) return null
  return `https://www.congress.gov/bill/${congress}th-congress/${path}/${number}`
}

function buildSourceBadges(node, graph, summary) {
  if (!node) return []
  const badges = []
  const edges = (graph?.edges || []).filter((edge) => edge.source === node.id || edge.target === node.id)
  const filingUuids = dedupeValues(edges.flatMap((edge) => edge.filing_uuids || []))
  const fecCommitteeIds = dedupeValues(edges.flatMap((edge) => edge.fec_committee_ids || []))

  if (node?.bioguide_id) {
    badges.push({
      label: 'Congress.gov ↗',
      url: `https://www.congress.gov/member/${encodeURIComponent(node.bioguide_id)}`,
    })
  }
  if (node?.lda_id) {
    badges.push({
      label: 'LDA ↗',
      url: `https://lda.gov/lobbyists/${encodeURIComponent(node.lda_id)}/`,
    })
  }
  if (filingUuids[0]) {
    badges.push({
      label: 'LDA ↗',
      url: `https://lda.gov/filings/${encodeURIComponent(filingUuids[0])}/`,
    })
  }
  if (fecCommitteeIds[0]) {
    badges.push({
      label: 'FEC ↗',
      url: `https://www.fec.gov/data/committee/${encodeURIComponent(fecCommitteeIds[0])}/`,
    })
  }

  const vote = (summary?.recent_votes || []).find((item) => item?.bill_id && item?.congress)
  const voteUrl = vote ? buildCongressBillUrl(vote.bill_id, vote.congress) : null
  if (voteUrl) {
    badges.push({
      label: 'Congress.gov ↗',
      url: voteUrl,
    })
  }

  const seen = new Set()
  return badges.filter((badge) => {
    const key = `${badge.label}|${badge.url}`
    if (seen.has(key)) return false
    seen.add(key)
    return true
  })
}

function buildGraphDrivenPanel(node, graph, summary) {
  if (!node) return { stats: [], connections: [] }
  const allEdges = graph?.edges || []

  if (node.type === 'organization') {
    const contributionEdges = allEdges
      .filter((e) => e.type === 'contribution' && e.source === node.id)
      .sort((a, b) => Number(b.amount || 0) - Number(a.amount || 0))
    const connectedLegislatorIds = new Set(
      contributionEdges
        .map((e) => e.target)
        .filter((id) => getNodeById(graph, id)?.type === 'legislator')
    )
    const hiredFirmCount = allEdges.filter((e) => e.type === 'hired_firm' && e.source === node.id).length
    return {
      stats: [
        { label: 'TOTAL LOBBYING SPEND', value: formatCurrency(summary?.total_lobbying_spend), money: true },
        { label: 'VISIBLE LEGISLATORS FUNDED', value: formatCount(connectedLegislatorIds.size) },
        { label: 'VISIBLE CONTRIBUTION EDGES', value: formatCount(contributionEdges.length) },
        { label: 'VISIBLE FIRMS HIRED', value: formatCount(hiredFirmCount) },
      ],
      connections: contributionEdges.map((edge) => {
        const legislator = getNodeById(graph, edge.target)
        return {
          name: legislator?.label || edge.target,
          detail: `${edge.amount_label || formatCurrency(edge.amount)} contributed`,
          money: true,
        }
      }),
    }
  }

  if (node.type === 'legislator') {
    const incomingContributions = allEdges
      .filter((e) => e.type === 'contribution' && e.target === node.id)
      .sort((a, b) => Number(b.amount || 0) - Number(a.amount || 0))
    const totalIncoming = incomingContributions.reduce((sum, edge) => sum + Number(edge.amount || 0), 0)
    const committees = allEdges
      .filter((e) => e.type === 'member_of' && e.source === node.id)
      .map((e) => getNodeById(graph, e.target))
      .filter(Boolean)
    return {
      stats: [
        { label: 'PARTY', value: node.party || 'N/A' },
        { label: 'STATE', value: node.state || 'N/A' },
        { label: 'COMMITTEES IN GRAPH', value: formatCount(committees.length) },
        { label: 'TOTAL INCOMING CONTRIBUTIONS', value: formatCurrency(totalIncoming), money: true },
      ],
      connections: [
        ...committees.map((committee) => ({
          name: committee.label,
          detail: 'Committee assignment',
        })),
        ...incomingContributions.map((edge) => {
          const sourceOrg = getNodeById(graph, edge.source)
          return {
            name: sourceOrg?.label || edge.source,
            detail: `${edge.amount_label || formatCurrency(edge.amount)} contributed`,
            money: true,
          }
        }),
      ],
    }
  }

  if (node.type === 'committee') {
    const members = allEdges
      .filter((e) => e.type === 'member_of' && e.target === node.id)
      .map((e) => getNodeById(graph, e.source))
      .filter(Boolean)
    return {
      stats: [
        { label: 'MEMBERS IN GRAPH', value: formatCount(members.length) },
        { label: 'CHAMBER', value: node.chamber || 'N/A' },
        { label: 'NODE TYPE', value: 'Committee' },
        { label: 'VISIBLE LINKS', value: formatCount(allEdges.filter((e) => e.target === node.id || e.source === node.id).length) },
      ],
      connections: members.map((member) => ({
        name: member.label,
        detail: `${member.party || 'N/A'} · ${member.state || 'N/A'}`,
      })),
    }
  }

  return {
    stats: [
      { label: 'NODE TYPE', value: resolveType(node) },
      { label: 'VISIBLE LINKS', value: formatCount(allEdges.filter((e) => e.target === node.id || e.source === node.id).length) },
    ],
    connections: [],
  }
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

export default function InfoPanel({ node, graph, onExpand, loadingNodeId }) {
  const [summary, setSummary] = useState(null)
  const [loading, setLoading] = useState(false)
  const resolved = useMemo(() => resolveEntity(node), [node])
  const typeLabel = resolveType(node)

  useEffect(() => {
    if (!resolved || (resolved.entityType !== 'organization' && resolved.entityType !== 'legislator')) {
      setSummary(null)
      return
    }

    setLoading(true)
    fetchEntitySummary(resolved.entityType, resolved.entityId)
      .then((data) => setSummary(data))
      .catch(() => setSummary(null))
      .finally(() => setLoading(false))
  }, [resolved])

  const { stats, connections } = useMemo(() => buildGraphDrivenPanel(node, graph, summary), [node, graph, summary])
  const sourceBadges = useMemo(() => buildSourceBadges(node, graph, summary), [node, graph, summary])
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
          {sourceBadges.length > 0 && (
            <div className="info-source-badges">
              {sourceBadges.map((badge) => (
                <a
                  key={badge.url}
                  href={badge.url}
                  target="_blank"
                  rel="noreferrer"
                  className="source-badge-link"
                >
                  {badge.label}
                </a>
              ))}
            </div>
          )}
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
          <button
            type="button"
            className="panel-network-btn"
            onClick={() => onExpand?.(node)}
            disabled={Boolean(loadingNodeId && loadingNodeId === node.id)}
          >
            [ EXPAND NETWORK ]
          </button>
          <div className="info-panel-rule-strong" />
        </div>
      )}
    </aside>
  )
}
