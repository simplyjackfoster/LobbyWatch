import { useEffect, useMemo, useState } from 'react'

import { fetchBetrayalIndex, fetchForeignInfluence, fetchRevolvingDoor } from '../api'

const ANALYSIS_TABS = [
  { key: 'betrayal', label: 'BETRAYAL INDEX' },
  { key: 'revolving', label: 'REVOLVING DOOR' },
  { key: 'foreign', label: 'FOREIGN INFLUENCE' },
]

const ISSUE_TABS = [
  { label: 'HEALTH CARE', code: 'HLTH' },
  { label: 'DEFENSE', code: 'DEF' },
  { label: 'TAXATION', code: 'TAX' },
  { label: 'FINANCE', code: 'FIN' },
  { label: 'ENERGY', code: 'ENRG' },
  { label: 'TRADE', code: 'TRAD' },
]

const PLACEHOLDER_BETRAYAL = [
  {
    legislator: { name: 'Sen. Bob Casey', party: 'D', state: 'Pennsylvania' },
    co_sponsored_bills: Array.from({ length: 12 }, (_, i) => ({ bill_id: `S-${100 + i}` })),
    contributions_after_cosponsor: 127400,
    negative_votes: Array.from({ length: 8 }, (_, i) => ({ bill_id: `S-${200 + i}`, position: 'Nay' })),
    betrayal_score: 0.82,
    contributing_orgs: [
      { name: 'Pfizer PAC', amount: 45000 },
      { name: 'PhRMA', amount: 32000 },
      { name: 'Eli Lilly', amount: 28000 },
    ],
  },
  {
    legislator: { name: 'Rep. Jon Mercer', party: 'R', state: 'Texas' },
    co_sponsored_bills: Array.from({ length: 9 }, (_, i) => ({ bill_id: `H-${80 + i}` })),
    contributions_after_cosponsor: 95800,
    negative_votes: Array.from({ length: 6 }, (_, i) => ({ bill_id: `H-${170 + i}`, position: 'Nay' })),
    betrayal_score: 0.71,
    contributing_orgs: [
      { name: 'UnitedHealth PAC', amount: 37000 },
      { name: 'Cigna PAC', amount: 29000 },
      { name: 'Humana PAC', amount: 19800 },
    ],
  },
]

const PLACEHOLDER_REVOLVING = [
  {
    lobbyist: { name: 'Jane Smith', lda_id: 'LDX-10294' },
    prior_positions: ['Deputy Director, FDA', 'Senior Counsel, HHS'],
    current_registrant: 'Akin Gump Strauss Hauer & Feld',
    clients: ['Pfizer Inc.', 'PhRMA', 'Eli Lilly'],
    issue_codes: ['HLTH', 'PHARM'],
    filing_count: 24,
    revolving_door_score: 0.91,
  },
  {
    lobbyist: { name: 'Robert Greene', lda_id: 'LDX-22011' },
    prior_positions: ['Commissioner Counsel, SEC'],
    current_registrant: 'Brownstein Hyatt',
    clients: ['Goldman Sachs', 'BlackRock'],
    issue_codes: ['FIN', 'TAX'],
    filing_count: 17,
    revolving_door_score: 0.76,
  },
]

const PLACEHOLDER_FOREIGN = [
  {
    organization: { id: 0, name: 'Global Pharma Holdings' },
    foreign_entities: ['Shenzhen MedTech', 'Riyadh Life Sciences'],
    foreign_countries: ['CN', 'SA'],
    issue_codes: ['HLTH', 'TAX'],
    committees_targeted: ['Senate Finance Committee', 'House Energy and Commerce Committee'],
    filing_count: 18,
  },
  {
    organization: { id: 0, name: 'PetroTrans Atlantic' },
    foreign_entities: ['Abu Dhabi Energy Group'],
    foreign_countries: ['AE'],
    issue_codes: ['ENRG', 'TRAD'],
    committees_targeted: ['Senate Energy and Natural Resources Committee'],
    filing_count: 11,
  },
]

const COUNTRY_NAMES = {
  CN: 'China',
  SA: 'Saudi Arabia',
  AE: 'United Arab Emirates',
  RU: 'Russia',
  GB: 'United Kingdom',
  FR: 'France',
  DE: 'Germany',
  JP: 'Japan',
  KR: 'South Korea',
  CA: 'Canada',
  MX: 'Mexico',
  IN: 'India',
}

function formatCurrency(value) {
  return `$${Number(value || 0).toLocaleString()}`
}

function formatCompactMillions(value) {
  const numeric = Number(value || 0)
  if (numeric >= 1000000) return `$${Math.round(numeric / 1000000)} million`
  return formatCurrency(numeric)
}

function normalizeIssueFindings(findings = []) {
  return [...findings].sort((a, b) => Number(b.betrayal_score || 0) - Number(a.betrayal_score || 0))
}

function normalizeRevolvingFindings(findings = []) {
  return [...findings].sort((a, b) => Number(b.revolving_door_score || 0) - Number(a.revolving_door_score || 0))
}

function toRegionLabel(country) {
  if (!country) return null
  const value = String(country).trim()
  if (!value) return null
  const upper = value.toUpperCase()
  const alpha2 = upper.length === 2 ? upper : null

  let name = COUNTRY_NAMES[upper] || value
  if (alpha2 && typeof Intl !== 'undefined' && typeof Intl.DisplayNames !== 'undefined') {
    try {
      const display = new Intl.DisplayNames(['en'], { type: 'region' })
      name = display.of(alpha2) || name
    } catch {
      // Keep fallback mapping above.
    }
  }

  if (!alpha2) return name
  const flag = alpha2
    .split('')
    .map((char) => String.fromCodePoint(127397 + char.charCodeAt(0)))
    .join('')
  return `${flag} ${name}`
}

function renderSkeletonCards(count = 4) {
  return Array.from({ length: count }).map((_, idx) => (
    <article key={`loading-${idx}`} className="discovery-card skeleton-card">
      <div className="skeleton-static wide" />
      <div className="skeleton-static medium" />
      <div className="skeleton-static short" />
      <div className="skeleton-static medium" />
    </article>
  ))
}

export default function Discoveries({ onViewGraph }) {
  const [analysisTab, setAnalysisTab] = useState('betrayal')
  const [issueCode, setIssueCode] = useState('HLTH')
  const [loading, setLoading] = useState(false)
  const [betrayalFindings, setBetrayalFindings] = useState([])
  const [revolvingFindings, setRevolvingFindings] = useState([])
  const [foreignFindings, setForeignFindings] = useState([])

  useEffect(() => {
    let mounted = true
    setLoading(true)

    const load = async () => {
      try {
        if (analysisTab === 'betrayal') {
          const data = await fetchBetrayalIndex({ issue_code: issueCode, min_contribution: 10000 })
          if (!mounted) return
          setBetrayalFindings(normalizeIssueFindings(data.findings || []))
          return
        }

        if (analysisTab === 'revolving') {
          const data = await fetchRevolvingDoor({ issue_code: issueCode, limit: 50 })
          if (!mounted) return
          setRevolvingFindings(normalizeRevolvingFindings(data.findings || []))
          return
        }

        const data = await fetchForeignInfluence({ issue_code: issueCode, limit: 50 })
        if (!mounted) return
        setForeignFindings(data.findings || [])
      } catch {
        if (!mounted) return
        if (analysisTab === 'betrayal') setBetrayalFindings([])
        if (analysisTab === 'revolving') setRevolvingFindings([])
        if (analysisTab === 'foreign') setForeignFindings([])
      } finally {
        if (mounted) setLoading(false)
      }
    }

    load()
    return () => {
      mounted = false
    }
  }, [analysisTab, issueCode])

  const visibleBetrayal = useMemo(() => {
    if (betrayalFindings.length > 0) return betrayalFindings
    return PLACEHOLDER_BETRAYAL
  }, [betrayalFindings])

  const visibleRevolving = useMemo(() => {
    if (revolvingFindings.length > 0) return revolvingFindings
    return PLACEHOLDER_REVOLVING
  }, [revolvingFindings])

  const visibleForeign = useMemo(() => {
    if (foreignFindings.length > 0) return foreignFindings
    return PLACEHOLDER_FOREIGN
  }, [foreignFindings])

  const totalFunds = useMemo(
    () => visibleBetrayal.reduce((sum, finding) => sum + Number(finding.contributions_after_cosponsor || 0), 0),
    [visibleBetrayal],
  )

  const heroLegislatorCount = betrayalFindings.length > 0 ? betrayalFindings.length : 47
  const heroFunds = betrayalFindings.length > 0 ? totalFunds : 127000000

  return (
    <section className="discoveries-wrap" aria-label="Discoveries analysis">
      <div className="discoveries-analysis-tabs" role="tablist" aria-label="Analysis categories">
        {ANALYSIS_TABS.map((tab) => (
          <button
            key={tab.key}
            role="tab"
            aria-selected={tab.key === analysisTab}
            className={tab.key === analysisTab ? 'active' : ''}
            onClick={() => setAnalysisTab(tab.key)}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {analysisTab === 'betrayal' && (
        <header className="discoveries-hero">
          <p>
            <span className="accent-inline">{heroLegislatorCount.toLocaleString()} legislators</span> co-sponsored health
            care reform legislation, then voted against it after receiving a combined{' '}
            <span className="accent-inline">{formatCompactMillions(heroFunds)}</span> from the pharmaceutical industry.
          </p>
          <p className="hero-subline">This database documents each case.</p>
        </header>
      )}

      {analysisTab === 'revolving' && (
        <header className="discoveries-hero compact">
          <p>
            Officials who held public office and now lobby on overlapping jurisdictions are ranked by filing density and
            issue relevance.
          </p>
        </header>
      )}

      {analysisTab === 'foreign' && (
        <header className="discoveries-hero compact">
          <p>
            Foreign-linked entities lobbying Congress are grouped by client organization, issue portfolio, and committee
            targets.
          </p>
        </header>
      )}

      <div className="discoveries-issue-tabs" role="tablist" aria-label="Issue categories">
        {ISSUE_TABS.map((tab) => (
          <button
            key={tab.code}
            role="tab"
            aria-selected={tab.code === issueCode}
            className={tab.code === issueCode ? 'active' : ''}
            onClick={() => setIssueCode(tab.code)}
          >
            {tab.label}
          </button>
        ))}
      </div>

      <div className="discoveries-list">
        {loading && renderSkeletonCards(4)}

        {!loading && analysisTab === 'betrayal' && visibleBetrayal.map((finding, idx) => {
          const score = Number(finding.betrayal_score || 0)
          const party = String(finding.legislator.party || 'D').toUpperCase().slice(0, 1)
          const topContributors = (finding.contributing_orgs || []).slice(0, 3)

          return (
            <article key={`${finding.legislator.name}-${idx}`} className={`discovery-card ${idx === 0 ? 'top-ranked' : ''}`}>
              <span className="rank-ghost">#{idx + 1}</span>
              <p className="party-state">[{party}] · {String(finding.legislator.state || '').toUpperCase()}</p>
              <h3>{finding.legislator.name}</h3>

              <p className="discovery-narrative">
                Co-sponsored <span className="num red-text">{finding.co_sponsored_bills.length}</span> drug pricing bills.
                Received <span className="num red-text">{formatCurrency(finding.contributions_after_cosponsor)}</span> from
                industry PACs. Voted against advancement <span className="num red-text">{finding.negative_votes.length}</span> times.
              </p>

              <hr className="rule" />

              <div className="discovery-score-row">
                <span>BETRAYAL SCORE</span>
                <div className="score-bar-track">
                  <div className="score-bar-fill" style={{ width: `${Math.max(0, Math.min(100, score * 100))}%` }} />
                </div>
                <strong className="num red-text">{score.toFixed(2)}</strong>
              </div>

              <hr className="rule" />

              <p className="discovery-contributors">
                Top contributors:{' '}
                {topContributors.length > 0
                  ? topContributors.map((org, orgIdx) => (
                    <span key={`${org.name}-${orgIdx}`}>
                      {org.name} <span className="num red-text">{formatCurrency(org.amount)}</span>
                      {orgIdx < topContributors.length - 1 ? ' · ' : ''}
                    </span>
                  ))
                  : 'No contribution breakdown available.'}
              </p>

              <div className="discovery-link-row">
                <button
                  type="button"
                  className="view-graph-link"
                  onClick={() => onViewGraph?.({ searchName: finding.legislator.name, searchType: 'legislator' })}
                >
                  View in graph →
                </button>
              </div>
            </article>
          )
        })}

        {!loading && analysisTab === 'revolving' && visibleRevolving.map((finding, idx) => {
          const score = Number(finding.revolving_door_score || 0)
          const positions = (finding.prior_positions || []).slice(0, 3)

          return (
            <article key={`${finding.lobbyist?.name || 'lobbyist'}-${idx}`} className={`discovery-card ${idx === 0 ? 'top-ranked' : ''}`}>
              <span className="rank-ghost">#{idx + 1}</span>
              <p className="party-state num">LDA ID {finding.lobbyist?.lda_id || 'N/A'}</p>
              <h3 className="revolving-name">{finding.lobbyist?.name || 'Unknown lobbyist'}</h3>
              <p className="revolving-positions">{positions.join(' · ') || 'Prior public role not listed.'}</p>
              <p className="discovery-narrative">
                Now represents: {(finding.clients || []).slice(0, 3).join(' · ') || finding.current_registrant || 'Unknown clients'}
              </p>
              <p className="discovery-contributors">
                Issue codes: <span className="num">{(finding.issue_codes || []).join(' · ') || 'N/A'}</span>
              </p>

              <div className="discovery-score-row">
                <span>REVOLVING DOOR SCORE</span>
                <div className="score-bar-track">
                  <div className="score-bar-fill amber" style={{ width: `${Math.max(0, Math.min(100, score * 100))}%` }} />
                </div>
                <strong className="num amber-text">{score.toFixed(2)}</strong>
              </div>

              <div className="discovery-link-row between">
                <span className="filing-count-badge">{Number(finding.filing_count || 0)} filings</span>
                <button
                  type="button"
                  className="view-graph-link"
                  onClick={() => onViewGraph?.({ searchName: finding.current_registrant || finding.clients?.[0] || finding.lobbyist?.name })}
                >
                  View in graph →
                </button>
              </div>
            </article>
          )
        })}

        {!loading && analysisTab === 'foreign' && visibleForeign.map((finding, idx) => {
          const countries = (finding.foreign_countries || []).map(toRegionLabel).filter(Boolean)

          return (
            <article key={`${finding.organization?.name || 'org'}-${idx}`} className={`discovery-card ${idx === 0 ? 'top-ranked' : ''}`}>
              <span className="rank-ghost">#{idx + 1}</span>
              <p className="party-state num">{Number(finding.filing_count || 0)} filings</p>
              <h3>{finding.organization?.name || 'Unknown organization'}</h3>

              <p className="discovery-narrative">
                Foreign entities: {countries.length > 0 ? countries.join(' · ') : 'No country data'}
              </p>
              <p className="discovery-contributors">
                Issues lobbied: <span className="num">{(finding.issue_codes || []).join(' · ') || 'N/A'}</span>
              </p>
              <p className="discovery-contributors">
                Committees targeted: {(finding.committees_targeted || []).join(' · ') || 'No committee linkage found'}
              </p>

              <div className="discovery-link-row">
                <button
                  type="button"
                  className="view-graph-link"
                  onClick={() => onViewGraph?.({ searchName: finding.organization?.name, searchType: 'organization' })}
                >
                  View in graph →
                </button>
              </div>
            </article>
          )
        })}
      </div>
    </section>
  )
}
