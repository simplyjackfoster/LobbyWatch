import { useCallback, useEffect, useRef, useState } from 'react'

import { fetchIssueCodes, fetchIssueGraph, fetchLegGraph, fetchOrgGraph, searchEntities } from './api'
import Discoveries from './components/Discoveries'
import ErrorBoundary from './components/ErrorBoundary'
import FilterBar from './components/FilterBar'
import GraphView from './components/GraphView'
import InfoPanel from './components/InfoPanel'
import SearchBar from './components/SearchBar'

const seedGraph = {
  nodes: [
    { id: 'org-1', label: 'Example Pharma Inc.', type: 'organization', subtype: 'client' },
    { id: 'leg-1', label: 'Sen. Alex Smith', type: 'legislator', party: 'D', state: 'PA' },
    { id: 'com-1', label: 'Senate Finance Committee', type: 'committee' },
  ],
  edges: [
    { source: 'org-1', target: 'leg-1', type: 'contribution', amount: 45000, cycle: 2024 },
    { source: 'leg-1', target: 'com-1', type: 'member_of' },
  ],
}

function mergeGraph(base, incoming) {
  const nodeMap = new Map(base.nodes.map((n) => [n.id, n]))
  incoming.nodes.forEach((n) => nodeMap.set(n.id, { ...nodeMap.get(n.id), ...n }))

  const edgeKey = (e) => `${e.source}|${e.target}|${e.type}`
  const edgeMap = new Map(base.edges.map((e) => [edgeKey(e), e]))
  incoming.edges.forEach((e) => edgeMap.set(edgeKey(e), { ...edgeMap.get(edgeKey(e)), ...e }))

  return { nodes: [...nodeMap.values()], edges: [...edgeMap.values()] }
}

function resolveNodeFetchId(node, results) {
  const [kind, rawId] = String(node.id).split('-')
  if (kind === 'org') return { kind, id: rawId }
  if (kind === 'issue') return { kind, id: node.label }
  if (kind === 'leg') {
    if (node.bioguide_id) return { kind, id: node.bioguide_id }
    const legSearch = results.find((r) => r.type === 'legislator' && r.name === node.label)
    if (legSearch) return { kind, id: legSearch.id }
  }
  return { kind, id: null }
}

export default function App() {
  const [results, setResults] = useState([])
  const [graph, setGraph] = useState(seedGraph)
  const [selectedNode, setSelectedNode] = useState(null)
  const [selectedEntity, setSelectedEntity] = useState(null)
  const [filters, setFilters] = useState({ year_min: 2023, year_max: 2025 })
  const [issueCodes, setIssueCodes] = useState([])
  const [graphLoading, setGraphLoading] = useState(false)
  const [loadingNodeId, setLoadingNodeId] = useState(null)
  const [tab, setTab] = useState('graph')
  const headerRef = useRef(null)
  const navRef = useRef(null)
  const filterRef = useRef(null)

  useEffect(() => {
    fetchIssueCodes()
      .then((data) => setIssueCodes(data.issue_codes || []))
      .catch(() => setIssueCodes([]))
  }, [])

  useEffect(() => {
    const root = document.documentElement

    const updateLayoutVars = () => {
      const headerHeight = headerRef.current?.offsetHeight || 0
      const navHeight = navRef.current?.offsetHeight || 0
      const filterHeight = filterRef.current?.offsetHeight || 0
      root.style.setProperty('--header-height', `${headerHeight}px`)
      root.style.setProperty('--nav-height', `${navHeight}px`)
      root.style.setProperty('--filter-height', `${filterHeight}px`)
    }

    updateLayoutVars()
    const resizeObserver = new ResizeObserver(updateLayoutVars)
    if (headerRef.current) resizeObserver.observe(headerRef.current)
    if (navRef.current) resizeObserver.observe(navRef.current)
    if (filterRef.current) resizeObserver.observe(filterRef.current)
    window.addEventListener('resize', updateLayoutVars)

    return () => {
      resizeObserver.disconnect()
      window.removeEventListener('resize', updateLayoutVars)
    }
  }, [tab])

  const onSearch = useCallback(async (q) => {
    const data = await searchEntities(q)
    setResults(data.results || [])
  }, [])

  const onSelectResult = useCallback(async (result) => {
    setSelectedEntity(result)
    setSelectedNode(null)
    setLoadingNodeId(null)
    setGraphLoading(true)
    let payload = null
    try {
      if (result.type === 'organization') {
        payload = await fetchOrgGraph(result.id, filters)
      } else if (result.type === 'legislator') {
        payload = await fetchLegGraph(result.id, filters)
      } else {
        payload = await fetchIssueGraph(result.name, filters)
      }
      setGraph(payload)
    } finally {
      setGraphLoading(false)
    }
  }, [filters])

  const onNodeClick = useCallback(async (node) => {
    setSelectedNode(node)
    setSelectedEntity(node)
    setLoadingNodeId(node.id)
    setGraphLoading(true)

    const { kind, id } = resolveNodeFetchId(node, results)
    let incoming = null
    try {
      if (kind === 'org' && id) incoming = await fetchOrgGraph(id, filters)
      if (kind === 'leg' && id) incoming = await fetchLegGraph(id, filters)
      if (kind === 'issue' && id) incoming = await fetchIssueGraph(id, filters)
      if (incoming) setGraph((prev) => mergeGraph(prev, incoming))
    } finally {
      setGraphLoading(false)
      setLoadingNodeId(null)
    }
  }, [filters, results])

  const onFilterApply = useCallback((nextFilters) => {
    setFilters(nextFilters)
  }, [])

  const onExpand = useCallback(async (node) => {
    if (!node) return
    const [kind, rawId] = String(node.id).split('-')
    const expanded = { ...filters, node_limit: 200 }
    let incoming = null
    setLoadingNodeId(node.id)
    setGraphLoading(true)
    try {
      if (kind === 'org') incoming = await fetchOrgGraph(rawId, expanded)
      if (kind === 'leg' && node.bioguide_id) incoming = await fetchLegGraph(node.bioguide_id, expanded)
      if (kind === 'issue') incoming = await fetchIssueGraph(node.label, expanded)
      if (incoming) setGraph((prev) => mergeGraph(prev, incoming))
    } finally {
      setGraphLoading(false)
      setLoadingNodeId(null)
    }
  }, [filters])

  const onDiscoveryViewGraph = useCallback(async (finding) => {
    setTab('graph')
    setSelectedNode(null)
    setLoadingNodeId(null)
    setGraphLoading(true)
    try {
      const searchTerm = finding?.searchName || finding?.legislator?.name
      if (!searchTerm) return
      const payload = await searchEntities(searchTerm)
      const matches = payload.results || []

      if (finding?.searchType === 'organization') {
        const org = matches.find((r) => r.type === 'organization')
        if (!org) return
        const graphPayload = await fetchOrgGraph(org.id, filters)
        setGraph(graphPayload)
        return
      }

      if (finding?.searchType === 'legislator') {
        const leg = matches.find((r) => r.type === 'legislator')
        if (!leg) return
        const graphPayload = await fetchLegGraph(leg.id, filters)
        setGraph(graphPayload)
        return
      }

      const leg = matches.find((r) => r.type === 'legislator')
      if (leg) {
        const graphPayload = await fetchLegGraph(leg.id, filters)
        setGraph(graphPayload)
        return
      }

      const org = matches.find((r) => r.type === 'organization')
      if (org) {
        const graphPayload = await fetchOrgGraph(org.id, filters)
        setGraph(graphPayload)
      }
    } finally {
      setGraphLoading(false)
    }
  }, [filters])

  return (
    <div className="app-shell">
      <header className="masthead header" ref={headerRef}>
        <div className="masthead-top-rule" aria-hidden="true" />
        <div className="masthead-row">
          <h1>LOBBY.WATCH</h1>
          <p className="masthead-tagline">Federal Influence Database</p>
        </div>
        <div className="masthead-meta-row">
          <span className="masthead-divider-line">── ── ── ── ── ──</span>
          <div className="masthead-live">
            <span className="live-dot" aria-hidden="true">●</span>
            <span>LIVE DATA</span>
          </div>
        </div>
        <p className="masthead-sources">Senate LDA · FEC · Congress.gov</p>
        <div className="masthead-bottom-rule" aria-hidden="true" />
      </header>

      <nav className="nav-tabs nav" ref={navRef} role="tablist" aria-label="Primary views">
        <button
          role="tab"
          aria-selected={tab === 'graph'}
          className={tab === 'graph' ? 'active' : ''}
          onClick={() => setTab('graph')}
        >
          Explore
        </button>
        <button
          role="tab"
          aria-selected={tab === 'discoveries'}
          className={tab === 'discoveries' ? 'active' : ''}
          onClick={() => setTab('discoveries')}
        >
          Discoveries
        </button>
      </nav>

      {tab === 'graph' && (
        <section className="explore-view">
          <SearchBar onSearch={onSearch} results={results} onSelect={onSelectResult} />
          <FilterBar ref={filterRef} onChange={onFilterApply} issueCodes={issueCodes} />
          <main className="graph-stage graph">
            <ErrorBoundary>
              <GraphView
                graph={graph}
                loading={graphLoading}
                selectedEntity={selectedEntity}
                loadingNodeId={loadingNodeId}
                onNodeClick={onNodeClick}
              />
            </ErrorBoundary>
            <InfoPanel node={selectedNode} onExpand={onExpand} filters={filters} />
          </main>
        </section>
      )}

      {tab === 'discoveries' && <Discoveries onViewGraph={onDiscoveryViewGraph} />}
    </div>
  )
}
