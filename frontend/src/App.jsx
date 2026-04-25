import { useCallback, useEffect, useMemo, useRef, useState } from 'react'

import { fetchDataStatus, fetchIssueCodes, fetchIssueGraph, fetchLegGraph, fetchOrgGraph, searchEntities } from './api'
import Discoveries from './components/Discoveries'
import ErrorBoundary from './components/ErrorBoundary'
import FilterBar from './components/FilterBar'
import GraphView from './components/GraphView'
import InfoPanel from './components/InfoPanel'
import SearchBar from './components/SearchBar'
import AboutData from './pages/AboutData'
import Developers from './pages/Developers'
import MyReps from './pages/MyReps'

const seedGraph = { nodes: [], edges: [] }

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

function pickRootNode(result, payload) {
  const nodes = payload?.nodes || []
  if (!result || !nodes.length) return null

  if (result.type === 'organization') {
    return nodes.find((n) => n.id === `org-${result.id}`) || nodes.find((n) => n.type === 'organization') || null
  }

  if (result.type === 'legislator') {
    return (
      nodes.find((n) => n.type === 'legislator' && n.bioguide_id === result.id) ||
      nodes.find((n) => n.id === `leg-${result.id}`) ||
      nodes.find((n) => n.type === 'legislator') ||
      null
    )
  }

  if (result.type === 'issue') {
    return nodes.find((n) => n.type === 'issue') || null
  }

  return null
}

function readRoute() {
  return { pathname: window.location.pathname, search: window.location.search }
}

function getActiveView(pathname) {
  if (pathname.startsWith('/explore')) return 'graph'
  if (pathname.startsWith('/discoveries')) return 'discoveries'
  if (pathname.startsWith('/developers')) return 'developers'
  if (pathname.startsWith('/about-data')) return 'about-data'
  if (pathname.startsWith('/my-reps')) return 'my-reps'
  if (pathname.startsWith('/rep/')) return 'my-reps'
  return 'my-reps'
}

function formatUpdatedAgo(timestamp) {
  if (!timestamp) return null
  const parsed = new Date(String(timestamp).replace('Z', ''))
  if (Number.isNaN(parsed.getTime())) return null
  const now = new Date()
  const diffDays = Math.max(0, Math.floor((now.getTime() - parsed.getTime()) / (24 * 60 * 60 * 1000)))
  if (diffDays === 0) return 'Updated today'
  if (diffDays === 1) return 'Updated 1 day ago'
  return `Updated ${diffDays} days ago`
}

function getSharedBioguide(pathname) {
  const match = pathname.match(/^\/rep\/([^/]+)$/)
  if (!match) return null
  return decodeURIComponent(match[1])
}

export default function App() {
  const [route, setRoute] = useState(() => readRoute())
  const [results, setResults] = useState([])
  const [graph, setGraph] = useState(seedGraph)
  const [selectedNode, setSelectedNode] = useState(null)
  const [selectedEntity, setSelectedEntity] = useState(null)
  const [filters, setFilters] = useState({ year_min: 2023, year_max: 2025 })
  const [issueCodes, setIssueCodes] = useState([])
  const [graphLoading, setGraphLoading] = useState(false)
  const [loadingNodeId, setLoadingNodeId] = useState(null)
  const [dataStatus, setDataStatus] = useState(null)
  const [dataStatusLoading, setDataStatusLoading] = useState(true)
  const [dataStatusError, setDataStatusError] = useState(false)
  const headerRef = useRef(null)
  const navRef = useRef(null)
  const filterRef = useRef(null)
  const lastRouteLegislatorRef = useRef(null)
  const latestSearchSeqRef = useRef(0)

  const activeView = useMemo(() => getActiveView(route.pathname), [route.pathname])
  const sharedBioguideId = useMemo(() => getSharedBioguide(route.pathname), [route.pathname])

  const navigate = useCallback((nextPath, { replace = false } = {}) => {
    const current = `${window.location.pathname}${window.location.search}`
    if (current === nextPath) return
    if (replace) {
      window.history.replaceState({}, '', nextPath)
    } else {
      window.history.pushState({}, '', nextPath)
    }
    setRoute(readRoute())
  }, [])

  useEffect(() => {
    if (window.location.pathname === '/') {
      navigate('/my-reps', { replace: true })
    }
  }, [navigate])

  useEffect(() => {
    const onPop = () => setRoute(readRoute())
    window.addEventListener('popstate', onPop)
    return () => window.removeEventListener('popstate', onPop)
  }, [])

  useEffect(() => {
    fetchIssueCodes()
      .then((data) => setIssueCodes(data.issue_codes || []))
      .catch(() => setIssueCodes([]))
  }, [])

  useEffect(() => {
    let active = true
    fetchDataStatus()
      .then((data) => {
        if (!active) return
        setDataStatus(data || null)
        setDataStatusError(false)
      })
      .catch(() => {
        if (!active) return
        setDataStatus(null)
        setDataStatusError(true)
      })
      .finally(() => {
        if (active) setDataStatusLoading(false)
      })
    return () => {
      active = false
    }
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
  }, [activeView])

  useEffect(() => {
    if (activeView !== 'graph' || route.pathname !== '/explore') return
    const params = new URLSearchParams(route.search)
    const legislator = params.get('legislator')
    if (!legislator) {
      lastRouteLegislatorRef.current = null
      return
    }
    if (lastRouteLegislatorRef.current === legislator) return
    lastRouteLegislatorRef.current = legislator

    setSelectedNode(null)
    setSelectedEntity(null)
    setGraphLoading(true)
    fetchLegGraph(legislator, filters)
      .then((payload) => setGraph(payload))
      .finally(() => setGraphLoading(false))
  }, [activeView, route.pathname, route.search, filters])

  const onSearch = useCallback(async (q) => {
    const searchSeq = latestSearchSeqRef.current + 1
    latestSearchSeqRef.current = searchSeq
    const data = await searchEntities(q)
    if (latestSearchSeqRef.current !== searchSeq) return []
    const nextResults = data.results || []
    setResults(nextResults)
    if (!nextResults.length) return nextResults

    const normalizedQ = (q || '').trim().toUpperCase()
    const top = nextResults.find((r) => (r.name || '').toUpperCase().includes(normalizedQ)) || nextResults[0]
    const organizationResults = nextResults.filter((r) => r.type === 'organization')
    const hasExactOrganization = organizationResults.some((r) => (r.name || '').toUpperCase() === normalizedQ)
    const isBroadOrganizationSearch = organizationResults.length >= 5 && !hasExactOrganization
    if (activeView !== 'graph' || route.pathname !== '/explore') {
      navigate('/explore')
    }

    setSelectedEntity(top)
    setSelectedNode(null)
    setLoadingNodeId(null)
    setGraphLoading(true)
    try {
      let payload = null
      if (isBroadOrganizationSearch) {
        const payloads = await Promise.all(
          organizationResults.slice(0, 6).map((orgResult) => fetchOrgGraph(orgResult.id, filters))
        )
        payload = payloads.reduce((acc, next) => mergeGraph(acc, next), seedGraph)
        setSelectedEntity({ id: `issue-${q}`, type: 'issue', name: q })
      } else if (top.type === 'organization') {
        payload = await fetchOrgGraph(top.id, filters)
      } else if (top.type === 'legislator') {
        payload = await fetchLegGraph(top.id, filters)
      } else {
        payload = await fetchIssueGraph(top.name, filters)
      }
      if (payload) {
        setGraph(payload)
        setSelectedNode(pickRootNode(top, payload))
      }
    } finally {
      setGraphLoading(false)
    }

    return nextResults
  }, [activeView, route.pathname, navigate, filters])

  const onSelectResult = useCallback(async (result) => {
    if (activeView !== 'graph' || route.pathname !== '/explore') {
      navigate('/explore')
    }
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
      setSelectedNode(pickRootNode(result, payload))
    } finally {
      setGraphLoading(false)
    }
  }, [activeView, route.pathname, navigate, filters])

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
    navigate('/explore')
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
  }, [navigate, filters])

  const onMyRepsViewNetwork = useCallback((rep) => {
    if (!rep?.bioguide_id) return
    navigate(`/explore?legislator=${encodeURIComponent(rep.bioguide_id)}`)
  }, [navigate])

  const updatedLabel = useMemo(() => formatUpdatedAgo(dataStatus?.last_exported_at), [dataStatus])

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
            {(dataStatusLoading || dataStatusError || !updatedLabel) ? (
              <>
                <span className="live-dot" aria-hidden="true">●</span>
                <span>LIVE DATA</span>
              </>
            ) : (
              <span>{updatedLabel}</span>
            )}
          </div>
        </div>
        <button type="button" className="masthead-sources-link" onClick={() => navigate('/about-data')}>
          Senate LDA · FEC · Congress.gov
        </button>
        <div className="masthead-utility-links">
          <button
            type="button"
            className={`masthead-utility-link ${activeView === 'about-data' ? 'active' : ''}`}
            onClick={() => navigate('/about-data')}
          >
            About Data
          </button>
          <button
            type="button"
            className={`masthead-utility-link ${activeView === 'developers' ? 'active' : ''}`}
            onClick={() => navigate('/developers')}
          >
            Developers / CLI Docs
          </button>
        </div>
        <div className="masthead-bottom-rule" aria-hidden="true" />
      </header>

      <nav className="nav-tabs nav" ref={navRef} role="tablist" aria-label="Primary views">
        <button
          role="tab"
          aria-selected={activeView === 'my-reps'}
          className={activeView === 'my-reps' ? 'active' : ''}
          onClick={() => navigate('/my-reps')}
        >
          MY REPS
        </button>
        <button
          role="tab"
          aria-selected={activeView === 'graph'}
          className={activeView === 'graph' ? 'active' : ''}
          onClick={() => navigate('/explore')}
        >
          EXPLORE
        </button>
        <button
          role="tab"
          aria-selected={activeView === 'discoveries'}
          className={activeView === 'discoveries' ? 'active' : ''}
          onClick={() => navigate('/discoveries')}
        >
          DISCOVERIES
        </button>
      </nav>

      {activeView === 'my-reps' && (
        <MyReps sharedBioguideId={sharedBioguideId} onViewNetwork={onMyRepsViewNetwork} />
      )}

      {activeView === 'graph' && (
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
            <InfoPanel node={selectedNode} graph={graph} onExpand={onExpand} loadingNodeId={loadingNodeId} />
          </main>
        </section>
      )}

      {activeView === 'discoveries' && <Discoveries onViewGraph={onDiscoveryViewGraph} />}
      {activeView === 'developers' && <Developers />}
      {activeView === 'about-data' && <AboutData />}
    </div>
  )
}
