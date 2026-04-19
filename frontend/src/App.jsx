import { useCallback, useEffect, useState } from 'react'

import { fetchIssueCodes, fetchIssueGraph, fetchLegGraph, fetchOrgGraph, searchEntities } from './api'
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

export default function App() {
  const [results, setResults] = useState([])
  const [graph, setGraph] = useState(seedGraph)
  const [selectedNode, setSelectedNode] = useState(null)
  const [selectedEntity, setSelectedEntity] = useState(null)
  const [filters, setFilters] = useState({ year_min: 2019, year_max: 2025 })
  const [issueCodes, setIssueCodes] = useState([])
  const [graphLoading, setGraphLoading] = useState(false)

  useEffect(() => {
    fetchIssueCodes()
      .then((data) => setIssueCodes(data.issue_codes || []))
      .catch(() => setIssueCodes([]))
  }, [])

  const onSearch = useCallback(async (q) => {
    const data = await searchEntities(q)
    setResults(data.results || [])
  }, [])

  const onSelectResult = useCallback(async (result) => {
    setSelectedEntity(result)
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
    setGraphLoading(true)
    const [kind, rawId] = String(node.id).split('-')
    let incoming = null
    try {
      if (kind === 'org') incoming = await fetchOrgGraph(rawId, filters)
      if (kind === 'leg') {
        if (node.bioguide_id) {
          incoming = await fetchLegGraph(node.bioguide_id, filters)
        } else {
          const legSearch = results.find((r) => r.type === 'legislator' && r.name === node.label)
          if (legSearch) incoming = await fetchLegGraph(legSearch.id, filters)
        }
      }
      if (kind === 'issue') incoming = await fetchIssueGraph(node.label, filters)
      if (incoming) setGraph((prev) => mergeGraph(prev, incoming))
    } finally {
      setGraphLoading(false)
    }
  }, [filters, results])

  const onNodeHover = useCallback((node) => setSelectedNode(node), [])

  const onFilterApply = useCallback((nextFilters) => {
    setFilters(nextFilters)
  }, [])

  const onExpand = useCallback(async (node) => {
    const [kind, rawId] = String(node.id).split('-')
    const expanded = { ...filters, node_limit: 200 }
    let incoming = null
    setGraphLoading(true)
    try {
      if (kind === 'org') incoming = await fetchOrgGraph(rawId, expanded)
      if (kind === 'leg' && node.bioguide_id) incoming = await fetchLegGraph(node.bioguide_id, expanded)
      if (kind === 'issue') incoming = await fetchIssueGraph(node.label, expanded)
      if (incoming) setGraph((prev) => mergeGraph(prev, incoming))
    } finally {
      setGraphLoading(false)
    }
  }, [filters])

  return (
    <div className="app">
      <header>
        <h1>LobbyWatch</h1>
      </header>
      <SearchBar onSearch={onSearch} results={results} onSelect={onSelectResult} />
      <FilterBar onChange={onFilterApply} issueCodes={issueCodes} />
      <main className="content-grid">
        <ErrorBoundary>
          <GraphView
            graph={graph}
            loading={graphLoading}
            selectedEntity={selectedEntity}
            onNodeClick={onNodeClick}
            onNodeHover={onNodeHover}
          />
        </ErrorBoundary>
        <InfoPanel node={selectedNode} onExpand={onExpand} />
      </main>
    </div>
  )
}
