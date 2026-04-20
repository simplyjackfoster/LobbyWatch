import { useEffect, useMemo, useRef } from 'react'
import cytoscape from 'cytoscape'
import cola from 'cytoscape-cola'

cytoscape.use(cola)

function formatAmountLabel(amount) {
  const value = Number(amount || 0)
  if (value >= 1000000) return `$${(value / 1000000).toFixed(1)}M`
  if (value >= 1000) return `$${(value / 1000).toFixed(0)}k`
  return `$${value.toFixed(0)}`
}

function resolveRootNodeId(selectedEntity) {
  if (!selectedEntity) return null
  const selectedId = String(selectedEntity.id || '')
  if (selectedId.startsWith('org-') || selectedId.startsWith('leg-') || selectedId.startsWith('com-') || selectedId.startsWith('lob-')) {
    return selectedId
  }
  if (selectedEntity.type === 'organization') return `org-${selectedId}`
  if (selectedEntity.type === 'legislator') {
    if (selectedEntity.bioguide_id) return `leg-${selectedEntity.bioguide_id}`
    return `leg-${selectedId}`
  }
  if (selectedEntity.type === 'committee') return `com-${selectedId}`
  if (selectedEntity.type === 'lobbyist') return `lob-${selectedId}`
  return null
}

function buildElements(graph, rootNodeId, selectedEntity) {
  const degreeMap = new Map(graph.nodes.map((node) => [node.id, 0]))
  graph.edges.forEach((edge) => {
    degreeMap.set(edge.source, (degreeMap.get(edge.source) || 0) + 1)
    degreeMap.set(edge.target, (degreeMap.get(edge.target) || 0) + 1)
  })

  const nodes = graph.nodes.map((node) => {
    const selectedLegislatorId =
      selectedEntity?.type === 'legislator' ? String(selectedEntity.bioguide_id || selectedEntity.id || '') : ''
    const isRoot = node.id === rootNodeId || (selectedLegislatorId && selectedLegislatorId === String(node.bioguide_id || ''))
    const classes = isRoot ? 'root' : ''
    return {
      data: {
        id: node.id,
        label: node.label,
        type: node.type,
        subtype: node.subtype,
        color: node.color,
        party: node.party,
        state: node.state,
        chamber: node.chamber,
        bioguide_id: node.bioguide_id,
        covered_positions: node.covered_positions,
        has_covered_position: node.has_covered_position,
        has_conviction: node.has_conviction,
        conviction_disclosure: node.conviction_disclosure,
        degree: Math.max(degreeMap.get(node.id) || 1, 1),
      },
      classes,
    }
  })

  const edges = graph.edges.map((edge) => ({
    data: {
      id: `${edge.source}-${edge.target}`,
      source: edge.source,
      target: edge.target,
      type: edge.type,
      amount: Number(edge.amount || 0),
      amount_label: edge.amount_label || formatAmountLabel(edge.amount),
      filing_count: Number(edge.filing_count || 0),
      contribution_count: Number(edge.contribution_count || 0),
      issue_codes: edge.issue_codes || [],
      role: edge.role,
    },
  }))

  return [...nodes, ...edges]
}

const style = [
  {
    selector: 'node',
    style: {
      'background-color': '#6b7280',
      label: '',
      color: '#0a0a0a',
      'font-family': 'Source Serif 4, Georgia, serif',
      'font-size': '11px',
      'font-style': 'italic',
      'text-valign': 'bottom',
      'text-margin-y': '6px',
      'text-outline-width': '2px',
      'text-outline-color': '#ffffff',
      width: 'mapData(degree, 1, 20, 20, 60)',
      height: 'mapData(degree, 1, 20, 20, 60)',
      'border-width': '1.5px',
      'border-color': '#0a0a0a',
    },
  },
  {
    selector: 'node[degree > 3]',
    style: {
      label: 'data(label)',
    },
  },
  {
    selector: 'node[type = "organization"]',
    style: {
      'background-color': '#1a1a1a',
      'border-color': '#1a1a1a',
    },
  },
  {
    selector: 'node[type = "organization"][subtype = "firm"]',
    style: {
      'background-color': '#374151',
      'border-color': '#374151',
    },
  },
  {
    selector: 'node[type = "legislator"]',
    style: {
      'background-color': '#2563eb',
      'border-color': '#1d4ed8',
    },
  },
  {
    selector: 'node[type = "committee"]',
    style: {
      'background-color': '#92400e',
      'border-color': '#78350f',
    },
  },
  {
    selector: 'node[type = "lobbyist"]',
    style: {
      'background-color': '#6b7280',
      'border-color': '#4b5563',
    },
  },
  {
    selector: 'node:selected',
    style: {
      label: 'data(label)',
      'border-width': '3px',
      'border-color': '#c41a1a',
    },
  },
  {
    selector: 'node.root',
    style: {
      label: 'data(label)',
      'font-size': '13px',
      'font-weight': 'bold',
    },
  },
  {
    selector: 'node.dimmed',
    style: { opacity: 0.12 },
  },
  {
    selector: 'edge',
    style: {
      width: 1,
      'line-color': '#e2e2de',
      'target-arrow-color': '#e2e2de',
      'target-arrow-shape': 'triangle',
      'curve-style': 'bezier',
    },
  },
  {
    selector: 'edge[type = "contribution"]',
    style: {
      'line-color': '#c41a1a',
      'target-arrow-color': '#c41a1a',
      width: 2,
      label: 'data(amount_label)',
      'font-family': 'JetBrains Mono, monospace',
      'font-size': '9px',
      color: '#c41a1a',
      'text-rotation': 'autorotate',
      'text-background-color': '#ffffff',
      'text-background-opacity': 1,
      'text-background-padding': '2px',
    },
  },
  {
    selector: 'edge[type = "hired_firm"]',
    style: {
      'line-color': '#9ca3af',
      label: 'data(amount_label)',
      'font-family': 'JetBrains Mono, monospace',
      'font-size': '9px',
      color: '#6b7280',
      'text-rotation': 'autorotate',
    },
  },
  {
    selector: 'edge[type = "member_of"]',
    style: {
      'line-color': '#d1d5db',
      'line-style': 'dashed',
      'line-dash-pattern': [4, 3],
      'target-arrow-shape': 'none',
    },
  },
  {
    selector: 'edge.highlighted',
    style: {
      'line-color': '#0a0a0a',
      'target-arrow-color': '#0a0a0a',
      width: 2,
    },
  },
]

export default function GraphView({ graph, onNodeClick, loading, selectedEntity }) {
  const containerRef = useRef(null)
  const cyRef = useRef(null)
  const rootNodeId = useMemo(() => resolveRootNodeId(selectedEntity), [selectedEntity])
  const elements = useMemo(() => buildElements(graph, rootNodeId, selectedEntity), [graph, rootNodeId, selectedEntity])

  useEffect(() => {
    if (!containerRef.current) return

    if (cyRef.current) {
      cyRef.current.destroy()
      cyRef.current = null
    }

    const cy = cytoscape({
      container: containerRef.current,
      elements,
      style,
    })

    const layout = cy.layout({
      name: 'cola',
      animate: true,
      animationDuration: 500,
      padding: 60,
      nodeSpacing: 80,
      edgeLength: 150,
      fit: true,
      randomize: false,
      avoidOverlap: true,
      handleDisconnected: true,
      convergenceThreshold: 0.01,
      maxSimulationTime: 3000,
    })
    layout.on('layoutstop', () => {
      cy.nodes().forEach((node) => {
        node.data('degree', node.degree())
      })
    })
    layout.run()

    const clearClasses = () => {
      cy.elements().removeClass('dimmed highlighted')
    }

    const applyHover = (node) => {
      const neighborhood = node.neighborhood()
      const connected = neighborhood.union(node)
      cy.elements().removeClass('dimmed highlighted')
      cy.elements().difference(connected).addClass('dimmed')
      node.connectedEdges().addClass('highlighted')
    }

    cy.on('tap', 'node', (evt) => {
      onNodeClick?.(evt.target.data())
    })

    cy.on('mouseover', 'node', (evt) => {
      const node = evt.target
      applyHover(node)
      const tooltip = document.getElementById('graph-tooltip')
      if (!tooltip) return
      tooltip.style.display = 'block'
      tooltip.style.left = `${evt.renderedPosition.x}px`
      tooltip.style.top = `${evt.renderedPosition.y - 40}px`
      tooltip.textContent = String(node.data('label') || '')
    })

    cy.on('mouseout', 'node', () => {
      clearClasses()
      const tooltip = document.getElementById('graph-tooltip')
      if (tooltip) tooltip.style.display = 'none'
    })

    cyRef.current = cy
    return () => {
      cy.destroy()
      cyRef.current = null
    }
  }, [elements, onNodeClick])

  return (
    <div className="graph-wrap">
      <div ref={containerRef} className="graph-canvas" />
      <div id="graph-tooltip" className="graph-tooltip" />

      <div className="graph-legend" aria-label="Graph legend">
        <p>■ Organization  ● Legislator  ◆ Committee</p>
        <p>─── Contribution  ─── Hired Firm  ╌╌╌ Committee Membership</p>
      </div>

      {loading && (
        <div className="graph-overlay" aria-live="polite" aria-busy="true">
          <div className="loader-line line-one" />
          <div className="loader-line line-two" />
          <div className="loader-line line-three" />
        </div>
      )}

      {!loading && graph.nodes.length === 0 && selectedEntity && (
        <div className="graph-empty">
          <h3>No network found</h3>
          <p>No lobbying activity or contributions are on record for this entity in the selected date range.</p>
        </div>
      )}
    </div>
  )
}
