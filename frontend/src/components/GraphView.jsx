import { useEffect, useMemo, useRef } from 'react'
import cytoscape from 'cytoscape'
import cola from 'cytoscape-cola'

cytoscape.use(cola)

const colorByType = {
  organization: '#1a1a1a',
  legislator: '#2563eb',
  committee: '#92400e',
  lobbyist: '#6b7280',
  issue: '#1a1a1a',
  vote: '#6b7280',
  registration: '#9a9a9a',
}

function formatAmountLabel(amount) {
  const value = Number(amount || 0)
  if (value >= 1000000) return `$${(value / 1000000).toFixed(1)}M`
  if (value >= 1000) return `$${(value / 1000).toFixed(0)}k`
  return `$${value.toFixed(0)}`
}

function buildElements(graph) {
  const degreeMap = new Map(graph.nodes.map((node) => [node.id, 0]))
  graph.edges.forEach((edge) => {
    degreeMap.set(edge.source, (degreeMap.get(edge.source) || 0) + 1)
    degreeMap.set(edge.target, (degreeMap.get(edge.target) || 0) + 1)
  })

  const nodes = graph.nodes.map((node) => ({
    data: {
      ...node,
      degree: Math.max(degreeMap.get(node.id) || 1, 1),
      color: node.color || colorByType[node.type] || '#1a1a1a',
    },
  }))

  const edges = graph.edges.map((edge, idx) => ({
    data: {
      id: `${edge.source}-${edge.target}-${edge.type}-${idx}`,
      ...edge,
      amount: Number(edge.amount || 0),
      amount_label: edge.amount_label || formatAmountLabel(edge.amount),
    },
  }))

  return [...nodes, ...edges]
}

const cyStyle = [
  {
    selector: 'node',
    style: {
      'background-color': 'data(color)',
      label: 'data(label)',
      color: '#0a0a0a',
      'font-family': 'Source Serif 4, Georgia, serif',
      'font-size': '11px',
      'font-style': 'italic',
      'text-wrap': 'wrap',
      'text-max-width': 160,
      'text-valign': 'bottom',
      'text-margin-y': '6px',
      'text-outline-width': '2px',
      'text-outline-color': '#ffffff',
      width: 'mapData(degree, 1, 20, 16, 48)',
      height: 'mapData(degree, 1, 20, 16, 48)',
      'border-width': '1.5px',
      'border-color': '#0a0a0a',
    },
  },
  {
    selector: 'node:hover',
    style: {
      'border-width': '2.5px',
      'border-color': 'data(color)',
    },
  },
  {
    selector: 'node[has_covered_position = true]',
    style: {
      'border-width': '2px',
      'border-color': '#c41a1a',
      'border-style': 'solid',
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
    selector: 'edge[type="contribution"]',
    style: {
      'line-color': '#c41a1a',
      'target-arrow-color': '#c41a1a',
      'line-style': 'solid',
      width: 'mapData(amount, 0, 100000, 1, 3)',
      label: 'data(amount_label)',
      'font-family': 'JetBrains Mono',
      'font-size': '9px',
      color: '#c41a1a',
      'text-rotation': 'autorotate',
      'text-background-color': '#ffffff',
      'text-background-opacity': 1,
      'text-background-padding': 2,
    },
  },
  {
    selector: 'edge[type="lobbied_on"]',
    style: {
      'line-color': '#9a9a9a',
      'line-style': 'dashed',
      'line-dash-pattern': [4, 3],
    },
  },
  {
    selector: 'edge[has_foreign_entity = true]',
    style: {
      'line-style': 'dotted',
      'line-color': '#f59e0b',
      'target-arrow-color': '#f59e0b',
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
  {
    selector: 'edge.dimmed',
    style: {
      opacity: 0.12,
    },
  },
]

export default function GraphView({ graph, onNodeClick, loading, selectedEntity }) {
  const containerRef = useRef(null)
  const cyRef = useRef(null)
  const elements = useMemo(() => buildElements(graph), [graph])

  useEffect(() => {
    if (!containerRef.current) return

    if (cyRef.current) {
      cyRef.current.destroy()
      cyRef.current = null
    }

    const cy = cytoscape({
      container: containerRef.current,
      elements,
      style: cyStyle,
      wheelSensitivity: 0.2,
      layout: {
        name: 'cola',
        nodeSpacing: 36,
        edgeLengthVal: 120,
        fit: true,
        animate: true,
        randomize: false,
      },
    })

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
      applyHover(evt.target)
    })

    cy.on('mouseout', 'node', () => {
      clearClasses()
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

      <div className="graph-legend" aria-label="Graph legend">
        <p>■ Organization  ● Legislator  ◆ Committee  · Lobbyist</p>
        <p>─── Contribution  ╌╌╌ Lobbying activity</p>
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
