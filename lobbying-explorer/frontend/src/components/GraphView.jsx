import { useEffect, useRef } from 'react'
import cytoscape from 'cytoscape'
import cola from 'cytoscape-cola'

cytoscape.use(cola)

const colorByType = {
  organization: '#1f77b4',
  legislator: '#0f766e',
  committee: '#d97706',
  lobbyist: '#6b7280',
  issue: '#7c3aed',
  vote: '#64748b',
  registration: '#334155',
}

export default function GraphView({ graph, onNodeClick, onNodeHover, loading, selectedEntity }) {
  const containerRef = useRef(null)
  const cyRef = useRef(null)

  useEffect(() => {
    if (!containerRef.current) return
    if (cyRef.current) {
      cyRef.current.destroy()
    }

    const elements = [
      ...graph.nodes.map((n) => ({ data: { ...n } })),
      ...graph.edges.map((e, i) => ({ data: { id: `${e.source}-${e.target}-${i}`, ...e } })),
    ]

    const cy = cytoscape({
      container: containerRef.current,
      elements,
      style: [
        {
          selector: 'node',
          style: {
            'background-color': (ele) => colorByType[ele.data('type')] || '#64748b',
            label: 'data(label)',
            color: '#111827',
            'font-size': 10,
            'text-wrap': 'wrap',
            'text-max-width': 120,
            'text-valign': 'bottom',
            'text-margin-y': 6,
            width: 26,
            height: 26,
          },
        },
        {
          selector: 'edge',
          style: {
            width: (ele) => {
              const amount = Number(ele.data('amount') || 0)
              return amount > 0 ? Math.min(10, Math.max(1, amount / 10000)) : 1.5
            },
            'line-color': '#94a3b8',
            'target-arrow-color': '#94a3b8',
            'target-arrow-shape': 'triangle',
            'curve-style': 'bezier',
          },
        },
        {
          selector: 'edge[type="contribution"]',
          style: {
            label: 'data(amount_label)',
            'font-size': 10,
            color: '#666',
            'text-rotation': 'autorotate',
            'text-margin-y': -8,
          },
        },
        {
          selector: 'edge[type="lobbied_on"]',
          style: {
            label: 'lobbied',
            'font-size': 10,
            color: '#888',
            'text-rotation': 'autorotate',
          },
        },
        {
          selector: 'edge[type="member_of"]',
          style: {
            label: 'member',
            'font-size': 10,
            color: '#888',
          },
        },
      ],
      layout: {
        name: 'cola',
        nodeSpacing: 30,
        edgeLengthVal: 120,
        animate: true,
      },
    })

    cy.on('tap', 'node', (evt) => {
      const data = evt.target.data()
      onNodeClick?.(data)
    })

    cy.on('mouseover', 'node', (evt) => {
      onNodeHover?.(evt.target.data())
    })

    cyRef.current = cy
    return () => cy.destroy()
  }, [graph, onNodeClick, onNodeHover])

  return (
    <div className="graph-wrap">
      <div ref={containerRef} className="graph-canvas" />
      {loading && (
        <div className="graph-overlay">
          <div className="spinner" />
          <div>Loading graph...</div>
        </div>
      )}
      {!loading && graph.nodes.length === 0 && selectedEntity && (
        <div className="graph-empty">
          <h3>No network found</h3>
          <p>
            No lobbying activity or contributions on record for this entity
            in the selected date range.
          </p>
        </div>
      )}
    </div>
  )
}
