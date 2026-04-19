import React from 'react'

export default class ErrorBoundary extends React.Component {
  constructor(props) {
    super(props)
    this.state = { hasError: false }
  }

  static getDerivedStateFromError() {
    return { hasError: true }
  }

  componentDidCatch(error) {
    // Keep console log for debugging crashes in rendering library internals.
    console.error('Graph rendering error:', error)
  }

  render() {
    if (this.state.hasError) {
      return (
        <div className="graph-error">
          Graph rendering failed. Try a different search or reduce the date range.
        </div>
      )
    }
    return this.props.children
  }
}
