import { useState } from 'react'

const RECENT_TERMS = ['Pfizer', 'Goldman Sachs', 'insulin']

function resultTypeLabel(type) {
  if (type === 'organization') return 'Organization'
  if (type === 'legislator') return 'Legislator'
  if (type === 'committee') return 'Committee'
  if (type === 'issue') return 'Issue'
  return 'Entity'
}

export default function SearchBar({ onSearch, results, onSelect }) {
  const [q, setQ] = useState('')

  const submit = (e) => {
    e.preventDefault()
    if (!q.trim()) return
    onSearch(q.trim())
  }

  const runRecentSearch = (term) => {
    setQ(term)
    onSearch(term)
  }

  return (
    <section className="search-wrap search" aria-label="Entity search">
      <p className="search-label">Search the influence database</p>

      <form onSubmit={submit} className="search-form" role="search">
        <div className="search-input-group">
          <input
            value={q}
            onChange={(e) => setQ(e.target.value)}
            className="search-input"
            spellCheck={false}
            placeholder="Company, legislator, or issue..."
            aria-label="Search company, legislator, or issue"
          />
          <button type="submit" className="search-submit">SEARCH</button>
        </div>
      </form>

      <div className="search-recent-row">
        <span>Recent:</span>
        {RECENT_TERMS.map((term, idx) => (
          <button key={term} type="button" className="search-recent-link" onClick={() => runRecentSearch(term)}>
            {term}
            {idx < RECENT_TERMS.length - 1 ? ' ·' : ''}
          </button>
        ))}
      </div>

      {results.length > 0 && (
        <div className="search-results">
          {results.map((r, idx) => (
            <button
              key={`${r.type}-${r.id}-${idx}`}
              type="button"
              className="search-result"
              onClick={() => onSelect(r)}
            >
              <span className="search-result-type">{resultTypeLabel(r.type)}</span>
              <span className="search-result-name">{r.name}</span>
              <small className="search-result-meta">{[r.party, r.state].filter(Boolean).join(' · ')}</small>
            </button>
          ))}
        </div>
      )}
    </section>
  )
}
