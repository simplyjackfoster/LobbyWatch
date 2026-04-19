import { useState } from 'react'

function ResultBadge({ type }) {
  const cls = `badge badge-${type}`
  return <span className={cls}>{type}</span>
}

export default function SearchBar({ onSearch, results, onSelect }) {
  const [q, setQ] = useState('')

  const submit = (e) => {
    e.preventDefault()
    if (!q.trim()) return
    onSearch(q.trim())
  }

  return (
    <div className="search-wrap">
      <form onSubmit={submit} className="search-form">
        <input
          value={q}
          onChange={(e) => setQ(e.target.value)}
          placeholder="Search company, legislator, or issue"
        />
        <button type="submit">Search</button>
      </form>
      {results.length > 0 && (
        <div className="search-results">
          {results.map((r, idx) => (
            <button key={`${r.type}-${r.id}-${idx}`} className="search-result" onClick={() => onSelect(r)}>
              <ResultBadge type={r.type} />
              <span>{r.name}</span>
              {r.state && <small>{r.party} - {r.state}</small>}
            </button>
          ))}
        </div>
      )}
    </div>
  )
}
