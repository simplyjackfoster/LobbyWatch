import { forwardRef, useMemo, useState } from 'react'

const FilterBar = forwardRef(function FilterBar({ onChange, issueCodes = [] }, ref) {
  const [yearMin, setYearMin] = useState(2023)
  const [yearMax, setYearMax] = useState(2025)
  const [issueCode, setIssueCode] = useState('')
  const [minContribution, setMinContribution] = useState(0)

  const filters = useMemo(() => ({
    year_min: yearMin,
    year_max: yearMax,
    issue_code: issueCode || undefined,
    min_contribution: minContribution || undefined,
  }), [yearMin, yearMax, issueCode, minContribution])

  return (
    <section className="filter-wrap filters" ref={ref} aria-label="Filter controls">
      <div className="filter-break">
        <span>FILTER</span>
      </div>

      <div className="filter-controls">
        <label className="filter-item">
          <span>Year:</span>
          <input
            type="number"
            min="1990"
            max="2030"
            value={yearMin}
            onChange={(e) => setYearMin(Number(e.target.value))}
            aria-label="Minimum year"
          />
          <span>–</span>
          <input
            type="number"
            min="1990"
            max="2030"
            value={yearMax}
            onChange={(e) => setYearMax(Number(e.target.value))}
            aria-label="Maximum year"
          />
        </label>

        <label className="filter-item">
          <span>Issue:</span>
          <select value={issueCode} onChange={(e) => setIssueCode(e.target.value)} aria-label="Issue code">
            {['', ...issueCodes].map((code) => (
              <option key={code || 'all'} value={code}>
                {code || 'All'}
              </option>
            ))}
          </select>
        </label>

        <label className="filter-item">
          <span>Min $:</span>
          <input
            type="number"
            min="0"
            step="1000"
            value={minContribution}
            onChange={(e) => setMinContribution(Number(e.target.value))}
            aria-label="Minimum contribution"
          />
        </label>

        <button type="button" className="filter-apply" onClick={() => onChange(filters)}>
          Apply
        </button>
      </div>
    </section>
  )
})

export default FilterBar
