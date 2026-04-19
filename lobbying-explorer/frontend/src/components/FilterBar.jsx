import { useMemo, useState } from 'react'

export default function FilterBar({ onChange, issueCodes = [] }) {
  const [yearMin, setYearMin] = useState(2019)
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
    <div className="filter-wrap">
      <label>
        Year Start
        <input type="range" min="2019" max="2025" value={yearMin} onChange={(e) => setYearMin(Number(e.target.value))} />
        <span>{yearMin}</span>
      </label>
      <label>
        Year End
        <input type="range" min="2019" max="2025" value={yearMax} onChange={(e) => setYearMax(Number(e.target.value))} />
        <span>{yearMax}</span>
      </label>
      <label>
        Issue Code
        <select value={issueCode} onChange={(e) => setIssueCode(e.target.value)}>
          {['', ...issueCodes].map((code) => (
            <option key={code || 'all'} value={code}>{code || 'All'}</option>
          ))}
        </select>
      </label>
      <label>
        Min Contribution
        <input type="number" min="0" value={minContribution} onChange={(e) => setMinContribution(Number(e.target.value))} />
      </label>
      <button onClick={() => onChange(filters)}>Apply</button>
    </div>
  )
}
