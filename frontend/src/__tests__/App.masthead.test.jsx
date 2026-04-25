import { render, screen, waitFor } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import App from '../App'

vi.mock('../components/Discoveries', () => ({ default: () => <div data-testid="discoveries" /> }))
vi.mock('../components/ErrorBoundary', () => ({ default: ({ children }) => <>{children}</> }))
vi.mock('../components/FilterBar', () => ({ default: () => <div data-testid="filter-bar" /> }))
vi.mock('../components/GraphView', () => ({ default: () => <div data-testid="graph-view" /> }))
vi.mock('../components/InfoPanel', () => ({ default: () => <div data-testid="info-panel" /> }))
vi.mock('../components/SearchBar', () => ({ default: () => <div data-testid="search-bar" /> }))
vi.mock('../pages/Developers', () => ({ default: () => <div data-testid="developers" /> }))
vi.mock('../pages/MyReps', () => ({ default: () => <div data-testid="my-reps" /> }))
vi.mock('../pages/AboutData', () => ({ default: () => <div data-testid="about-data" /> }))

const fetchIssueCodes = vi.fn(() => Promise.resolve({ issue_codes: [] }))
const fetchDataStatus = vi.fn(() => Promise.reject(new Error('boom')))

vi.mock('../api', () => ({
  fetchIssueCodes: () => fetchIssueCodes(),
  fetchDataStatus: () => fetchDataStatus(),
  searchEntities: vi.fn(),
  fetchOrgGraph: vi.fn(),
  fetchLegGraph: vi.fn(),
  fetchIssueGraph: vi.fn(),
}))

beforeEach(() => {
  window.history.pushState({}, '', '/my-reps')
  fetchIssueCodes.mockClear()
  fetchDataStatus.mockClear()
})

describe('masthead data status fallback', () => {
  it('shows LIVE DATA when /meta/data-status fails', async () => {
    render(<App />)

    expect(screen.getByText('LIVE DATA')).toBeInTheDocument()

    await waitFor(() => {
      expect(fetchDataStatus).toHaveBeenCalledTimes(1)
    })

    expect(screen.getByText('LIVE DATA')).toBeInTheDocument()
  })
})
