import axios from 'axios'

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || '/api'

const client = axios.create({
  baseURL: API_BASE_URL,
  timeout: 15000,
})

export async function searchEntities(q) {
  const { data } = await client.get('/search', { params: { q } })
  return data
}

export async function fetchIssueCodes() {
  const { data } = await client.get('/meta/issue-codes')
  return data
}

export async function fetchDataStatus() {
  const { data } = await client.get('/meta/data-status')
  return data
}

export async function fetchEntitySummary(entityType, entityId) {
  const { data } = await client.get(`/entity/${entityType}/${entityId}/summary`)
  return data
}

export async function fetchOrgGraph(id, filters = {}) {
  const { data } = await client.get(`/graph/organization/${id}`, { params: filters })
  return data
}

export async function fetchLegGraph(id, filters = {}) {
  const { data } = await client.get(`/graph/legislator/${id}`, { params: filters })
  return data
}

export async function fetchIssueGraph(q, filters = {}) {
  const { data } = await client.get('/graph/issue', { params: { q, ...filters } })
  return data
}

export async function fetchBetrayalIndex(params = {}) {
  const { data } = await client.get('/analysis/betrayal-index', { params })
  return data
}

export async function fetchRevolvingDoor(params = {}) {
  const { data } = await client.get('/analysis/revolving-door', { params })
  return data
}

export async function fetchForeignInfluence(params = {}) {
  const { data } = await client.get('/analysis/foreign-influence', { params })
  return data
}

export async function fetchRepresentatives(params = {}) {
  const { data } = await client.get('/representatives', { params })
  return data
}
