import { useEffect, useMemo, useRef, useState } from 'react'

import { fetchRepresentatives } from '../api'

const CACHE_TTL_MS = 24 * 60 * 60 * 1000
const LAST_ZIP_KEY = 'lobbywatch:myreps:last-zip'
const ZIP_CACHE_PREFIX = 'lobbywatch:myreps:zip:v2:'
let myRepsViewMemory = null

const ISSUE_TO_INDUSTRY = {
  HLTH: ['HLTH', 'PHARM'],
  PHARM: ['HLTH', 'PHARM'],
  FIN: ['FIN', 'BANK', 'TAX'],
  TAX: ['FIN', 'TAX'],
  ENRG: ['ENRG', 'ENER'],
  ENERGY: ['ENRG', 'ENER'],
  REAL: ['REAL', 'HOUS'],
  TECH: ['TECH', 'TEC'],
}

function formatCurrency(value) {
  return `$${Number(value || 0).toLocaleString()}`
}

function formatMillions(value) {
  const amount = Number(value || 0)
  if (amount >= 1000000) return `$${(amount / 1000000).toFixed(1)}M`
  return formatCurrency(amount)
}

function hoursAgo(ts) {
  if (!ts) return null
  const elapsed = Date.now() - Number(ts)
  if (!Number.isFinite(elapsed) || elapsed < 0) return null
  return Math.max(0, Math.floor(elapsed / (60 * 60 * 1000)))
}

function zipCacheKey(zip) {
  return `${ZIP_CACHE_PREFIX}${zip}`
}

function readZipCache(zip) {
  try {
    const raw = localStorage.getItem(zipCacheKey(zip))
    if (!raw) return null
    const parsed = JSON.parse(raw)
    if (!parsed?.timestamp || !parsed?.data) return null
    if ((Date.now() - Number(parsed.timestamp)) > CACHE_TTL_MS) return null
    return parsed
  } catch {
    return null
  }
}

function writeZipCache(zip, data) {
  try {
    const payload = { timestamp: Date.now(), data }
    localStorage.setItem(zipCacheKey(zip), JSON.stringify(payload))
    localStorage.setItem(LAST_ZIP_KEY, zip)
    return payload.timestamp
  } catch {
    return Date.now()
  }
}

function hasRepresentativeData(data) {
  const reps = data?.representatives || []
  const unmatched = data?.unmatched || []
  return reps.length > 0 || unmatched.length > 0
}

function getVoteSymbol(position) {
  const value = String(position || '').toUpperCase()
  if (['YEA', 'AYE', 'YES'].includes(value)) return 'yes'
  if (['NAY', 'NO', 'NOT VOTING'].includes(value)) return 'no'
  return 'neutral'
}

function getIndustryForVote(vote, industries) {
  const issueCode = String(vote?.issue_code || '').toUpperCase()
  const candidates = ISSUE_TO_INDUSTRY[issueCode] || [issueCode]
  return (industries || []).find((industry) => candidates.includes(String(industry.industry_code || '').toUpperCase()))
}

async function buildShareImage(rep) {
  const canvas = document.createElement('canvas')
  canvas.width = 1200
  canvas.height = 630
  const ctx = canvas.getContext('2d')
  if (!ctx) throw new Error('Canvas unsupported')

  const primaryIndustry = (rep.top_industries || [])[0]
  const betrayalText = rep.betrayal_score > 0.5
    ? `${rep.name} received ${formatCurrency(primaryIndustry?.total || 0)} from ${primaryIndustry?.label || 'major donors'} and has a betrayal score of ${Number(rep.betrayal_score || 0).toFixed(2)}.`
    : `${rep.name} has ${formatCurrency(rep.total_contributions_received || 0)} in tracked campaign funding in this dataset.`

  ctx.fillStyle = '#ffffff'
  ctx.fillRect(0, 0, canvas.width, canvas.height)

  ctx.strokeStyle = '#0a0a0a'
  ctx.lineWidth = 3
  ctx.strokeRect(18, 18, canvas.width - 36, canvas.height - 36)

  ctx.fillStyle = '#0a0a0a'
  ctx.font = '700 42px "Playfair Display"'
  ctx.fillText('lobby.watch', 70, 110)

  ctx.font = '700 56px "Playfair Display"'
  const badge = `${rep.title.toUpperCase()} ${rep.name.toUpperCase()} (${rep.party}-${rep.state})`
  ctx.fillText(badge, 70, 220)

  ctx.font = '400 42px "Source Serif 4"'
  const lines = [
    betrayalText,
    'See the full public record at lobby.watch',
  ]
  let y = 320
  for (const line of lines) {
    const words = line.split(' ')
    let chunk = ''
    for (const word of words) {
      const next = chunk ? `${chunk} ${word}` : word
      if (ctx.measureText(next).width > 1030) {
        ctx.fillText(chunk, 70, y)
        y += 58
        chunk = word
      } else {
        chunk = next
      }
    }
    if (chunk) {
      ctx.fillText(chunk, 70, y)
      y += 58
    }
  }

  return new Promise((resolve, reject) => {
    canvas.toBlob((blob) => {
      if (!blob) {
        reject(new Error('Failed to generate image blob'))
        return
      }
      resolve(blob)
    }, 'image/png')
  })
}

export default function MyReps({ sharedBioguideId, onViewNetwork }) {
  const [zip, setZip] = useState('')
  const [representatives, setRepresentatives] = useState([])
  const [unmatched, setUnmatched] = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [validationError, setValidationError] = useState('')
  const [lastUpdatedTs, setLastUpdatedTs] = useState(null)
  const [resolvedZip, setResolvedZip] = useState('')
  const [shareState, setShareState] = useState({})
  const zipInputRef = useRef(null)
  const shareStateRef = useRef({})

  const persistViewMemory = ({
    zip: nextZip,
    representatives: nextRepresentatives,
    unmatched: nextUnmatched,
    lastUpdatedTs: nextLastUpdatedTs,
    resolvedZip: nextResolvedZip,
  }) => {
    myRepsViewMemory = {
      zip: nextZip ?? '',
      representatives: nextRepresentatives ?? [],
      unmatched: nextUnmatched ?? [],
      lastUpdatedTs: nextLastUpdatedTs ?? null,
      resolvedZip: nextResolvedZip ?? '',
    }
  }

  const lookupByZip = async (nextZip, { useCache = true } = {}) => {
    const normalized = String(nextZip || '').trim()
    if (!/^\d{5}$/.test(normalized)) {
      setValidationError('Enter a valid 5-digit ZIP code.')
      return
    }

    setValidationError('')
    setError('')
    setLoading(true)

    if (useCache) {
      const cached = readZipCache(normalized)
      if (cached) {
        if (hasRepresentativeData(cached.data)) {
          const nextRepresentatives = cached.data.representatives || []
          const nextUnmatched = cached.data.unmatched || []
          setRepresentatives(nextRepresentatives)
          setUnmatched(nextUnmatched)
          setLastUpdatedTs(cached.timestamp)
          setResolvedZip(normalized)
          persistViewMemory({
            zip: normalized,
            representatives: nextRepresentatives,
            unmatched: nextUnmatched,
            lastUpdatedTs: cached.timestamp,
            resolvedZip: normalized,
          })
          setLoading(false)
          return
        }
        try {
          localStorage.removeItem(zipCacheKey(normalized))
        } catch {
          // Ignore cache cleanup issues and continue with live fetch.
        }
      }
    }

    try {
      const data = await fetchRepresentatives({ zip: normalized })
      if (!hasRepresentativeData(data)) {
        setError("We couldn't find representatives for that zip code. Try a nearby zip or search by name in the Explore tab.")
        setRepresentatives([])
        setUnmatched([])
        setResolvedZip('')
        setLastUpdatedTs(null)
        myRepsViewMemory = null
        return
      }
      const nextRepresentatives = data.representatives || []
      const nextUnmatched = data.unmatched || []
      setRepresentatives(nextRepresentatives)
      setUnmatched(nextUnmatched)
      setResolvedZip(normalized)
      const ts = writeZipCache(normalized, data)
      setLastUpdatedTs(ts)
      persistViewMemory({
        zip: normalized,
        representatives: nextRepresentatives,
        unmatched: nextUnmatched,
        lastUpdatedTs: ts,
        resolvedZip: normalized,
      })
    } catch {
      setError('Unable to look up representatives right now. Please try again.')
      setRepresentatives([])
      setUnmatched([])
      setResolvedZip('')
      setLastUpdatedTs(null)
      myRepsViewMemory = null
    } finally {
      setLoading(false)
    }
  }

  const lookupByBioguide = async (bioguideId) => {
    if (!bioguideId) return
    setValidationError('')
    setError('')
    setLoading(true)
    try {
      const data = await fetchRepresentatives({ bioguide_id: bioguideId })
      const nextRepresentatives = data.representatives || []
      const nextUnmatched = data.unmatched || []
      const ts = Date.now()
      const nextResolvedZip = data.zip || ''
      setRepresentatives(nextRepresentatives)
      setUnmatched(nextUnmatched)
      setResolvedZip(nextResolvedZip)
      setLastUpdatedTs(ts)
      persistViewMemory({
        zip: nextResolvedZip,
        representatives: nextRepresentatives,
        unmatched: nextUnmatched,
        lastUpdatedTs: ts,
        resolvedZip: nextResolvedZip,
      })
      if (!nextRepresentatives.length) {
        setError("Representative record wasn't found in our dataset yet. Try searching by name in Explore.")
      }
    } catch {
      setError('Unable to load this representative record right now.')
      setRepresentatives([])
      setUnmatched([])
      setResolvedZip('')
      setLastUpdatedTs(null)
      myRepsViewMemory = null
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    if (sharedBioguideId) {
      lookupByBioguide(sharedBioguideId)
      return
    }
    if (myRepsViewMemory) {
      setZip(myRepsViewMemory.zip || '')
      setRepresentatives(myRepsViewMemory.representatives || [])
      setUnmatched(myRepsViewMemory.unmatched || [])
      setResolvedZip(myRepsViewMemory.resolvedZip || '')
      setLastUpdatedTs(myRepsViewMemory.lastUpdatedTs || null)
      setError('')
      setValidationError('')
      return
    }

    // Fresh page load: show the original landing prompt.
    setZip('')
    setRepresentatives([])
    setUnmatched([])
    setResolvedZip('')
    setLastUpdatedTs(null)
    setError('')
    setValidationError('')
  }, [sharedBioguideId])

  useEffect(() => {
    if (!sharedBioguideId && zipInputRef.current) {
      zipInputRef.current.focus()
    }
  }, [sharedBioguideId])

  useEffect(() => {
    shareStateRef.current = shareState
  }, [shareState])

  useEffect(() => () => {
    Object.values(shareStateRef.current).forEach((item) => {
      if (item?.url) URL.revokeObjectURL(item.url)
    })
  }, [])

  const onSubmit = async (event) => {
    event.preventDefault()
    await lookupByZip(zip, { useCache: true })
  }

  const startShare = async (rep) => {
    const shareUrl = `${window.location.origin}/rep/${rep.bioguide_id}`
    let copied = false
    try {
      await navigator.clipboard.writeText(shareUrl)
      copied = true
    } catch {
      copied = false
    }

    let current = shareState[rep.bioguide_id]
    if (!current?.blob) {
      try {
        const blob = await buildShareImage(rep)
        const url = URL.createObjectURL(blob)
        current = { blob, url }
        setShareState((prev) => {
          const existing = prev[rep.bioguide_id]
          if (existing?.url) URL.revokeObjectURL(existing.url)
          return {
            ...prev,
            [rep.bioguide_id]: { ...current, copied, copiedImage: false },
          }
        })
      } catch {
        setShareState((prev) => ({ ...prev, [rep.bioguide_id]: { ...current, copied, copiedImage: false } }))
      }
    } else {
      setShareState((prev) => ({ ...prev, [rep.bioguide_id]: { ...current, copied, copiedImage: false } }))
    }

    const shouldUseNativeShare = window.innerWidth < 768 && navigator.share
    if (shouldUseNativeShare && current?.blob) {
      try {
        const file = new File([current.blob], `lobby-watch-${rep.bioguide_id}.png`, { type: 'image/png' })
        if (!navigator.canShare || navigator.canShare({ files: [file] })) {
          await navigator.share({
            title: `lobby.watch record: ${rep.name}`,
            text: `Public record for ${rep.title} ${rep.name}`,
            url: shareUrl,
            files: [file],
          })
        }
      } catch {
        // Silent fallback to copied URL.
      }
    }
  }

  const copyImage = async (bioguideId) => {
    const asset = shareState[bioguideId]
    if (!asset?.blob || !window.ClipboardItem || !navigator.clipboard?.write) return
    try {
      await navigator.clipboard.write([new ClipboardItem({ 'image/png': asset.blob })])
      setShareState((prev) => ({ ...prev, [bioguideId]: { ...asset, copiedImage: true } }))
    } catch {
      setShareState((prev) => ({ ...prev, [bioguideId]: { ...asset, copiedImage: false } }))
    }
  }

  const lastUpdatedHours = useMemo(() => hoursAgo(lastUpdatedTs), [lastUpdatedTs])
  const showLanding = !loading
    && representatives.length === 0
    && unmatched.length === 0
    && !sharedBioguideId

  return (
    <section className="myreps-page" aria-label="Your representatives">
      {showLanding && (
        <div className="myreps-landing">
          <h2>Who represents you in Congress?</h2>
          <p className="myreps-subtitle">And who&apos;s paying for their campaigns?</p>

          <form className="myreps-zip-form" onSubmit={onSubmit}>
            <input
              ref={zipInputRef}
              value={zip}
              onChange={(event) => {
                const next = event.target.value.replace(/\D/g, '').slice(0, 5)
                setZip(next)
              }}
              placeholder="Enter your zip code..."
              aria-label="ZIP code"
              maxLength={5}
              inputMode="numeric"
              pattern="\d{5}"
            />
            <button type="submit">LOOK UP</button>
          </form>

          {validationError && <p className="myreps-error">{validationError}</p>}

          <p className="myreps-note">
            Enter your zip code to see your senators and house representative, their committee assignments, who funds
            their campaigns, and how they vote.
          </p>
          <p className="myreps-note muted">All data from Senate LDA, FEC, and Congress.gov. All public record.</p>
        </div>
      )}

      {loading && (
        <div className="myreps-loading">
          <div className="dots" aria-hidden="true">
            <span />
            <span />
            <span />
          </div>
          <p>Looking up your representatives...</p>
        </div>
      )}

      {error && !loading && (
        <div className="myreps-error-state">
          <p>{error}</p>
        </div>
      )}

      {!loading && representatives.length > 0 && (
        <>
          <header className="myreps-results-header">
            <h3>Your Congressional Delegation {resolvedZip ? `for ${resolvedZip}` : ''}</h3>
            <form className="myreps-inline-zip-form" onSubmit={onSubmit}>
              <input
                value={zip}
                onChange={(event) => setZip(event.target.value.replace(/\D/g, '').slice(0, 5))}
                placeholder="ZIP"
                maxLength={5}
                inputMode="numeric"
                aria-label="Change ZIP code"
              />
              <button type="submit">LOOK UP</button>
            </form>
          </header>

          <div className="myreps-grid">
            {representatives.map((rep) => {
              const maxIndustry = Math.max(...(rep.top_industries || []).map((item) => Number(item.total || 0)), 1)
              const share = shareState[rep.bioguide_id] || {}
              const betrayalActive = Number(rep.betrayal_score || 0) > 0.5

              return (
                <article key={rep.bioguide_id} className="myrep-card">
                  <header className="myrep-card-header">
                    <img
                      src={rep.photo_url}
                      alt={`${rep.name} portrait`}
                      width="56"
                      height="56"
                      loading="lazy"
                      onError={(event) => {
                        event.currentTarget.style.visibility = 'hidden'
                      }}
                    />
                    <div className="myrep-header-text">
                      <div className="myrep-name-row">
                        <h4>{`${rep.title} ${rep.name}`}</h4>
                        <span className={`party-badge ${String(rep.party || '').toUpperCase()}`}>
                          [{rep.party || '?'}]
                        </span>
                        <span className="state-code">· {rep.state}</span>
                      </div>
                      {(rep.committees || []).slice(0, 2).map((committee) => (
                        <p key={committee.name} className="committee-line">{committee.name}</p>
                      ))}
                    </div>
                  </header>

                  <section className="myrep-section">
                    <p className="section-label">CAMPAIGN FUNDING</p>
                    {(rep.top_industries || []).length === 0 && (
                      <p className="no-data-message">No campaign contributions on record for this legislator in our current dataset.</p>
                    )}
                    {(rep.top_industries || []).map((industry) => {
                      const ratio = Math.max(0, Math.min(100, (Number(industry.total || 0) / maxIndustry) * 100))
                      return (
                        <div key={`${rep.bioguide_id}-${industry.industry_code}`} className="funding-row">
                          <span>{industry.label}</span>
                          <strong>{formatCurrency(industry.total)}</strong>
                          <div className="funding-bar">
                            <div style={{ width: `${ratio}%` }} />
                          </div>
                        </div>
                      )
                    })}
                    <p className="funding-total">Total received: {formatCurrency(rep.total_contributions_received)} this cycle</p>
                  </section>

                  <section className="myrep-section">
                    <p className="section-label">COMMITTEE ACTIVITY</p>
                    {(rep.committees || []).slice(0, 1).map((committee) => (
                      <p key={`${rep.bioguide_id}-${committee.name}`}>Sits on {committee.name}</p>
                    ))}
                    <p>
                      Industry spent {formatMillions(rep.lobbying_spend_on_committees)} lobbying this committee network.
                    </p>
                  </section>

                  <section className="myrep-section">
                    <p className="section-label">RECENT VOTES</p>
                    <ul className="votes-list">
                      {(rep.recent_votes || []).slice(0, 3).map((vote, index) => {
                        const symbol = getVoteSymbol(vote.position)
                        const matchingIndustry = getIndustryForVote(vote, rep.top_industries)
                        return (
                          <li key={`${rep.bioguide_id}-${vote.bill_id || index}`}>
                            <p className="vote-title">
                              <span className={`vote-mark ${symbol}`}>{symbol === 'no' ? '✗' : '✓'}</span>
                              {vote.bill_title || vote.bill_id || 'Recorded vote'}
                            </p>
                            {matchingIndustry && symbol === 'no' && (
                              <p className="vote-conflict-line">
                                After receiving {formatCurrency(matchingIndustry.total)} from {matchingIndustry.label}
                              </p>
                            )}
                          </li>
                        )
                      })}
                    </ul>
                  </section>

                  {betrayalActive && (
                    <section className="myrep-conflict">
                      <p className="conflict-title">⚠ FUNDING CONFLICT DETECTED</p>
                      <p>
                        Co-sponsored {rep.co_sponsorships_count} bills. Betrayal score {Number(rep.betrayal_score || 0).toFixed(2)}
                        {' '}on issue code {rep.betrayal_issue || 'HLTH'}.
                      </p>
                    </section>
                  )}

                  <footer className="myrep-actions">
                    <button type="button" onClick={() => onViewNetwork?.(rep)}>View full network →</button>
                    <button type="button" onClick={() => startShare(rep)}>
                      {share.copied ? 'Copied!' : 'Share this record →'}
                    </button>
                  </footer>

                  {(share.url || share.blob) && (
                    <div className="myrep-share-tools">
                      <button type="button" onClick={() => copyImage(rep.bioguide_id)}>
                        {share.copiedImage ? 'Image copied' : 'Copy image'}
                      </button>
                      {share.url && (
                        <a href={share.url} download={`lobby-watch-${rep.bioguide_id}.png`}>
                          Download image
                        </a>
                      )}
                    </div>
                  )}
                </article>
              )
            })}
          </div>
        </>
      )}

      {!loading && unmatched.length > 0 && (
        <div className="myreps-unmatched">
          {unmatched.map((row, idx) => (
            <p key={`${row.name || 'missing'}-${idx}`}>
              {(row.name || 'A representative')} isn&apos;t in our database yet. We currently cover all current federal
              legislators. Search by name in the Explore tab.
            </p>
          ))}
        </div>
      )}

      {!loading && lastUpdatedHours !== null && (
        <p className="myreps-last-updated">Last updated: {lastUpdatedHours} hours ago</p>
      )}
    </section>
  )
}
