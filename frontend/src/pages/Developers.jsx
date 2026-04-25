import { useEffect, useRef, useState } from 'react'

const MAJOR_SECTIONS = [
  { id: 'overview', label: 'Overview' },
  { id: 'installation', label: 'Installation' },
  { id: 'quickstart', label: 'Quickstart' },
  { id: 'command-reference', label: 'Command Reference' },
  { id: 'output-format', label: 'Output Format' },
  { id: 'integration-examples', label: 'Integration Examples' },
]

const COMMAND_CATEGORIES = [
  {
    id: 'command-setup',
    label: 'Setup',
    commands: [
      {
        id: 'cmd-update',
        signature: 'lobbywatch update [OPTIONS]',
        description: 'Download the latest local SQLite snapshot from GitHub Releases.',
        flags: [['--url', 'GitHub releases URL', 'Override snapshot download URL']],
        output: `{"ok":true,"snapshot":"2026-04-24","db_path":"~/.local/share/lobbywatch/lobbywatch.db"}`,
      },
      {
        id: 'cmd-status',
        signature: 'lobbywatch status',
        description: 'Show the local database path, export timestamp, and file size.',
        output: `{"ok":true,"db_path":"~/.local/share/lobbywatch/lobbywatch.db","snapshot":"2026-04-24","size_bytes":84210432}`,
      },
      {
        id: 'cmd-issue-codes',
        signature: 'lobbywatch issue-codes',
        description: 'List issue codes available in the local dataset.',
        output: `{"results":[{"code":"HLTH","label":"Health"},{"code":"TAX","label":"Taxation"}]}`,
      },
    ],
  },
  {
    id: 'command-search',
    label: 'Search',
    commands: [
      {
        id: 'cmd-search',
        signature: 'lobbywatch search <query> [OPTIONS]',
        description: 'Search organizations, legislators, and issue text in one command.',
        flags: [['--type', '(none)', 'Filter by type: org, legislator, or issue']],
        output: `{"results":[{"id":1234,"type":"organization","name":"Pfizer Inc."}]}`,
      },
    ],
  },
  {
    id: 'command-entities',
    label: 'Entities',
    commands: [
      {
        id: 'cmd-entity-org',
        signature: 'lobbywatch entity org <id>',
        description: 'Return an organization summary with spend, issues, recipients, and lobbyists.',
        output: `{"type":"organization","id":1,"name":"Pfizer Inc.","total_spend":12300000}`,
      },
      {
        id: 'cmd-entity-legislator',
        signature: 'lobbywatch entity legislator <bioguide_id>',
        description: 'Return a legislator summary with committee roles and top contributors.',
        output: `{"type":"legislator","bioguide_id":"A000001","name":"Rep. Example","top_contributors":[{"name":"Example PAC","amount":220000}]}`,
      },
      {
        id: 'cmd-entity-committee',
        signature: 'lobbywatch entity committee <code>',
        description: 'Return committee membership, issue focus, and active organizations.',
        output: `{"type":"committee","code":"SSHR","name":"Senate Committee on Health","active_org_count":214}`,
      },
    ],
  },
  {
    id: 'command-graphs',
    label: 'Graphs',
    commands: [
      {
        id: 'cmd-graph-org',
        signature: 'lobbywatch graph org <id> [OPTIONS]',
        description: 'Generate an organization-centered influence graph as JSON.',
        flags: [
          ['--year-min', '(none)', 'Filter filings from this year forward'],
          ['--year-max', '(none)', 'Filter filings up to this year'],
          ['--issue-code', '(none)', 'Filter by issue code (e.g. HLTH)'],
          ['--node-limit', '50', 'Max nodes in graph output'],
        ],
        output: `{"nodes":[{"id":"org:1","type":"organization","label":"Pfizer Inc."}],"edges":[]}`,
      },
      {
        id: 'cmd-graph-legislator',
        signature: 'lobbywatch graph legislator <bioguide_id> [OPTIONS]',
        description: 'Generate a legislator-centered influence graph as JSON.',
        flags: [
          ['--year-min', '(none)', 'Filter filings from this year forward'],
          ['--year-max', '(none)', 'Filter filings up to this year'],
          ['--node-limit', '50', 'Max nodes in graph output'],
        ],
        output: `{"nodes":[{"id":"leg:A000001","type":"legislator","label":"Rep. Example"}],"edges":[]}`,
      },
      {
        id: 'cmd-graph-issue',
        signature: 'lobbywatch graph issue <query> [OPTIONS]',
        description: 'Generate an issue-centered influence graph as JSON.',
        flags: [
          ['--year-min', '(none)', 'Filter filings from this year forward'],
          ['--year-max', '(none)', 'Filter filings up to this year'],
          ['--node-limit', '50', 'Max nodes in graph output'],
        ],
        output: `{"nodes":[{"id":"issue:hlth","type":"issue","label":"Healthcare"}],"edges":[]}`,
      },
    ],
  },
  {
    id: 'command-analysis',
    label: 'Analysis',
    commands: [
      {
        id: 'cmd-analysis-betrayal-index',
        signature: 'lobbywatch analysis betrayal-index [OPTIONS]',
        description: 'Rank possible donor-aligned vote contradictions for a focus issue.',
        flags: [
          ['--issue-code', 'HLTH', 'Issue code to analyze'],
          ['--min-contribution', '10000', 'Minimum contribution amount in dollars'],
          ['--contribution-window-days', '365', 'Days before vote to look for contributions'],
        ],
        output: `{"findings":[{"legislator":"Rep. Example","score":0.87,"issue_code":"HLTH"}]}`,
      },
      {
        id: 'cmd-analysis-revolving-door',
        signature: 'lobbywatch analysis revolving-door [OPTIONS]',
        description: 'Identify former public officials now lobbying related policy areas.',
        flags: [
          ['--agency', '(none)', 'Filter by former government agency'],
          ['--issue-code', '(none)', 'Filter by issue code'],
          ['--limit', '50', 'Max results'],
        ],
        output: `{"results":[{"name":"Jane Doe","former_role":"FDA Counsel","current_firm":"Example Strategies"}]}`,
      },
      {
        id: 'cmd-analysis-foreign-influence',
        signature: 'lobbywatch analysis foreign-influence [OPTIONS]',
        description: 'Find filings with foreign-entity influence patterns.',
        flags: [
          ['--country', '(none)', 'Filter by 2-letter country code (e.g. UK)'],
          ['--issue-code', '(none)', 'Filter by issue code'],
          ['--limit', '50', 'Max results'],
        ],
        output: `{"results":[{"organization":"Example Group","country":"UK","filings":9}]}`,
      },
    ],
  },
]

const QUICKSTART_STEPS = [
  {
    id: 'quickstart-install',
    text: 'Install the CLI from PyPI so the lobbywatch command is available locally.',
    command: 'pip install lobbywatch',
  },
  {
    id: 'quickstart-update',
    text: 'Download the latest snapshot into your local SQLite database.',
    command: 'lobbywatch update',
  },
  {
    id: 'quickstart-status',
    text: 'Confirm your local database path and snapshot metadata.',
    command: 'lobbywatch status',
  },
  {
    id: 'quickstart-search',
    text: 'Run a first query to verify end-to-end output.',
    command: 'lobbywatch search "healthcare"',
  },
]

const SUCCESS_OUTPUT = `{"results":[{"id":1234,"type":"organization","name":"Pfizer Inc."}]}`
const ERROR_OUTPUT = `{"error":"No local database found. Run: lobbywatch update"}`

const PYTHON_SNIPPET = `import json
import subprocess

res = subprocess.run(
    ["lobbywatch", "analysis", "betrayal-index", "--issue-code", "HLTH"],
    capture_output=True,
    text=True,
    check=True,
)
payload = json.loads(res.stdout)
print(len(payload["findings"]))`

const BASH_SNIPPET = `lobbywatch search "pfizer" | jq '.results[].name'`

function FlagsTable({ rows }) {
  return (
    <table className="developers-flags-table">
      <thead>
        <tr>
          <th>Flag</th>
          <th>Default</th>
          <th>Description</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((row) => (
          <tr key={row[0]}>
            <td>
              <code>{row[0]}</code>
            </td>
            <td>{row[1]}</td>
            <td>{row[2]}</td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}

function CodeBlock({ code, blockId, copiedBlock, onCopy }) {
  return (
    <div className="developers-code-wrap">
      <button className="developers-copy-btn" type="button" onClick={() => onCopy(blockId, code)}>
        {copiedBlock === blockId ? 'Copied!' : 'Copy'}
      </button>
      <pre>
        <code>{code}</code>
      </pre>
    </div>
  )
}

export default function Developers() {
  const docsRef = useRef(null)
  const copyResetRef = useRef(null)
  const [copiedBlock, setCopiedBlock] = useState('')
  const [expandedOutputs, setExpandedOutputs] = useState({})
  const [activeSection, setActiveSection] = useState('overview')

  useEffect(() => {
    const node = docsRef.current
    if (!node) return undefined

    const headings = Array.from(node.querySelectorAll('h2[id], h3[id]'))
    if (!headings.length) return undefined

    const observer = new IntersectionObserver(
      () => {
        const positions = headings.map((heading) => ({
          id: heading.id,
          top: heading.getBoundingClientRect().top,
        }))
        const active =
          positions
            .filter((item) => item.top <= 180)
            .sort((a, b) => b.top - a.top)
            .at(0)?.id ?? positions[0]?.id
        if (active) setActiveSection(active)
      },
      { root: null, rootMargin: '-20% 0px -65% 0px', threshold: [0, 1] },
    )

    headings.forEach((heading) => observer.observe(heading))

    return () => {
      observer.disconnect()
    }
  }, [])

  useEffect(
    () => () => {
      if (copyResetRef.current) {
        clearTimeout(copyResetRef.current)
      }
    },
    [],
  )

  const handleCopy = async (blockId, code) => {
    if (!navigator?.clipboard?.writeText) return
    try {
      await navigator.clipboard.writeText(code)
      setCopiedBlock(blockId)
      if (copyResetRef.current) clearTimeout(copyResetRef.current)
      copyResetRef.current = setTimeout(() => setCopiedBlock(''), 1500)
    } catch {
      // Ignore clipboard failures silently; keep inline copy UX only.
    }
  }

  const toggleOutput = (commandId) => {
    setExpandedOutputs((current) => ({ ...current, [commandId]: !current[commandId] }))
  }

  return (
    <section className="developers-page" aria-label="Developers CLI documentation" ref={docsRef}>
      <aside className="developers-sidebar" aria-label="Developers table of contents">
        <p className="developers-sidebar-title">LobbyWatch CLI</p>
        <nav>
          <ul className="developers-toc-list">
            {MAJOR_SECTIONS.map((section) => (
              <li key={section.id}>
                <a
                  href={`#${section.id}`}
                  className={activeSection === section.id ? 'developers-toc-link active' : 'developers-toc-link'}
                >
                  {section.label}
                </a>
                {section.id === 'command-reference' ? (
                  <ul className="developers-toc-sublist">
                    {COMMAND_CATEGORIES.map((category) => (
                      <li key={category.id}>
                        <a
                          href={`#${category.id}`}
                          className={
                            activeSection === category.id
                              ? 'developers-toc-link developers-toc-sub-link active'
                              : 'developers-toc-link developers-toc-sub-link'
                          }
                        >
                          {category.label}
                        </a>
                      </li>
                    ))}
                  </ul>
                ) : null}
              </li>
            ))}
          </ul>
        </nav>
      </aside>

      <div className="developers-content">
        <section className="developers-section">
          <h2 id="overview">Overview</h2>
          <p>
            LobbyWatch CLI gives developers deterministic access to the LobbyWatch dataset with machine-readable JSON so
            automation, scripts, and agent workflows can run locally without depending on the hosted API.
          </p>
          <p className="developers-version-line">
            <span className="num">v0.1.1</span> · <code>pip install lobbywatch</code>
          </p>
          <CodeBlock code="pip install lobbywatch" blockId="overview-install" copiedBlock={copiedBlock} onCopy={handleCopy} />
        </section>

        <hr className="developers-divider" />

        <section className="developers-section">
          <h2 id="installation">Installation</h2>
          <CodeBlock code="pip install lobbywatch" blockId="install-command" copiedBlock={copiedBlock} onCopy={handleCopy} />
          <p>Python 3.10+ is required.</p>
          <p>
            Global flags are available on all commands: <code>--db &lt;path&gt;</code> overrides the local database path and{' '}
            <code>--pretty</code> prints human-readable JSON while debugging.
          </p>
        </section>

        <hr className="developers-divider" />

        <section className="developers-section">
          <h2 id="quickstart">Quickstart</h2>
          <ol className="developers-quickstart-list">
            {QUICKSTART_STEPS.map((step) => (
              <li key={step.id}>
                <p>{step.text}</p>
                <CodeBlock code={step.command} blockId={step.id} copiedBlock={copiedBlock} onCopy={handleCopy} />
              </li>
            ))}
          </ol>
        </section>

        <hr className="developers-divider" />

        <section className="developers-section">
          <h2 id="command-reference">Command Reference</h2>
          <p className="developers-global-flags-label">Global flags (apply to every command)</p>
          <FlagsTable
            rows={[
              ['--pretty', 'false', 'Pretty-print JSON output'],
              ['--db <path>', '~/.local/share/lobbywatch/lobbywatch.db', 'Override local DB path'],
            ]}
          />

          {COMMAND_CATEGORIES.map((category) => (
            <section key={category.id} className="developers-category-section">
              <h3 id={category.id}>{category.label}</h3>
              <div className="developers-command-stack">
                {category.commands.map((command) => (
                  <article key={command.id} className="developers-command-block">
                    <div className="developers-command-signature">
                      <code>{command.signature}</code>
                    </div>
                    <p>{command.description}</p>
                    {command.flags ? <FlagsTable rows={command.flags} /> : null}
                    <button
                      type="button"
                      className="developers-output-toggle"
                      onClick={() => toggleOutput(command.id)}
                      aria-expanded={Boolean(expandedOutputs[command.id])}
                    >
                      {expandedOutputs[command.id] ? '[hide output ↑]' : '[show output ↓]'}
                    </button>
                    {expandedOutputs[command.id] ? (
                      <CodeBlock
                        code={command.output}
                        blockId={`${command.id}-output`}
                        copiedBlock={copiedBlock}
                        onCopy={handleCopy}
                      />
                    ) : null}
                  </article>
                ))}
              </div>
            </section>
          ))}
        </section>

        <hr className="developers-divider" />

        <section className="developers-section">
          <h2 id="output-format">Output Format</h2>
          <p>Success shape:</p>
          <CodeBlock code={SUCCESS_OUTPUT} blockId="output-success" copiedBlock={copiedBlock} onCopy={handleCopy} />
          <p>Error shape:</p>
          <CodeBlock code={ERROR_OUTPUT} blockId="output-error" copiedBlock={copiedBlock} onCopy={handleCopy} />
          <table className="developers-exit-table">
            <tbody>
              <tr>
                <td>
                  <code>0</code>
                </td>
                <td>success</td>
              </tr>
              <tr>
                <td>
                  <code>1</code>
                </td>
                <td>error</td>
              </tr>
            </tbody>
          </table>
          <p>
            Use global <code>--pretty</code> when debugging CLI responses and <code>--db</code> when you need a custom
            local database path.
          </p>
        </section>

        <hr className="developers-divider" />

        <section className="developers-section">
          <h2 id="integration-examples">Integration Examples</h2>
          <p>Python subprocess:</p>
          <CodeBlock code={PYTHON_SNIPPET} blockId="integration-python" copiedBlock={copiedBlock} onCopy={handleCopy} />
          <p>Bash pipeline:</p>
          <CodeBlock code={BASH_SNIPPET} blockId="integration-bash" copiedBlock={copiedBlock} onCopy={handleCopy} />
        </section>
      </div>
    </section>
  )
}
