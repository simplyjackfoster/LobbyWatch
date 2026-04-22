const CLI_COMMANDS = [
  {
    command: 'lobbywatch update',
    purpose: 'Download latest local SQLite snapshot from GitHub Releases.',
  },
  {
    command: 'lobbywatch status',
    purpose: 'Show local DB path, export timestamp, and file size.',
  },
  {
    command: 'lobbywatch issue-codes',
    purpose: 'List issue codes available in the local dataset.',
  },
  {
    command: 'lobbywatch search "pfizer"',
    purpose: 'Search organizations, legislators, and issue text.',
  },
  {
    command: 'lobbywatch graph org 1 --year-min 2023 --node-limit 75',
    purpose: 'Generate organization influence graph JSON.',
  },
  {
    command: 'lobbywatch graph legislator A000001',
    purpose: 'Generate legislator influence graph JSON.',
  },
  {
    command: 'lobbywatch graph issue "drug pricing"',
    purpose: 'Generate issue-centered influence graph JSON.',
  },
  {
    command: 'lobbywatch entity org 1',
    purpose: 'Organization summary: spend, issue codes, recipients, lobbyists.',
  },
  {
    command: 'lobbywatch entity legislator A000001',
    purpose: 'Legislator summary: committees and top contributors.',
  },
  {
    command: 'lobbywatch entity committee SSHR',
    purpose: 'Committee summary: members, issue codes, active orgs.',
  },
  {
    command: 'lobbywatch analysis betrayal-index --issue-code HLTH',
    purpose: 'Rank possible donor-aligned vote contradictions.',
  },
  {
    command: 'lobbywatch analysis revolving-door --limit 25',
    purpose: 'Find former officials now lobbying relevant issues.',
  },
  {
    command: 'lobbywatch analysis foreign-influence --country UK',
    purpose: 'Find filings with foreign-entity influence patterns.',
  },
]

export default function Developers() {
  return (
    <section className="developers-page" aria-label="Developers CLI documentation">
      <header className="developers-hero">
        <p className="developers-kicker">FOR DEVELOPERS</p>
        <h2>LobbyWatch CLI</h2>
        <p>
          Run LobbyWatch locally, script it in pipelines, and call it from agents with stable JSON output and no runtime
          dependency on the hosted API after setup.
        </p>
        <pre>{`pip install lobbywatch`}</pre>
      </header>

      <div className="developers-grid">
        <article className="developers-card">
          <h3>Quickstart</h3>
          <ol>
            <li>
              Install:
              <pre>{`pip install lobbywatch`}</pre>
            </li>
            <li>
              Pull latest data snapshot:
              <pre>{`lobbywatch update`}</pre>
            </li>
            <li>
              Check local status:
              <pre>{`lobbywatch status`}</pre>
            </li>
            <li>
              Run a first query:
              <pre>{`lobbywatch search "healthcare"`}</pre>
            </li>
          </ol>
        </article>

        <article className="developers-card">
          <h3>Output Contract</h3>
          <p>Success always returns one JSON object on stdout.</p>
          <pre>{`{"results":[{"id":1234,"type":"organization","name":"Pfizer Inc."}]}`}</pre>
          <p>Errors are machine-readable and use non-zero exit codes.</p>
          <pre>{`{"error":"No local database found. Run: lobbywatch update"}`}</pre>
          <p>Use <code>--pretty</code> for human-readable JSON while debugging.</p>
        </article>

        <article className="developers-card">
          <h3>Agent / Script Example</h3>
          <pre>{`# Python subprocess pattern
import json, subprocess

res = subprocess.run(
    ["lobbywatch", "analysis", "betrayal-index", "--issue-code", "HLTH"],
    capture_output=True, text=True, check=True
)
data = json.loads(res.stdout)
print(len(data["findings"]))`}</pre>
        </article>
      </div>

      <section className="developers-reference">
        <h3>Command Reference</h3>
        <div className="developers-command-list">
          {CLI_COMMANDS.map((item) => (
            <article key={item.command} className="developers-command-item">
              <code>{item.command}</code>
              <p>{item.purpose}</p>
            </article>
          ))}
        </div>
      </section>
    </section>
  )
}
