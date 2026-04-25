# Developers Page Redesign

**Date:** 2026-04-25  
**Status:** Approved

## Overview

Redesign the `/developers` page from a simple scrolling card layout into a proper developer docs experience — sticky left sidebar with table of contents, structured command reference grouped by category, flags tables, collapsible output examples, and copy buttons on all code blocks. The page stays at `/developers` within the existing SPA shell with no router changes.

## Layout

Two-column layout that fills the full available width below the app nav.

**Left sidebar (~240px, sticky):**
- `position: sticky; top: 0; height: 100vh; overflow-y: auto`
- Background: `var(--bg-surface)`, right border: `var(--border)`
- Contains a "LobbyWatch CLI" title and flat TOC
- Command Reference section expands to show indented sub-items per category
- Active section tracked via `IntersectionObserver` on each `<h2>` and `<h3>` anchor
- Active link: `var(--red)` text + 2px `var(--red)` left border + `var(--bg-raised)` background
- Sub-items indented 12px, muted color until active

**Main content (~760px max-width, scrollable):**
- Sections flow top-to-bottom
- Each section has an `id` anchor the sidebar links to
- Generous vertical rhythm (~32px gap between commands, full `var(--border)` horizontal rules between major sections)

**Mobile (< 768px):**
- Sidebar becomes a horizontal scrollable pill-nav strip pinned to the top of the page section
- Main content goes full-width below it

## Sidebar TOC Structure

```
Overview
Installation
Quickstart
Command Reference
  ↳ Setup
  ↳ Search
  ↳ Entities
  ↳ Graphs
  ↳ Analysis
Output Format
Integration Examples
```

## Content Sections

### Overview
- One-paragraph description of what LobbyWatch CLI is
- Version + install badge line: `v0.1.1 · pip install lobbywatch`
- Install command in a code block with copy button
- No hero cards

### Installation
- Single `pip install lobbywatch` code block with copy button
- Note on Python version requirement (3.10+)
- Global flags (`--db`, `--pretty`) note

### Quickstart
- 4 numbered steps, each with a short explanatory sentence and a code block with copy button
- Rendered as a vertical list, not cards
- Steps: install → `lobbywatch update` → `lobbywatch status` → `lobbywatch search "healthcare"`

### Command Reference
Five category groups. Each group has an `<h3>` header with a `var(--border)` bottom rule.

**Categories and commands:**
- **Setup:** `update`, `status`, `issue-codes`
- **Search:** `search`
- **Entities:** `entity org`, `entity legislator`, `entity committee`
- **Graphs:** `graph org`, `graph legislator`, `graph issue`
- **Analysis:** `analysis betrayal-index`, `analysis revolving-door`, `analysis foreign-influence`

**Per-command structure:**
1. Monospace signature line (e.g. `lobbywatch graph org <id> [OPTIONS]`) — full-width block, `var(--bg-raised)` background, `var(--red)` left border
2. One-sentence description
3. Flags table (if the command has options): three columns — `Flag | Default | Description` — with `var(--border)` cell borders and `var(--bg-surface)` header row
4. Collapsed JSON output example with a `[show output ↓]` toggle; clicking expands inline

**Global flags (apply to every command):**

| Flag | Default | Description |
|---|---|---|
| `--pretty` | false | Pretty-print JSON output |
| `--db <path>` | `~/.local/share/lobbywatch/lobbywatch.db` | Override local DB path |

**Per-command flags:**

`lobbywatch update`
| Flag | Default | Description |
|---|---|---|
| `--url` | GitHub releases URL | Override snapshot download URL |

`lobbywatch search <query>`
| Flag | Default | Description |
|---|---|---|
| `--type` | (none) | Filter by type: `org`, `legislator`, or `issue` |

`lobbywatch graph org <id>`
| Flag | Default | Description |
|---|---|---|
| `--year-min` | (none) | Filter filings from this year forward |
| `--year-max` | (none) | Filter filings up to this year |
| `--issue-code` | (none) | Filter by issue code (e.g. `HLTH`) |
| `--node-limit` | 50 | Max nodes in graph output |

`lobbywatch graph legislator <bioguide_id>`
| Flag | Default | Description |
|---|---|---|
| `--year-min` | (none) | Filter filings from this year forward |
| `--year-max` | (none) | Filter filings up to this year |
| `--node-limit` | 50 | Max nodes in graph output |

`lobbywatch graph issue <query>`
| Flag | Default | Description |
|---|---|---|
| `--year-min` | (none) | Filter filings from this year forward |
| `--year-max` | (none) | Filter filings up to this year |
| `--node-limit` | 50 | Max nodes in graph output |

`lobbywatch analysis betrayal-index`
| Flag | Default | Description |
|---|---|---|
| `--issue-code` | `HLTH` | Issue code to analyze |
| `--min-contribution` | 10000 | Minimum contribution amount in dollars |
| `--contribution-window-days` | 365 | Days before vote to look for contributions |

`lobbywatch analysis revolving-door`
| Flag | Default | Description |
|---|---|---|
| `--agency` | (none) | Filter by former government agency |
| `--issue-code` | (none) | Filter by issue code |
| `--limit` | 50 | Max results |

`lobbywatch analysis foreign-influence`
| Flag | Default | Description |
|---|---|---|
| `--country` | (none) | Filter by 2-letter country code (e.g. `UK`) |
| `--issue-code` | (none) | Filter by issue code |
| `--limit` | 50 | Max results |

`lobbywatch entity org/legislator/committee` — no command-level flags; global flags apply.

### Output Format
- Success shape code block + error shape code block, both with copy buttons
- Small exit codes table: `0 = success`, `1 = error`
- Note on global `--pretty` flag for human-readable output during debugging
- Note on global `--db` flag for overriding the local database path

### Integration Examples
- Python subprocess snippet (existing, cleaned up)
- Bash pipeline example: `lobbywatch search "pfizer" | jq '.results[].name'`
- Both with copy buttons

## Interactivity

**Copy button:**
- Each code block has a `Copy` button flush top-right, `var(--mono)` 11px, `var(--ink-muted)` color
- On click: writes to clipboard, swaps text to `Copied!` for 1.5s, then resets
- No toast or modal — inline state only

**Output toggles:**
- JSON output examples hidden by default under each command
- `[show output ↓]` / `[hide output ↑]` link toggles display
- State is per-command (expanding one doesn't affect others)
- No animation — plain display toggle

**Scroll tracking:**
- `IntersectionObserver` watches each section anchor
- Sidebar active state updates as sections enter the viewport

## Visual Design

All tokens from existing `:root` variables — no new colors or fonts.

| Element | Style |
|---|---|
| Sidebar link | `var(--mono)` 12px, `var(--ink-secondary)` |
| Sidebar active | `var(--red)` text, 2px red left border, `var(--bg-raised)` bg |
| Section `<h2>` | `var(--display)` 32px |
| Category `<h3>` | `var(--display)` 22px + `var(--border)` bottom rule |
| Command signature | `var(--mono)` 14px, `var(--bg-raised)` bg, `var(--red)` left border |
| Flags table header | `var(--bg-surface)` background |
| Flags table borders | `var(--border)` |
| Code blocks | `var(--bg-surface)` bg, `var(--mono)` 13px, `var(--border)` border |
| Copy button | `var(--mono)` 11px, `var(--ink-muted)` |

## Files to Change

- `frontend/src/pages/Developers.jsx` — full rewrite
- `frontend/src/styles.css` — replace all `.developers-*` rules with new layout rules

## Out of Scope

- Syntax highlighting (no new dependencies)
- Search within docs
- Dark mode toggle
- Versioned docs / multiple CLI versions
