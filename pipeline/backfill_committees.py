"""
Backfill committee memberships from Congress.gov.
Uses the committee-membership endpoint which is more reliable
than per-member lookups that 404.
"""
import asyncio
import aiohttp
import os
import re
from dotenv import load_dotenv
import asyncpg
from tqdm import tqdm

load_dotenv()

CONGRESS_API_KEY = os.getenv('CONGRESS_API_KEY')
BASE_URL = 'https://api.congress.gov/v3'
DATABASE_URL = os.getenv('DATABASE_URL')
LEGISLATORS_MEMBERSHIP_YAML_URL = (
    'https://raw.githubusercontent.com/unitedstates/congress-legislators/main/committee-membership-current.yaml'
)


def parse_membership_yaml(text: str):
    """
    Lightweight parser for committee-membership-current.yaml.
    We only need:
      COMMITTEECODE:
      - name: ...
        bioguide: X000000
        title: ...
    """
    memberships = {}
    current_code = None
    current_member = None

    def flush_member():
        nonlocal current_member
        if current_code and current_member:
            memberships.setdefault(current_code, []).append(current_member)
        current_member = None

    for raw_line in text.splitlines():
        line = raw_line.rstrip()
        if not line or line.lstrip().startswith('#'):
            continue

        key_match = re.match(r'^([A-Z0-9]{4,6}):\s*$', line)
        if key_match:
            flush_member()
            current_code = key_match.group(1)
            continue

        if line.startswith('- name:'):
            flush_member()
            current_member = {'name': line.split(':', 1)[1].strip()}
            continue

        if current_member is None:
            continue

        stripped = line.strip()
        if ':' not in stripped:
            continue
        field, value = stripped.split(':', 1)
        current_member[field.strip()] = value.strip().strip("'").strip('"')

    flush_member()
    return memberships


def normalize_committee_code(raw_code: str) -> str:
    # congress-legislators main committees are 4 chars (e.g., SSFI) while DB stores ssfi00
    # subcommittees are already 6 chars (e.g., SSFI16) and map directly to ssfi16.
    code = (raw_code or '').strip().lower()
    if len(code) == 4:
        return f'{code}00'
    return code


async def fetch_committee_memberships():
    conn = await asyncpg.connect(DATABASE_URL)

    # Load existing bioguide IDs and committee IDs for matching
    legislators = await conn.fetch(
        'SELECT id, bioguide_id FROM legislators'
    )
    leg_map = {r['bioguide_id']: r['id'] for r in legislators}

    committees = await conn.fetch(
        'SELECT id, committee_id FROM committees'
    )
    com_map = {r['committee_id']: r['id'] for r in committees}

    print(f"Loaded {len(leg_map)} legislators, {len(com_map)} committees")

    inserted = 0
    skipped = 0
    saw_404 = False

    async with aiohttp.ClientSession() as session:
        for congress in [118, 119]:
            for chamber in ['senate', 'house']:
                offset = 0
                total = None

                pbar = tqdm(desc=f"{congress}th {chamber} committees")

                while True:
                    url = f"{BASE_URL}/committee-membership/{congress}"
                    params = {
                        'api_key': CONGRESS_API_KEY,
                        'limit': 250,
                        'offset': offset,
                        'chamber': chamber,
                    }

                    async with session.get(url, params=params) as resp:
                        if resp.status == 404:
                            print(f"  404 for {congress} {chamber}, skipping")
                            saw_404 = True
                            break
                        if resp.status != 200:
                            print(f"  HTTP {resp.status} for {congress} {chamber}")
                            break

                        data = await resp.json()

                    memberships = data.get('committeeMembership',
                                  data.get('committee_membership',
                                  data.get('members', [])))

                    if total is None:
                        total = data.get('pagination', {}).get('count', 0)
                        pbar.total = total

                    if not memberships:
                        break

                    rows = []
                    for m in memberships:
                        # Handle different response shapes
                        bioguide = (
                            m.get('bioguideId') or
                            m.get('member', {}).get('bioguideId') or
                            m.get('legislator', {}).get('bioguideId')
                        )
                        committee_id = (
                            m.get('committeeCode') or
                            m.get('committee', {}).get('systemCode') or
                            m.get('systemCode')
                        )
                        role = m.get('rank', {}).get('name', 'Member') if isinstance(m.get('rank'), dict) else m.get('rank', 'Member')

                        leg_id = leg_map.get(bioguide)
                        com_id = com_map.get(committee_id)

                        if leg_id and com_id:
                            rows.append((leg_id, com_id, role))
                        else:
                            skipped += 1

                    if rows:
                        await conn.executemany('''
                            INSERT INTO committee_memberships
                              (legislator_id, committee_id, role)
                            VALUES ($1, $2, $3)
                            ON CONFLICT (legislator_id, committee_id) DO NOTHING
                        ''', rows)
                        inserted += len(rows)

                    pbar.update(len(memberships))
                    offset += len(memberships)

                    if offset >= (total or 0) and total is not None:
                        break

                    await asyncio.sleep(0.2)
                
                pbar.close()

        # Fallback source if Congress API membership endpoint is unavailable.
        if inserted == 0 and saw_404:
            print("\nCongress API committee-membership endpoint unavailable; using fallback membership dataset.")
            async with session.get(LEGISLATORS_MEMBERSHIP_YAML_URL) as resp:
                if resp.status != 200:
                    raise RuntimeError(f"Failed to fetch fallback membership YAML: HTTP {resp.status}")
                yaml_text = await resp.text()

            parsed = parse_membership_yaml(yaml_text)
            fallback_rows = []
            fallback_skipped = 0
            for raw_committee_code, members in parsed.items():
                committee_code = normalize_committee_code(raw_committee_code)
                com_id = com_map.get(committee_code)
                if not com_id:
                    fallback_skipped += len(members or [])
                    continue
                for member in members or []:
                    bioguide = (member.get('bioguide') or '').strip()
                    leg_id = leg_map.get(bioguide)
                    if not leg_id:
                        fallback_skipped += 1
                        continue
                    role = (member.get('title') or 'Member').strip() or 'Member'
                    fallback_rows.append((leg_id, com_id, role))

            if fallback_rows:
                await conn.executemany('''
                    INSERT INTO committee_memberships
                      (legislator_id, committee_id, role)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (legislator_id, committee_id) DO NOTHING
                ''', fallback_rows)
                inserted += len(fallback_rows)
            skipped += fallback_skipped
            print(f"Fallback rows prepared: {len(fallback_rows)}")

    await conn.close()
    print(f"\nDone. Inserted: {inserted}, Skipped (no match): {skipped}")
    print("Final committee_memberships count:")

    conn2 = await asyncpg.connect(DATABASE_URL)
    count = await conn2.fetchval('SELECT count(*) FROM committee_memberships')
    print(f"  {count} rows")
    await conn2.close()


if __name__ == '__main__':
    asyncio.run(fetch_committee_memberships())
