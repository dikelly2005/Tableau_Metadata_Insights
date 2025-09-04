# ==============================
# USER VARIABLES (EDIT HERE)
# ==============================

import os

TOKEN_NAME = os.environ.get("TOKEN_NAME")             # Your PAT token name
TOKEN_SECRET = os.environ.get("TOKEN_SECRET")         # Your PAT secret
SERVER_URL = 'https://YOUR_SERVER_INSTANCE.online.tableau.com' # Your Tableau Cloud server instance (example: "https://prod-useast-a.online.tableau.com").  For Tableau Cloud, the server address in the URI must contain the pod name, such as 10az, 10ay, or us-east-1.
SITE_CONTENT_URL = 'your-site-content-url' # Your Tableau Cloud site name or ID

# Output
CSV_FILE_NAME = "REST_db_connection_datasource_details.csv"
SHARED_FOLDER = "C:/Path/To/Your/Shared/Folder"
OUTPUT_CSV_PATH = os.path.join(SHARED_FOLDER, CSV_FILE_NAME)

# Desired CSV header order (leave empty to auto-discover)
OUTPUT_HEADERS = [
    'datasource_id', 'datasource_name', 'datasource_hasExtracts', 'datasource_extractLastRefreshTime',
    'table_id', 'table_name', 'table_schema', 'table_fullName', 'site_id',
    'database_id', 'database_name', 'database_connectionType'
]

# GraphQL Query
GRAPHQL_QUERY = """
query DatasourceConnections {
  datasources {
    id
    name
    hasExtracts
    extractLastRefreshTime
    upstreamDatabases {
      id
      name
      connectionType
    }
    upstreamTables {
      id
      name
      schema
      fullName
      database {
        id
        name
        connectionType
      }
    }
  }
}
"""

# ==============================
# LIBRARIES
# ==============================

import csv
import json
import re
import sys
import time
from datetime import datetime, timezone
import requests
import xml.etree.ElementTree as ET

# ==============================
# TIMER CLASS
# ==============================

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

class ScriptTimer:
    def __init__(self, label="Script"):
        self.label = label
        self.start = None
    def __enter__(self):
        self.start = datetime.now(timezone.utc)
        print(f"[INFO] {self.label} start (UTC): {self.start.isoformat()}")
        return self
    def __exit__(self, exc_type, exc, tb):
        end = datetime.now(timezone.utc)
        dur = (end - self.start).total_seconds() if self.start else 0.0
        print(f"[INFO] {self.label} end   (UTC): {end.isoformat()}")
        print(f"[INFO] Duration: {dur:.2f} seconds")

# ==============================
# LOGGING
# ==============================

def print_progress(msg: str):
    print(f"[PROGRESS] {msg}")

# ==============================
# FLATTEN HELPERS
# ==============================    

_splitter = re.compile(r"[^A-Za-z0-9]+")
special_cases = {
    'id': 'ID', 'url': 'URL', 'uri': 'URI',
    'api': 'API', 'html': 'HTML', 'xml': 'XML',
    'pdf': 'PDF', 'csv': 'CSV'
}

def title_case(key: str) -> str:
    if not key:
        return key
    key_lower = key.lower()
    if key_lower in special_cases:
        return special_cases[key_lower]
    parts = [p for p in _splitter.split(key) if p]
    return " ".join([p[:1].upper() + p[1:] for p in parts])

# Normalize header/keys for mapping
def _norm(s: str) -> str:
    s_lower = re.sub(r"[^a-z0-9]+", "", s.lower())
    # Map ID back to id
    if s_lower == "id":
        return "id"
    return s_lower

# ==============================
# WRITE CSV
# ==============================

def write_csv(rows, path, desired_headers=None):
    print_progress(f"write_csv called with {len(rows) if rows else 0} rows")
    print_progress(f"Target path: {path}")
    
    rows = rows or []
    published_at = now_utc_iso()
    for r in rows:
        r["AdminInsightsPublishedAt"] = published_at
    if not rows:
        rows = [{"AdminInsightsPublishedAt": published_at}]
        print_progress("No data rows, creating placeholder row")

    # If caller provided an explicit set/order of headers (from original scripts),
    # enforce that order and add AdminInsightsPublishedAt at the end if not present.
    if desired_headers:
        # Enforce Title Case + special cases
        headers_tc = [title_case(h) for h in desired_headers]
        if "AdminInsightsPublishedAt" not in headers_tc:
            headers_tc.append("AdminInsightsPublishedAt")
        fieldnames = headers_tc

        # Build mapping from normalized header to actual row keys
        print_progress(f"Writing CSV with explicit headers ({len(fieldnames)} columns) → {path}")
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            w.writeheader()
            for r in rows:
                # Try to find value for each header by normalization
                mapped = {}
                source_keys = list(r.keys())
                norm_index = {_norm(k): k for k in source_keys}
                for h in fieldnames:
                    if h == "AdminInsightsPublishedAt":
                        mapped[h] = r.get("AdminInsightsPublishedAt", "")
                    else:
                        # try exact
                        if h in r:
                            mapped[h] = r[h]
                        else:
                            # normalized match
                            nk = norm_index.get(_norm(h))
                            if nk and nk in r:
                                mapped[h] = r[nk]
                            else:
                                mapped[h] = ""
                w.writerow(mapped)
        print_progress(f"Wrote {len(rows)} rows")
        return

    # Fallback: discover keys and title-case
    keys = set()
    for r in rows:
        keys.update(r.keys())
    header_map = {k: title_case(k) for k in sorted(keys)}
    fieldnames = [header_map[k] for k in sorted(keys)]
    if "AdminInsightsPublishedAt" not in fieldnames:
        fieldnames.append("AdminInsightsPublishedAt")

    print_progress(f"Writing CSV → {path}")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            row_out = {header_map.get(k, k): r.get(k, "") for k in r}
            if "AdminInsightsPublishedAt" not in row_out:
                row_out["AdminInsightsPublishedAt"] = published_at
            w.writerow(row_out)
    print_progress(f"Wrote {len(rows)} rows")

# ==============================
# AUTH & API VERSION & SIGNOUT
# ==============================

def get_latest_api_version() -> str:
    url = f"{SERVER_URL}/api/3.21/serverinfo"
    r = requests.get(url)
    r.raise_for_status()
    root = ET.fromstring(r.text)
    api = root.find('.//t:restApiVersion', {'t': 'http://tableau.com/api'}).text
    print_progress(f"Using API version: {api}")
    return api

def sign_in(api_version: str):
    signin_url = f"{SERVER_URL}/api/{api_version}/auth/signin"
    body = '''
    <tsRequest>
      <credentials personalAccessTokenName="{TOKEN_NAME}" personalAccessTokenSecret="{TOKEN_SECRET}">
        <site contentUrl="{SITE_CONTENT_URL}"/>
      </credentials>
    </tsRequest>
    '''.strip()
    body = body.replace("{TOKEN_NAME}", TOKEN_NAME).replace("{TOKEN_SECRET}", TOKEN_SECRET).replace("{SITE_CONTENT_URL}", SITE_CONTENT_URL)
    r = requests.post(signin_url, data=body, headers={'Content-Type': 'application/xml'})
    r.raise_for_status()
    root = ET.fromstring(r.text)
    token = root.find('.//t:credentials', {'t': 'http://tableau.com/api'}).attrib['token']
    site_id = root.find('.//t:site', {'t': 'http://tableau.com/api'}).attrib['id']
    print_progress(f"Authenticated. Site ID: {site_id}")
    return token, site_id

def sign_out(api_version: str, token: str):
    try:
        url = f"{SERVER_URL}/api/{api_version}/auth/signout"
        requests.post(url, headers={'X-Tableau-Auth': token}).raise_for_status()
        print_progress("Signed out.")
    except Exception as e:
        print(f"[WARN] Sign out error: {e}")

# ==============================
# GRAPHQL API CALLS
# ==============================

def execute_graphql_query(query: str, token: str) -> dict:
    """Execute GraphQL query against Tableau's metadata API."""
    url = f"{SERVER_URL}/api/metadata/graphql"
    headers = {
        'X-Tableau-Auth': token,
        'Content-Type': 'application/json'
    }
    
    payload = {
        'query': query
    }
    
    print_progress("Executing GraphQL query...")
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    
    result = response.json()
    
    # Check for GraphQL errors, but be permissive for obfuscation warnings
    if 'errors' in result:
        error_messages = []
        for err in result['errors']:
            msg = err.get('message', 'Unknown error')
            error_messages.append(msg)
            # Log warnings but don't fail for obfuscation messages
            if 'obfuscation' in msg.lower():
                print_progress(f"Warning: {msg}")
            else:
                # Only raise for non-obfuscation errors
                raise Exception(f"GraphQL error: {msg}")
    
    # Check if we got data despite warnings
    if 'data' not in result or result['data'] is None:
        raise Exception("No data returned from GraphQL query")
    
    print_progress("GraphQL query executed successfully")
    return result

# ==============================
# DATA FLATTENING
# ==============================

def flatten_datasource_connections(graphql_response: dict) -> list:
    """
    Create a table-centric flat structure:
    Each row = one upstream table + its parent database + datasource info
    """
    flattened_rows = []

    datasources = graphql_response.get('data', {}).get('datasources', [])
    print_progress(f"Processing {len(datasources)} datasources (table-centric mode)")

    for i, datasource in enumerate(datasources):
        print_progress(f"Processing datasource {i+1}/{len(datasources)}: {datasource.get('name', 'Unnamed')}")

        base_info = {
            'datasource_id': datasource.get('id', ''),
            'datasource_name': datasource.get('name', ''),
            'datasource_hasExtracts': str(datasource.get('hasExtracts', '')),
            'datasource_extractLastRefreshTime': datasource.get('extractLastRefreshTime', '')
        }

        upstream_tables = datasource.get('upstreamTables', [])
        print_progress(f"  Found {len(upstream_tables)} upstream tables")

        if upstream_tables:
            for table in upstream_tables:
                db = table.get('database') or {}
                row = {
                    **base_info,
                    'table_id': table.get('id', ''),
                    'table_name': table.get('name', ''),
                    'table_schema': table.get('schema', ''),
                    'table_fullName': table.get('fullName', ''),
                    'database_id': db.get('id', ''),
                    'database_name': db.get('name', ''),
                    'database_connectionType': db.get('connectionType', '')
                }
                flattened_rows.append(row)
        else:
            # No upstream tables; create a minimal row with empty table/db fields
            row = {
                **base_info,
                'table_id': '',
                'table_name': '',
                'table_schema': '',
                'table_fullName': '',
                'database_id': '',
                'database_name': '',
                'database_connectionType': ''
            }
            flattened_rows.append(row)

    print_progress(f"Flattened to {len(flattened_rows)} table-centric rows")
    return flattened_rows

# ==============================
# DATA RETRIEVAL
# ==============================

def fetch_rows(api_version: str, site_id: str, token: str):
    """Fetch datasource connections data via GraphQL."""
    try:
        # Execute GraphQL query
        graphql_response = execute_graphql_query(GRAPHQL_QUERY, token)
        
        # Debug: Show raw response structure
        print_progress(f"Raw GraphQL response has {len(str(graphql_response))} characters")
        
        # Flatten the response
        rows = flatten_datasource_connections(graphql_response)

        # Add site_id to each row
        for row in rows:
            row['site_id'] = site_id        
        
        print_progress(f"Final flattened rows count: {len(rows)}")
        if rows:
            print_progress(f"NEW Sample row keys: {list(rows[0].keys())}")
            print_progress(f"NEW Sample row values: {rows[0]}")
        
        return rows
        
    except Exception as e:
        print_progress(f"Error fetching data: {e}")
        raise

# ==============================
# MAIN
# ==============================
def main():
    with ScriptTimer("GraphQL_Datasource_Connections"):
        print_progress(f"Output path: {OUTPUT_CSV_PATH}")
        api = get_latest_api_version()
        token, site_id = sign_in(api)

        try:
            rows = fetch_rows(api, site_id, token)
            write_csv(rows, OUTPUT_CSV_PATH, desired_headers=OUTPUT_HEADERS if OUTPUT_HEADERS else None)
        finally:
            sign_out(api, token)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(1)
