# ==============================
# USER VARIABLES (EDIT HERE)
# ==============================

import os, tempfile

TOKEN_NAME = os.environ.get("TOKEN_NAME")             # Preferred variable names kept
TOKEN_SECRET = os.environ.get("TOKEN_SECRET")         # <<< Put your PAT secret here
SERVER_URL = 'https://YOUR_SERVER_INSTANCE.online.tableau.com' # Your Tableau Cloud server instance (example: "https://prod-useast-a.online.tableau.com").  For Tableau Cloud, the server address in the URI must contain the pod name, such as 10az, 10ay, or us-east-1.
SITE_CONTENT_URL = 'your-site-content-url' # Your Tableau Cloud site name or ID

# Output
CSV_FILE_NAME = "REST_personal_access_tokens.csv"
SHARED_FOLDER = "C:/Path/To/Your/Shared/Folder"
OUTPUT_CSV_PATH = os.path.join(SHARED_FOLDER, CSV_FILE_NAME)

# Desired CSV header order (extracted from original script if available)
OUTPUT_HEADERS = [
    "userLuid",
    "tokenName",
    "tokenGuid",
    'site.id',
    "lastUsedAt",
    "expiresAt",
    "AdminInsightsPublishedAt"
]

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
    'id': 'LUID', 'url': 'URL', 'uri': 'URI',
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

def _join(prefix: str, key: str) -> str:
    return f"{prefix}.{key}" if prefix else key

def flatten_record(obj, prefix=""):
    out = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            out.update(flatten_record(v, _join(prefix, str(k))))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            out.update(flatten_record(v, _join(prefix, f"[{i}]")))
    else:
        out[prefix] = obj
    return out

def flatten_xml_element(elem: ET.Element, ns=None, prefix: str = ""):
    ns = ns or {}
    out = {}
    for k, v in elem.attrib.items():
        out[_join(prefix, k)] = v
    text = (elem.text or "").strip()
    if text and not list(elem):
        out[_join(prefix, "text")] = text
    for child in list(elem):
        tag = child.tag.split('}')[-1]
        out.update(flatten_xml_element(child, ns, _join(prefix, tag)))
    return out

# Normalize header/keys for mapping
def _norm(s: str) -> str:
    s_lower = re.sub(r"[^a-z0-9]+", "", s.lower())
    # Map LUID back to id
    if s_lower == "luid":
        return "id"
    return s_lower

# ==============================
# WRITE CSV
# ==============================

def write_csv(rows, path, desired_headers=None):
    rows = rows or []
    published_at = now_utc_iso()
    for r in rows:
        r["AdminInsightsPublishedAt"] = published_at
    if not rows:
        rows = [{"AdminInsightsPublishedAt": published_at}]

    # If caller provided an explicit set/order of headers (from original scripts),
    # enforce that order and add AdminInsightsPublishedAt at the end if not present.
    if desired_headers:
        # Enforce Title Case + special cases
        headers_tc = [title_case(h) for h in desired_headers]
        if "AdminInsightsPublishedAt" not in headers_tc:
            headers_tc.append("AdminInsightsPublishedAt")
        fieldnames = headers_tc

        # Build mapping from normalized header to actual row keys
        # For each row, try to match by normalization
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
# PAGINATION
# ==============================

def paginate_xml(api_version: str, path: str, token: str, page_size: int = 1000):
    page = 1
    ns = {'t': 'http://tableau.com/api'}
    headers = {'X-Tableau-Auth': token}
    while True:
        url = f"{SERVER_URL}/api/{api_version}{path}?pageSize={page_size}&pageNumber={page}"
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        root = ET.fromstring(r.text)
        yield root
        p = root.find('.//t:pagination', ns)
        if p is not None:
            total = int(p.attrib.get('totalAvailable', '0'))
            size = int(p.attrib.get('pageSize', str(page_size)))
            number = int(p.attrib.get('pageNumber', str(page)))
            if number * size >= total:
                break
        else:
            break
        page += 1

# ==============================
# DATA RETRIEVAL
# ==============================

def get_users(api_version: str, token: str, site_id: str):
    """Get all licensed users from the site (excludes Unlicensed users)."""
    users_map = {}
    ns = {'t': 'http://tableau.com/api'}
    total_users = 0
    licensed_users = 0
    
    print_progress("Fetching users...")
    for root in paginate_xml(api_version, f"/sites/{site_id}/users", token):
        users = root.findall('.//t:user', ns)
        for user in users:
            total_users += 1
            user_luid = user.attrib.get('id', '')
            full_name = user.attrib.get('fullName', '')
            username = user.attrib.get('name', '')
            site_role = user.attrib.get('siteRole', '')
            
            # Filter out unlicensed users
            if site_role.lower() != 'unlicensed':
                licensed_users += 1
                users_map[user_luid] = {
                    'fullName': full_name,
                    'username': username,
                    'siteRole': site_role
                }
    
    print_progress(f"Found {total_users} total users, {licensed_users} licensed users")
    return users_map

def get_user_personal_access_tokens(api_version: str, token: str, site_id: str, user_luid: str):
    """Retrieve personal access tokens for a specific user"""
    url = f"{SERVER_URL}/api/{api_version}/sites/{site_id}/users/{user_luid}/personal-access-tokens"
    headers = {'X-Tableau-Auth': token}
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        root = ET.fromstring(response.text)
        ns = {'t': 'http://tableau.com/api'}
        tokens = root.findall('.//t:personalAccessToken', ns)
        
        token_list = []
        for pat in tokens:
            # Use the actual XML attributes from the response
            token_info = {
                'tokenName': pat.attrib.get('tokenName', ''),
                'tokenGuid': pat.attrib.get('tokenGuid', ''),
                'site.id': site_id,
                'lastUsedAt': pat.attrib.get('lastUsedAt', ''),
                'expiresAt': pat.attrib.get('expiresAt', '')
            }
            token_list.append(token_info)
        
        return token_list
    
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            # User has no personal access tokens or user doesn't exist
            return []
        elif e.response.status_code == 403:
            # Insufficient permissions
            print_progress(f"Warning: Insufficient permissions to access PATs for user {user_luid}")
            return []
        else:
            print_progress(f"Error fetching PATs for user {user_luid}: {e}")
            return []
    except Exception as e:
        print_progress(f"Unexpected error fetching PATs for user {user_luid}: {e}")
        return []

def fetch_rows(api_version: str, site_id: str, token: str):
    """Fetch personal access tokens for all users."""
    users_map = get_users(api_version, token, site_id)
    print_progress(f"Retrieved {len(users_map)} users.")
    
    all_content = []

    # Fetch PATs for each user
    print_progress("Fetching Personal Access Tokens for all users...")
    for user_luid, user_info in users_map.items():
        full_name = user_info['fullName']
        username = user_info['username']
        site_role = user_info['siteRole']
        
        print_progress(f"Processing user: {full_name} ({username}) - {site_role}")
        user_pats = get_user_personal_access_tokens(api_version, token, site_id, user_luid)
        
        if user_pats:
            for pat in user_pats:
                pat_record = {
                    'userLuid': user_luid,
                    'tokenName': pat['tokenName'],
                    'tokenGuid': pat['tokenGuid'],
                    'site.id': site_id,
                    'lastUsedAt': pat['lastUsedAt'],
                    'expiresAt': pat['expiresAt']
                }
                all_content.append(pat_record)

    print_progress(f"Found {len(all_content)} Personal Access Tokens from {len(set(r['userLuid'] for r in all_content))} users.")
    return all_content

# ==============================
# MAIN
# ==============================
def main():
    with ScriptTimer("REGEN_GET_personal_access_tokens"):
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
