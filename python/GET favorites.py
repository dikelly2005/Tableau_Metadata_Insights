# ==============================
# USER VARIABLES (EDIT HERE)
# ==============================

import os, tempfile

TOKEN_NAME = os.environ.get("TOKEN_NAME")             # Preferred variable names kept
TOKEN_SECRET = os.environ.get("TOKEN_SECRET")         # <<< Put your PAT secret here
SERVER_URL = 'https://YOUR_SERVER_INSTANCE.online.tableau.com' # Your Tableau Cloud server instance (example: "https://prod-useast-a.online.tableau.com").  For Tableau Cloud, the server address in the URI must contain the pod name, such as 10az, 10ay, or us-east-1.
SITE_CONTENT_URL = 'your-site-content-url' # Your Tableau Cloud site name or ID

# Output
CSV_FILE_NAME = "REST_user_favorites.csv"
SHARED_FOLDER = "C:/Path/To/Your/Shared/Folder"
OUTPUT_CSV_PATH = os.path.join(SHARED_FOLDER, CSV_FILE_NAME)

# Desired CSV header order (extracted from original script if available)
OUTPUT_HEADERS = [
    "User.Id",
    "Favorite Type",
    "project.id",
    "workbook.id",
    "view.id",
    'site.id',
    "datasource.id",
    "collection.id",
    "flow.id",
    "virtualConnection.id",
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

def fetch_rows(api_version: str, site_id: str, token: str):
    ns = {'t': 'http://tableau.com/api'}
    headers = {'X-Tableau-Auth': token}
    rows = []

    print("[INFO] Collecting licensed users...")
    users = []
    for root in paginate_xml(api_version, f"/sites/{site_id}/users", token):
        for u in root.findall('.//t:user', ns):
            site_role = u.attrib.get('siteRole', '')
            if site_role.lower() != "unlicensed":
                users.append({
                    "id": u.attrib.get("id"),
                    "name": u.attrib.get("name", ""),
                    "siteRole": site_role
                })

    print_progress(f"Discovered {len(users)} licensed users for favorites")

    total = 0
    for idx, u in enumerate(users, start=1):
        url = f"{SERVER_URL}/api/{api_version}/sites/{site_id}/favorites/{u['id']}"
        try:
            r = requests.get(url, headers=headers)
            if r.status_code == 404:
                print(f"[WARN] Favorites not found for user {u['name']} ({u['id']})")
                continue
            r.raise_for_status()
        except Exception as e:
            print(f"[WARN] Failed to fetch favorites for user {u['name']} ({u['id']}): {e}")
            continue

        root = ET.fromstring(r.text)

        for fav in root.findall('.//t:favorite', ns):
            row = {"user.id": u["id"]}

            # --- project (favorited directly) ---
            proj = fav.find('t:project', ns)
            if proj is not None and fav.find('t:workbook', ns) is None \
                                and fav.find('t:datasource', ns) is None \
                                and fav.find('t:view', ns) is None \
                                and fav.find('t:flow', ns) is None \
                                and fav.find('t:collection', ns) is None \
                                and fav.find('t:virtualConnection', ns) is None:
                row["favorite.type"] = "project"
                row["project.id"] = proj.attrib.get("id", "")
                row["site.id"] = site_id

            # --- workbook ---
            wb = fav.find('t:workbook', ns)
            if wb is not None:
                row["favorite.type"] = "workbook"
                row["workbook.id"] = wb.attrib.get("id", "")
                row["site.id"] = site_id
                p = wb.find('t:project', ns)
                if p is not None:
                    row["project.id"] = p.attrib.get("id", "")

            # --- view ---
            vw = fav.find('t:view', ns)
            if vw is not None:
                row["favorite.type"] = "view"
                row["view.id"] = vw.attrib.get("id", "")
                row["site.id"] = site_id
                wb = vw.find('t:workbook', ns)
                if wb is not None:
                    row["workbook.id"] = wb.attrib.get("id", "")
                    proj = wb.find('t:project', ns)
                    if proj is not None:
                        row["project.id"] = proj.attrib.get("id", "")
                    elif "projectId" in wb.attrib:
                        row["project.id"] = wb.attrib.get("projectId", "")
                    # Fallback: fetch workbook explicitly
                    elif row.get("workbook.id"):
                        wb_url = f"{SERVER_URL}/api/{api_version}/sites/{site_id}/workbooks/{row['workbook.id']}"
                        r_wb = requests.get(wb_url, headers={'X-Tableau-Auth': token})
                        if r_wb.status_code == 200:
                            wb_root = ET.fromstring(r_wb.text)
                            proj = wb_root.find('.//t:project', ns)
                            if proj is not None:
                                row["project.id"] = proj.attrib.get("id", "")

            # --- datasource ---
            ds = fav.find('t:datasource', ns)
            if ds is not None:
                row["favorite.type"] = "datasource"
                row["datasource.id"] = ds.attrib.get("id", "")
                row["site.id"] = site_id
                p = ds.find('t:project', ns)
                if p is not None:
                    row["project.id"] = p.attrib.get("id", "")

            # --- collection ---
            coll = fav.find('t:collection', ns)
            if coll is not None:
                row["favorite.type"] = "collection"
                row["collection.id"] = coll.attrib.get("id", "")
                row["site.id"] = site_id
                p = coll.find('t:project', ns)
                if p is not None:
                    row["project.id"] = p.attrib.get("id", "")

            # --- flow ---
            flow = fav.find('t:flow', ns)
            if flow is not None:
                row["favorite.type"] = "flow"
                row["flow.id"] = flow.attrib.get("id", "")
                row["site.id"] = site_id
                p = flow.find('t:project', ns)
                if p is not None:
                    row["project.id"] = p.attrib.get("id", "")

            # --- virtual connection ---
            vc = fav.find('t:virtualConnection', ns)
            if vc is not None:
                row["favorite.type"] = "virtualConnection"
                row["virtualConnection.id"] = vc.attrib.get("id", "")
                row["site.id"] = site_id
                p = vc.find('t:project', ns)
                if p is not None:
                    row["project.id"] = p.attrib.get("id", "")

            rows.append(row)
            total += 1

        if idx % 50 == 0:
            print_progress(f"Processed favorites for {idx}/{len(users)} users")

    print_progress(f"Total favorite rows collected: {total}")
    return rows

# ==============================
# MAIN
# ==============================
def main():
    with ScriptTimer("REGEN_GET_user_favorites"):
        print_progress(f"Output path: {OUTPUT_CSV_PATH}")
        api = get_latest_api_version()
        token, site_id = sign_in(api)

        try:
            rows = fetch_rows(api, site_id, token)
            if rows:
                print_progress(f"Writing {len(rows)} favorites to CSV")
            else:
                print("[WARN] No favorites returned — CSV will only include AdminInsightsPublishedAt")
            write_csv(rows, OUTPUT_CSV_PATH, desired_headers=OUTPUT_HEADERS if OUTPUT_HEADERS else None)
        finally:
            sign_out(api, token)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(1)
