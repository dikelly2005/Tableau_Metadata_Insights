# ==============================
# USER VARIABLES (EDIT HERE)
# ==============================

import os, tempfile

TOKEN_NAME = os.environ.get("TOKEN_NAME")             # Preferred variable names kept
TOKEN_SECRET = os.environ.get("TOKEN_SECRET")         # <<< Put your PAT secret here
SERVER_URL = 'https://YOUR_SERVER_INSTANCE.online.tableau.com' # Your Tableau Cloud server instance (example: "https://prod-useast-a.online.tableau.com").  For Tableau Cloud, the server address in the URI must contain the pod name, such as 10az, 10ay, or us-east-1.
SITE_CONTENT_URL = 'your-site-content-url' # Your Tableau Cloud site name or ID

# Output
CSV_FILE_NAME = "REST_db_conn_virtualConnections.csv"
SHARED_FOLDER = "C:/Path/To/Your/Shared/Folder"
OUTPUT_CSV_PATH = os.path.join(SHARED_FOLDER, CSV_FILE_NAME)

# Enhanced header order with more potential fields
OUTPUT_HEADERS = [
    "id",
    "virtualConnection.name",
    "connectionType",
    "serverName", 
    "serverPort",
    "userName",
    "site.id",
    "dbname",
    "connectionId",                    # Added connection ID
    "embedPassword",                   # Added password embedding flag
    "AdminInsightsPublishedAt"
]

# Enable debug mode to see what fields are available
DEBUG_MODE = False  # Set to True for debugging
SAVE_RAW_XML = False  # Set to True to save XML samples

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

def debug_print(msg: str):
    if DEBUG_MODE:
        print(f"[DEBUG] {msg}")    

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
# DATA RETRIEVAL - ENHANCED
# ==============================

def save_xml_sample(content, filename_prefix):
    """Save raw XML for debugging"""
    if SAVE_RAW_XML:
        try:
            sample_file = os.path.join(tempfile.gettempdir(), f"{filename_prefix}_sample.xml")
            with open(sample_file, 'w', encoding='utf-8') as f:
                f.write(content)
            debug_print(f"Saved XML sample to: {sample_file}")
        except Exception as e:
            debug_print(f"Could not save XML sample: {e}")

def extract_connection_attributes(conn_elem, namespaces):
    """Enhanced attribute extraction with comprehensive debugging"""
    debug_print("=" * 50)
    debug_print("RAW CONNECTION ELEMENT:")
    raw_xml = ET.tostring(conn_elem, encoding='unicode')
    debug_print(raw_xml)
    debug_print("=" * 50)
    
    attrs = {}
    
    # Get all attributes from the connection element
    debug_print("DIRECT ATTRIBUTES:")
    for key, value in conn_elem.attrib.items():
        attrs[key] = value
        debug_print(f"  {key} = '{value}'")
    
    if not attrs:
        debug_print("  NO DIRECT ATTRIBUTES FOUND!")
    
    # Check for nested elements that might contain connection info
    debug_print("CHILD ELEMENTS:")
    for child in conn_elem:
        tag = child.tag.split('}')[-1] if '}' in child.tag else child.tag
        debug_print(f"  Child: {tag}")
        
        # Show child attributes
        for attr_name, attr_value in child.attrib.items():
            debug_print(f"    {tag}.{attr_name} = '{attr_value}'")
            attrs[f"{tag}.{attr_name}"] = attr_value
        
        # Show child text
        if child.text and child.text.strip():
            debug_print(f"    {tag}.text = '{child.text.strip()}'")
            attrs[f"{tag}.text"] = child.text.strip()
            
        # Check grandchildren too
        for grandchild in child:
            gc_tag = grandchild.tag.split('}')[-1] if '}' in grandchild.tag else grandchild.tag
            debug_print(f"    Grandchild: {tag}.{gc_tag}")
            for gc_attr, gc_value in grandchild.attrib.items():
                debug_print(f"      {tag}.{gc_tag}.{gc_attr} = '{gc_value}'")
                attrs[f"{tag}.{gc_tag}.{gc_attr}"] = gc_value
            if grandchild.text and grandchild.text.strip():
                debug_print(f"      {tag}.{gc_tag}.text = '{grandchild.text.strip()}'")
                attrs[f"{tag}.{gc_tag}.text"] = grandchild.text.strip()
    
    if not any(child for child in conn_elem):
        debug_print("  NO CHILD ELEMENTS FOUND!")
    
    debug_print("FINAL EXTRACTED ATTRIBUTES:")
    for k, v in attrs.items():
        debug_print(f"  {k} = '{v}'")
    
    return attrs

def fetch_virtual_connection_details(api_version: str, site_id: str, token: str, vc_id: str):
    """Fetch detailed information about a specific virtual connection"""
    headers = {'X-Tableau-Auth': token}
    ns = {'t': 'http://tableau.com/api'}
    
    try:
        # Get full virtual connection details
        detail_url = f"{SERVER_URL}/api/{api_version}/sites/{site_id}/virtualconnections/{vc_id}"
        r = requests.get(detail_url, headers=headers)
        if r.status_code == 404:
            debug_print(f"Virtual connection {vc_id} details not found (404)")
            return {}
        r.raise_for_status()
        
        root = ET.fromstring(r.text)
        debug_print(f"Virtual connection detail XML: {ET.tostring(root, encoding='unicode')[:500]}...")
        
        # Extract additional details if available
        vc_elem = root.find('.//t:virtualConnection', ns)
        if vc_elem is not None:
            details = {}
            for attr, value in vc_elem.attrib.items():
                details[f"vc.{attr}"] = value
            return details
            
    except Exception as e:
        debug_print(f"Error fetching virtual connection details for {vc_id}: {e}")
    
    return {}

def fetch_rows(api_version: str, site_id: str, token: str):
    ns = {'t': 'http://tableau.com/api'}
    headers = {'X-Tableau-Auth': token}
    rows = []
    total_conn = 0
    empty_connections = 0

    # First, get all virtual connections
    parent_ids = []
    for root in paginate_xml(api_version, f"/sites/{site_id}/virtualconnections", token):
        parents = root.findall('.//t:virtualConnection', ns)
        for p in parents:
            vc_info = {
                'id': p.attrib.get('id'),
                'name': p.attrib.get('name', ''),
            }
            # Extract any additional virtual connection attributes
            for attr, value in p.attrib.items():
                if attr not in ['id', 'name']:
                    vc_info[f'vc.{attr}'] = value
            parent_ids.append(vc_info)

    print_progress(f"Discovered {len(parent_ids)} virtualConnections to fetch connections")

    for idx, vc_info in enumerate(parent_ids, start=1):
        vc_id = vc_info['id']
        vc_name = vc_info['name']
        
        # Get virtual connection details
        vc_details = fetch_virtual_connection_details(api_version, site_id, token, vc_id)
        
        # Get connections for this virtual connection
        url = f"{SERVER_URL}/api/{api_version}/sites/{site_id}/virtualconnections/{vc_id}/connections"
        debug_print(f"Fetching connections from: {url}")
        
        try:
            r = requests.get(url, headers=headers)
            if r.status_code == 404:
                debug_print(f"No connections found for virtual connection {vc_id} (404)")
                empty_connections += 1
                # Record virtual connection with no connections
                row = {
                    "id": vc_id,
                    "virtualConnection.name": vc_name,
                    "connectionType": "",
                    "serverName": "",
                    "serverPort": "",
                    "userName": "",
                    "site.id": site_id,
                    "dbname": "",
                    "connectionId": "",
                    "embedPassword": ""
                }
                row.update(vc_info)
                row.update(vc_details)
                rows.append(row)
                continue
                
            r.raise_for_status()
            debug_print(f"Response status: {r.status_code}")
            debug_print(f"Full response content for VC {vc_id}:")
            debug_print(r.text)
            
            # Save sample XML for inspection
            save_xml_sample(r.text, f"connections_{vc_id}")
            
        except Exception as e:
            debug_print(f"Error fetching connections for {vc_id}: {e}")
            continue

        root = ET.fromstring(r.text)
        conns = root.findall('.//t:connection', ns)
        debug_print(f"Found {len(conns)} connections for virtual connection {vc_id}")
        
        # Let's also check if connections are under different element names
        all_elements = root.findall('.//*')
        debug_print(f"All XML elements in response ({len(all_elements)} total):")
        for elem in all_elements:
            tag = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag
            attrs_str = ", ".join([f"{k}={v}" for k, v in elem.attrib.items()]) if elem.attrib else "no attributes"
            debug_print(f"  <{tag}> ({attrs_str})")

        if not conns:
            empty_connections += 1
            # Still record the virtualConnection with empty connection info
            row = {
                "id": vc_id,
                "virtualConnection.name": vc_name,
                "connectionType": "",
                "serverName": "",
                "serverPort": "",
                "site.id": site_id,
                "userName": "",
                "dbname": "",
                "connectionId": "",
                "embedPassword": ""
            }
            row.update(vc_info)
            row.update(vc_details)
            rows.append(row)
        else:
            for conn_idx, c in enumerate(conns):
                debug_print(f"Processing connection {conn_idx + 1}/{len(conns)} for VC {vc_id}")
                
                # Enhanced attribute extraction with full debugging
                conn_attrs = extract_connection_attributes(c, ns)
                
                # Build the row - let's preserve ALL found attributes
                row = {
                    "id": vc_id,
                    "virtualConnection.name": vc_name,
                    "site.id": site_id,
                }
                
                # Add all connection attributes with their original keys
                for key, value in conn_attrs.items():
                    row[f"raw.{key}"] = value
                
                # Map to expected fields using the ACTUAL attribute names from XML
                row["connectionType"] = conn_attrs.get("dbClass", "")  # dbClass contains the connection type
                row["serverName"] = conn_attrs.get("server", "")       # server (often empty for cloud connections)
                row["serverPort"] = conn_attrs.get("port", "")         # port
                row["userName"] = conn_attrs.get("username", "")       # username (lowercase!)
                row["dbname"] = ""  # Not available in this API response
                row["connectionId"] = conn_attrs.get("connectionId", "")  # connectionId
                row["embedPassword"] = ""  # Not available in this API response
                row["queryTaggingEnabled"] = ""  # Not available in this API response
                
                # Add virtual connection info and details
                row.update(vc_info)
                row.update(vc_details)
                
                rows.append(row)
                total_conn += 1

        if idx % 50 == 0:
            print_progress(f"Processed {idx}/{len(parent_ids)} virtual connections ...")

    print_progress(f"Total connections: {total_conn}")
    print_progress(f"Virtual connections with no connections: {empty_connections}")
    
    # Debug: Show sample of discovered fields
    if rows and DEBUG_MODE:
        sample_keys = set()
        for row in rows[:5]:  # Sample first 5 rows
            sample_keys.update(row.keys())
        debug_print(f"Sample of discovered fields: {sorted(sample_keys)}")
    
    return rows

# ==============================
# MAIN
# ==============================
def main():
    with ScriptTimer("REGEN_GET_db_connections_virtual_connection"):
        print_progress(f"Output path: {OUTPUT_CSV_PATH}")
        print_progress(f"Debug mode: {'ON' if DEBUG_MODE else 'OFF'}")
        
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
