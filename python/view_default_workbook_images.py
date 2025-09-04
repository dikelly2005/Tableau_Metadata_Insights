# ==============================
# USER VARIABLES (EDIT HERE)
# ==============================

import os, tempfile

TOKEN_NAME = os.environ.get("TOKEN_NAME")             # Preferred variable names kept
TOKEN_SECRET = os.environ.get("TOKEN_SECRET")         # <<< Put your PAT secret here
SERVER_URL = 'https://YOUR_SERVER_INSTANCE.online.tableau.com' # Your Tableau Cloud server instance (example: "https://prod-useast-a.online.tableau.com").  For Tableau Cloud, the server address in the URI must contain the pod name, such as 10az, 10ay, or us-east-1.
SITE_CONTENT_URL = 'your-site-content-url' # Your Tableau Cloud site name or ID

# Image configuration
IMAGE_FOLDER = 'your-image-repository-path' #where you are storing image files. For Image URL to work in Tableau, you cannot have spaces 
IMAGE_FORMAT = 'png'  # Options: png, pdf (png recommended for previews)
IMAGE_RESOLUTION = 'high'  # Options: high, standard
MAX_AGE = 1  # Cache age in minutes (1 = fresh images)

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
# DATA RETRIEVAL FUNCTIONS
# ==============================

def extract_all_data_from_element(element, ns):
    """Extract all data from an XML element including attributes and child elements"""
    data = {}
    
    # Get all attributes
    for key, value in element.attrib.items():
        data[key] = value
    
    # Get child elements
    for child in element:
        tag = child.tag.split('}')[-1]  # Remove namespace
        
        if child.attrib:
            # If child has attributes, create nested structure
            if tag not in data:
                data[tag] = {}
            for key, value in child.attrib.items():
                data[f"{tag}_{key}"] = value
        
        if child.text and child.text.strip():
            data[f"{tag}_text"] = child.text.strip()
    
    return data

def get_all_workbooks(api_version: str, token: str, site_id: str):
    """Retrieve all workbooks from the site with their default view IDs"""
    workbooks = []
    ns = {'t': 'http://tableau.com/api'}

    try:
        for root in paginate_xml(api_version, f"/sites/{site_id}/workbooks", token):
            workbook_elements = root.findall('.//t:workbook', ns)
            
            for wb in workbook_elements:
                # Extract all data from the workbook element
                workbook_info = extract_all_data_from_element(wb, ns)
                workbooks.append(workbook_info)
            
            print_progress(f"Retrieved {len(workbook_elements)} workbooks from current page")
            
    except Exception as e:
        print(f"[ERROR] Error fetching workbooks: {e}")
        raise
    
    print_progress(f"Total workbooks retrieved: {len(workbooks)}")
    return workbooks

def download_view_image(api_version: str, token: str, site_id: str, view_id: str, workbook_id: str):
    """Download the image for a specific view"""
    url = f"{SERVER_URL}/api/{api_version}/sites/{site_id}/workbooks/{workbook_id}/views/{view_id}/previewImage"
    headers = {'X-Tableau-Auth': token}
    
    params = {
        'resolution': IMAGE_RESOLUTION,
        'maxAge': MAX_AGE
    }
    
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        
        content_type = response.headers.get('content-type', '')
        if 'png' in content_type.lower():
            file_extension = 'png'
        elif 'pdf' in content_type.lower():
            file_extension = 'pdf'
        elif 'jpeg' in content_type.lower() or 'jpg' in content_type.lower():
            file_extension = 'jpg'
        else:
            file_extension = IMAGE_FORMAT
        
        filename = f"{view_id}.{file_extension}"
        
        # Ensure image folder exists
        os.makedirs(IMAGE_FOLDER, exist_ok=True)
        filepath = os.path.join(IMAGE_FOLDER, filename)
        
        with open(filepath, 'wb') as f:
            f.write(response.content)
        
        file_size = len(response.content)
        
        return {
            'success': True,
            'filename': filename,
            'filepath': filepath,
            'file_size': file_size,
            'content_type': content_type
        }
        
    except Exception as e:
        print_progress(f"Error downloading image for view {view_id}: {e}")
        return {
            'success': False,
            'error': str(e),
            'filename': None,
            'filepath': None,
            'file_size': None,
            'content_type': None
        }

def create_safe_filename(text, max_length=50):
    """Create a safe filename from text"""
    if not text:
        return "unknown"
    
    # Remove or replace unsafe characters
    safe_text = re.sub(r'[<>:"/\\|?*]', '_', str(text))
    safe_text = re.sub(r'\s+', '_', safe_text)  # Replace spaces with underscores
    safe_text = safe_text.strip('._')  # Remove leading/trailing dots and underscores
    
    # Limit length
    if len(safe_text) > max_length:
        safe_text = safe_text[:max_length]
    
    return safe_text if safe_text else "unknown"

# ==============================
# MAIN SCRIPT
# ==============================

def main():
    with ScriptTimer("Tableau REST API Image Download"):
        print_progress(f"Images will be saved to: {IMAGE_FOLDER}")
        print_progress(f"Image format: {IMAGE_FORMAT}, Resolution: {IMAGE_RESOLUTION}")
        
        # Ensure output directories exist
        os.makedirs(IMAGE_FOLDER, exist_ok=True)
        
        # Get API version and authenticate
        api_version = get_latest_api_version()
        token, site_id = sign_in(api_version)
        
        try:
            # Get all workbooks
            print_progress("Fetching all workbooks...")
            workbooks = get_all_workbooks(api_version, token, site_id)
            print_progress(f"Found {len(workbooks)} workbooks.")
            
            # Process each workbook and download its default view image
            print_progress("Processing workbooks and downloading default view images...")
            image_mapping_data = []
            successful_downloads = 0
            failed_downloads = 0
            
            for i, wb in enumerate(workbooks, 1):
                workbook_name = wb.get('name', 'Unknown')
                workbook_id = wb.get('id', '')
                default_view_id = wb.get('defaultViewId', '')
                
                print_progress(f"Processing workbook {i}/{len(workbooks)}: {workbook_name}")
                                
                if default_view_id:
                    # Download the default view image
                    download_result = download_view_image(api_version, token, site_id, default_view_id, workbook_id)
                    
                    if download_result['success']:
                        successful_downloads += 1
                        print_progress(f"  ✓ Downloaded image: {download_result['filename']}")
                    else:
                        failed_downloads += 1
                        print_progress(f"  ✗ Failed to download image: {download_result['error']}")
                else:
                    print_progress(f"  ⚠ No default view ID found")
                
            
            print_progress(f"Image download summary:")
            print_progress(f"  ✓ Successful downloads: {successful_downloads}")
            print_progress(f"  ✗ Failed downloads: {failed_downloads}")
                
        finally:
            # Sign out
            sign_out(api_version, token)
        
        print_progress("Script completed successfully!")

if __name__ == '__main__':
    main()
