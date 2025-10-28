import gzip
from datetime import datetime
from typing import List, Dict
import re, os


def parse_pageview_line(line: str):
    # expected: domain page_title view_count response_size
    parts = line.strip().split()
    if len(parts) < 4:
        return None
    domain, page_title, view_count, response_size = parts[0], parts[1], parts[2], parts[3]
    try:
        view_count = int(view_count)
        response_size = int(response_size)
    except ValueError:
        return None
    return {
        "domain": domain,
        "page_title": page_title,
        "view_count": view_count,
        "response_size": response_size
    }

def extract_companies_from_gz(gz_path: str, companies: List[str], match_mode: str = "Exact") -> List[Dict]:

    results = []
    basename = os.path.basename(gz_path)
    m = re.match(r"pageviews-(\d{8})-(\d{2})0000\.gz", basename)
    if m:
        datepart = m.group(1)
        hourpart = m.group(2)
        hour_ts = datetime.strptime(f"{datepart}{hourpart}", "%Y%m%d%H")
    else:
        hour_ts = datetime.utcnow()

    with gzip.open(gz_path, "rt", encoding="utf-8", errors="ignore") as fh:
        for line in fh:
            parsed = parse_pageview_line(line)
            if not parsed:
                continue
            title = parsed["page_title"]
            if match_mode == "Exact":
                if title in companies or title.replace(" ", "_") in companies:
                    record = parsed.copy()
                    record["hour_timestamp"] = hour_ts
                    results.append(record)
            else:  # Contains
                low = title.lower()
                if any(c.lower() in low for c in companies):
                    record = parsed.copy()
                    record["hour_timestamp"] = hour_ts
                    results.append(record)
    return results


def extract_for_dag(gz_path):
    return extract_companies_from_gz(
        gz_path, companies=["Amazon", "Apple", "Facebook", "Google", "Microsoft"], match_mode="Exact"
    )
