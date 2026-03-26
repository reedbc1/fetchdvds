import requests
import logging
import re

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

CONFIG = {"searchText":"*","pageSize":1}

def fetch_bibs(pageNum=0, get_pages=False):
    logger.info(f"Fetching bibs with parameters searchText:{CONFIG.get("searchText")} and pageSize:{CONFIG.get("pageSize")}")

    url = "https://na2.iiivega.com/api/search-result/search/format-groups"

    headers= {
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-US,en;q=0.9",
        "anonymous-user-id": "86d9e401-ea99-4bc3-a5aa-08a9a00a9e0e",
        "api-version": "2",
        "content-type": "application/json",
        "iii-customer-domain": "slouc.na2.iiivega.com",
        "iii-host-domain": "slouc.na2.iiivega.com",
        "priority": "u=1, i",
        "sec-ch-ua": "Chromium;v=146, Not-A.Brand;v=24, Google Chrome;v=146",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "Windows",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "Referer": "https://slouc.na2.iiivega.com/"
    }

    payload = {"searchText":CONFIG.get("searchText",""),"sorting":"relevance","sortOrder":"asc","searchType":"everything","pageNum":pageNum,"pageSize":CONFIG.get("pageSize",""),"resourceType":"FormatGroup"}

    response = requests.post(url=url, headers=headers, json=payload)
    response.raise_for_status()
    records = response.json()

    if get_pages:
        total_pages = records.get('totalPages')
        return total_pages

    parsed = []
    ids = set()

    for r in records.get('data'):
        parsed.append(
            (
                r.get("id"),
                r.get('title'),
                r.get('publicationDate'),
                r.get('coverUrl', {}).get('medium'),
                r.get('materialTabs', [])[0].get('editions', [])[0].get('id')
            )
        )
        ids.add(r.get("id"))
    return parsed, ids

def fetch_edition(id):
    url = f"https://na2.iiivega.com/api/search-result/editions/{id}"
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-US,en;q=0.9",
        "anonymous-user-id": "8f979f0f-2cda-46d7-9da5-4c3dddec18b0",
        "api-version": "1",
        "iii-customer-domain": "slouc.na2.iiivega.com",
        "iii-host-domain": "slouc.na2.iiivega.com",
        "priority": "u=1, i",
        "sec-ch-ua": "Chromium;v=146, Not-A.Brand;v=24, Google Chrome;v=146",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "Referer": "https://slouc.na2.iiivega.com/"
    }

    response = requests.get(url=url, headers=headers)
    response.raise_for_status()

    data = response.json()
    e = data.get("edition", {})

    # create subjects string
    for k, v in e.items():
        subjects_str = ""
        if re.match("subj", k):
            subjects_str += v + ", "
        
    edition_info = (
        id,
        e.get("author"),
        e.get("itemLanguage"),
        subjects_str,
        e.get("summary")
    )

    return edition_info

def fetch_all_editions(edition_ids):
    editions = []
    for id in edition_ids:
        editions.append(fetch_edition(id))
    return editions
    
if __name__ == "__main__":
    results = fetch_bibs()
    print(results)
