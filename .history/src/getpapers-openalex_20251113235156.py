import requests

BASE_URL = "https://api.openalex.org/works"
FILTERS = (
    "type:journal-article"
    ",host_venue.display_name.search:Journal of Finance"
    ",from_publication_date:2024-01-01"
    ",to_publication_date:2024-12-31"
)

def fetch_articles():
    cursor = "*"
    while cursor:
        params = {"filter": FILTERS, "per-page": 200, "cursor": cursor}
        resp = requests.get(BASE_URL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        for work in data.get("results", []):
            yield {
                "id": work.get("id"),
                "title": work.get("title"),
                "publication_date": work.get("publication_date"),
                "doi": work.get("doi"),
                "authors": [
                    {
                        "name": auth.get("author", {}).get("display_name"),
                        "orcid": auth.get("author", {}).get("orcid"),
                        "institutions": [
                            inst.get("display_name") for inst in auth.get("institutions", [])
                        ],
                    }
                    for auth in work.get("authorships", [])
                ],
            }
        cursor = data.get("meta", {}).get("next_cursor")

if __name__ == "__main__":
    for article in fetch_articles():
        print(article)