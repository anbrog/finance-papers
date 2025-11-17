import requests

BASE_URL = "https://api.openalex.org/works"
# Using primary_location.source.id for The Journal of Finance
# Source ID: https://openalex.org/S5353659
FILTERS = (
    "type:journal-article"
    ",primary_location.source.id:S5353659"
    ",from_publication_date:2024-01-01"
    ",to_publication_date:2024-12-31"
)

def fetch_articles():
    cursor = "*"
    while cursor:
        params = {"filter": FILTERS, "per-page": 200, "cursor": cursor}
        # Optional: include a mailto to be a good API citizen (read from env if set)
        import os
        mailto = os.getenv("OPENALEX_MAILTO")
        if mailto:
            params["mailto"] = mailto

        resp = requests.get(BASE_URL, params=params, timeout=30)
        try:
            resp.raise_for_status()
        except requests.HTTPError:
            # Print API error details to help diagnose bad filters or params
            content = None
            try:
                content = resp.json()
            except Exception:
                content = resp.text
            raise SystemExit(f"OpenAlex API error {resp.status_code}: {content}")
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
    print(f"Fetching articles with filters: {FILTERS}")
    count = 0
    for article in fetch_articles():
        count += 1
        print(article)
    print(f"\nTotal articles found: {count}")