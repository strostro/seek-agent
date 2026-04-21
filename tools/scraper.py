from playwright.sync_api import sync_playwright, TimeoutError
import pandas as pd
import time
from urllib.parse import urljoin
from datetime import datetime, timedelta
import re

# ========================
# CONFIG
# ========================

KEYWORDS = {
    "Data Engineer": "Data-Engineer-jobs",
    "Analyst": "Analyst-jobs",
    "Data Scientist": "Data-Scientist-jobs",
    "Business Intelligence Analyst": "Business-Intelligence-Analyst-jobs",
    "Data Analyst": "Data-Analyst-jobs",
    "AI Engineer": "AI-Engineer-jobs"
}

BASE_DOMAIN = "https://www.seek.co.nz"
OUTPUT_FILE = "seek_all_roles_last14days.csv"

HEADLESS = False
PAGE_DELAY = 1.5
DETAIL_DELAY = 2


# ========================
# UTIL
# ========================

def extract_job_id(url):
    match = re.search(r"/job/(\d+)", url)
    return match.group(1) if match else None


def safe_text(page, selector):
    try:
        elem = page.query_selector(selector)
        return elem.inner_text().strip() if elem else ""
    except:
        return ""


def parse_posted_date(text):
    if not text:
        return None

    text = text.lower()
    now = datetime.utcnow()

    if "today" in text:
        return now.date()

    if "yesterday" in text:
        return (now - timedelta(days=1)).date()

    m = re.search(r"(\d+)d", text)
    if m:
        return (now - timedelta(days=int(m.group(1)))).date()

    m = re.search(r"(\d+)h", text)
    if m:
        return now.date()

    m = re.search(r"(\d+)m", text)
    if m:
        return now.date()

    return None


# ========================
# BUILD URL
# ========================

def build_url(slug, page_number):
    base = f"{BASE_DOMAIN}/{slug}/in-All-New-Zealand"

    if page_number == 1:
        return f"{base}?daterange=14&sortmode=ListedDate"

    return f"{base}?daterange=14&page={page_number}&sortmode=ListedDate"


# ========================
# COLLECT LINKS
# ========================

def collect_links(slug, keyword_label):
    links = []

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=HEADLESS,
            args=["--disable-blink-features=AutomationControlled"]
        )
        page = browser.new_page()

        page_number = 1

        while True:
            url = build_url(slug, page_number)
            print(f"\n[{keyword_label}] Visiting: {url}")

            try:
                page.goto(url, wait_until="domcontentloaded", timeout=60000)
            except:
                print("Page load timeout, retrying...")
                continue

            try:
                page.wait_for_selector('[data-testid="job-card"]', timeout=8000)
            except TimeoutError:
                break

            cards = page.query_selector_all('[data-testid="job-card"]')
            print(f"[{keyword_label}] Page {page_number}: {len(cards)} jobs")

            if len(cards) == 0:
                break

            for card in cards:
                title_elem = card.query_selector('[data-testid="job-card-title"]')
                if title_elem:
                    href = title_elem.get_attribute("href")
                    if href:
                        full_url = urljoin(BASE_DOMAIN, href)
                        job_id = extract_job_id(full_url)
                        links.append({
                            "job_id": job_id,
                            "url": full_url,
                            "source_keyword": keyword_label
                        })

            page_number += 1
            time.sleep(PAGE_DELAY)

        browser.close()

    return links


# ========================
# SCRAPE DETAILS
# ========================

def scrape_details(job_records):
    rows = []

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=HEADLESS,
            args=["--disable-blink-features=AutomationControlled"]
        )
        page = browser.new_page()

        for i, record in enumerate(job_records, start=1):
            try:
                page.goto(record["url"], wait_until="domcontentloaded", timeout=60000)
                page.wait_for_selector("h1", timeout=8000)

                title = safe_text(page, '[data-automation="job-detail-title"]') \
                        or safe_text(page, "h1")

                company = safe_text(page, '[data-automation="advertiser-name"]')

                location = safe_text(page, '[data-automation="job-detail-location"]') \
                           or safe_text(page, '[data-automation="jobLocation"]')

                classification = safe_text(page, '[data-automation="job-detail-classifications"]')
                work_type = safe_text(page, '[data-automation="job-detail-work-type"]')
                salary = safe_text(page, '[data-automation="job-detail-salary"]')

                posted_raw = safe_text(page, 'span:has-text("Posted")')
                posted_date = parse_posted_date(posted_raw)

                description = safe_text(page, '[data-automation="jobAdDetails"]')

                rows.append({
                    "job_id": record["job_id"],
                    "source_keyword": record["source_keyword"],
                    "scrape_time_utc": datetime.utcnow().isoformat(),
                    "title": title,
                    "company": company,
                    "location": location,
                    "classification": classification,
                    "work_type": work_type,
                    "salary": salary,
                    "posted_raw": posted_raw,
                    "posted_date": posted_date,
                    "description": description,
                    "url": record["url"]
                })

                print(f"[{i}/{len(job_records)}] {title}")
                time.sleep(DETAIL_DELAY)

            except Exception:
                print(f"Failed: {record['url']}")
                continue

        browser.close()

    return rows


# ========================
# PUBLIC FUNCTION (for import)
# ========================

def pull_seek_data():
    """Main entry point for SEEK scraping (importable)."""

    all_links = []

    for label, slug in KEYWORDS.items():
        links = collect_links(slug, label)
        all_links.extend(links)

    print(f"\nTotal raw collected links: {len(all_links)}")

    df_links = pd.DataFrame(all_links)
    df_links = df_links.drop_duplicates(subset=["job_id"])

    print(f"After dedupe: {len(df_links)} unique jobs")

    results = scrape_details(df_links.to_dict("records"))

    df_final = pd.DataFrame(results)
    df_final.to_csv(OUTPUT_FILE, index=False)

    print(f"\nFinal dataset saved to: {OUTPUT_FILE}")
    print(f"Total jobs in final dataset: {len(df_final)}")

    return df_final


# ========================
# SCRIPT MODE
# ========================

if __name__ == "__main__":
    pull_seek_data()