"""Search and scrape public pages for business-safe contacts only."""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from typing import Iterable, List, Set, Tuple
from urllib.parse import quote_plus, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")

@dataclass
class CandidateSite:
    website: str
    source_url: str


@dataclass
class ContactFinding:
    email: str
    source_url: str


class TopicLeadScraper:
    """Discover relevant websites by topic and scrape business-safe contacts."""

    def __init__(self, timeout: int = 20, request_delay: float = 1.5) -> None:
        self.timeout = timeout
        self.request_delay = request_delay
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
            }
        )

    def discover_websites(self, topic: str, max_sites: int = 25) -> List[CandidateSite]:
        """Use a search engine to find topical public websites."""
        import random

        # Introduce variety in the search query to pull different results per run
        query_variants = [
            f'"{topic}" company official site',
            f'{topic} businesses "contact us"',
            f'"{topic}" services agency',
            f'top {topic} companies',
            f'{topic} firm "about us"'
        ]

        # We can also paginate randomly through the first 50 results (b parameter in Yahoo)
        random_page_offset = random.choice([1, 11, 21, 31, 41])
        chosen_query = random.choice(query_variants)

        url = f"https://search.yahoo.com/search?p={quote_plus(chosen_query)}&n={max_sites + 10}&b={random_page_offset}"
        logger.info("Searching for topic websites: %s (Query: '%s', Offset: %d)", topic, chosen_query, random_page_offset)

        try:
            res = self.session.get(url, timeout=self.timeout)
            res.raise_for_status()
        except Exception as exc:
            logger.error("Search request failed: %s", exc)
            return []

        soup = BeautifulSoup(res.text, "html.parser")
        results = []
        seen_domains: Set[str] = set()

        from urllib.parse import unquote

        for link in soup.select(".algo a"):
            href = link.get("href", "").strip()

            # Yahoo wraps links in a redirect (e.g., .../RU=https%3a%2f%2f.../RK=...)
            if "RU=" in href:
                try:
                    href = unquote(href.split("RU=")[1].split("/")[0])
                except IndexError:
                    continue

            if not href.startswith("http"):
                continue

            parsed = urlparse(href)
            domain = parsed.netloc.lower().replace("www.", "")
            if not domain or domain in seen_domains:
                continue
            # Skip directory-style domains that usually do not host contacts.
            if domain.endswith("linkedin.com") or domain.endswith("facebook.com"):
                continue

            seen_domains.add(domain)
            results.append(CandidateSite(website=f"https://{domain}", source_url=href))
            if len(results) >= max_sites:
                break

        return results

    def scrape_business_contacts(self, website: str, max_pages: int = 1500) -> Tuple[List[ContactFinding], List[str]]:
        """Scrape the website and its internal subpages entirely for business-safe emails, and discover external business links."""
        from urllib.parse import urldefrag

        parsed_base = urlparse(website)
        base_domain = parsed_base.netloc.lower().replace("www.", "")

        findings: List[ContactFinding] = []
        seen_emails: Set[str] = set()
        
        external_domains_found: Set[str] = set()
        
        # Domains to ignore when recursively exploring external websites
        blacklisted_domains = {
            "google.com", "facebook.com", "linkedin.com", "twitter.com", "x.com",
            "instagram.com", "youtube.com", "apple.com", "microsoft.com", "amazon.com",
            "tiktok.com", "pinterest.com", "snapchat.com", "reddit.com", "whatsapp.com",
            "duckduckgo.com", "yahoo.com", "bing.com", "wikipedia.org", "adobe.com",
            "github.com", "w3.org", "medium.com", "vimeo.com", "tumblr.com"
        }

        queue = [website]
        visited = set()

        logger.info("Deep scraping starting at %s (max %d pages)", website, max_pages)

        def get_priority(url: str) -> int:
            """Prioritize URLs containing key staff/directory terms so they are crawled first."""
            score = 0
            slr = url.lower()
            important_keywords = ["staff", "team", "about", "contact", "people", "direct", "leader", "board", "management"]
            if any(k in slr for k in important_keywords):
                score -= 10
            # De-prioritize things like generic blog posts or privacy policies if needed
            if "privacy" in slr or "terms" in slr:
                score += 10
            return score

        while queue and len(visited) < max_pages:
            current_url = queue.pop(0)
            current_url, _ = urldefrag(current_url) # Strip #fragments

            if current_url in visited:
                continue
            visited.add(current_url)

            try:
                res = self.session.get(current_url, timeout=self.timeout)
                if res.status_code >= 400 or not res.text:
                    continue

                soup = BeautifulSoup(res.text, "html.parser")

                # Check for emails natively in RAW text (finds everything hidden)
                emails_dict = self._extract_emails(res.text, soup)
                for email in emails_dict:
                    if email in seen_emails:
                        continue
                    maybe_finding = self._classify_business_email(email, current_url)
                    if maybe_finding:
                        findings.append(maybe_finding)
                        seen_emails.add(email)

                # Find new internal links to crawl
                new_links_found = False
                for link in soup.find_all("a", href=True):
                    next_url = urljoin(current_url, link["href"])
                    parsed_next = urlparse(next_url)

                    if parsed_next.scheme not in ("http", "https"):
                        continue

                    next_domain = parsed_next.netloc.lower().replace("www.", "")
                    if not next_domain:
                        continue

                    # Only queue internal subpages
                    if next_domain == base_domain:
                        clean_next, _ = urldefrag(next_url)
                        if clean_next not in visited and clean_next not in queue:
                            queue.append(clean_next)
                            new_links_found = True
                    else:
                        # Map out external links to turn this into a recursive web tree
                        found_root_domain = ".".join(next_domain.split('.')[-2:]) # naive e.g., company.com
                        if found_root_domain and found_root_domain not in blacklisted_domains:
                            external_url = f"https://{next_domain}"
                            external_domains_found.add(external_url)

                # Re-sort queue to ensure priority pages are hit first
                if new_links_found:
                    queue.sort(key=get_priority)

                time.sleep(self.request_delay)
            except Exception:
                continue

        return findings, list(external_domains_found)

    def _extract_emails(self, html: str, soup: BeautifulSoup) -> dict[str, str | None]:
        found: dict[str, str | None] = {}

        # Scrape all raw emails from HTML using regex
        for raw_email in EMAIL_RE.findall(html):
            clean_email = raw_email.lower()
            if clean_email not in found:
                found[clean_email] = None

        # Extract "mailto:" links which may contain text suitable for person_name
        for node in soup.find_all("a", href=True):
            href = node.get("href", "").strip().lower()
            if href.startswith("mailto:"):
                # Clean up "mailto:" prefix and any "?subject=" parameters
                email_part = href[7:].split("?")[0].strip()
                if EMAIL_RE.match(email_part):
                    clean_email = email_part
                    text_content = node.get_text(strip=True)

                    # Only assign if the text content isn't just the email itself
                    if text_content and "@" not in text_content:
                        found[clean_email] = text_content

        return found

    def _classify_business_email(self, email: str, source_url: str) -> ContactFinding | None:
        return ContactFinding(
            email=email,
            source_url=source_url,
        )
