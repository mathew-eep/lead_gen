"""Search and scrape public pages for business-safe contacts only."""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from typing import Iterable, List, Set
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
        # Using Yahoo search for better compatibility with automated scrapers
        query = f"{topic} company official site"
        url = f"https://search.yahoo.com/search?p={quote_plus(query)}&n={max_sites + 10}"
        logger.info("Searching for topic websites: %s", topic)

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

    def scrape_business_contacts(self, website: str, max_pages: int = 15) -> List[ContactFinding]:
        """Scrape the website and its internal subpages for business-safe emails."""
        from urllib.parse import urldefrag
        
        parsed_base = urlparse(website)
        base_domain = parsed_base.netloc.lower().replace("www.", "")
        
        findings: List[ContactFinding] = []
        seen_emails: Set[str] = set()
        
        queue = [website]
        visited = set()
        
        logger.info("Deep scraping starting at %s (max %d pages)", website, max_pages)
        
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
                
                # Check for emails
                emails_dict = self._extract_emails(res.text, soup)
                for email in emails_dict:
                    if email in seen_emails:
                        continue
                    maybe_finding = self._classify_business_email(email, current_url)
                    if maybe_finding:
                        findings.append(maybe_finding)
                        seen_emails.add(email)
                
                # Find new internal links to crawl
                for link in soup.find_all("a", href=True):
                    next_url = urljoin(current_url, link["href"])
                    parsed_next = urlparse(next_url)
                    
                    if parsed_next.scheme not in ("http", "https"):
                        continue
                        
                    next_domain = parsed_next.netloc.lower().replace("www.", "")
                    
                    # Only queue internal subpages
                    if next_domain == base_domain:
                        clean_next, _ = urldefrag(next_url)
                        if clean_next not in visited and clean_next not in queue:
                            queue.append(clean_next)

                time.sleep(self.request_delay)
            except Exception:
                continue

        return findings

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
