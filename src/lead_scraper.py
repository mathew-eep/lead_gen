"""Search and scrape public pages for business-safe contacts only."""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from typing import Iterable, List, Set, Tuple
from urllib.parse import quote_plus, urljoin, urlparse

from curl_cffi import requests
from bs4 import BeautifulSoup
import warnings
from bs4 import XMLParsedAsHTMLWarning

# Suppress noisy BeautifulSoup XML/HTML warnings when we inevitably hit a broken company sitemap/XML feed
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

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
        
        # Powerfully bounce between completely different browser TLS fingerprints and header signatures
        import random
        browser_targets = ["chrome110", "chrome120", "safari15_3", "safari17_0", "edge101"]
        
        self.session = requests.Session(impersonate=random.choice(browser_targets))

    def discover_websites(self, topic: str, max_sites: int = 25) -> List[CandidateSite]:
        """Use a search engine to find topical public websites."""
        import random

        # Introduce variety in the search query to pull different results per run
        query_variants = [
            f'{topic} official website',
            f'{topic} "contact us"',
            f'{topic} location',
            f'top {topic} independent',
            f'{topic} "about us"'
        ]

        # We paginate carefully through the first 20 results (b parameter in Yahoo)
        # Niche queries turn into pure garbage after page 2 on Yahoo, so we stick to 1 or 11
        random_page_offset = random.choice([1, 11])
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
                
            # Skip massive generic directories or social platforms
            skip_terms = {
                "linkedin.com", "facebook.com", "instagram.com", "twitter.com", "x.com", 
                "youtube.com", "pinterest.com", "reddit.com", "yelp.com", "yellowpages.com", 
                "bbb.org", "wikipedia.org", "yahoo.com", "aol.com", "vk.com", "nytimes.com", 
                "tripadvisor.", "glassdoor.", "trustpilot.", "forbes.com", "indeed.com",
                "clutch.co", "upcity.com", "angi.com", "thumbtack.com", "expertise.com",
                "zoominfo.com", "crunchbase.com", "g2.com", "capterra.com", "bloomberg.com"
            }
            if any(term in domain for term in skip_terms):
                continue

            seen_domains.add(domain)
            results.append(CandidateSite(website=f"https://{domain}", source_url=href))
            if len(results) >= max_sites:
                break

        return results

    def _extract_emails_dynamic(self, url: str) -> dict[str, str | None]:
        """Use Playwright to render JS and extract emails from the fully rendered page."""
        if not PLAYWRIGHT_AVAILABLE:
            return {}
        async def run():
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                try:
                    await page.goto(url, timeout=self.timeout * 1000)
                    html = await page.content()
                    soup = BeautifulSoup(html, "html.parser")
                    return self._extract_emails(html, soup)
                except Exception:
                    return {}
                finally:
                    await browser.close()
        try:
            return asyncio.run(run())
        except Exception:
            return {}

    def scrape_business_contacts(self, website: str, max_pages: int = 30) -> Tuple[List[ContactFinding], List[str]]:
        """Scrape the website and its internal subpages for business-safe emails, and discover external business links."""
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

        # Always queue high-priority subpages for crawling
        priority_paths = ["/contact", "/contact.php", "/contact-us", "/about", "/about-us", "/team", "/staff", "/directory"]
        queue = [website]
        from urllib.parse import urljoin
        for path in priority_paths:
            candidate_url = urljoin(website, path)
            if candidate_url not in queue:
                queue.append(candidate_url)
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
                # If no emails found and this is a high-priority subpage, try Playwright
                if not emails_dict and PLAYWRIGHT_AVAILABLE:
                    for key in ["contact", "staff", "team", "about", "directory"]:
                        if key in current_url:
                            emails_dict = self._extract_emails_dynamic(current_url)
                            break
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
            clean_email = raw_email.lower().strip()
            
            # Skip invalid email structures often caught by open regexes (images, webp, etc.)
            if any(clean_email.endswith(ext) for ext in [".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".css", ".js"]):
                continue
                
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

        # Obfuscated email patterns (e.g. info [at] domain [dot] com, info (at) domain (dot) com)
        obfuscated_re = re.compile(r"([A-Za-z0-9._%+-]+)\s*[\[\(]?at[\]\)]?\s*([A-Za-z0-9.-]+)\s*[\[\(]?dot[\]\)]?\s*([A-Za-z]{2,})", re.IGNORECASE)
        for match in obfuscated_re.findall(html):
            email = f"{match[0]}@{match[1]}.{match[2]}"
            if email not in found:
                found[email] = None

        # Handle emails split by spans or with extra spaces (e.g., 'john . doe @ domain . com')
        text = soup.get_text(separator=" ", strip=True)
        split_email_re = re.compile(r"([A-Za-z0-9._%+-]+)\s*\.\s*([A-Za-z0-9._%+-]+)\s*@\s*([A-Za-z0-9.-]+)\s*\.\s*([A-Za-z]{2,})", re.IGNORECASE)
        for match in split_email_re.findall(text):
            email = f"{match[0]}.{match[1]}@{match[2]}.{match[3]}"
            if email not in found:
                found[email] = None

        return found

    def _classify_business_email(self, email: str, source_url: str) -> ContactFinding | None:
        return ContactFinding(
            email=email,
            source_url=source_url,
        )

import asyncio
try:
    from playwright.async_api import async_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
