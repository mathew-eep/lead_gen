"""Long-running compliant lead collector."""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from typing import Optional

import schedule

from lead_database import LeadDatabase
from lead_scraper import TopicLeadScraper


def setup_lead_logging(log_file: Optional[str] = None) -> None:
    log_level = os.getenv("LEAD_LOG_LEVEL", "INFO")
    fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    handlers = [logging.StreamHandler()]
    if log_file:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file))
    logging.basicConfig(level=getattr(logging, log_level), format=fmt, handlers=handlers)


class LeadCollector:
    """Continuously collect business contacts related to a topic."""

    def __init__(
        self,
        topics: list[str],
        db_path: str = "./data/leads.db",
        run_every_minutes: int = 30,
        max_sites_per_run: int = 20,
    ) -> None:
        self.topics = topics
        self.run_every_minutes = run_every_minutes
        self.max_sites_per_run = max_sites_per_run

        self.db = LeadDatabase(db_path=db_path)
        self.scraper = TopicLeadScraper()
        self.logger = logging.getLogger(__name__)

    def fetch_dynamic_topic(self) -> str:
        """Fetch a highly random business industry from a vast corpus, including location factors."""
        import random
        try:
            from faker import Faker
            fake = Faker()
            
            # Generate a job or industry
            job = fake.job()
            
            # Clean up complex job titles (e.g. "Engineer, civil (consulting)" -> "Civil Engineer")
            if "(" in job:
                job = job.split("(")[0].strip()
            if ", " in job:
                parts = job.split(", ")
                if len(parts) == 2:
                    job = f"{parts[1]} {parts[0]}"
            
            # Gather real location factors
            real_cities = [
                "New York", "Los Angeles", "Chicago", "Houston", "Phoenix", 
                "Philadelphia", "San Antonio", "San Diego", "Dallas", "Austin",
                "San Jose", "Fort Worth", "Jacksonville", "Columbus", "Charlotte",
                "Indianapolis", "San Francisco", "Seattle", "Denver", "Washington",
                "Boston", "El Paso", "Nashville", "Portland", "Las Vegas", "Detroit",
                "Memphis", "Louisville", "Baltimore", "Milwaukee", "Albuquerque",
                "Tucson", "Fresno", "Sacramento", "Atlanta", "Kansas City", "Miami",
                "Raleigh", "Omaha", "Oakland", "Minneapolis", "Tulsa", "Cleveland",
                "Wichita", "New Orleans", "Arlington", "Tampa", "Orlando", "Irvine"
            ]
            city = random.choice(real_cities)
            state = fake.state_abbr()
            
            # Compose powerful search variations with real locations only, favoring small businesses
            smallbiz_keywords = ["small", "local", "independent", "family-owned", "boutique", "private"]
            variations = [
                f"{job} small business in {city}",
                f"{job} local company in {city}",
                f"{job} independent firm in {city}",
                f"{job} family-owned business in {city}",
                f"{job} boutique agency in {city}",
                f"{job} private contractor in {city}",
                f"{job} companies in {city}",
                f"{job} services in {state}",
                f"{job} agencies in {city}",
                f"{job} contractors in {state}",
                f"{job} consultants {city}",
                f"{job} firms in {state}"
            ]
            # Add some random small business keyword combos
            for kw in smallbiz_keywords:
                variations.append(f"{job} {kw} business in {city}")
                variations.append(f"{job} {kw} agency in {city}")
                variations.append(f"{job} {kw} services in {state}")
            
            # Add some purely industry-wide generic searches (sometimes we just want the highest ranking ones across the board)
            try:
                import requests
                res = requests.get("https://raw.githubusercontent.com/dariusk/corpora/master/data/corporations/industries.json", timeout=2)
                if res.status_code == 200:
                    ind = random.choice(res.json().get("industries", []))
                    variations.extend([
                        f"{ind} companies in {city}",
                        f"{ind} startups in {state}"
                    ])
            except Exception:
                pass
                
            return random.choice(variations)
        except Exception as exc:
            self.logger.warning("Dynamic topic generation failed: %s. Falling back to default list.", exc)
            return random.choice(["software startups in Texas", "marketing agencies in NY", "construction firms in CA"])

    def run_once(self) -> None:
        import random
        
        if "DYNAMIC" in self.topics:
            current_topic = self.fetch_dynamic_topic()
        else:
            # Pick a random topic from the given list
            current_topic = random.choice(self.topics)

        run_id = self.db.start_run(current_topic)
        companies_added = 0
        contacts_added = 0

        try:
            site_queue = self.scraper.discover_websites(current_topic, max_sites=self.max_sites_per_run)
            
            from lead_scraper import CandidateSite
            seen_sites = set([s.website for s in site_queue])
            sites_crawled = 0

            while site_queue and sites_crawled < self.max_sites_per_run:
                site = site_queue.pop(0)
                sites_crawled += 1
                
                self.logger.info("Processing site %d/%d: %s", sites_crawled, self.max_sites_per_run, site.website)
                
                if self.db.add_company(
                    topic=current_topic,
                    website=site.website,
                    name=None,
                    source_url=site.source_url,
                ):
                    companies_added += 1

                findings, external_domains = self.scraper.scrape_business_contacts(site.website)
                for finding in findings:
                    added = self.db.add_contact(
                        topic=current_topic,
                        company_website=site.website,
                        email=finding.email,
                        source_url=finding.source_url,
                    )
                    if added:
                        contacts_added += 1
                
                # Append purely new, organically discovered external business partners to the back of the tree queue
                for ext_domain in external_domains:
                    if ext_domain not in seen_sites:
                        seen_sites.add(ext_domain)
                        site_queue.append(CandidateSite(website=ext_domain, source_url=site.website))

            self.db.finish_run(
                run_id,
                companies_found=companies_added,
                contacts_found=contacts_added,
                status="success",
            )
            self.logger.info(
                "Run complete | topic='%s' new_companies=%d new_contacts=%d",
                current_topic,
                companies_added,
                contacts_added,
            )
        except Exception as exc:
            self.db.finish_run(
                run_id,
                companies_found=companies_added,
                contacts_found=contacts_added,
                status="failed",
                notes=str(exc),
            )
            self.logger.error("Run failed: %s", exc)

    def start_forever(self) -> None:
        self.logger.info("Starting lead collector for topics: %s", self.topics)
        self.logger.info(
            "Schedule: every %d minute(s), max %d sites/run",
            self.run_every_minutes,
            self.max_sites_per_run,
        )

        schedule.every(self.run_every_minutes).minutes.do(self.run_once)
        self.run_once()

        try:
            while True:
                schedule.run_pending()
                time.sleep(1)
        except KeyboardInterrupt:
            self.logger.info("Lead collector stopped")

    def print_stats(self) -> None:
        stats = self.db.get_stats()
        self.logger.info("Lead DB stats: %s", stats)
