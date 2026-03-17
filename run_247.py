"""
Launcher for 24/7 Lead Collection.

Usage on a VPS:
    # 1. Using TMUX or SCREEN (easiest):
    tmux new -s collector
    python run_247.py
    # Press Ctrl+B, then D to detach. It will run forever.

    # 2. Using NOHUP:
    nohup python run_247.py > collector.log 2>&1 &
"""

import sys
import argparse
from pathlib import Path

# Fix python path so it can import from src/
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from lead_collector import LeadCollector, setup_lead_logging


def main():
    parser = argparse.ArgumentParser(description="24/7 B2B Lead Collector")
    parser.add_argument("--topics", type=str, 
                        default="DYNAMIC", 
                        help="Comma-separated list of topics, or 'DYNAMIC' to let the API randomly generate topics.")
    parser.add_argument("--interval", type=int, default=30,
                        help="How often to run the discovery loop (in minutes).")
    parser.add_argument("--max-sites", type=int, default=20,
                        help="Max sites to crawl per cycle.")

    args = parser.parse_args()
    
    # Parse comma-separated topics into a list
    topics_list = [t.strip() for t in args.topics.split(",") if t.strip()]

    # Initializes logging (prints to console and saves to ./logs/leads.log if configured)
    setup_lead_logging(log_file="./logs/leads_247.log")

    collector = LeadCollector(
        topics=topics_list,
        run_every_minutes=args.interval,
        max_sites_per_run=args.max_sites
    )

    print("=" * 60)
    print(f"🚀 Starting 24/7 Lead Collector")
    print(f"📌 Topics loaded: {len(topics_list)} (Bot will pick one randomly each run)")
    for t in topics_list:
        print(f"   - {t}")
    print("=" * 60)
    
    # This runs continuously until interrupted
    collector.start_forever()


if __name__ == "__main__":
    main()
