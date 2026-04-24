"""
Run the full GTM pipeline end-to-end.

Each stage reads from the database and only processes new/unprocessed data.
Running this multiple times is safe — it will not duplicate or overwrite
existing prospects, messages, or scheduled emails.

Usage:
  python run_pipeline.py
  python run_pipeline.py --query "B2B SaaS startup Europe series B" --num 20
  python run_pipeline.py --skip-search   # skip stage 1 (company search)
  python run_pipeline.py --from 3        # start from stage 3
"""

from __future__ import annotations

import argparse
import sys


def main() -> None:
    p = argparse.ArgumentParser(description="Run the full Avery GTM pipeline")
    p.add_argument("--query", "-q", default=None, help="Company search query (stage 1)")
    p.add_argument("--location", "-l", default=None, help="Country code for search, e.g. NL, DE")
    p.add_argument("--num", "-n", type=int, default=10, help="Number of companies to search")
    p.add_argument("--from", dest="from_stage", type=int, default=1, help="Start from stage N (1-5)")
    p.add_argument("--skip-search", action="store_true", help="Skip stage 1 (use existing companies)")
    args = p.parse_args()

    start = args.from_stage
    if args.skip_search:
        start = max(start, 2)

    # Stage 1 — Company search
    if start <= 1:
        print("=" * 60)
        print("STAGE 1: Company Discovery")
        print("=" * 60)
        from pipeline.company_search import run_company_search
        run_company_search(query=args.query, location=args.location, num_results=args.num)
        print()

        print("=" * 60)
        print("STAGE 1.5: Company ICP Filter")
        print("=" * 60)
        from pipeline.company_filter import run_company_filter
        run_company_filter()
        print()

    # Stage 2 — People search
    if start <= 2:
        print("=" * 60)
        print("STAGE 2: People Search")
        print("=" * 60)
        from pipeline.people_search import run_people
        run_people()
        print()

    # Stage 3 — ICP filter
    if start <= 3:
        print("=" * 60)
        print("STAGE 3: ICP Filter")
        print("=" * 60)
        from pipeline.people_filter import run_filter
        run_filter()
        print()

    # Stage 4 — Email enrichment
    if start <= 4:
        print("=" * 60)
        print("STAGE 4: Email Enrichment")
        print("=" * 60)
        from pipeline.people_email import run_email_enrichment
        run_email_enrichment()
        print()

    # Stage 5 — Message generation
    if start <= 5:
        print("=" * 60)
        print("STAGE 5: Message Generation")
        print("=" * 60)
        from pipeline.reachout import run_reachout
        run_reachout()
        print()

    print("=" * 60)
    print("Pipeline complete!")
    print("=" * 60)

    # Summary
    from pipeline.db import get_connection, init_db
    conn = get_connection()
    init_db(conn)
    companies = conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
    prospects = conn.execute("SELECT COUNT(*) FROM prospects").fetchone()[0]
    with_email = conn.execute("SELECT COUNT(*) FROM prospects WHERE email IS NOT NULL AND email != ''").fetchone()[0]
    messages = conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
    print(f"  Companies: {companies}")
    print(f"  Prospects: {prospects} ({with_email} with email)")
    print(f"  Messages:  {messages}")
    conn.close()


if __name__ == "__main__":
    main()
