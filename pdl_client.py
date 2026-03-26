"""
People Data Labs (PDL) API client.

PDL has a pre-built LinkedIn database — no scraping, no session cookies,
your LinkedIn account is completely uninvolved.

Docs: https://docs.peopledatalabs.com
Auth: X-Api-Key header
Base: https://api.peopledatalabs.com/v5

Key advantage over LinkdAPI/NinjaPear:
  Experience data is returned INSIDE the search results — no separate
  profile lookup needed, so 1 API call does the work of many.

Free tier: 100 calls/month (never expires).
"""

import requests
import re
import time
from typing import Optional, List


BASE_URL = "https://api.peopledatalabs.com/v5"


class PDLClient:
    def __init__(self, api_key: str):
        self.session = requests.Session()
        self.session.headers.update({
            "X-Api-Key": api_key,
            "Content-Type": "application/json",
        })

    def _post(self, path: str, payload: dict, retries: int = 2) -> Optional[dict]:
        url = f"{BASE_URL}{path}"
        for attempt in range(retries + 1):
            resp = self.session.post(url, json=payload, timeout=20)
            if resp.status_code == 429:
                time.sleep(int(resp.headers.get("Retry-After", 5)))
                continue
            if resp.status_code in (200, 404):
                return resp.json()
            # Surface the full PDL error message for easier debugging
            try:
                body = resp.json()
                msg = body.get("error", {}).get("message") or body.get("message") or str(body)
            except Exception:
                msg = resp.text or "no response body"
            raise Exception(f"PDL {resp.status_code}: {msg}")
        return None

    # ------------------------------------------------------------------ #
    #  Core search                                                          #
    # ------------------------------------------------------------------ #

    def search_people(
        self,
        company_linkedin_url: str,
        title_keyword: str,
        size: int = 25,
        include_past: bool = True,
    ) -> dict:
        """
        Search for people at a specific company (by LinkedIn URL) whose
        job title contains the keyword.

        Always runs the reliable current-employee query first
        (job_company_linkedin_url + job_title wildcard).

        include_past=True also runs a second query for people who have
        the target company in their experience history but are now
        working elsewhere with the keyword in their CURRENT job title.

        Returns merged { total, data: [...] }
        Each person includes full 'experience' list — no extra calls needed.
        """
        seen_ids: set = set()
        all_data = []

        # ── Query A: current employees (most reliable PDL field) ──────────
        current_must = [
            {"term": {"job_company_linkedin_url": company_linkedin_url}},
            {"wildcard": {"job_title": f"*{title_keyword.lower()}*"}},
        ]
        result_a = self._post(
            "/person/search",
            {"query": {"bool": {"must": current_must}}, "size": size, "pretty": False},
        ) or {"total": 0, "data": []}

        for person in result_a.get("data", []):
            pid = person.get("id") or person.get("linkedin_url") or person.get("full_name")
            if pid and pid not in seen_ids:
                seen_ids.add(pid)
                all_data.append(person)

        # ── Query B: past employees now in same-keyword roles elsewhere ───
        if include_past:
            past_must = [
                {"term": {"experience.company.linkedin_url": company_linkedin_url}},
                {"wildcard": {"job_title": f"*{title_keyword.lower()}*"}},
            ]
            past_must_not = [
                {"term": {"job_company_linkedin_url": company_linkedin_url}},
            ]
            result_b = self._post(
                "/person/search",
                {
                    "query": {
                        "bool": {
                            "must": past_must,
                            "must_not": past_must_not,
                        }
                    },
                    "size": size,
                    "pretty": False,
                },
            ) or {"total": 0, "data": []}

            for person in result_b.get("data", []):
                pid = person.get("id") or person.get("linkedin_url") or person.get("full_name")
                if pid and pid not in seen_ids:
                    seen_ids.add(pid)
                    all_data.append(person)

            total = result_a.get("total", 0) + result_b.get("total", 0)
        else:
            total = result_a.get("total", 0)

        return {"total": total, "data": all_data}

    def find_candidates_at_company(
        self,
        company_linkedin_url: str,
        title_keyword: str,
        size: int = 25,
    ) -> list:
        """
        Find people currently working at a company with keyword in their title.
        Used for Step 4 — searching the company universe for candidates.
        """
        result = self.search_people(
            company_linkedin_url=company_linkedin_url,
            title_keyword=title_keyword,
            size=size,
            include_past=False,
        )
        return result.get("data", [])

    # ------------------------------------------------------------------ #
    #  Helpers                                                              #
    # ------------------------------------------------------------------ #

    @staticmethod
    def extract_linkedin_company_slug(url: str) -> str:
        """
        Pull the company slug from a LinkedIn URL.
        'https://www.linkedin.com/company/analogue-creative/' → 'analogue-creative'
        """
        m = re.search(r"linkedin\.com/company/([^/?#]+)", url or "")
        return m.group(1).rstrip("/") if m else ""

    @staticmethod
    def normalise_linkedin_url(url: str) -> str:
        """
        Normalise to the format PDL uses: 'linkedin.com/company/slug'
        (no https, no www, no trailing slash, no sub-paths like /people/)
        """
        url = re.sub(r"https?://(www\.)?", "", url.strip().rstrip("/"))
        url = url.lower()
        # Strip any trailing path after the company slug
        # e.g. linkedin.com/company/analogue-creative/people → linkedin.com/company/analogue-creative
        url = re.sub(r"(linkedin\.com/company/[^/?#]+).*", r"\1", url)
        return url

    @staticmethod
    def extract_work_history(person: dict, target_slug: str) -> dict:
        """
        Given a PDL person record, return companies worked at BEFORE and AFTER
        the target company.

        PDL experience entries are ordered most-recent first.
        Each entry has: start_date, end_date (None = current), company dict.

        Returns:
          {
            "before": [ {name, linkedin_url, industry}, ... ],
            "after":  [ ... ],
            "all":    [ ... ],
            "confirmed": bool   # True if target company found in history
          }
        """
        experiences = person.get("experience") or []
        target_slug = target_slug.lower().strip("/")

        def matches_target(exp: dict) -> bool:
            li = (exp.get("company") or {}).get("linkedin_url") or ""
            slug_in_li = target_slug in li.lower()
            name = (exp.get("company") or {}).get("name") or ""
            slug_clean = target_slug.replace("-", "").replace(" ", "")
            name_clean = name.lower().replace("-", "").replace(" ", "")
            return slug_in_li or slug_clean in name_clean

        target_idx = next(
            (i for i, e in enumerate(experiences) if matches_target(e)),
            None
        )

        before, after = [], []
        seen = set()

        for i, exp in enumerate(experiences):
            if matches_target(exp):
                continue
            comp = exp.get("company") or {}
            name = comp.get("name") or ""
            if not name or name.lower() in seen:
                continue
            seen.add(name.lower())

            entry = {
                "name": name,
                "linkedin_url": comp.get("linkedin_url") or "",
                "industry": comp.get("industry") or "",
            }

            if target_idx is None:
                after.append(entry)
            elif i < target_idx:
                after.append(entry)   # more recent → worked there after target
            else:
                before.append(entry)  # older → worked there before target

        return {
            "before": before,
            "after": after,
            "all": before + after,
            "confirmed": target_idx is not None,
        }
