"""
LinkdAPI client — replacement for the Proxycurl client.

Auth:  X-linkdapi-apikey header
Docs:  https://linkdapi.com

Endpoints used
--------------
GET /api/v1/search/people          — search people by keyword + optional filters
GET /api/v1/profile/full           — full profile incl. complete position history
GET /api/v1/companies/company/info — company details by LinkedIn username/id
"""

import json
import re
import time
import requests
from typing import Optional, List


BASE_URL = "https://linkdapi.com"


class LinkdAPIClient:
    def __init__(self, api_key: str):
        self.session = requests.Session()
        self.session.headers.update({"X-linkdapi-apikey": api_key})

    def _get(self, path: str, params: dict, retries: int = 2) -> Optional[dict]:
        """GET with retry on 429 rate-limit."""
        url = f"{BASE_URL}{path}"
        for attempt in range(retries + 1):
            resp = self.session.get(url, params=params, timeout=20)
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 5))
                time.sleep(wait)
                continue
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            data = resp.json()
            # LinkdAPI wraps everything in {success, data, message}
            if data.get("success") and data.get("data"):
                return data["data"]
            return None
        return None

    # ------------------------------------------------------------------ #
    #  People search                                                        #
    # ------------------------------------------------------------------ #

    def search_people(
        self,
        keywords: str,
        company_username: str = None,
        title_filter: str = None,
        start: int = 0,
    ) -> dict:
        """
        Search for people.

        Returns the raw 'data' dict:
          { people: [...], total: N, hasMore: bool, start: N }

        Each person has: urn, url, fullName, headline, location, profilePictureURL
        """
        params: dict = {"keywords": keywords, "start": start}
        if company_username:
            # API expects a JSON array string e.g. '["analogue"]'
            params["currentCompany"] = json.dumps([company_username])
        if title_filter:
            params["title"] = title_filter

        url = f"{BASE_URL}/api/v1/search/people"
        resp = self.session.get(url, params=params, timeout=20)
        if resp.status_code != 200:
            return {"people": [], "total": 0, "hasMore": False}
        body = resp.json()
        return body.get("data") or {"people": [], "total": 0, "hasMore": False}

    def search_people_at_company(
        self,
        keyword: str,
        company_name: str,
        pages: int = 3,
        location_hint: str = None,
    ) -> List[dict]:
        """
        Find people whose headline/title contains keyword and who work at company.
        Uses keyword search + optional location hint to reduce false positives
        (e.g. generic words like 'analogue' appear in other languages).

        Returns list of people dicts.
        """
        results = []
        seen_urns = set()

        # Build a targeted query: keyword + quoted company name for precision
        query = f'"{keyword}" "{company_name}"'
        if location_hint:
            query += f" {location_hint}"

        for page in range(pages):
            data = self.search_people(
                keywords=query,
                start=page * 10,
            )
            people = data.get("people", [])
            if not people:
                # Fall back to unquoted search on first page if no results
                if page == 0:
                    data = self.search_people(
                        keywords=f"{keyword} {company_name}",
                        start=0,
                    )
                    people = data.get("people", [])
                if not people:
                    break
            for p in people:
                if p.get("urn") not in seen_urns:
                    seen_urns.add(p.get("urn"))
                    results.append(p)
            if not data.get("hasMore"):
                break

        return results

    # ------------------------------------------------------------------ #
    #  Person profile                                                       #
    # ------------------------------------------------------------------ #

    def get_profile(self, urn: str) -> Optional[dict]:
        """
        Fetch a full LinkedIn profile by URN.
        Key field: 'position' — list of all past + present jobs.

        Each position:
          companyName, companyUsername, companyURL, companyIndustry,
          title, start{year,month}, end{year,month,day}
          (end.year == 0 means current role)
        """
        url = f"{BASE_URL}/api/v1/profile/full"
        resp = self.session.get(url, params={"urn": urn}, timeout=20)
        if resp.status_code != 200:
            return None
        body = resp.json()
        if body.get("success") and body.get("data"):
            return body["data"]
        return None

    # ------------------------------------------------------------------ #
    #  Company                                                              #
    # ------------------------------------------------------------------ #

    def get_company(self, company_id: str) -> Optional[dict]:
        """
        Fetch company info by LinkedIn username/id.
        Returns dict with: name, staffCount, followerCount, description, etc.
        """
        return self._get(
            "/api/v1/companies/company/info",
            {"id": company_id}
        )

    # ------------------------------------------------------------------ #
    #  Helpers                                                              #
    # ------------------------------------------------------------------ #

    @staticmethod
    def extract_company_name_from_url(url: str) -> str:
        """
        Derive a short company name to use in searches from a URL.
        e.g. 'https://madebyanalogue.co.uk/' → 'analogue'
             'https://buck.tv/' → 'buck'
        """
        from urllib.parse import urlparse
        host = urlparse(url).netloc or url
        host = host.replace("www.", "").split(".")[0]
        # strip common prefixes like 'madeBy', 'studio', 'team', 'we'
        for prefix in ("madeby", "studio", "teamby", "weare", "hello", "meet"):
            if host.startswith(prefix) and len(host) > len(prefix) + 2:
                host = host[len(prefix):]
        return host

    @staticmethod
    def extract_linkedin_username(url: str) -> str:
        """Pull username from linkedin.com/in/username or linkedin.com/company/username."""
        m = re.search(r"linkedin\.com/(?:in|company)/([^/?#]+)", url or "")
        return m.group(1) if m else ""

    @staticmethod
    def extract_work_history(profile: dict, target_company_slug: str) -> dict:
        """
        Given a full profile, return companies worked at BEFORE and AFTER
        the target company.

        Positions are ordered most-recent first (end.year==0 = current).
        Index < target_idx  → worked there AFTER leaving target
        Index > target_idx  → worked there BEFORE joining target

        Returns:
            { "before": [...company dicts], "after": [...], "all": [...] }
        where each company dict has: name, username, url, industry
        """
        positions = profile.get("position") or profile.get("fullPositions") or []
        slug = target_company_slug.lower()

        def matches_target(pos: dict) -> bool:
            name = (pos.get("companyName") or "").lower().replace(" ", "")
            username = (pos.get("companyUsername") or "").lower()
            return slug in name or slug in username

        target_idx = next(
            (i for i, p in enumerate(positions) if matches_target(p)),
            None
        )

        before, after = [], []
        seen = set()

        for i, pos in enumerate(positions):
            if matches_target(pos):
                continue
            comp_name = pos.get("companyName") or ""
            if not comp_name or comp_name in seen:
                continue
            seen.add(comp_name)

            entry = {
                "name": comp_name,
                "username": pos.get("companyUsername") or "",
                "url": pos.get("companyURL") or "",
                "industry": pos.get("companyIndustry") or "",
            }

            if target_idx is None:
                after.append(entry)
            elif i < target_idx:
                after.append(entry)   # more recent = worked there after leaving target
            else:
                before.append(entry)  # older = worked there before joining target

        return {
            "before": before,
            "after": after,
            "all": before + after,
            "confirmed": target_idx is not None,   # True = target company found in profile
        }
