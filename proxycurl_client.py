"""
Proxycurl API client — wraps the endpoints we need with caching.
All requests go through Proxycurl's servers; your LinkedIn account
and IP are never involved.

Endpoints used:
  GET /proxycurl/api/linkedin/company/resolve          — domain → LinkedIn company URL
  GET /proxycurl/api/linkedin/company/employees/       — list employees at a company
  GET /proxycurl/api/v2/linkedin                       — full person profile (incl. experiences)
"""

import requests
import time
import re
from typing import Optional, List


BASE_URL = "https://nubela.co/proxycurl/api"


class ProxycurlClient:
    def __init__(self, api_key: str):
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {api_key}"})

    def _get(self, path: str, params: dict, retries: int = 2) -> Optional[dict]:
        """Make a GET request with simple retry on rate limit (429)."""
        url = f"{BASE_URL}{path}"
        for attempt in range(retries + 1):
            resp = self.session.get(url, params=params, timeout=20)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 5))
                time.sleep(wait)
                continue
            if resp.status_code == 404:
                return None  # Not found — not an error worth retrying
            resp.raise_for_status()
        return None

    # ------------------------------------------------------------------ #
    #  Company                                                              #
    # ------------------------------------------------------------------ #

    def resolve_company(self, domain: str) -> Optional[str]:
        """
        Given a company domain (e.g. 'madebyanalogue.co.uk') return
        its LinkedIn company URL, or None if not found.
        """
        domain = domain.replace("https://", "").replace("http://", "").strip("/")
        data = self._get(
            "/linkedin/company/resolve",
            {"company_domain": domain}
        )
        return data.get("url") if data else None

    def get_employees(self, company_linkedin_url: str) -> List[dict]:
        """
        Return a list of current + past employees for a LinkedIn company URL.
        Each item has at minimum: 'profile_url', 'name', 'role'.
        Proxycurl paginates — we collect all pages (max 10 to keep costs down).
        """
        employees = []
        params = {
            "linkedin_profile_url": company_linkedin_url,
            "page_size": 100,
        }
        # Proxycurl returns a 'next_page' token when there are more results
        for _page in range(10):  # hard cap at ~1000 employees
            data = self._get("/linkedin/company/employees/", params)
            if not data:
                break
            employees.extend(data.get("employees", []))
            next_page = data.get("next_page")
            if not next_page:
                break
            params["page"] = next_page
        return employees

    def get_employees_keyword_filtered(
        self, company_linkedin_url: str, keyword: str
    ) -> List[dict]:
        """
        Get employees and filter client-side by keyword in their role/title.
        This saves API credits vs. fetching full profiles for everyone.
        """
        all_employees = self.get_employees(company_linkedin_url)
        pattern = re.compile(keyword, re.IGNORECASE)
        return [
            e for e in all_employees
            if pattern.search(e.get("role") or e.get("title") or "")
        ]

    # ------------------------------------------------------------------ #
    #  Person                                                               #
    # ------------------------------------------------------------------ #

    def get_profile(self, linkedin_url: str) -> Optional[dict]:
        """
        Fetch a full LinkedIn profile.  The 'experiences' list is what
        we care about — it contains all past and current roles with company
        names and dates.
        """
        return self._get(
            "/v2/linkedin",
            {
                "url": linkedin_url,
                "extra": "exclude",   # skip patents, awards etc — saves cost
                "skills": "exclude",
            }
        )

    # ------------------------------------------------------------------ #
    #  Helpers                                                              #
    # ------------------------------------------------------------------ #

    @staticmethod
    def extract_company_names(
        profile: dict,
        target_domain: str,
        keyword: str
    ) -> dict:
        """
        Given a full profile dict, walk through 'experiences' and
        return two lists of company names: those that appear BEFORE
        the person joined the target company and those AFTER.

        Proxycurl returns experiences ordered most-recent-first.
        So index 0 = current/most recent job.

        Returns:
            {
                "before": ["Company A", "Company B"],  # prior employers
                "after":  ["Company X"],               # subsequent employers
                "all":    [...]                         # everything (excl. target)
            }
        """
        experiences = profile.get("experiences") or []
        target_slug = target_domain.split(".")[0].lower()

        # Find the index of the target company in the experience list
        target_idx = None
        for i, exp in enumerate(experiences):
            comp = (exp.get("company") or "").lower()
            if target_slug in comp:
                target_idx = i
                break

        before, after = [], []

        for i, exp in enumerate(experiences):
            comp_name = exp.get("company")
            if not comp_name:
                continue
            comp_lower = comp_name.lower()
            if target_slug in comp_lower:
                continue  # skip the target company itself

            if target_idx is None:
                # Can't determine order — add to 'all' only
                after.append(comp_name)
            elif i < target_idx:
                # More recent than target → worked there AFTER leaving target
                after.append(comp_name)
            else:
                # Older than target → worked there BEFORE joining target
                before.append(comp_name)

        return {
            "before": before,
            "after": after,
            "all": before + after,
        }
