"""
Recruitment Sourcing Tool — Streamlit app (People Data Labs edition).

Workflow
--------
1. Paste the target company's LinkedIn URL + a job title keyword
2. PDL finds everyone (current + past) at that company with the keyword in their title
3. Extracts company names from their full career histories (no personal data stored)
4. Builds a ranked company universe of where those people worked before & after
5. Searches each universe company for people currently there with the keyword
6. Exports company universe + candidate list as CSV

Run: streamlit run app.py
"""

import streamlit as st
import pandas as pd
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

from pdl_client import PDLClient
import database as db

st.set_page_config(
    page_title="Recruitment Sourcer",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded",
)

db.init_db()

# ------------------------------------------------------------------ #
#  Sidebar                                                              #
# ------------------------------------------------------------------ #
with st.sidebar:
    st.title("🎯 Recruitment Sourcer")
    st.caption("Powered by People Data Labs")
    st.divider()

    api_key = st.text_input(
        "PDL API Key",
        value=os.getenv("PDL_API_KEY", ""),
        type="password",
        help="Get your free key (100 calls/month) at peopledatalabs.com",
    )

    st.divider()
    st.subheader("New Search")

    company_li_url = st.text_input(
        "Company LinkedIn URL",
        placeholder="https://www.linkedin.com/company/analogue-creative/",
        help="Paste the company's LinkedIn URL — this is precise and avoids false matches",
    )
    keyword = st.text_input(
        "Job Title Keyword — universe building",
        placeholder="motion",
        help="Used in Step 1 to find seed people at the target company. "
             "'motion' finds Motion Designer, Motion Director, Head of Motion, etc.",
    )
    candidate_keyword = st.text_input(
        "Job Title Keyword — candidate search",
        placeholder="motion director",
        help="Used in Step 3 to search universe companies for candidates. "
             "Can be narrower than the universe keyword — e.g. 'motion director'. "
             "Leave blank to use the same keyword as above.",
    )
    include_past = st.toggle(
        "Include past employees",
        value=True,
        help="On: finds anyone who has EVER worked there with this title. "
             "Off: current employees only.",
    )
    max_companies = st.slider(
        "Top N companies to search for candidates",
        min_value=5, max_value=30, value=15,
        help="After building the universe, how many companies to search. "
             "Each company = 1 PDL API call.",
    )

    run_btn = st.button("▶  Run Search", type="primary", use_container_width=True)

    st.divider()
    st.subheader("Past Searches")
    past = db.list_searches()
    if past:
        labels = {
            s["id"]: f"{s['company_name'] or s['company_url']} — {s['keyword']}  "
                     f"({s['created_at'][:10]})"
            for s in past
        }
        selected_id = st.selectbox(
            "Load a previous search",
            options=[None] + [s["id"] for s in past],
            format_func=lambda x: "— select —" if x is None else labels[x],
        )
    else:
        selected_id = None
        st.caption("No searches yet.")


# ------------------------------------------------------------------ #
#  Show saved results                                                   #
# ------------------------------------------------------------------ #
def show_results(search_id: int):
    search = db.get_search(search_id)
    st.header(
        f"Results — **{search['company_name'] or search['company_url']}**  "
        f"· keyword: **{search['keyword']}**"
    )
    tab1, tab2 = st.tabs(["🏢 Company Universe", "👤 Candidates"])

    with tab1:
        universe = db.get_company_universe(search_id)
        if universe:
            df = pd.DataFrame(universe)[[
                "company_name", "count_total",
                "count_before", "count_after", "company_linkedin_url"
            ]]
            df.columns = [
                "Company", "Total",
                "Came from here (before)", "Went here after", "LinkedIn URL"
            ]
            st.dataframe(
                df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "LinkedIn URL": st.column_config.LinkColumn(
                        "LinkedIn URL", display_text="Open ↗"
                    )
                },
            )
            st.download_button(
                "⬇ Download company list",
                df.to_csv(index=False).encode(),
                "company_universe.csv", "text/csv",
            )
        else:
            st.info("No company universe data yet.")

    with tab2:
        candidates = db.get_candidates(search_id)
        if candidates:
            df = pd.DataFrame(candidates)[["name", "title", "company", "linkedin_url"]]
            df.columns = ["Name", "Title", "Company", "LinkedIn URL"]
            st.dataframe(
                df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "LinkedIn URL": st.column_config.LinkColumn(
                        "LinkedIn URL", display_text="Open ↗"
                    )
                },
            )
            st.download_button(
                "⬇ Download candidate list",
                df.to_csv(index=False).encode(),
                "candidates.csv", "text/csv",
            )
        else:
            st.info("No candidates found yet.")


# ------------------------------------------------------------------ #
#  Load past search                                                     #
# ------------------------------------------------------------------ #
if not run_btn and selected_id:
    show_results(selected_id)
    st.stop()

if not run_btn:
    st.markdown("""
    ## How it works

    1. **Paste** the company's LinkedIn URL (e.g. `linkedin.com/company/analogue-creative`) and a keyword
    2. **Step 1** — PDL finds everyone with that keyword in their title who works/worked there
    3. **Step 2** — Extracts company names from their career histories *(no personal data stored — just company names)*
    4. **Step 3** — Searches those companies for people currently there with the keyword
    5. **Export** — Download your company universe and candidate list as CSV

    ---
    > **Example:** LinkedIn URL = `linkedin.com/company/analogue-creative` · Keyword = `motion`
    """)
    st.stop()

# ------------------------------------------------------------------ #
#  Validate                                                             #
# ------------------------------------------------------------------ #
if not api_key:
    st.error("Please enter your PDL API key in the sidebar.")
    st.stop()
if not company_li_url or not keyword:
    st.error("Please enter a LinkedIn URL and keyword.")
    st.stop()

client = PDLClient(api_key)

# Normalise the LinkedIn URL for PDL
pdl_company_url = PDLClient.normalise_linkedin_url(company_li_url)
company_slug = PDLClient.extract_linkedin_company_slug(company_li_url)

st.header(f"Searching: **{company_li_url}**  ·  keyword: **{keyword}**")

# Debug box — confirms exactly what values Streamlit has captured from your inputs
with st.expander("🔍 Debug — confirm what's being sent to PDL", expanded=True):
    st.markdown(f"""
| Field | Value |
|---|---|
| Raw input URL | `{company_li_url}` |
| Normalised for PDL | `{pdl_company_url}` |
| Company slug | `{company_slug}` |
| Universe keyword | `{keyword}` |
| Candidate search keyword | `{candidate_keyword.strip() if candidate_keyword.strip() else keyword + ' (same as above)'}` |
| Include past employees | `{include_past}` |
| Max universe companies | `{max_companies}` |
""")

search_id = db.create_search(company_li_url, keyword, company_name=company_slug)

# ------------------------------------------------------------------ #
#  Step 1: Find seed people at target company                          #
# ------------------------------------------------------------------ #
with st.status(
    f"Step 1 — Finding '{keyword}' people at {company_slug}…", expanded=True
) as step1:
    try:
        result = client.search_people(
            company_linkedin_url=pdl_company_url,
            title_keyword=keyword,
            size=25,
            include_past=include_past,
        )
    except Exception as e:
        st.error(f"PDL API error: {e}")
        st.stop()

    seed_people = result.get("data", [])
    total_in_db = result.get("total", 0)

    if not seed_people:
        st.error(
            f"No people found with **'{keyword}'** at **{company_slug}**. "
            "Check the LinkedIn URL is correct and try a broader keyword."
        )
        st.stop()

    current_count = sum(
        1 for p in seed_people
        if (p.get("job_company_linkedin_url") or "").lower() == pdl_company_url.lower()
    )
    past_count = len(seed_people) - current_count
    st.write(
        f"✅ Found **{len(seed_people)}** people total — "
        f"**{current_count}** current · **{past_count}** past "
        f"(PDL reports **{total_in_db}** matching in database)"
    )
    step1.update(
        label=f"Step 1 ✅ — {len(seed_people)} seed profiles found "
              f"({'current + past' if include_past else 'current only'})",
        state="complete",
    )

# ------------------------------------------------------------------ #
#  Step 2: Build company universe from career histories                #
# ------------------------------------------------------------------ #
st.subheader("Step 2 — Building company universe from career histories")

# PDL returns experience data in the same call — no extra API calls needed!
confirmed = 0
skipped = 0

for person in seed_people:
    companies = PDLClient.extract_work_history(person, company_slug)

    if not companies["confirmed"]:
        skipped += 1
        continue

    confirmed += 1
    for rel in ("before", "after"):
        for comp in companies[rel]:
            db.upsert_company(
                search_id=search_id,
                company_name=comp["name"],
                company_linkedin_url=comp.get("linkedin_url", ""),
                relationship=rel,
            )

st.success(
    f"✅ Processed **{confirmed}** confirmed profiles "
    f"({skipped} skipped) — company universe built"
)

universe = db.get_company_universe(search_id)
if universe:
    udf = pd.DataFrame(universe)[[
        "company_name", "count_total", "count_before",
        "count_after", "company_linkedin_url"
    ]]
    udf.columns = [
        "Company", "Total",
        "Came from here (before Analogue)", "Went here after", "LinkedIn URL"
    ]
    st.dataframe(udf, use_container_width=True, hide_index=True)
else:
    st.warning("No companies extracted — the people found may not have work history in PDL.")
    st.stop()

# ------------------------------------------------------------------ #
#  Step 3: Find candidates at universe companies                        #
# ------------------------------------------------------------------ #
search_keyword = candidate_keyword.strip() if candidate_keyword.strip() else keyword
st.subheader(f"Step 3 — Finding **'{search_keyword}'** candidates at top {max_companies} companies")

# Only search companies that have a LinkedIn URL (needed for PDL query)
searchable = [
    u for u in universe
    if u.get("company_linkedin_url")
][:max_companies]

if not searchable:
    st.warning(
        "None of the universe companies have LinkedIn URLs in PDL — "
        "this can happen for very small/obscure studios. "
        "You can manually search the company list above on LinkedIn."
    )
else:
    prog = st.progress(0, text="Searching companies…")
    total_candidates = 0

    for i, company in enumerate(searchable):
        prog.progress(
            (i + 1) / len(searchable),
            text=f"Searching {company['company_name']} ({i+1}/{len(searchable)})…",
        )
        try:
            people = client.find_candidates_at_company(
                company_linkedin_url=PDLClient.normalise_linkedin_url(
                    company["company_linkedin_url"]
                ),
                title_keyword=search_keyword,
                size=25,
            )
        except Exception:
            continue

        for person in people:
            li_url = (
                f"https://www.{person.get('linkedin_url', '')}"
                if person.get("linkedin_url") else ""
            )
            db.save_candidate(
                search_id=search_id,
                name=person.get("full_name", ""),
                linkedin_url=li_url,
                title=person.get("job_title", ""),
                company=company["company_name"],
            )
            total_candidates += 1

    prog.empty()
    st.success(
        f"✅ Found **{total_candidates}** candidates across "
        f"**{len(searchable)}** companies"
    )

# ------------------------------------------------------------------ #
#  Final results                                                        #
# ------------------------------------------------------------------ #
st.divider()
show_results(search_id)
