# streamlit_jira_worklogs_public.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Public Streamlit UI for Jira Worklogs (REST API v3, enhanced /rest/api/3/search/jql)

Features
- Login form (Base URL, Email, API Token, Time zone) — nothing persisted; kept only in session RAM
- Tab A: Month -> XLSX with one sheet per employee (Weekly/Monthly grain toggle)
- Tab B: Employee + Time range -> XLSX for that employee (sheets per Month or per ISO Week)
- Security: no secret logging, masked error details, no caching of credentials, in-memory downloads, Logout

Dependencies:
  pip install streamlit requests pandas openpyxl
"""
import io
from datetime import datetime, timezone, timedelta, date
from typing import Dict, Any, List, Tuple, Optional

import requests
import pandas as pd
import streamlit as st
from requests.auth import HTTPBasicAuth

# zoneinfo (stdlib on Py ≥3.9; fallback to backports for <3.9)
try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # pip install backports.zoneinfo

# ------------------------ Security / UI setup ------------------------
st.set_page_config(page_title="Jira Worklogs → Excel", page_icon="📊", layout="centered")
# Hide detailed tracebacks in the browser
st.set_option("client.showErrorDetails", False)

# Masking helper to avoid printing secrets anywhere
def redact(text: str, secrets: List[str]) -> str:
    try:
        txt = str(text)
    except Exception:
        txt = "<unprintable>"
    for s in secrets:
        if s:
            txt = txt.replace(s, "******")
    return txt

# ------------------------ Date & utils ------------------------
WEEKDAY_EN = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

def month_bounds(ym: str) -> Tuple[datetime, datetime]:
    # ym: "YYYY-MM"
    start = datetime.strptime(ym + "-01", "%Y-%m-%d").replace(tzinfo=timezone.utc)
    nextm = start.replace(year=start.year + 1, month=1) if start.month == 12 else start.replace(month=start.month + 1)
    end = nextm - timedelta(seconds=1)
    return start, end

def month_iter(from_ym: str, to_ym: str) -> List[str]:
    sf = datetime.strptime(from_ym + "-01", "%Y-%m-%d")
    stp = datetime.strptime(to_ym + "-01", "%Y-%m-%d")
    if sf > stp:
        sf, stp = stp, sf
    months = []
    y, m = sf.year, sf.month
    while (y < stp.year) or (y == stp.year and m <= stp.month):
        months.append(f"{y:04d}-{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return months

def parse_started(ts: str) -> datetime:
    for f in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            return datetime.strptime(ts, f)
        except ValueError:
            pass
    raise ValueError(f"Unknown timestamp format in worklog.started: {ts}")

def safe_sheet_name(s: str) -> str:
    for ch in ['\\','/','*','?','[',']',':']:
        s = s.replace(ch, ' ')
    return (s or "Unknown")[:31]

def week_key_local(dt_local: datetime) -> str:
    # ISO week label like 2025-W05
    iso = dt_local.isocalendar()  # (year, week, weekday)
    return f"{iso[0]:04d}-W{iso[1]:02d}"

# ------------------------ Jira HTTP ------------------------
def jira_req(base: str, email: str, token: str, method: str, path: str, **kwargs):
    url = f"{base.rstrip('/')}{path}"
    try:
        r = requests.request(
            method, url,
            auth=HTTPBasicAuth(email, token),
            headers={"Accept": "application/json", "Content-Type": "application/json"},
            timeout=90,
            **kwargs
        )
        r.raise_for_status()
        return r.json() if r.text.strip() else {}
    except requests.HTTPError as e:
        detail = r.text if 'r' in locals() else str(e)
        # Mask secrets + base URL
        raise RuntimeError(redact(f"HTTP {getattr(r, 'status_code', '?')} at {url}\n{detail}", [email, token, base])) from e

def search_issues_jql(base, email, token, jql: str,
                      max_results: int = 100,
                      fields: Optional[List[str]] = None,
                      expand: Optional[str] = None) -> List[Dict[str, Any]]:
    """Enhanced search: POST /rest/api/3/search/jql with nextPageToken pagination"""
    def _post(next_token=None):
        payload = {"jql": jql, "maxResults": max_results}
        if fields: payload["fields"] = fields
        if expand: payload["expand"] = expand
        if next_token: payload["nextPageToken"] = next_token
        return jira_req(base, email, token, "POST", "/rest/api/3/search/jql", json=payload)

    issues, next_tok = [], None
    while True:
        page = _post(next_tok)
        issues.extend(page.get("issues", []))
        if page.get("isLast", True): break
        next_tok = page.get("nextPageToken")
        if not next_tok: break
    return issues

def collect_issue_worklogs(base, email, token, issue_key: str) -> List[Dict[str, Any]]:
    """GET /rest/api/3/issue/{key}/worklog (paginated)"""
    all_logs, start_at = [], 0
    while True:
        data = jira_req(
            base, email, token, "GET",
            f"/rest/api/3/issue/{issue_key}/worklog",
            params={"startAt": start_at, "maxResults": 100}
        )
        logs = data.get("worklogs", []) or []
        all_logs.extend(logs)
        total = data.get("total", len(all_logs))
        start_at += len(logs)
        if start_at >= total or not logs:
            break
    return all_logs

# ------------------------ Exports ------------------------
def export_month_all_staff(base, email, token, ym: str, jql_scope: Optional[str], local_tz: str, grain: str) -> bytes:
    """
    grain: 'Monthly' or 'Weekly'
    - Monthly: one sheet per employee (+ Overview)
    - Weekly:   one sheet per employee (includes Week columns) + Weekly Overview sheet
    """
    tzinfo_local = ZoneInfo(local_tz)
    start, end = month_bounds(ym)
    date_from = start.astimezone(timezone.utc).strftime("%Y-%m-%d")
    date_to = end.astimezone(timezone.utc).strftime("%Y-%m-%d")

    jql = f'worklogDate >= "{date_from}" AND worklogDate <= "{date_to}"'
    if jql_scope: jql = f"({jql_scope}) AND ({jql})"

    issues = search_issues_jql(base, email, token, jql=jql, fields=["summary"])
    by_person: Dict[str, List[Dict[str, Any]]] = {}

    for it in issues:
        key = it.get("key"); summary = (it.get("fields") or {}).get("summary")
        for wl in collect_issue_worklogs(base, email, token, key):
            try:
                dt_utc = parse_started(wl.get("started"))
            except Exception:
                continue
            if not (start <= dt_utc <= end): 
                continue
            dt_local = dt_utc.astimezone(tzinfo_local)
            secs = wl.get("timeSpentSeconds") or 0
            comment = wl.get("comment", "")
            if isinstance(comment, dict):
                comment = comment.get("content") or ""

            row = {
                "Local datetime": dt_local.strftime("%Y-%m-%d %H:%M:%S"),
                "Weekday": WEEKDAY_EN[dt_local.weekday()],
                "ISO Week": week_key_local(dt_local),           # weekly metadata
                "UTC datetime": dt_utc.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "Issue": key,
                "Summary": summary,
                "Seconds": secs,
                "Hours": round(secs / 3600.0, 2),
                "Comment": comment if isinstance(comment, str) else str(comment),
            }
            author = (wl.get("author") or {})
            person = author.get("displayName") or author.get("accountId") or "Unknown"
            by_person.setdefault(person, []).append(row)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        overview = []
        weekly_overview_rows = []
        for person, rows in by_person.items():
            df = pd.DataFrame(rows).sort_values(["Local datetime", "Issue"])
            df.to_excel(writer, index=False, sheet_name=safe_sheet_name(person))
            overview.append({
                "Employee": person,
                "Entries": len(df),
                "Total Hours": round(float(df["Hours"].sum()), 2) if not df.empty else 0.0
            })
            if grain == "Weekly" and not df.empty:
                wk = df.groupby("ISO Week", dropna=False)["Hours"].sum().reset_index()
                for _, r in wk.iterrows():
                    weekly_overview_rows.append({"Employee": person, "ISO Week": r["ISO Week"], "Total Hours": round(float(r["Hours"]), 2)})

        if overview:
            pd.DataFrame(overview).sort_values("Employee").to_excel(writer, index=False, sheet_name="Overview")
        if grain == "Weekly" and weekly_overview_rows:
            pd.DataFrame(weekly_overview_rows).sort_values(["Employee", "ISO Week"]).to_excel(writer, index=False, sheet_name="Weekly Overview")

    output.seek(0)
    return output.read()

def list_employees_in_range(base, email, token, from_ym: str, to_ym: str, jql_scope: Optional[str]) -> Dict[str, Dict[str, str]]:
    start_all, end_all = month_bounds(from_ym)[0], month_bounds(to_ym)[1]
    date_from = start_all.astimezone(timezone.utc).strftime("%Y-%m-%d")
    date_to = end_all.astimezone(timezone.utc).strftime("%Y-%m-%d")
    jql = f'worklogDate >= "{date_from}" AND worklogDate <= "{date_to}"'
    if jql_scope: jql = f"({jql_scope}) AND ({jql})"

    emp: Dict[str, Dict[str, str]] = {}
    issues = search_issues_jql(base, email, token, jql=jql, fields=["summary"])
    for it in issues:
        key = it.get("key")
        for wl in collect_issue_worklogs(base, email, token, key):
            try:
                dt_utc = parse_started(wl.get("started"))
            except Exception:
                continue
            if not (start_all <= dt_utc <= end_all):
                continue
            author = (wl.get("author") or {})
            acc = author.get("accountId")
            name = author.get("displayName") or acc or "Unknown"
            if acc and acc not in emp:
                emp[acc] = {"accountId": acc, "displayName": name}
    return emp

def export_employee_timeline(base, email, token, account_id: str,
                             from_ym: str, to_ym: str,
                             jql_scope: Optional[str], local_tz: str,
                             grain: str) -> bytes:
    """
    For a single employee:
      - Monthly grain:  sheet per YYYY-MM
      - Weekly grain:   sheet per ISO week (YYYY-Www)
    """
    tzinfo_local = ZoneInfo(local_tz)
    start_all, end_all = month_bounds(from_ym)[0], month_bounds(to_ym)[1]
    date_from = start_all.astimezone(timezone.utc).strftime("%Y-%m-%d")
    date_to = end_all.astimezone(timezone.utc).strftime("%Y-%m-%d")
    jql = f'worklogDate >= "{date_from}" AND worklogDate <= "{date_to}"'
    if jql_scope:
        jql = f"({jql_scope}) AND ({jql})"

    issues = search_issues_jql(base, email, token, jql=jql, fields=["summary"])

    # Prepare buckets
    if grain == "Monthly":
        buckets = {ym: [] for ym in month_iter(from_ym, to_ym)}
        def bucket_key(dt_local: datetime) -> str:
            return f"{dt_local.year:04d}-{dt_local.month:02d}"
    else:
        buckets = {}  # create on the fly
        def bucket_key(dt_local: datetime) -> str:
            return week_key_local(dt_local)

    for it in issues:
        key = it.get("key")
        summary = (it.get("fields") or {}).get("summary")
        for wl in collect_issue_worklogs(base, email, token, key):
            try:
                dt_utc = parse_started(wl.get("started"))
            except Exception:
                continue
            if not (start_all <= dt_utc <= end_all):
                continue
            author = (wl.get("author") or {})
            if author.get("accountId") != account_id:
                continue

            dt_local = dt_utc.astimezone(tzinfo_local)
            secs = wl.get("timeSpentSeconds") or 0
            comment = wl.get("comment", "")
            if isinstance(comment, dict):
                comment = comment.get("content") or ""
            row = {
                "Local datetime": dt_local.strftime("%Y-%m-%d %H:%M:%S"),
                "Weekday": WEEKDAY_EN[dt_local.weekday()],
                "ISO Week": week_key_local(dt_local),   # helpful column even for Monthly grain
                "UTC datetime": dt_utc.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "Issue": key,
                "Summary": summary,
                "Seconds": secs,
                "Hours": round(secs / 3600.0, 2),
                "Comment": comment if isinstance(comment, str) else str(comment),
            }
            bkey = bucket_key(dt_local)
            buckets.setdefault(bkey, []).append(row)

    # Build workbook in-memory
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        overview_rows = []
        for bkey in sorted(buckets.keys()):
            rows = buckets[bkey]
            df = pd.DataFrame(rows)
            if not df.empty:
                df = df.sort_values(["Local datetime", "Issue"])
            else:
                df = pd.DataFrame(columns=[
                    "Local datetime","Weekday","ISO Week","UTC datetime","Issue",
                    "Summary","Seconds","Hours","Comment"
                ])
            df.to_excel(writer, index=False, sheet_name=safe_sheet_name(bkey))
            total_h = round(float(df["Hours"].sum()), 2) if not df.empty else 0.0
            label = "Month" if grain == "Monthly" else "ISO Week"
            overview_rows.append({label: bkey, "Entries": len(df), "Total Hours": total_h})

        if overview_rows:
            pd.DataFrame(overview_rows).to_excel(writer, index=False, sheet_name="Overview")
    output.seek(0)
    return output.read()

# ------------------------ Login / Session ------------------------
def logged_in() -> bool:
    return "creds" in st.session_state and all(
        st.session_state["creds"].get(k) for k in ("base", "email", "token", "tz")
    )

with st.sidebar:
    st.header("🔐 Authentication")
    st.text("Enter your Jira credentials. No data is stored server-side; only in session RAM. Create an API token in https://id.atlassian.com/manage-profile/security/api-tokens.")
    if not logged_in():
        with st.form("login"):
            base = st.text_input("Jira Base URL", placeholder="https://your-domain.atlassian.net")
            email = st.text_input("Email")
            token = st.text_input("API Token", type="password")
            tz = st.text_input("Time zone (IANA)", value="Europe/Vienna", help="e.g., Europe/Vienna, UTC, America/New_York")
            submit = st.form_submit_button("Sign in")
        if submit:
            # Store creds only in session (RAM), never cache, never log
            st.session_state["creds"] = {"base": base.strip(), "email": email.strip(), "token": token, "tz": tz.strip()}
            try:
                st.rerun()  # Streamlit ≥ 1.27
            except AttributeError:
                # Fallback für sehr alte Versionen, in denen st.rerun noch nicht existierte
                st.experimental_rerun()
    else:
        st.success("Signed in")
        if st.button("Logout"):
            st.session_state.clear()
            try:
                st.rerun()  # Streamlit ≥ 1.27
            except AttributeError:
                # Fallback für sehr alte Versionen, in denen st.rerun noch nicht existierte
                st.experimental_rerun()

if not logged_in():
    st.stop()

# For convenience
creds = st.session_state["creds"]
BASE, EMAIL, TOKEN, TZ = creds["base"], creds["email"], creds["token"], creds["tz"]

st.title("📊 Jira Worklogs → Excel")
st.caption("Generate Excel files from Jira worklogs — no credentials stored server-side. Choose Monthly or Weekly granularity.")

tab1, tab2 = st.tabs(["Month → All employees", "Employee + Time range"])

# ------------------------ Tab 1: Month → All employees ------------------------
with tab1:
    today = date.today()
    default_month = f"{today.year}-{today.month:02d}"
    month = st.text_input("Month (YYYY-MM)", value=default_month, placeholder="e.g., 2025-09", key="t1_month")
    scope = st.text_input("Optional JQL scope (e.g., project = ABC)", value="", key="t1_scope")
    grain = st.selectbox("Granularity", options=["Monthly", "Weekly"], index=0, key="t1_grain",
                         help="Monthly: one sheet per employee; Weekly: plus ISO week columns & a Weekly Overview sheet.")
    if st.button("Generate Excel (all employees)", key="t1_btn"):
        try:
            datetime.strptime(month + "-01", "%Y-%m-%d")
        except ValueError:
            st.error("Please enter month as YYYY-MM, e.g., 2025-09.")
            st.stop()
        try:
            st.info("Fetching issues and worklogs…")
            xlsx_bytes = export_month_all_staff(
                base=BASE, email=EMAIL, token=TOKEN,
                ym=month.strip(), jql_scope=scope.strip() or None,
                local_tz=TZ, grain=grain
            )
            filename = f"worklogs_all_{month}_{grain.lower()}.xlsx"
            st.success("Done. Your file is ready.")
            st.download_button(
                label="📥 Download Excel",
                data=xlsx_bytes,
                file_name=filename,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        except Exception as e:
            st.error("Request failed.")
            with st.expander("Error details"):
                st.code(redact(e, [EMAIL, TOKEN, BASE]))

# ------------------------ Tab 2: Employee + Time range ------------------------
with tab2:
    c1, c2 = st.columns(2)
    with c1:
        from_month = st.text_input("Start month (YYYY-MM)", value=default_month, key="t2_from")
    with c2:
        to_month = st.text_input("End month (YYYY-MM)", value=default_month, key="t2_to")
    scope2 = st.text_input("Optional JQL scope (e.g., project = ABC)", value="", key="t2_scope")

    # Load employees present in the selected range
    if st.button("Load employees from range", key="t2_load"):
        try:
            datetime.strptime(from_month + "-01", "%Y-%m-%d")
            datetime.strptime(to_month + "-01", "%Y-%m-%d")
        except ValueError:
            st.error("Please provide months as YYYY-MM, e.g., 2025-01 and 2025-03.")
            st.stop()
        try:
            st.info("Scanning worklogs to list employees in range…")
            emps = list_employees_in_range(
                base=BASE, email=EMAIL, token=TOKEN,
                from_ym=from_month.strip(), to_ym=to_month.strip(),
                jql_scope=scope2.strip() or None
            )
            if not emps:
                st.warning("No employees with worklogs in the selected period.")
            st.session_state["t2_emps"] = emps
        except Exception as e:
            st.error("Failed to list employees.")
            with st.expander("Error details"):
                st.code(redact(e, [EMAIL, TOKEN, BASE]))

    emps = st.session_state.get("t2_emps", {})
    if emps:
        # Sort by displayName
        options = sorted(emps.values(), key=lambda x: (x.get("displayName") or "").lower())
        labels = [f'{e["displayName"]}  ({e["accountId"][:8]}…)' for e in options]
        idx = st.selectbox("Choose employee", options=list(range(len(options))), format_func=lambda i: labels[i], key="t2_select")
        selected_acc = options[idx]["accountId"]
        selected_name = options[idx]["displayName"]

        grain2 = st.selectbox("Granularity", options=["Monthly", "Weekly"], index=0, key="t2_grain",
                              help="Monthly: sheets per month; Weekly: sheets per ISO week.")
        if st.button("Generate Excel for employee", key="t2_btn"):
            try:
                st.info(f"Generating file for {selected_name}…")
                xlsx_bytes = export_employee_timeline(
                    base=BASE, email=EMAIL, token=TOKEN,
                    account_id=selected_acc,
                    from_ym=from_month.strip(), to_ym=to_month.strip(),
                    jql_scope=scope2.strip() or None,
                    local_tz=TZ, grain=grain2
                )
                fn = f"worklogs_{selected_name.replace(' ', '_')}_{from_month}_{to_month}_{grain2.lower()}.xlsx"
                st.success("Done. Your file is ready.")
                st.download_button(
                    label="📥 Download Excel",
                    data=xlsx_bytes,
                    file_name=fn,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            except Exception as e:
                st.error("Request failed.")
                with st.expander("Error details"):
                    st.code(redact(e, [EMAIL, TOKEN, BASE]))
