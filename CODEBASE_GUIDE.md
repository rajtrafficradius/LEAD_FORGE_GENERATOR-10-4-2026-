# LeadForge V5 — Complete Codebase Guide for Claude Code

## Project Overview

LeadForge is an automated B2B lead generation system. Given an **industry** (e.g. "Dentist", "Electrician") and **country** (AU/USA/UK/India), it:
1. Discovers real business domains via keyword research
2. Finds named contacts (decision-makers) at those businesses
3. Enriches contacts with personal emails + phone numbers
4. Exports a ranked, deduplicated CSV

**Stack:** Python 3.11+, Flask (web UI + API), deployed on Railway via Gunicorn.

---

## File Structure

```
LEAD_FORGE_LEAD_GENERATOR/
├── V5.py              # EVERYTHING: pipeline engine + Flask routes + GUI (tkinter)
├── wsgi.py            # Production Flask app (standalone, mirrors V5.py routes)
├── index.html         # Single-page frontend UI
├── utils.py           # Standalone utility functions (phone/email/name/CSV helpers)
└── README_SETUP.md    # Deployment documentation
```

---

## Architecture: Two Flask Apps (Important!)

**There are two separate Flask `app` objects:**

### 1. `wsgi.py` (Production — used by Railway/Gunicorn)
- `app = Flask(...)` created at module level
- All routes defined at module level
- `/generate` calls `LeadGenerationPipeline` imported from `V5.py`
- Run via: `gunicorn wsgi:app`

### 2. `V5.py` bottom section (Legacy — used when `V5.py` run directly)
- A second `app = Flask(...)` at module level (after line ~3664)
- `main_web()` function also creates a third app internally (dead code in production)
- Run via: `python V5.py` → calls `main_web()`

**For all Railway deployments, `wsgi.py` is the entry point. Only edit routes there.**

---

## V5.py Internal Structure (3838 lines)

### Section 1: Configuration (lines 1–190)
```python
API_KEYS = {
    "semrush": os.environ.get("SEMRUSH_API_KEY", "<hardcoded_fallback>"),
    "serpapi": ..., "apollo": ..., "lusha": ..., "openai": ...
}
COUNTRY_CONFIG = { "AU": {...}, "USA": {...}, "UK": {...}, "India": {...} }
PLATFORM_DOMAINS = { ... }  # ~100 domains to always exclude (Google, Facebook, news sites, etc.)
NON_DECISION_MAKER_KEYWORDS = { "intern", "trainee", ... }  # roles to blank out
DECISION_MAKER_KEYWORDS = { "ceo", "director", "manager", ... }  # for relevance scoring
LOW_RELEVANCE_KEYWORDS = { "intern", "assistant", "receptionist", ... }  # skip enrichment
```

### Section 2: INDUSTRY_KEYWORDS dict (lines 193–760)
```python
INDUSTRY_KEYWORDS = {
    "Dentist": ["dental implants", "root canal treatment", ...],  # ~20-25 keywords each
    "Doctor / General Practitioner": [...],
    "Lawyer / Attorney": [...],
    # 50+ industries total
}
```
This dict powers the `/industries` endpoint and seeds the keyword search.

### Section 3: Helper Functions (lines 760–1132)
- `domain_to_company_name(domain)` — strips TLD, humanizes slug
- `_extract_name_from_company(first, company)` — "Matt" + "Matthew Cornell Photography" → "Matthew Cornell"
- `_extract_name_from_domain(first, domain)` — "Matt" + "matthewcornell.com.au" → "Matthew Cornell"
- `_extract_name_from_linkedin_url(first, url)` — "Matt" + "linkedin.com/in/matthew-cornell-123" → "Matthew Cornell"
- `_get_name_variants(first)` — "matt" → ["matt", "matthew", "mathew"]
- `format_phone(raw, country)` — normalizes to E.164
- `is_platform_domain(domain)` — checks PLATFORM_DOMAINS blocklist + edu/gov/org patterns + news heuristics
- `is_personal_email(email)` — checks if local part is NOT in GENERIC_EMAIL_PREFIXES
- `classify_email_smart(email, name, company)` — 4-rule heuristic classifier
- `generate_email_candidates(first, last, domain)` — generates first.last@, firstlast@, etc.
- `is_decision_maker(role)` — checks DECISION_MAKER_KEYWORDS

### Section 4: API Clients (lines 1133–1811)

#### `RateLimiter` class
Simple token-bucket rate limiter. Every API client holds one.

#### `SemrushClient`
- `get_related_keywords(phrase, database, limit)` → `[{"keyword": str, "volume": int, "cpc": float}]`
- `get_domains_for_keyword(phrase, database, limit)` → `[domain_str, ...]`
- Uses SEMrush REST API (`https://api.semrush.com/`)

#### `SerpApiClient`
- `search_google(query, gl, num)` → `[domain_str, ...]`
- Hits `https://serpapi.com/search.json` with Google search queries
- Used as fallback/supplement to SEMrush domain discovery

#### `ApolloClient`
- `enrich_organization(domain)` → `{"company_name", "phone", "employees", "linkedin", ...}`
- `search_people_by_domain(domain, per_page=10)` → `[{first_name, last_name, title, email, phone_numbers, linkedin_url, ...}]`
- `enrich_person(first, last, domain, linkedin_url)` → `{"name", "role", "email", "phone", "company"}`
- Uses Apollo REST API (`https://api.apollo.io/v1/`)

#### `LushaClient`
- `get_company_info(domain)` → `{"company_name", "description", "employees", "linkedin", ...}`
- `enrich_person(first, last, domain)` → `{"name", "role", "email", "phone", "company"}`
- Uses Lusha REST API (`https://api.lusha.com/v2/`)

#### `OpenAIEmailVerifier`
- `is_personal_email_ai(email, name, company)` → `True/False/None`
- `suggest_emails(name, domain)` → `[email_str, ...]` (OpenAI suggests candidate addresses)
- `verify_leads_batch(leads)` → mutates each lead, adds `lead["_email_type"]` = "Personal"/"Generic"/"Inferred"
- Uses `gpt-4o-mini` via `https://api.openai.com/v1/chat/completions`

#### `WebScraper`
- `scrape_domain(domain)` → `{"emails": [], "phones": [], "company_name": str, "name_email_pairs": []}`
- Hits `https://{domain}` and `https://{domain}/contact`
- Extracts emails via regex + `<a href="mailto:">` tags
- Extracts phones via country-specific regex + `<a href="tel:">` tags
- Extracts company name from `<meta og:site_name>` or `<title>`
- Extracts structured name+email pairs from team/staff HTML sections

### Section 5: LeadGenerationPipeline class (lines 1813–2972)

**Constructor:**
```python
LeadGenerationPipeline(
    industry: str,       # e.g. "Dentist"
    country: str,        # "AU" | "USA" | "UK" | "India"
    min_volume: int,     # minimum keyword search volume (default 100)
    min_cpc: float,      # minimum cost-per-click filter (default 1.0)
    output_folder: str,  # where to write CSVs
    progress_callback,   # fn(pct: int, status: str)
    log_callback,        # fn(message: str)
    max_leads: int,      # 0 = unlimited, N = cap and optimize
)
```

**Instance state:**
```python
self.keywords = []       # populated in Phase 1
self.domains = []        # populated in Phase 3
self.leads = []          # populated in Phase 4, cleaned in Phase 5
self._cancelled = False  # set by cancel()
self._api_counter = {}   # tracks API call counts per service
```

**`pipeline.run()` — the main method (returns path to TOP CSV):**

#### Phase 1: Keyword Expansion (progress 0→15%)
```
For each seed keyword in INDUSTRY_KEYWORDS[industry][:10]:
    → SemrushClient.get_related_keywords(keyword, country_db, limit=15)
    Filter by: volume >= min_volume AND cpc >= min_cpc
    Deduplicate by phrase
Result: self.keywords = [{"keyword", "volume", "cpc"}, ...]  (up to ~150 keywords)
```

#### Phase 2: Domain Discovery from Keywords (progress 15→30%)
```
For each keyword in self.keywords[:50]:
    → SemrushClient.get_domains_for_keyword(keyword, db, limit=10)
    Filter via is_platform_domain()
Result: semrush_domains set
```

#### Phase 3: SerpAPI Supplemental Discovery (progress 30→45%)
```
For each keyword in self.keywords[:20]:
    → SerpApiClient.search_google(f"{keyword} {country}", gl, num=10)
    Filter via is_platform_domain()

# V5.8: Smart domain cap based on max_leads
optimal_domain_cap = max(30, max_leads * 3)  if max_leads > 0 else 150

Merge semrush_domains + serpapi_domains → deduplicated set
self.domains = list(merged)[:optimal_domain_cap]

Fallback: if 0 domains found → ApolloClient.search_organizations(industry, country)
```

#### Phase 4: Lead Enrichment (progress 46→90%) — CORE ENGINE
```
ThreadPoolExecutor(max_workers=8)
For each domain (parallel):
    _enrich_single_domain(domain, index, total)
```

**`_enrich_single_domain(domain)` detail:**
```
# V5.9 Credit gate: skip if already have enough complete leads
if _has_enough_leads(): return []

Step 1: ApolloClient.enrich_organization(domain)
    → company_name, company_phone

Step 2: ApolloClient.search_people_by_domain(domain, per_page=10)
    → list of people with: first_name, last_name, title, email, phone_numbers, linkedin_url

# V5.8: Filter people by relevance if max_leads > 0 and people > 10
people = _filter_people_by_relevance(people, max_leads)

For each person:
    Build lead dict: {name, role, email, phone, domain, company, source}
    
    if lead is complete (full name + personal email + phone): skip to next
    
    Step 2b: ApolloClient.enrich_person(first, last, domain, linkedin_url)
        → merge in any new email/phone
        Skip if LOW_RELEVANCE role
    
    Step 2c: ApolloClient.enrich_person via LinkedIn URL (if available)
        → may get better email
        Skip if LOW_RELEVANCE role
    
    Step 3: LushaClient.get_company_info(domain)
        → company details (skip if all people low-relevance)
    
    Step 4: LushaClient.enrich_person(first, last, domain)
        → personal email + phone from Lusha
        Skip if LOW_RELEVANCE role
    
    Step 5: WebScraper.scrape_domain(domain)
        → extract emails + phones from website
        Try to match scraped emails to person's name
    
    Step 6: Email inference
        generate_email_candidates(first, last, domain)
        → if no email yet, use generated candidate; mark _email_inferred=True

Return domain_leads (list of lead dicts)
```

#### Phase 5: Data Cleanup (progress 90→95%)
```
For each lead:
    Format phone → format_phone(phone, country) → strict E.164 validation
    Clean company name → domain_to_company_name() if missing
    Validate email → regex check, blank if invalid
    Blank non-decision-maker roles (but KEEP the lead)
    Filter: skip if no email AND no phone AND no name
    Deduplicate: key = "name|domain" or email or "phone|domain"
```

#### Phase 5b: OpenAI Email Verification (progress 95%)
```
Batch leads in groups of 20:
    OpenAIEmailVerifier.verify_leads_batch(batch)
    → mutates lead["_email_type"] = "Personal" | "Generic" | "Inferred"
```

#### Phase 6: Scoring, Sorting & CSV Export (progress 95→100%)

**Partition scoring (higher = better lead):**
```
6000 = Name + Email + Phone  (+500 if personal email)
4000 = Name + Phone only
3000 = Name + Email only     (+500 if personal email)
2000 = Phone only
1000 = Email only            (+500 if personal email)
Tiebreakers: +20 domain, +15 role, +10 full name, +5 company
```

**V5.5 Company grouping:** max 3 leads per company (decision-makers first), extras go to `rest_section`.

**CSV output fields:** `Name, Company Name, Domain, Role, Phone Number, Email, Email Type, Notes`

**Two CSVs written:**
- `leads_ALL_{industry}_{country}_{timestamp}.csv` — all leads
- `leads_TOP_{N}_{industry}_{country}_{timestamp}.csv` — top N (or all if max_leads=0)

Returns path to TOP CSV.

---

## Flask API Endpoints (wsgi.py)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | Serves `index.html` |
| GET | `/health` | Returns `{"status": "ok", "version": "V5.7"}` |
| GET | `/<filename>` | Static files (.png, .jpg, .css, .js, etc.) |
| GET | `/industries` | Returns `{"industries": [list of industry names]}` |
| GET | `/api/credits` | Returns service credit status JSON |
| POST | `/api/credits/refresh` | Same as GET credits (force refresh) |
| POST | `/generate` | Starts lead gen job, returns `{"job_id": "abc12345"}` |
| GET | `/status/<job_id>` | Returns job progress, logs, leads, CSVs |
| POST | `/cancel` | Cancels the most recent running job |

### `/generate` request body:
```json
{
    "industry": "Dentist",
    "country": "AU",
    "min_volume": 100,
    "min_cpc": 1.0,
    "max_leads": 20
}
```

### `/status/<job_id>` response (when done):
```json
{
    "state": "done",
    "progress": 100,
    "status_text": "Done! 20 top leads exported",
    "new_logs": ["..."],
    "leads": [{"name", "company", "domain", "role", "phone", "email", "email_type"}],
    "top_csv": "<csv string>",
    "all_csv": "<csv string>",
    "api_usage": {"apollo": 45, "semrush": 12, "lusha": 8, "openai": 3}
}
```

### Job state machine:
```
"running" → "done"
           → "error"  (result["error"] = message)
           → "cancelled"
```

---

## utils.py — Standalone Utilities

Independent module (no Flask dependency). Key functions:
- `normalise_phone(raw, country)` → E.164 string
- `is_valid_email(email)` / `is_generic_email(email)` / `is_disposable_email(email)`
- `clean_name(raw)` → strips honorifics, title-cases
- `strip_domain_tld(domain)` → "matthewcornell.com.au" → "matthewcornell"
- `domain_from_url(url)` → bare domain
- `write_leads_csv(leads, filepath)` / `read_leads_csv(filepath)`
- `deduplicate_leads(leads)` — by (domain+name) and (domain+email)
- `merge_leads(primary, secondary)` — fills blanks from secondary
- `lead_fingerprint(lead)` → MD5 hash for change detection
- `flatten_lead(apollo_response)` → maps Apollo API format to internal schema
- `safe_json_get(data, *keys, default=None)` — safe nested dict traversal

**Note:** `utils.py` has its own `LEAD_FIELDNAMES` schema (different from V5.py's CSV headers). V5.py does NOT import from `utils.py` — they're parallel implementations.

---

## Key Data Schemas

### Internal lead dict (used throughout pipeline):
```python
{
    "name": str,           # "John Smith" or "John" (single name = lower quality)
    "email": str,          # personal preferred over generic
    "phone": str,          # E.164 format e.g. "+61412345678"
    "company": str,        # "Smith Dental"
    "role": str,           # "Practice Manager" (blanked if non-decision-maker)
    "domain": str,         # "smithdental.com.au"
    "source": str,         # "Apollo" | "Lusha" | "Org+Scrape" | "Apollo+Lusha"
    "_email_type": str,    # "Personal" | "Generic" | "Inferred" (internal, removed in CSV)
    "_email_inferred": bool,  # True if email was generated, not found (internal)
    "_score": int,         # partition score (internal, removed in CSV)
}
```

### CSV output schema:
```
Name, Company Name, Domain, Role, Phone Number, Email, Email Type, Notes
```

---

## V5.8 Credit Optimization Logic

```python
# Domain cap (Phase 3)
optimal_domain_cap = max(30, max_leads * 3) if max_leads > 0 else 150

# Relevance filtering (before Phase 4 enrichment)
def _filter_people_by_relevance(people, max_leads):
    # Keep top N*2.5 people, prioritizing decision-maker titles
    keep_count = max(15, int(max_leads * 2.5))
    scored = [(person, _calculate_lead_relevance_score(title)) for person in people]
    scored.sort(key=lambda x: x[1], reverse=True)
    return [p for p, s in scored[:keep_count]]

def _calculate_lead_relevance_score(title):
    # Returns 85+ for decision-makers, 55 for regular, 25 for low-relevance
```

**Skip enrichment (inside `_enrich_single_domain`) for LOW_RELEVANCE roles:**
- Step 2b (Apollo enrich_person)
- Step 2c (Apollo LinkedIn enrich)
- Step 4 (Lusha enrich_person)

---

## Common Modification Patterns

### Add a new industry:
```python
# In V5.py, add to INDUSTRY_KEYWORDS dict:
INDUSTRY_KEYWORDS["Florist"] = [
    "florist near me", "wedding flowers", "flower delivery", ...
]
```

### Add a new country:
```python
# In V5.py COUNTRY_CONFIG:
COUNTRY_CONFIG["CA"] = {
    "name": "Canada",
    "semrush_db": "ca",
    "serpapi_gl": "ca",
    "phone_code": "+1",
    "phone_regex": r"...",
    "phone_digits": 11,
    "location_suffix": "Canada",
}
```

### Add a new API endpoint (wsgi.py):
```python
@app.route("/api/new-endpoint", methods=["GET"])
def new_endpoint():
    return jsonify({"key": "value"})
```

### Change enrichment order in pipeline:
Edit `_enrich_single_domain()` in `V5.py`. Steps 1–6 run sequentially per domain.

### Change lead scoring/ranking:
Edit `_partition_score()` inside `_phase6_export_csv()` in `V5.py`.

### Change CSV output columns:
Edit `fieldnames` list and the `row = {...}` dict inside `_phase6_export_csv()`.

---

## V5.19 Changes (Full-Name Resolution Fix)

**Problem:** Most leads showed only a first name (Matt, Jarod, Riley) instead of the full name.

**Root causes fixed:**
1. Apollo's `mixed_people/api_search` returns `last_name: null` for unrevealed contacts — the name "Matt" (single word) triggers all downstream resolution steps.  
   *(Older API versions returned "Matt M." with a fake initial — `_is_obfuscated_name()` strips that too)*
2. `people/match` was called with just `first_name + domain` — ambiguous for common names.  
   **Fix:** Store Apollo person `id` from search result (`_apollo_id`), pass it to every `enrich_person` call for exact record lookup.
3. `reveal_phone_number: True` in `people/match` payload caused 400 errors (requires a `webhook_url`).  
   **Fix:** Removed that flag — phone comes from `search_people_by_domain` instead.
4. Generic emails (`admin@company.com`) were not replaced by name-based work emails (`riley@24hrpower.net.au`).  
   **Fix:** Step 2b now prefers emails where the local part contains the person's first name over generic-prefix emails.

**New helpers:** `_is_obfuscated_name(name)` — detects "FirstName I." format.

---

## Known Issues / Technical Debt

1. **Duplicate Flask apps:** `V5.py` has TWO `app = Flask(...)` at module level (lines ~1664 and ~3668). The bottom one is the WSGI app. The top one (inside `main_web()`) is only used with `python V5.py`. This causes import confusion.

2. **wsgi.py imports V5.py:** `/generate` does `from V5 import LeadGenerationPipeline` at request time (lazy import). If V5.py has import errors, `/generate` will 500 but other routes still work.

3. **Hardcoded API keys:** `API_KEYS` in V5.py has fallback hardcoded keys. Production should use environment variables via Railway Variables tab.

4. **No persistence:** `_jobs` dict in wsgi.py is in-memory. Jobs lost on server restart. No database.

5. **utils.py not imported by V5.py:** Both files implement overlapping phone/email utilities independently. If refactoring, consolidate to utils.py.

6. **ThreadPoolExecutor 8 workers + rate limiters:** Each worker has its own rate limiter instance per API client. The pipeline creates one shared `ApolloClient`, `LushaClient`, etc. — their `limiter.wait()` is thread-safe (uses `threading.Lock`).

---

## Environment Variables

| Variable | Default (hardcoded fallback) | Purpose |
|----------|------|---------|
| `SEMRUSH_API_KEY` | hardcoded | SEMrush keyword + domain API |
| `SERPAPI_API_KEY` | hardcoded | Google search domain discovery |
| `APOLLO_API_KEY` | hardcoded | Contact enrichment (people + orgs) |
| `LUSHA_API_KEY` | hardcoded | Contact enrichment (person + company) |
| `OPENAI_API_KEY` | hardcoded | Email classification (gpt-4o-mini) |
| `PORT` | 8080 (wsgi.py), 5000 (V5.py) | Server port (Railway sets this automatically) |

---

## Railway Deployment

- **Procfile:** `gunicorn -w 1 -b 0.0.0.0:$PORT --timeout 60 wsgi:app`
- **Entry point:** `wsgi:app` → `wsgi.py` → `app` Flask object
- **Static files:** served directly by Flask from same directory as `wsgi.py`
- **Output CSVs:** written to `./output/{job_id}/` relative to wsgi.py location (ephemeral, lost on restart)
