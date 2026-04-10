-#!/usr/bin/env python3
"""
Lead Generation Automation Tool — V5.12
========================================
A production-ready B2B lead generation tool optimized for PAID traffic only (Google Ads).

V5.12 ENHANCEMENTS (MAJOR):
✓ PAID-ONLY MODE: Removed get_organic_domains() entirely. Keeps ONLY domains with active Google Ads (paid_traffic != 0)
✓ DOUBLED KEYWORDS: Each of 50+ industries has 20-25 NEW keywords (originals kept exactly)
✓ TOKEN CONSERVATION: Only enrich top N leads with API calls (phone/email per max_leads)
✓ TOKEN TRACKING PANEL: Comprehensive run summary showing:
  - Lead sources: PAID vs ORGANIC breakdown
  - Contact coverage: Phone / Email / Personal Email counts
  - API token usage: SEMrush / Apollo / Lusha / SerpApi / OpenAI per run
  - Enrichment efficiency: How many leads got direct phone & verified emails
✓ PERSONAL EMAIL PRIORITY: Search APIs manually for personal emails, prefer business owners
✓ LOGGING: End-of-run summary with paid/organic/token breakdown

V5.4-V5.11 Features (inherited):
- Partition-based sorting: Name+Email+Phone → Name+Phone → Name+Email → Phone → Email
- Three-tier decision maker engine (HARD_DM / SOFT_DM / TRADE_ROLE_WORDS)
- 60+ personal email domains classifier (gmail, yahoo, hotmail, icloud, bigpond, etc.)
- ThreadPoolExecutor with batched submission (8 workers, 8-domain batches)
- Credit gate with phone validation (_direct_phone flag)
- Company size filter (skip >500 employees — not SMB targets)

Pipeline: SEMrush Keywords → SEMrush/PAID-ONLY Domain Discovery → Apollo/Lusha Enrichment → CSV Export + Token Summary

Requirements: requests, beautifulsoup4
Target: Python 3.11+ / PyCharm 2025.3
"""

import csv
import html as html_mod
import json
import os
import platform
import random
import re
import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from urllib.parse import urlencode, urlparse

import requests
from bs4 import BeautifulSoup

# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION & CONSTANTS
# ══════════════════════════════════════════════════════════════════════════════

API_KEYS = {
    "semrush": os.environ.get("SEMRUSH_API_KEY", ""),
    "serpapi": os.environ.get("SERPAPI_API_KEY", ""),
    "apollo": os.environ.get("APOLLO_API_KEY", ""),
    "lusha": os.environ.get("LUSHA_API_KEY", ""),
    "openai": os.environ.get("OPENAI_API_KEY", ""),
    "hunter": os.environ.get("HUNTER_API_KEY", ""),  # Optional: Hunter.io email enrichment (set env var)
}

# V5.13: Credit cost per API call (for cost tracking panel)
API_CREDIT_COSTS = {
    "semrush": 10,    # ~10 units per API call
    "apollo": 1,      # 1 Apollo export credit per enrichment call
    "lusha": 1,       # 1 Lusha credit per person lookup
    "serpapi": 1,     # 1 SerpApi search credit
    "openai": 0.01,   # ~$0.01 per OpenAI gpt-4o-mini call
    "hunter": 1,      # 1 Hunter.io request credit
}

# V5.13: Rotating User-Agent headers for anti-detection
_ROTATING_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
]

def _get_random_ua() -> str:
    return random.choice(_ROTATING_USER_AGENTS)

# V5.26: Apollo webhook-based phone reveal
# Set WEBHOOK_BASE_URL to your app's public URL (e.g., https://your-app.up.railway.app)
# When set, the Flask endpoint at /api/apollo-phone-callback receives phone data directly.
# When NOT set, a temporary webhook.site token is created automatically for each pipeline run.
WEBHOOK_BASE_URL = os.environ.get("WEBHOOK_BASE_URL", "")
_APOLLO_PHONE_CALLBACK_PATH = "/api/apollo-phone-callback"

# V5.26: Thread-safe phone reveal store — receives async phone data from Apollo webhook
_phone_reveal_store: dict = {}  # person_id -> {"phone_numbers": [...], "received": bool}
_phone_reveal_lock = threading.Lock()
_webhook_site_token: str = ""  # webhook.site token UUID (used when no WEBHOOK_BASE_URL)

def _receive_phone_reveal(person_id: str, phone_numbers: list):
    """Called by webhook handler when Apollo delivers phone data."""
    with _phone_reveal_lock:
        _phone_reveal_store[person_id] = {"phone_numbers": phone_numbers, "received": True}

def _collect_phone_reveal(person_id: str):
    """Collect phone reveal data if available. Returns phone_numbers list or None."""
    with _phone_reveal_lock:
        entry = _phone_reveal_store.get(person_id)
        if entry and entry.get("received"):
            return entry.get("phone_numbers")
    return None

def _register_phone_reveal(person_id: str):
    """Register a person_id as pending phone reveal."""
    with _phone_reveal_lock:
        if person_id not in _phone_reveal_store:
            _phone_reveal_store[person_id] = {"phone_numbers": None, "received": False}

def _cleanup_phone_reveals():
    """Clear all phone reveal entries (call between pipeline runs)."""
    with _phone_reveal_lock:
        _phone_reveal_store.clear()

def _create_webhook_site_token() -> str:
    """Create a temporary webhook.site token for receiving Apollo phone callbacks."""
    try:
        resp = requests.post("https://webhook.site/token", timeout=15)
        if resp.status_code == 201:
            return resp.json().get("uuid", "")
    except Exception:
        pass
    return ""

def _poll_webhook_site_phones(token_uuid: str):
    """Poll webhook.site for received Apollo phone reveal data and store it."""
    if not token_uuid:
        return
    try:
        resp = requests.get(
            f"https://webhook.site/token/{token_uuid}/requests?sorting=newest&per_page=50",
            timeout=15
        )
        if resp.status_code == 200:
            import json as _json
            for req in resp.json().get("data", []):
                content = req.get("content", "")
                if not content:
                    continue
                try:
                    payload = _json.loads(content)
                    for person in payload.get("people", []):
                        pid = person.get("id", "")
                        phones = person.get("phone_numbers") or []
                        if pid and phones:
                            _receive_phone_reveal(pid, phones)
                except Exception:
                    pass
    except Exception:
        pass

def _delete_webhook_site_token(token_uuid: str):
    """Delete a webhook.site token after use."""
    if token_uuid:
        try:
            requests.delete(f"https://webhook.site/token/{token_uuid}", timeout=5)
        except Exception:
            pass

def _get_webhook_url() -> str:
    """Get the webhook URL for Apollo phone reveal.
    Uses WEBHOOK_BASE_URL if set (self-hosted), otherwise creates a webhook.site token."""
    global _webhook_site_token
    if WEBHOOK_BASE_URL:
        return WEBHOOK_BASE_URL.rstrip("/") + _APOLLO_PHONE_CALLBACK_PATH
    # Create or reuse webhook.site token
    if not _webhook_site_token:
        _webhook_site_token = _create_webhook_site_token()
    if _webhook_site_token:
        return f"https://webhook.site/{_webhook_site_token}"
    return ""

# V5.7: Credit tracking constants
LUSHA_PLAN_CREDITS = 1000  # Set to your Lusha plan's credit allocation
SEMRUSH_PLAN_TOTAL = 50000  # Default Semrush plan total (used when only remaining is available)
_lusha_calls_total = 0  # Running total of Lusha API calls since server start

COUNTRY_CONFIG = {
    "AU": {
        "name": "Australia",
        "semrush_db": "au",
        "serpapi_gl": "au",
        "phone_code": "+61",
        "phone_regex": r"(?:\+61\s?|0)[2-478](?:[\s.-]?\d){8}",
        "phone_digits": 11,
        "location_suffix": "Australia",
    },
    "USA": {
        "name": "United States",
        "semrush_db": "us",
        "serpapi_gl": "us",
        "phone_code": "+1",
        "phone_regex": r"(?:\+1[\s.-]?)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}",
        "phone_digits": 11,
        "location_suffix": "United States",
    },
    "UK": {
        "name": "United Kingdom",
        "semrush_db": "uk",
        "serpapi_gl": "uk",
        "phone_code": "+44",
        "phone_regex": r"(?:\+44\s?|0)\d{2,4}[\s.-]?\d{3,4}[\s.-]?\d{3,4}",
        "phone_digits": 12,
        "location_suffix": "United Kingdom",
    },
    "India": {
        "name": "India",
        "semrush_db": "in",
        "serpapi_gl": "in",
        "phone_code": "+91",
        "phone_regex": r"(?:\+91[\s.-]?|0)?[6-9]\d{9}",
        "phone_digits": 12,
        "location_suffix": "India",
    },
}

# Platform domains to filter out during domain discovery
PLATFORM_DOMAINS = {
    "google.com", "google.com.au", "google.co.uk", "google.co.in",
    "facebook.com", "instagram.com", "twitter.com", "x.com",
    "linkedin.com", "youtube.com", "tiktok.com", "pinterest.com",
    "yelp.com", "yelp.com.au", "yellowpages.com", "yellowpages.com.au",
    "wikipedia.org", "reddit.com", "quora.com", "medium.com",
    "amazon.com", "ebay.com", "ebay.com.au", "alibaba.com",
    "tripadvisor.com", "trustpilot.com", "bbb.org",
    "apple.com", "microsoft.com", "adobe.com",
    "healthgrades.com", "webmd.com", "zocdoc.com",
    "thumbtack.com", "homeadvisor.com", "angi.com", "angieslist.com",
    "glassdoor.com", "indeed.com", "seek.com.au",
    "truelocal.com.au", "hotfrog.com.au", "startlocal.com.au",
    "whitepages.com.au", "yell.com", "justdial.com", "sulekha.com",
    "indiamart.com", "practo.com", "justlanded.com",
    "crunchbase.com", "bloomberg.com", "forbes.com",
    "gov.au", "nhs.uk", "gov.uk", "gov.in", "fda.gov",
    "healthengine.com.au", "hotdoc.com.au", "ratemds.com",
    "wordofmouth.com.au", "localsearch.com.au",
    "finder.com.au", "canstar.com.au", "productreview.com.au",
    "serviceseeking.com.au", "hipages.com.au", "oneflare.com.au",
    "airtasker.com", "bark.com",
    # Health/medical info sites (not actual practices)
    "healthline.com", "mayoclinic.org", "clevelandclinic.org",
    "my.clevelandclinic.org", "webmd.com", "medicalnewstoday.com",
    "verywellhealth.com", "betterhealth.vic.gov.au",
    # Large retailers/corporates (not SMBs)
    "woolworths.com.au", "chemistwarehouse.com.au", "priceline.com.au",
    "amazon.com.au", "colgate.com.au", "colgate.com",
    "bupa.com", "bupa.com.au", "bupaglobal.com",
    "bupaagedcare.com.au", "bupatravelinsurance.com.au",
    # Educational / government
    "sydney.edu.au", "unimelb.edu.au", "uq.edu.au",
    "monash.edu", "adelaide.edu.au", "unsw.edu.au",
    # News / media sites (not SMBs)
    "news.com.au", "smh.com.au", "theaustralian.com.au", "abc.net.au",
    "9news.com.au", "7news.com.au", "sbs.com.au", "dailytelegraph.com.au",
    "couriermail.com.au", "heraldsun.com.au", "theage.com.au",
    "newcastleherald.com.au", "illawarramercury.com.au", "canberratimes.com.au",
    "brisbanetimes.com.au", "watoday.com.au", "examiner.com.au",
    "perthnow.com.au", "adelaidenow.com.au", "geelongadvertiser.com.au",
    "goldcoastbulletin.com.au", "townsvillebulletin.com.au", "cairnspost.com.au",
    "cnn.com", "bbc.com", "bbc.co.uk", "nytimes.com", "theguardian.com",
    "foxnews.com", "nbcnews.com", "cbsnews.com", "abcnews.go.com",
    "reuters.com", "apnews.com", "usatoday.com", "washingtonpost.com",
    "wsj.com", "huffpost.com", "nypost.com", "latimes.com", "chicagotribune.com",
    "dailymail.co.uk", "telegraph.co.uk", "independent.co.uk",
    "mirror.co.uk", "express.co.uk", "thesun.co.uk", "sky.com", "itv.com",
    "metro.co.uk", "standard.co.uk", "scotsman.com", "walesonline.co.uk",
    "ndtv.com", "timesofindia.indiatimes.com", "hindustantimes.com",
    "thehindu.com", "indianexpress.com", "news18.com", "livemint.com",
    "dnaindia.com", "deccanherald.com", "tribuneindia.com",
    "firstpost.com", "scroll.in", "theprint.in", "thewire.in",
    "buzzfeed.com", "vice.com", "vox.com", "businessinsider.com",
    "techcrunch.com", "theverge.com", "wired.com", "mashable.com",
    "arstechnica.com", "engadget.com", "gizmodo.com",
}

# Non-decision-maker role keywords — leads with these roles get role blanked (lead kept)
NON_DECISION_MAKER_KEYWORDS = {
    "intern", "trainee", "volunteer", "student", "apprentice",
    "janitor", "custodian", "mail room", "filing",
    "warehouse", "driver", "delivery", "labourer", "laborer","plumber"
}

# ── Decision-maker engine (V5.11) ─────────────────────────────────────────────
# HARD_DM: these titles ALWAYS indicate a genuine decision maker regardless of industry
HARD_DM_KEYWORDS = {
    "owner", "co-owner", "business owner",
    "founder", "co-founder", "cofounder",
    "ceo", "chief executive", "managing director", "md",
    "cfo", "cto", "coo", "cmo", "cio", "cpo",
    "chief", "chairman", "chairwoman", "chairperson",
    "president", "vice president", "vp",
    "director", "general manager", "gm",
    "partner", "principal",
    "board member", "board director",
    "svp", "evp", "avp",
}

# SOFT_DM: only a DM if no trade/craft word is also in the title
SOFT_DM_KEYWORDS = {
    "manager", "head of", "head,",
    "executive", "managing", "supervisor",
    "operations manager", "office manager",
    "business development", "account director",
}

# TRADE_ROLE_WORDS: craft/trade words — override SOFT_DM if present in title
# e.g. "Lead Plumber" → NOT a DM; "Managing Director" → DM
TRADE_ROLE_WORDS = {
    "plumber", "plumbing",
    "electrician", "electrical worker",
    "carpenter", "joiner", "cabinetmaker",
    "painter", "decorator",
    "mechanic", "automotive technician",
    "welder", "boilermaker",
    "bricklayer", "stonemason", "concreter",
    "plasterer", "tiler", "renderer",
    "roofer", "guttering",
    "landscaper", "gardener", "arborist",
    "cleaner", "cleaning staff",
    "handyman", "maintenance worker",
    "locksmith", "glazier",
    "hvac technician", "refrigeration mechanic",
    "forklift operator", "crane operator",
    "labourer", "laborer",
    "apprentice", "trainee",
    "installer", "fitter",
    "surveyor", "drafter",
    "estimator",  # only soft DM when combined with trade words
    # V5.25: Additional service/trade roles — these are practitioners, not decision-makers
    "dentist", "general dentist", "dental assistant", "dental hygienist",
    "dental nurse", "dental therapist", "hygienist",
    "photographer", "videographer",
    "accountant", "bookkeeper",
    "nurse", "registered nurse", "nurse practitioner",
    "therapist", "physiotherapist", "chiropractor",
    "pest control technician", "pest technician",
    "real estate agent", "property manager",
    "barber", "hairdresser", "stylist",
    "chef", "cook",
    "driver", "courier",
}

# V5.8: Decision-maker role keywords (combined for backward compat with scoring)
DECISION_MAKER_KEYWORDS = HARD_DM_KEYWORDS | SOFT_DM_KEYWORDS

# V5.13: Words that should NEVER appear as part of a person's name
_NAME_FORBIDDEN_WORDS = (
    HARD_DM_KEYWORDS | SOFT_DM_KEYWORDS | TRADE_ROLE_WORDS |
    NON_DECISION_MAKER_KEYWORDS |
    {
        # Common title words
        "director", "manager", "officer", "executive", "president", "chairman",
        "supervisor", "coordinator", "specialist", "analyst", "consultant",
        "engineer", "technician", "advisor", "associate", "assistant",
        "trainee", "intern", "apprentice", "graduate", "professional",
        # Business/company words often appearing in team page context
        "pty", "ltd", "inc", "llc", "corp", "group", "team", "staff",
        "operations", "services", "solutions", "company", "business",
        "plumbing", "dental", "medical", "clinic", "rescue", "local",
        # Common false positives
        "the", "and", "for", "our", "new", "top", "pro", "all",
    }
)


def _is_valid_person_name(name: str) -> bool:
    """V5.13: Return True only if `name` looks like a genuine person's name.
    Rules:
    - Must have exactly 2 or 3 words (First Last or First Middle Last)
    - Each word must be 2-20 chars, alphabetic (hyphens/apostrophes OK)
    - NO word may be a known title, role, trade, or business keyword
    """
    if not name:
        return False
    words = name.strip().split()
    if not (2 <= len(words) <= 3):
        return False
    for w in words:
        clean = w.replace("'", "").replace("-", "")
        if not clean.isalpha():
            return False
        if not (2 <= len(clean) <= 20):
            return False
        if w.lower() in _NAME_FORBIDDEN_WORDS:
            return False
    return True


def _is_obfuscated_name(name: str) -> bool:
    """V5.19: Detect Apollo's locked-contact obfuscated name format.
    Apollo returns 'FirstName I.' (first name + single letter + period) for contacts
    that haven't been revealed yet, e.g. 'Matt M.', 'Jarod L.', 'Riley H.'.
    Returns True if name matches this pattern — these must be treated as single-name leads.
    """
    if not name or " " not in name:
        return False
    parts = name.strip().split()
    if len(parts) == 2:
        last = parts[1].rstrip(".")
        return len(last) == 1 and last.isalpha()
    return False

# Low-relevance keywords (supports/admin - skip expensive enrichment)
LOW_RELEVANCE_KEYWORDS = {
    "intern", "apprentice", "trainee", "student", "junior",
    "support officer", "support staff", "help desk",
    "receptionist", "secretary", "personal assistant",
    "data entry", "filing", "mail room", "delivery", "driver",
    "warehouse", "janitor", "custodian", "cleaner",
    "customer service representative", "call centre",
}

# ── Personal email domains (V5.11) ────────────────────────────────────────────
# ONLY these domains count as "personal" emails (gmail, yahoo, hotmail etc.)
# Company emails like firstname@company.com are classified as "Work" not "Personal"
PERSONAL_EMAIL_DOMAINS = {
    # Global
    "gmail.com", "googlemail.com",
    "yahoo.com", "yahoo.co.uk", "yahoo.com.au", "yahoo.ca", "yahoo.co.in",
    "ymail.com", "rocketmail.com",
    "hotmail.com", "hotmail.co.uk", "hotmail.com.au", "hotmail.ca",
    "outlook.com", "outlook.com.au", "live.com", "live.com.au",
    "msn.com", "passport.com",
    "icloud.com", "me.com", "mac.com",
    "aol.com",
    "protonmail.com", "proton.me", "pm.me",
    "fastmail.com", "fastmail.fm",
    "zoho.com",
    "tutanota.com", "tutamail.com",
    "hey.com",
    "mail.com", "email.com",
    # Australian ISP / consumer
    "bigpond.com", "bigpond.net.au", "telstra.com",
    "optusnet.com.au",
    "tpg.com.au", "tpg.com",
    "internode.on.net",
    "aapt.net.au",
    "iprimus.com.au",
    "westnet.com.au",
    "dodo.com.au",
    # UK consumer
    "btinternet.com", "btopenworld.com",
    "sky.com", "talktalk.net", "virgin.net",
    "ntlworld.com", "blueyonder.co.uk",
    # Indian consumer
    "rediffmail.com", "indiatimes.com",
    # Other common
    "gmx.com", "gmx.net", "gmx.de",
    "web.de", "t-online.de",
    "seznam.cz",
    "yandex.com", "yandex.ru",
}

# ══════════════════════════════════════════════════════════════════════════════
# INDUSTRY KEYWORD DICTIONARY — 25+ industries with 20-25 keywords each
# ══════════════════════════════════════════════════════════════════════════════

INDUSTRY_KEYWORDS = {
    "Dentist": [
        # Original keywords (V5.12: kept exactly as-is)
        "dental implants", "root canal treatment", "teeth whitening",
        "orthodontist near me", "emergency dentist", "dental clinic",
        "cosmetic dentistry", "dental crown", "wisdom tooth removal",
        "periodontal treatment", "dental veneers", "invisalign provider",
        "pediatric dentist", "teeth cleaning service",
        "best dentist near me", "affordable dental care", "top rated dentist",
        "dental practice", "family dentist", "denture clinic",
        "dental surgery", "tooth extraction near me", "dental check up",
        "sedation dentistry", "dental bridge specialist",
        # V5.12: 20+ NEW KEYWORDS (doubled)
        "dental bonding service", "teeth alignment treatment", "gum disease treatment",
        "dental filling replacement", "laser teeth whitening", "smile makeover dentist",
        "teeth straightening near me", "dental check up and cleaning", "tooth-colored fillings",
        "cavity filling dentist", "gum surgery specialist", "oral hygiene cleaning service",
        "dental bridge replacement", "tooth implant specialist", "professional teeth cleaning",
        "cosmetic smile design", "emergency tooth extraction", "dental crown restoration",
        "full mouth dental implants", "teeth grinding treatment", "affordable implant dentist",
    ],
    "Doctor / General Practitioner": [
        "family doctor near me", "general practitioner clinic", "bulk billing doctor",
        "medical centre", "walk in clinic", "health check up",
        "vaccination clinic", "GP appointment", "after hours doctor",
        "women's health clinic", "men's health check", "pathology services",
        "best GP near me", "doctor accepting new patients", "skin check doctor",
        "travel doctor vaccination", "chronic disease management GP",
        "mental health GP", "telehealth doctor", "occupational health doctor",
        "sports medicine doctor", "urgent care clinic", "allied health centre",
        # V5.14: Doubled keywords
        "GP bulk billing near me", "medical clinic appointment", "doctor online consultation",
        "childhood immunisation GP", "annual health check doctor", "repeat prescription GP",
        "DVA doctor", "aged care GP visit", "care plan GP",
        "STI testing clinic", "mental health care plan GP", "diabetes management doctor",
        "blood pressure check GP", "cancer screening GP", "skin cancer doctor",
        "workplace injury doctor", "immigration medical GP", "private GP clinic",
        "same day GP appointment", "GP accepting medicare", "24 hour medical centre",
    ],
    "Lawyer / Attorney": [
        "family lawyer", "criminal defence lawyer", "personal injury attorney",
        "divorce lawyer near me", "immigration lawyer", "business lawyer",
        "estate planning attorney", "property conveyancer", "employment lawyer",
        "traffic lawyer", "wills and probate", "commercial litigation",
        "best lawyer near me", "affordable legal services", "top rated law firm",
        "corporate lawyer", "intellectual property lawyer", "construction lawyer",
        "medical negligence lawyer", "workers compensation lawyer",
        "debt recovery lawyer", "small business legal advice", "contract lawyer",
        "tax dispute lawyer", "strata lawyer",
        # V5.14: Doubled keywords
        "unfair dismissal lawyer", "defamation lawyer", "shareholder dispute lawyer",
        "franchise lawyer", "leasing lawyer", "property settlement lawyer",
        "de facto relationship lawyer", "child custody lawyer", "criminal charges lawyer",
        "AVO application lawyer", "guardianship lawyer", "elder law attorney",
        "migration agent lawyer", "business acquisition lawyer", "trademark lawyer",
        "privacy law consultant", "discrimination lawyer", "restraint of trade lawyer",
        "litigation lawyer near me", "will dispute lawyer", "power of attorney lawyer",
    ],
    "Accountant": [
        "tax accountant near me", "small business accountant", "bookkeeping services",
        "tax return preparation", "BAS lodgement service", "financial auditing",
        "payroll services", "business advisory", "self managed super fund accountant",
        "company tax planning", "forensic accounting", "xero certified accountant",
        "best accountant near me", "affordable tax services", "CPA near me",
        "startup accountant", "trust accountant", "GST registration accountant",
        "property tax accountant", "tax planning advisor", "cloud accounting service",
        "quarterly BAS preparation", "business structure advice", "capital gains tax accountant",
        # V5.14: Doubled keywords
        "myob accountant", "individual tax return", "negative gearing accountant",
        "investment property tax return", "contractor tax accountant", "ABN registration accountant",
        "crypto tax accountant", "company registration accountant", "business restructure accountant",
        "fringe benefits tax accountant", "R&D tax incentive accountant", "due diligence accountant",
        "family trust tax accountant", "partnership tax return", "sole trader accountant",
        "NFP accountant", "aged care financial accountant", "SMSF audit accountant",
        "accounting software setup", "business sale accountant", "grant application accountant",
    ],
    "Plumber": [
        # Original keywords (V5.12: kept exactly as-is)
        "emergency plumber", "blocked drain plumber", "hot water system repair",
        "gas plumber near me", "bathroom renovation plumber", "leak detection service",
        "pipe relining", "backflow prevention", "plumbing maintenance",
        "sewer repair service", "tap replacement", "toilet repair plumber",
        "best plumber near me", "affordable plumbing service", "24 hour plumber",
        "commercial plumber", "licensed gas fitter", "water heater installation",
        "burst pipe repair", "stormwater drainage plumber", "kitchen plumbing",
        "plumber quote", "rainwater tank installation", "grease trap cleaning",
        # V5.12: 20+ NEW KEYWORDS (doubled)
        "drain cleaning service", "pipe repair plumber", "emergency burst pipe",
        "water leak repair", "gas fitting specialist", "plumbing inspection service",
        "bathroom plumbing installation", "drain blockage clearing", "pipe replacement plumber",
        "hot water service repair", "toilet installation plumber", "roof gutter plumber",
        "underground pipe repair", "commercial plumbing contractor", "plumbing renovation service",
        "water pressure adjustment", "drainage system installation", "emergency plumbing call out",
        "plumbing maintenance plan", "storm water drainage specialist", "water meter plumber",
    ],
    "Electrician": [
        "emergency electrician", "electrical contractor near me", "solar panel installer",
        "switchboard upgrade", "LED lighting installation", "smoke alarm installation",
        "electrical safety inspection", "ceiling fan installation", "EV charger installer",
        "commercial electrician", "security lighting", "power point installation",
        "best electrician near me", "affordable electrical services", "24 hour electrician",
        "licensed electrician", "home rewiring", "electrical fault finding",
        "three phase power installation", "data cabling electrician",
        "outdoor lighting installation", "generator installation", "smart home electrician",
        "industrial electrician", "strata electrician",
        # V5.14: Doubled keywords
        "RCD installation electrician", "underground power connection", "pool electrical inspection",
        "home theatre wiring", "CCTV installation electrician", "intercom system installation",
        "solar battery installation", "energy efficiency electrician", "test and tag service",
        "private power pole installation", "caravan power connection", "switchboard fault repair",
        "emergency lighting electrician", "exit sign installation", "electrical compliance certificate",
        "domestic wiring electrician", "air conditioner wiring", "CBUS home automation",
        "NBN connection electrician", "hot water system wiring", "electrical quote near me",
    ],
    "Real Estate Agent": [
        "real estate agent near me", "property valuation", "house for sale",
        "property management service", "real estate auctioneer", "buyer's agent",
        "commercial real estate", "rental property manager", "land for sale",
        "investment property advisor", "first home buyer agent", "luxury real estate",
        "best real estate agent", "top selling agent", "property appraisal free",
        "sell my house fast", "local real estate office", "real estate agency",
        "property market analysis", "off market properties", "strata management",
        "real estate consultant", "auction specialist agent",
        # V5.14: Doubled keywords
        "townhouse for sale near me", "apartment for sale near me", "unit for rent near me",
        "property leasing agent", "residential property sales", "semi-detached house sale",
        "acreage property for sale", "rural property agent", "estate agent open home",
        "deceased estate sale agent", "downsizing real estate agent", "prestige property agent",
        "property styling service", "tenant finding service", "rental yield analysis",
        "suburb property report", "online property listing", "flat fee real estate agent",
        "seller's agent near me", "subdivision development agent", "house auction result",
    ],
    "Restaurant / Cafe": [
        "restaurant near me", "cafe near me", "fine dining restaurant",
        "pizza delivery", "catering service", "private dining",
        "brunch cafe", "takeaway food", "function venue",
        "restaurant booking", "food delivery service", "organic cafe",
        "best restaurant near me", "top rated cafe", "family restaurant",
        "italian restaurant", "thai restaurant near me", "sushi restaurant",
        "vegan cafe", "breakfast cafe", "coffee roaster cafe",
        "licensed restaurant", "seafood restaurant", "indian restaurant near me",
        # V5.14: Doubled keywords
        "wood fired pizza restaurant", "degustation menu restaurant", "BYO restaurant near me",
        "outdoor dining restaurant", "rooftop bar restaurant", "live music restaurant",
        "gluten free cafe", "dessert cafe near me", "bubble tea cafe",
        "smoothie bar near me", "juice bar near me", "high tea venue",
        "waterfront restaurant near me", "korean BBQ restaurant", "greek restaurant near me",
        "mexican restaurant near me", "Lebanese restaurant near me", "buffet restaurant near me",
        "halal restaurant near me", "pet friendly cafe", "child friendly restaurant",
    ],
    "Gym / Fitness": [
        "gym near me", "personal trainer", "fitness centre",
        "crossfit gym", "yoga studio near me", "pilates classes",
        "boxing gym", "24 hour gym", "group fitness classes",
        "strength training gym", "weight loss program", "martial arts studio",
        "best gym near me", "affordable gym membership", "women's only gym",
        "functional fitness gym", "HIIT classes near me", "spin class",
        "gym with pool", "bootcamp fitness", "senior fitness classes",
        "powerlifting gym", "reformer pilates studio",
        # V5.14: Doubled keywords
        "muay thai gym near me", "BJJ gym near me", "olympic weightlifting gym",
        "no contract gym membership", "gym with childcare", "corporate gym membership",
        "outdoor bootcamp fitness", "obstacle course training", "rock climbing gym",
        "online personal trainer", "virtual fitness classes", "home gym equipment near me",
        "bodybuilding gym near me", "swimming pool fitness", "aqua aerobics classes",
        "senior fitness centre", "post natal fitness class", "kettlebell training gym",
        "functional movement gym", "agility training gym", "sports performance gym",
    ],
    "Auto Repair / Mechanic": [
        "car mechanic near me", "auto repair shop", "car service centre",
        "brake repair", "transmission repair", "tyre replacement",
        "roadworthy certificate", "logbook service", "car air conditioning repair",
        "diesel mechanic", "mobile mechanic", "pre purchase car inspection",
        "best mechanic near me", "affordable car service", "auto electrician",
        "clutch repair", "suspension repair", "wheel alignment near me",
        "car battery replacement", "exhaust repair", "engine diagnostic",
        "hybrid car mechanic", "fleet vehicle servicing",
    ],
    "Salon / Spa / Beauty": [
        "hair salon near me", "beauty salon", "day spa",
        "nail salon", "barber shop near me", "laser hair removal",
        "facial treatment", "massage therapy", "eyebrow threading",
        "bridal hair and makeup", "skin clinic", "waxing salon",
        "best hair salon near me", "affordable beauty treatments", "keratin treatment",
        "balayage specialist", "men's grooming salon", "eyelash extensions",
        "microdermabrasion", "chemical peel treatment", "anti aging facial",
        "hair colour specialist", "scalp treatment", "body contouring spa",
        # V5.14: Doubled keywords
        "spray tan salon", "lash lift near me", "brow lamination near me",
        "lip blush tattoo", "microblading near me", "permanent makeup artist",
        "skin needling clinic", "hydrafacial near me", "LED light therapy salon",
        "IPL hair removal salon", "cryotherapy beauty", "infrared sauna spa",
        "couples spa package", "prenatal massage near me", "teen facial service",
        "men's waxing salon", "beard grooming barber", "natural hair salon",
        "nail art studio", "gel nail removal near me", "hair extension specialist",
    ],
    "Chiropractor": [
        "chiropractor near me", "back pain treatment", "spinal adjustment",
        "sports chiropractor", "neck pain relief", "sciatica treatment",
        "posture correction", "chiropractic clinic", "headache treatment chiropractor",
        "pregnancy chiropractor", "pediatric chiropractor",
        "best chiropractor near me", "affordable chiropractic care",
        "chiropractic adjustment", "lower back pain chiropractor",
        "disc herniation treatment", "whiplash treatment chiropractor",
        "TMJ chiropractor", "chiropractic wellness centre", "spinal decompression therapy",
        "shoulder pain chiropractor", "hip pain chiropractor",
        # V5.14: Doubled keywords
        "knee pain chiropractic", "foot pain chiropractor", "pinched nerve chiropractic",
        "scoliosis chiropractor", "chronic pain chiropractic", "tension headache chiropractor",
        "workplace injury chiropractor", "car accident chiropractor", "sports injury chiropractor",
        "activator chiropractic technique", "manual chiropractic adjustment", "dry needling chiropractor",
        "NDIS chiropractor", "chiropractic massage combo", "chiropractic x-ray near me",
        "vertebral subluxation treatment", "chiropractic family care", "infant chiropractic",
        "sacroiliac joint chiropractor", "functional neurology chiropractor", "corrective chiropractic",
    ],
    "Veterinarian": [
        "vet near me", "emergency vet", "pet vaccination",
        "dog grooming", "cat vet", "animal hospital",
        "pet dental care", "pet surgery", "veterinary clinic",
        "exotic animal vet", "pet microchipping", "puppy health check",
        "best vet near me", "affordable vet clinic", "24 hour emergency vet",
        "mobile vet service", "pet desexing", "senior pet care vet",
        "avian vet", "reptile vet", "pet allergy treatment",
        "veterinary specialist", "pet ultrasound", "dog behaviorist vet",
        # V5.14: Doubled keywords
        "dog vet near me", "cat vet near me", "rabbit vet near me",
        "guinea pig vet", "fish vet near me", "farm animal vet",
        "horse vet near me", "cattle vet service", "pet blood test vet",
        "pet X-ray service", "vet pain management", "pet oncology vet",
        "pet dermatology specialist", "ophthalmology vet", "pet cardiology vet",
        "vet physiotherapy", "pet rehabilitation vet", "acupuncture vet",
        "holistic vet near me", "pet grief support vet", "animal euthanasia vet",
    ],
    "Insurance Agent": [
        "insurance broker near me", "car insurance quote", "home insurance",
        "life insurance advisor", "business insurance", "health insurance broker",
        "income protection insurance", "travel insurance", "landlord insurance",
        "professional indemnity insurance", "workers compensation insurance",
        "best insurance broker", "affordable insurance quotes", "insurance agent near me",
        "commercial vehicle insurance", "public liability insurance",
        "cyber insurance broker", "strata insurance", "trade insurance",
        "fleet insurance broker", "insurance comparison service",
        "general insurance broker", "risk management insurance",
        # V5.14: Doubled keywords
        "building and contents insurance", "marine insurance broker", "aviation insurance",
        "farm insurance broker", "event insurance broker", "group life insurance",
        "key person insurance", "business interruption insurance", "directors officers insurance",
        "management liability insurance", "construction insurance broker", "product liability insurance",
        "engineering insurance", "agribusiness insurance", "SMSF insurance",
        "funeral insurance advisor", "pet insurance broker", "disability insurance",
        "rural property insurance", "hospitality insurance broker", "NFP insurance",
    ],
    "Financial Advisor": [
        "financial planner near me", "investment advisor", "retirement planning",
        "wealth management", "superannuation advice", "mortgage broker",
        "financial planning service", "estate planning advisor", "debt consolidation",
        "self managed super fund advisor", "tax effective investment",
        "best financial advisor near me", "certified financial planner",
        "independent financial advisor", "pension advisor", "portfolio management",
        "financial coach", "business financial planning", "insurance planning advisor",
        "property investment advisor", "succession planning advisor",
        "fee only financial planner", "first home buyer financial advisor",
        # V5.14: Doubled keywords
        "divorce financial planning", "aged care financial advice", "redundancy financial planning",
        "ethical investment advisor", "ESG investment advisor", "impact investing advisor",
        "socially responsible investing", "robo advisor alternative", "shares investment advisor",
        "education savings plan advisor", "child trust fund advisor", "expat financial planning",
        "gig economy financial advisor", "small business exit planning", "employee share plan advisor",
        "share portfolio management", "term deposit advice", "annuity planning advisor",
        "financial hardship advisor", "debt management plan", "bankruptcy financial advice",
    ],
    "Photographer": [
        "wedding photographer", "portrait photographer", "commercial photographer",
        "real estate photographer", "event photographer", "newborn photographer",
        "family photographer", "headshot photographer", "product photography",
        "corporate photographer", "drone photographer",
        "best photographer near me", "affordable photography services",
        "graduation photographer", "maternity photographer", "pet photographer",
        "food photographer", "fashion photographer", "architectural photographer",
        "photo studio near me", "ecommerce product photography",
        "sports photographer", "school photographer",
        # V5.14: Doubled keywords
        "engagement photographer", "anniversary photographer", "boudoir photographer",
        "lifestyle photographer", "documentary photographer", "street photographer",
        "concert photographer", "music band photographer", "automotive photographer",
        "construction site photographer", "industrial photographer", "medical photographer",
        "underwater photographer", "360 degree photographer", "virtual tour photographer",
        "photo editing service", "retouching photographer", "composite photography",
        "photo booth rental", "photo walk photographer", "photography workshop",
    ],
    "Landscaping": [
        "landscaper near me", "garden design service", "lawn mowing service",
        "tree removal", "irrigation installation", "retaining wall builder",
        "landscape architect", "garden maintenance", "artificial turf installer",
        "paving contractor", "outdoor living design", "hedge trimming service",
        "best landscaper near me", "affordable landscaping", "garden makeover",
        "pool landscaping", "native garden design", "commercial landscaping",
        "stump grinding service", "mulching service", "garden lighting installation",
        "deck and pergola builder", "vertical garden installer",
        # V5.14: Doubled keywords
        "garden clean up service", "turf laying service", "lawn care service",
        "garden edging service", "lawn fertiliser treatment", "weed removal garden",
        "tree pruning service", "arborist near me", "palm tree removal",
        "water feature installation", "raised garden bed installation", "succulent garden design",
        "kitchen garden design", "outdoor kitchen landscaper", "bbq area landscaping",
        "fire pit landscaping", "zen garden design", "sloping block landscaping",
        "council approved landscaping", "body corporate garden maintenance", "strata garden service",
    ],
    "HVAC": [
        "air conditioning installation", "heating repair", "HVAC contractor",
        "ducted air conditioning", "split system installation", "furnace repair",
        "commercial HVAC", "air conditioning service", "ventilation system",
        "heat pump installer", "evaporative cooling", "air duct cleaning",
        "best HVAC contractor near me", "affordable air conditioning",
        "refrigerated cooling installation", "gas heating installation",
        "underfloor heating", "air conditioning maintenance plan",
        "commercial refrigeration", "HVAC energy audit", "zone control system",
        "hydronic heating installer", "air purification system",
        # V5.14: Doubled keywords
        "reverse cycle air conditioner", "portable air conditioner install",
        "wall mounted split system", "multi split air conditioning", "cassette air conditioner",
        "industrial ventilation system", "kitchen exhaust fan installation",
        "bathroom exhaust fan install", "server room cooling", "cold room installation",
        "refrigerant regas service", "AC thermostat replacement", "AC compressor repair",
        "ducted gas heating service", "gas log fire installation", "pellet heater installer",
        "zoning system installation", "HVAC BMS control system", "air balancing service",
        "HVAC design consultant", "building HVAC compliance", "AC noise problem repair",
    ],
    "Roofing": [
        "roof repair near me", "roofing contractor", "roof replacement",
        "metal roofing", "tile roof repair", "gutter installation",
        "roof restoration", "commercial roofing", "roof leak repair",
        "colorbond roofing", "roof painting", "roof inspection service",
        "best roofer near me", "affordable roof repair", "flat roof specialist",
        "gutter guard installation", "skylight installation", "roof ventilation",
        "emergency roof repair", "fascia and soffit repair", "roof cleaning service",
        "asbestos roof removal", "terracotta roof restoration",
        # V5.14: Doubled keywords
        "slate roof repair", "zincalume roofing", "polycarbonate roof installation",
        "industrial roof replacement", "school roof repair", "stormwater management roofing",
        "gutter cleaning service", "leaf guard gutters", "gutter replacement near me",
        "roof membrane waterproofing", "heritage slate roofer", "concrete tile roofer",
        "ridge capping repair", "valley iron replacement", "roof truss repair",
        "solar roof installation", "green roof installation", "translucent roofing sheets",
        "factory roof maintenance", "church roof restoration", "roof hatch installation",
    ],
    "Pest Control": [
        "pest control near me", "termite inspection", "cockroach treatment",
        "rodent control", "bed bug treatment", "ant control service",
        "spider treatment", "commercial pest control", "pre purchase pest inspection",
        "possum removal", "wasp nest removal", "flea treatment",
        "best pest control near me", "affordable pest treatment",
        "termite barrier installation", "mosquito control", "bird proofing service",
        "silverfish treatment", "timber pest inspection", "eco friendly pest control",
        "fumigation service", "integrated pest management", "annual pest control plan",
        # V5.14: Doubled keywords
        "snake removal service", "fly control service", "mite treatment service",
        "carpet beetle treatment", "clothes moth treatment", "dermestid beetle control",
        "grain weevil treatment", "stored product pest control", "whitefly treatment",
        "restaurant pest control", "hotel pest management", "warehouse pest control",
        "strata pest control", "body corporate pest inspection", "real estate pest report",
        "thermal termite inspection", "termite monitoring system", "termite bait station",
        "soil treatment termites", "chemical termite barrier", "pre construction termite treatment",
    ],
    "Cleaning Service": [
        "house cleaning service", "commercial cleaning", "carpet cleaning",
        "end of lease cleaning", "office cleaning service", "window cleaning",
        "deep cleaning service", "pressure washing", "tile and grout cleaning",
        "upholstery cleaning", "regular house cleaning", "spring cleaning service",
        "best cleaning service near me", "affordable house cleaning",
        "strata cleaning service", "medical facility cleaning", "gym cleaning service",
        "after construction cleaning", "airbnb cleaning service", "oven cleaning service",
        "blind cleaning service", "school cleaning contractor", "warehouse cleaning",
        # V5.14: Doubled keywords
        "bond cleaning service", "vacate cleaning service", "move in cleaning service",
        "covid cleaning service", "biohazard cleaning", "trauma cleaning service",
        "high pressure cleaning", "concrete cleaning service", "driveway cleaning service",
        "solar panel cleaning", "gutter cleaning service", "industrial cleaning contractor",
        "food factory cleaning", "restaurant kitchen deep clean", "childcare cleaning service",
        "aged care cleaning", "hospital cleaning service", "church cleaning contractor",
        "retail cleaning service", "shopping centre cleaning", "car park cleaning service",
    ],
    "IT Services": [
        "IT support near me", "managed IT services", "computer repair",
        "network setup", "cybersecurity services", "cloud computing solutions",
        "IT consulting", "data recovery service", "business IT support",
        "VoIP phone systems", "server maintenance", "IT helpdesk outsourcing",
        "best IT support near me", "affordable managed IT", "IT security audit",
        "Microsoft 365 setup", "backup and disaster recovery", "wireless network setup",
        "website hosting service", "IT infrastructure management",
        "remote IT support", "IT project management", "software development company",
        # V5.14: Doubled keywords
        "small business IT support", "cloud migration service", "Google Workspace setup",
        "IT outsourcing company", "network security audit", "CCTV IT integration",
        "endpoint protection service", "firewall setup company", "email spam filtering",
        "business continuity IT", "dark web monitoring service", "compliance IT consulting",
        "hardware procurement IT", "SD-WAN solution provider", "virtualisation services",
        "Azure cloud services", "AWS managed services", "Salesforce IT partner",
        "IT onboarding service", "IT documentation company", "phishing simulation service",
    ],
    "Marketing Agency": [
        "digital marketing agency", "SEO services", "social media marketing",
        "PPC management", "content marketing agency", "web design agency",
        "branding agency", "email marketing service", "Google Ads management",
        "video production agency", "PR agency", "lead generation service",
        "best marketing agency near me", "affordable digital marketing",
        "local SEO services", "ecommerce marketing agency", "Facebook Ads agency",
        "marketing strategy consultant", "conversion rate optimization",
        "influencer marketing agency", "LinkedIn marketing service",
        "reputation management agency", "marketing automation service",
        # V5.14: Doubled keywords
        "TikTok marketing agency", "YouTube advertising agency", "programmatic advertising",
        "growth marketing agency", "performance marketing agency", "B2B marketing agency",
        "startup marketing agency", "healthcare marketing agency", "legal marketing agency",
        "trade marketing agency", "events marketing company", "direct mail marketing",
        "SMS marketing service", "chatbot marketing agency", "account based marketing",
        "Amazon advertising agency", "Shopify marketing agency", "keyword research service",
        "white label marketing agency", "nearshore marketing agency", "marketing analytics service",
    ],
    "Construction": [
        "home builder near me", "construction company", "renovation contractor",
        "commercial construction", "custom home builder", "bathroom renovation",
        "kitchen renovation", "extension builder", "granny flat builder",
        "project home builder", "demolition contractor", "concrete contractor",
        "best builder near me", "affordable home renovation", "new home construction",
        "duplex builder", "townhouse builder", "shopfitting contractor",
        "structural steel builder", "civil construction company",
        "industrial construction", "site preparation contractor", "formwork contractor",
        # V5.14: Doubled keywords
        "heritage restoration builder", "pool construction company", "garage builder near me",
        "retaining wall construction", "underpinning specialist", "house raising specialist",
        "owner builder assistance", "knockdown rebuild company", "modular home builder",
        "tiny home builder", "eco-friendly builder", "passive house builder",
        "commercial fitout contractor", "medical fitout builder", "school construction contractor",
        "aged care construction", "hotel construction company", "warehouse builder",
        "earthworks contractor", "drainage contractor", "building surveyor",
    ],
    "Architecture": [
        "architect near me", "residential architect", "commercial architect",
        "interior designer", "building designer", "sustainable architecture",
        "heritage architect", "architectural drafting", "house design service",
        "landscape architect", "3D architectural rendering",
        "best architect near me", "affordable architectural services",
        "dual occupancy architect", "renovation architect", "passive house architect",
        "town planning consultant", "development application architect",
        "multi storey architect", "aged care facility architect",
        "restaurant fit out designer", "retail design architect",
        # V5.14: Doubled keywords
        "custom home design architect", "knockdown rebuild architect", "granny flat architect",
        "industrial building architect", "warehouse conversion architect", "adaptive reuse architect",
        "education facility architect", "healthcare building architect", "hospitality architect",
        "mixed use development architect", "high rise architect", "boutique developer architect",
        "facade design architect", "building extension architect", "rezoning application architect",
        "BIM architect", "parametric design architect", "virtual reality architecture",
        "green building architect", "NABERS rating architect", "NCC compliance architect",
    ],
    "Physiotherapy": [
        "physiotherapist near me", "sports physio", "back pain physiotherapy",
        "post surgery rehabilitation", "neck pain treatment physio",
        "shoulder physio", "knee rehabilitation", "workplace injury physio",
        "dry needling treatment", "hydrotherapy", "exercise physiologist",
        "best physio near me", "affordable physiotherapy", "pelvic floor physio",
        "hand therapy physiotherapist", "vestibular physiotherapy",
        "clinical pilates physio", "paediatric physiotherapy",
        "aged care physiotherapy", "telehealth physiotherapy",
        "chronic pain physiotherapist", "running injury physio",
        # V5.14: Doubled keywords
        "ankle sprain physio", "hip replacement rehab physio", "ACL rehabilitation physio",
        "frozen shoulder treatment", "rotator cuff physio", "tennis elbow treatment",
        "plantar fasciitis physio", "sciatica physiotherapy", "concussion rehabilitation",
        "neurological physiotherapy", "stroke rehabilitation physio", "NDIS physiotherapy",
        "pre surgery physio", "post natal physiotherapy", "lymphoedema physio",
        "oncology physiotherapy", "cardiorespiratory physio", "balance and falls physio",
        "pilates injury prevention", "sports taping physio", "ergonomic assessment physio",
    ],
    "Pharmacy": [
        "pharmacy near me", "compounding pharmacy", "online pharmacy",
        "late night pharmacy", "prescription delivery", "vaccination pharmacy",
        "travel health clinic pharmacy", "medication management",
        "health screening pharmacy", "weight management pharmacy",
        "best pharmacy near me", "24 hour pharmacy", "discount pharmacy",
        "diabetes management pharmacy", "blister pack pharmacy",
        "naturopathic pharmacy", "veterinary compounding pharmacy",
        "sleep apnea pharmacy", "mobility aids pharmacy",
        "hormone compounding pharmacy", "pain management pharmacy",
        # V5.14: Doubled keywords
        "chemist near me", "discount chemist", "script pharmacy",
        "PBS medication pharmacy", "pharmacy delivery service", "flu vaccine pharmacy",
        "COVID vaccine pharmacy", "baby formula pharmacy", "sports nutrition pharmacy",
        "wound care pharmacy", "stoma care pharmacy", "medsafe compounding",
        "fertility medication pharmacy", "HRT pharmacy", "mental health medication pharmacy",
        "antibiotic prescription pharmacy", "healthcare product pharmacy", "pharmacy reward program",
        "pharmacy dispensary service", "accredited pharmacist", "medication review pharmacy",
    ],
    # ── V5.4: 25 NEW INDUSTRIES ──────────────────────────────────────────────
    "Wedding Planner / Event Planner": [
        "wedding planner near me", "event planning services", "wedding coordinator",
        "corporate event planner", "party planner", "wedding venue coordinator",
        "destination wedding planner", "event management company", "wedding stylist",
        "birthday party planner", "fundraiser event planner", "conference organizer",
        "bridal consultant", "wedding day coordinator", "event decorator",
        "engagement party planner", "anniversary event planner", "gala event planner",
        "outdoor wedding planner", "wedding planning services",
        # V5.14: Doubled keywords
        "micro wedding planner", "elopement coordinator", "luxury wedding planner",
        "beach wedding planner", "garden wedding planner", "winery wedding planner",
        "LGBTQ wedding planner", "multicultural wedding planner", "baby shower planner",
        "hen's night planner", "bucks night planner", "school formal organizer",
        "product launch event planner", "trade show event organizer", "awards night organizer",
        "team building event planner", "virtual event planner", "hybrid event organizer",
        "charity gala planner", "sports event organizer", "festival event management",
    ],
    "Tattoo Artist / Body Art": [
        "tattoo shop near me", "custom tattoo artist", "tattoo studio",
        "fine line tattoo artist", "traditional tattoo shop", "realism tattoo artist",
        "tattoo removal service", "watercolor tattoo artist", "minimalist tattoo",
        "portrait tattoo specialist", "sleeve tattoo artist", "japanese tattoo artist",
        "tattoo parlor", "cover up tattoo specialist", "body piercing studio",
        "blackwork tattoo artist", "geometric tattoo", "best tattoo artist near me",
    ],
    "Florist / Flower Shop": [
        "florist near me", "flower delivery service", "wedding florist",
        "funeral flowers delivery", "flower shop", "event floral arrangements",
        "custom bouquet delivery", "same day flower delivery", "floral designer",
        "corporate flower arrangements", "flower subscription service", "dried flower arrangements",
        "bridal bouquet florist", "sympathy flowers delivery", "tropical flower arrangements",
        "flower workshop classes", "wholesale flowers", "seasonal flower arrangements",
    ],
    "Baker / Bakery": [
        "bakery near me", "custom cake shop", "wedding cake baker",
        "artisan bread bakery", "cupcake shop", "gluten free bakery",
        "birthday cake order", "pastry shop", "sourdough bakery",
        "cake decorator near me", "French pastry shop", "vegan bakery",
        "wholesale bakery", "specialty cake shop", "donut shop",
        "patisserie near me", "cake delivery service", "best bakery near me",
    ],
    "Caterer / Catering": [
        "catering service near me", "wedding catering", "corporate catering",
        "event catering company", "BBQ catering", "buffet catering service",
        "private chef catering", "office lunch catering", "cocktail party catering",
        "food truck catering", "halal catering service", "vegan catering",
        "funeral catering service", "breakfast catering", "finger food catering",
        "outdoor event catering", "gourmet catering service", "affordable catering near me",
    ],
    "Personal Trainer": [
        "personal trainer near me", "online personal training", "fitness coach",
        "weight loss personal trainer", "strength training coach", "HIIT trainer",
        "mobile personal trainer", "group fitness trainer", "sports conditioning coach",
        "body transformation trainer", "prenatal fitness trainer", "senior fitness trainer",
        "CrossFit coach", "nutrition and fitness coach", "functional training specialist",
        "private gym trainer", "certified personal trainer", "best personal trainer near me",
    ],
    "Yoga / Pilates Studio": [
        "yoga studio near me", "pilates classes", "hot yoga studio",
        "beginner yoga classes", "prenatal yoga", "aerial yoga studio",
        "reformer pilates near me", "yoga teacher training", "yin yoga classes",
        "corporate yoga instructor", "private yoga lessons", "vinyasa yoga studio",
        "mat pilates classes", "yoga retreat center", "meditation and yoga studio",
        "kids yoga classes", "online yoga classes", "best yoga studio near me",
    ],
    "Massage Therapist": [
        "massage therapist near me", "deep tissue massage", "sports massage therapy",
        "remedial massage", "Swedish massage", "pregnancy massage",
        "lymphatic drainage massage", "hot stone massage", "myotherapy near me",
        "mobile massage service", "couples massage", "relaxation massage",
        "trigger point therapy", "aromatherapy massage", "Thai massage near me",
        "therapeutic massage clinic", "back pain massage", "best massage therapist near me",
    ],
    "Interior Designer": [
        "interior designer near me", "home interior design", "commercial interior design",
        "kitchen design consultant", "bathroom renovation designer", "office interior designer",
        "residential interior styling", "modern interior design", "luxury interior designer",
        "sustainable interior design", "color consultation service", "space planning consultant",
        "interior decorator near me", "home staging service", "furniture selection consultant",
        "restaurant interior design", "hotel interior design", "affordable interior designer",
    ],
    "Web Developer / Web Design": [
        "web developer near me", "website design service", "ecommerce website development",
        "WordPress developer", "Shopify developer", "custom web application",
        "responsive web design", "SEO web design", "landing page design",
        "web development agency", "mobile app developer", "UI UX design service",
        "website maintenance service", "website redesign", "small business website",
        "React developer", "full stack developer", "affordable web design",
        # V5.14: Doubled keywords
        "Vue.js developer", "Next.js developer", "Node.js developer",
        "Laravel developer", "Django web developer", "Ruby on Rails developer",
        "headless CMS developer", "Webflow developer", "Squarespace web designer",
        "website speed optimization", "website accessibility audit", "ADA compliant website",
        "progressive web app developer", "API development service", "SaaS web developer",
        "membership website developer", "booking system developer", "custom CRM developer",
        "website security service", "SSL certificate setup", "website migration service",
    ],
    "Graphic Designer": [
        "graphic designer near me", "logo design service", "brand identity design",
        "print design service", "packaging design", "marketing material design",
        "social media graphic designer", "business card design", "brochure design service",
        "infographic designer", "illustration service", "book cover design",
        "banner design service", "freelance graphic designer", "corporate branding agency",
        "signage design", "menu design service", "affordable graphic design",
    ],
    "Copywriter / Content Writer": [
        "copywriter near me", "content writing service", "SEO copywriting",
        "website content writer", "blog writing service", "advertising copywriter",
        "product description writer", "email copywriter", "social media content writer",
        "technical writer", "press release writing", "brand storytelling",
        "freelance copywriter", "conversion copywriting", "content marketing agency",
        "scriptwriter for business", "ghostwriter", "B2B copywriting service",
    ],
    "Tutor / Education": [
        "tutor near me", "math tutor", "English tutor",
        "online tutoring service", "SAT prep tutor", "science tutor",
        "reading tutor for kids", "university tutor", "language tutor",
        "music tutor", "STEM tutoring", "test preparation tutor",
        "special needs tutor", "private tutor", "homework help service",
        "study skills coach", "academic coaching", "best tutor near me",
    ],
    "Music Teacher / Music School": [
        "music lessons near me", "piano teacher", "guitar lessons",
        "singing lessons", "violin teacher", "drum lessons near me",
        "music school", "private music tutor", "online music lessons",
        "music theory classes", "band coaching", "music production lessons",
        "kids music classes", "adult music lessons", "songwriting workshop",
        "jazz music lessons", "classical music teacher", "best music school near me",
    ],
    "Driving School": [
        "driving school near me", "driving lessons", "learner driver instructor",
        "automatic driving lessons", "defensive driving course", "driving test preparation",
        "truck driving school", "motorcycle riding lessons", "driving instructor",
        "intensive driving course", "driving refresher course", "senior driver assessment",
        "P plate driving lessons", "driving school for teens", "manual driving lessons",
        "road test preparation", "best driving school near me", "affordable driving lessons",
    ],
    "Pet Grooming": [
        "pet grooming near me", "dog grooming salon", "cat grooming service",
        "mobile pet grooming", "puppy grooming", "dog bathing service",
        "pet nail trimming", "dog haircut near me", "luxury pet grooming",
        "breed specific grooming", "pet spa near me", "hypoallergenic dog grooming",
        "show dog grooming", "pet deshedding service", "flea treatment grooming",
        "senior pet grooming", "large dog grooming", "best pet groomer near me",
    ],
    "Locksmith": [
        "locksmith near me", "emergency locksmith", "24 hour locksmith",
        "car locksmith", "residential locksmith", "commercial locksmith",
        "lock change service", "lockout service", "master key system",
        "safe locksmith", "smart lock installation", "lock repair service",
        "key cutting near me", "garage door lock", "deadbolt installation",
        "automotive locksmith", "rekeying service", "affordable locksmith near me",
    ],
    "Moving Company": [
        "moving company near me", "local movers", "interstate removalist",
        "office relocation service", "furniture removalist", "packing service",
        "piano moving service", "storage and moving", "commercial moving company",
        "last minute movers", "small moves specialist", "long distance moving",
        "apartment movers", "house moving service", "moving truck hire",
        "senior moving service", "corporate relocation", "affordable movers near me",
    ],
    "Printing Service": [
        "printing service near me", "business card printing", "banner printing",
        "flyer printing service", "poster printing", "sticker printing",
        "t-shirt printing", "booklet printing", "large format printing",
        "custom printing service", "digital printing near me", "offset printing",
        "brochure printing", "invitation printing", "signage printing",
        "photo printing service", "canvas printing", "same day printing service",
    ],
    "Optometrist / Eye Care": [
        "optometrist near me", "eye exam near me", "prescription glasses",
        "contact lens fitting", "children's eye test", "eye care clinic",
        "optical shop", "progressive lenses", "eye health check",
        "dry eye treatment", "glaucoma screening", "macular degeneration test",
        "sports vision specialist", "bulk billed eye test", "designer eyeglasses",
        "vision therapy", "diabetic eye screening", "best optometrist near me",
    ],
    "Podiatrist": [
        "podiatrist near me", "foot doctor", "ingrown toenail treatment",
        "plantar fasciitis treatment", "custom orthotics", "diabetic foot care",
        "sports podiatry", "heel pain treatment", "bunion treatment",
        "children's podiatrist", "foot pain specialist", "toenail fungus treatment",
        "flat feet treatment", "running injury podiatrist", "biomechanical assessment",
        "podiatric surgery", "foot care clinic", "best podiatrist near me",
    ],
    "Dermatologist": [
        "dermatologist near me", "skin specialist", "acne treatment clinic",
        "skin cancer check", "mole removal", "eczema treatment",
        "psoriasis specialist", "cosmetic dermatology", "anti aging skin treatment",
        "laser skin treatment", "rosacea treatment", "skin biopsy clinic",
        "dermatology clinic", "pediatric dermatologist", "hair loss treatment",
        "skin allergy specialist", "botox dermatologist", "best dermatologist near me",
    ],
    "Home Inspector": [
        "home inspector near me", "building inspection", "pre purchase inspection",
        "pest inspection service", "property inspection report", "new home inspection",
        "commercial building inspection", "pool inspection", "roof inspection service",
        "asbestos inspection", "mold inspection", "home energy audit",
        "structural inspection", "pre sale building report", "strata inspection",
        "termite inspection", "building and pest inspection", "best home inspector near me",
    ],
    "Painter / Decorator": [
        "house painter near me", "interior painting service", "exterior house painting",
        "commercial painter", "residential painting", "wallpaper installation",
        "spray painting service", "cabinet painting", "deck staining",
        "mural artist", "office painting service", "roof painting",
        "fence painting", "texture coating", "color consulting painter",
        "heritage restoration painter", "epoxy floor coating", "affordable painter near me",
    ],
    "Solar Panel Installation": [
        "solar panel installer near me", "solar energy system", "residential solar panels",
        "commercial solar installation", "solar battery storage", "solar power system",
        "solar panel quotes", "rooftop solar panels", "off grid solar system",
        "solar hot water system", "solar inverter installation", "solar panel cleaning service",
        "solar financing options", "solar panel repair", "EV charger installation",
        "solar energy consultant", "green energy solutions", "best solar installer near me",
        # V5.14: Doubled keywords
        "STC solar rebate installer", "feed in tariff solar", "virtual power plant solar",
        "Tesla Powerwall installer", "LG Chem battery installer", "SolarEdge installer",
        "Enphase microinverter installer", "Fronius inverter installer", "Sungrow installer",
        "solar monitoring system", "smart meter solar installer", "solar ground mount",
        "solar carport installation", "agrivoltaic solar", "floating solar installer",
        "solar for business", "solar for farms", "solar for schools",
        "solar finance no deposit", "solar power purchase agreement", "solar leasing service",
    ],
}

# Decision-maker role keywords for sorting (V5.11: uses HARD_DM_KEYWORDS | SOFT_DM_KEYWORDS)
# Defined earlier in the module — this reassignment keeps it consistent
DECISION_MAKER_KEYWORDS = HARD_DM_KEYWORDS | SOFT_DM_KEYWORDS

# ══════════════════════════════════════════════════════════════════════════════
# UTILITY FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════


def safe_str(val) -> str:
    """Safely convert a value to string, treating None as empty string."""
    if val is None:
        return ""
    return str(val).strip()


def is_decision_maker(role: str) -> bool:
    """Check if a role indicates a decision maker."""
    if not role:
        return False
    role_lower = role.lower()
    return any(kw in role_lower for kw in DECISION_MAKER_KEYWORDS)


# V5.20: Role hierarchy for company-level person prioritization
# Higher score = more senior decision maker. Used in Phase 6 domain grouping.
ROLE_HIERARCHY_LEVELS = [
    (100, ["owner", "co-owner", "business owner"]),
    (95,  ["founder", "co-founder", "cofounder"]),
    (90,  ["ceo", "chief executive", "managing director", "md", "cfo", "cto", "coo",
            "cmo", "cio", "cpo", "chief", "chairman", "chairwoman", "chairperson", "president"]),
    (85,  ["partner", "principal"]),
    (80,  ["vice president", "vp", "svp", "evp", "avp"]),
    (75,  ["head of", "head,"]),
    (70,  ["director"]),
    (60,  ["manager", "gm", "general manager"]),
    (50,  ["senior"]),
    (10,  ["intern", "trainee", "apprentice"]),
]


def _role_hierarchy_score(role: str) -> int:
    """V5.20: Score a role based on decision-maker hierarchy.
    Returns 0-100 where higher = more senior decision maker.
    Uses word-boundary matching so 'director' doesn't falsely match 'cto'.
    'Managing Director' matches 'managing director' (90) taking the highest.
    """
    if not role:
        return 0
    role_lower = role.lower()
    best_score = 0
    for level_score, keywords in ROLE_HIERARCHY_LEVELS:
        for kw in keywords:
            # Use regex word boundary for short keywords to avoid substring false positives
            # e.g. "cto" should NOT match inside "director"
            if re.search(r'(?:^|[\s,/&(]|\b)' + re.escape(kw) + r'(?:[\s,/&)]|\b|$)', role_lower):
                best_score = max(best_score, level_score)
    return best_score


def get_full_name(person: dict) -> str:
    """V5.13: Extract the best possible full name from an Apollo/Lusha person dict.
    Checks name, full_name, first_name+last_name, display_name in order.
    """
    if person.get("name") and str(person["name"]).strip():
        return str(person["name"]).strip()
    if person.get("full_name") and str(person["full_name"]).strip():
        return str(person["full_name"]).strip()
    first = safe_str(person.get("first_name"))
    last = safe_str(person.get("last_name"))
    full = f"{first} {last}".strip()
    if full:
        return full
    if person.get("display_name") and str(person["display_name"]).strip():
        return str(person["display_name"]).strip()
    return ""


class RateLimiter:
    """Simple per-API rate limiter with minimum interval between calls."""

    def __init__(self, min_interval: float = 1.0):
        self.min_interval = min_interval
        self._last_call = 0.0
        self._lock = threading.Lock()

    def wait(self):
        with self._lock:
            elapsed = time.time() - self._last_call
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
            self._last_call = time.time()


def extract_domain(url: str) -> str:
    """Extract clean domain from a URL."""
    try:
        parsed = urlparse(url if "://" in url else f"https://{url}")
        domain = parsed.netloc or parsed.path.split("/")[0]
        domain = domain.lower().strip()
        if domain.startswith("www."):
            domain = domain[4:]
        # Remove port
        if ":" in domain:
            domain = domain.split(":")[0]
        return domain
    except Exception:
        return ""


def domain_to_company_name(domain: str) -> str:
    """Convert domain string to a readable company name.
    e.g., 'smith-dental.com.au' -> 'Smith Dental'
    """
    name = domain.lower().strip()
    for prefix in ("https://", "http://", "www."):
        if name.startswith(prefix):
            name = name[len(prefix):]
    name = name.split("/")[0]
    # Remove TLDs (order matters — longer first)
    tld_patterns = [
        ".com.au", ".co.uk", ".org.au", ".net.au", ".gov.au",
        ".co.in", ".org.in", ".net.in",
        ".co.nz", ".com", ".org", ".net", ".io", ".co",
        ".biz", ".info", ".au", ".uk", ".in", ".us",
    ]
    for tld in tld_patterns:
        if name.endswith(tld):
            name = name[: -len(tld)]
            break
    name = name.replace("-", " ").replace("_", " ").replace(".", " ")
    name = " ".join(name.split())
    return name.title() if name else domain


# ── V5.3: Name extraction utilities (use data we already have) ──────────

# Common business suffixes to strip when extracting person names from company names
_BUSINESS_SUFFIXES = {
    "photography", "photo", "photos", "studio", "studios", "creative", "creatives",
    "design", "designs", "media", "digital", "agency", "group", "co", "company",
    "consulting", "consultancy", "solutions", "services", "enterprises", "pty", "ltd",
    "inc", "llc", "corp", "plumbing", "electrical", "construction", "building",
    "dental", "medical", "health", "wellness", "fitness", "beauty", "salon",
    "law", "legal", "accounting", "finance", "marketing", "events", "catering",
    "landscaping", "cleaning", "painting", "roofing", "interiors", "productions",
    "films", "video", "visuals", "imaging", "images", "pictures", "portraits",
}

# Common first-name abbreviations → full forms (for matching domain/company names)
_NAME_ABBREVIATIONS = {
    "matt": ["matthew", "mathew"], "mike": ["michael"], "chris": ["christopher", "christine", "christina"],
    "rob": ["robert", "robin"], "bob": ["robert"], "dave": ["david"], "dan": ["daniel", "danny"],
    "nick": ["nicholas", "nicolas"], "tom": ["thomas"], "ben": ["benjamin"], "sam": ["samuel", "samantha"],
    "alex": ["alexander", "alexandra"], "max": ["maxwell", "maximilian"], "will": ["william"],
    "jim": ["james"], "joe": ["joseph"], "steve": ["steven", "stephen"], "tony": ["anthony"],
    "kate": ["katherine", "kathryn", "catherine"], "liz": ["elizabeth"], "meg": ["megan", "margaret"],
    "jen": ["jennifer", "jenna"], "pat": ["patrick", "patricia"], "andy": ["andrew"],
    "rick": ["richard"], "dick": ["richard"], "bill": ["william"], "ted": ["edward", "theodore"],
    "pete": ["peter"], "greg": ["gregory"], "tim": ["timothy"], "jon": ["jonathan", "jonathon"],
    "stu": ["stuart", "stewart"], "phil": ["philip", "phillip"], "ed": ["edward", "edmund"],
    "ash": ["ashley", "ashton"], "jake": ["jacob"], "jack": ["jackson", "john"],
    "nate": ["nathan", "nathaniel"], "josh": ["joshua"], "zach": ["zachary"],
    "luke": ["lucas"], "brad": ["bradley"], "drew": ["andrew"],
    "mel": ["melissa", "melanie"], "bec": ["rebecca"], "soph": ["sophia", "sophie"],
    "nat": ["natalie", "natasha", "nathan"], "em": ["emma", "emily"],
    "kel": ["kelly", "kelvin"], "les": ["leslie", "lester"],
    "russ": ["russell"], "mick": ["michael"],
}


def _get_name_variants(first_name: str) -> list[str]:
    """Return all possible full-form variants of a first name (including itself)."""
    lower = first_name.lower()
    variants = [lower]
    if lower in _NAME_ABBREVIATIONS:
        variants.extend(_NAME_ABBREVIATIONS[lower])
    # Also check if any abbreviation maps TO this name (reverse lookup)
    for abbrev, fulls in _NAME_ABBREVIATIONS.items():
        if lower in fulls and abbrev not in variants:
            variants.append(abbrev)
    return variants


def _extract_name_from_company(first_name: str, company_name: str) -> str:
    """V5.3: Extract full name from a company name that contains the person's name.
    e.g., first_name="Matt", company_name="Matthew Cornell Photography" → "Matthew Cornell"
          first_name="Julia", company_name="Julia Nance Photography" → "Julia Nance"
    """
    if not first_name or not company_name:
        return ""
    variants = _get_name_variants(first_name)
    words = company_name.split()
    if len(words) < 2:
        return ""
    # Check if the first word of the company name matches any variant of the person's first name
    first_word = words[0].lower()
    if first_word not in variants:
        return ""
    # Collect name words (skip business suffixes)
    name_words = [words[0]]  # Keep original casing
    for w in words[1:]:
        if w.lower() in _BUSINESS_SUFFIXES:
            break
        if w.lower() in ("&", "and", "the", "of", "by"):
            break
        # Must look like a name (capitalized, alpha, reasonable length)
        if w[0].isupper() and w.replace("'", "").replace("-", "").isalpha() and len(w) <= 20:
            name_words.append(w)
        else:
            break
    if len(name_words) >= 2:
        return " ".join(name_words)
    return ""


def _extract_name_from_domain(first_name: str, domain: str) -> str:
    """V5.3: Extract full name from a domain that encodes the person's name.
    e.g., first_name="Matt", domain="matthewcornell.com.au" → "Matthew Cornell"
          first_name="Julia", domain="julianance.com.au" → "Julia Nance"
    """
    if not first_name or not domain:
        return ""
    # Strip TLDs to get the domain root
    root = domain.lower()
    for tld in [".com.au", ".co.uk", ".org.au", ".net.au", ".co.nz",
                ".com", ".org", ".net", ".io", ".co", ".au", ".uk"]:
        if root.endswith(tld):
            root = root[:-len(tld)]
            break
    # Remove www prefix
    if root.startswith("www."):
        root = root[4:]
    # Remove hyphens for matching (many domains use firstname-lastname)
    root_clean = root.replace("-", "").replace(".", "")
    root_hyphen = root  # keep hyphens for splitting

    variants = _get_name_variants(first_name)

    for variant in variants:
        if root_clean.startswith(variant) and len(root_clean) > len(variant) + 1:
            suffix = root_clean[len(variant):]
            # Filter out business suffixes in domain (e.g. mattphoto.com)
            if suffix in _BUSINESS_SUFFIXES:
                continue
            # Check suffix starts with a letter and is alphabetic (likely a last name)
            if suffix.isalpha() and 2 <= len(suffix) <= 18:
                # Use the variant as the first name (may be fuller than the lead's current name)
                full_first = variant.title()
                return f"{full_first} {suffix.title()}"

    # Also try hyphenated domains: matthew-cornell.com.au
    if "-" in root_hyphen:
        parts = root_hyphen.split("-")
        if len(parts) >= 2 and parts[0] in variants:
            last = parts[1]
            if last.isalpha() and last not in _BUSINESS_SUFFIXES and 2 <= len(last) <= 18:
                return f"{parts[0].title()} {last.title()}"

    return ""


def _extract_name_from_linkedin_url(first_name: str, linkedin_url: str) -> str:
    """V5.3: Extract full name from a LinkedIn URL slug.
    e.g., first_name="Matt", url="linkedin.com/in/matthew-cornell-123abc" → "Matthew Cornell"
    """
    if not first_name or not linkedin_url:
        return ""
    # Extract the slug from the URL
    match = re.search(r"linkedin\.com/in/([^/?]+)", linkedin_url)
    if not match:
        return ""
    slug = match.group(1).lower()
    # Split slug by hyphens, filter out trailing IDs (hex strings, digits)
    parts = slug.split("-")
    name_parts = []
    for p in parts:
        # Stop at numeric/hex suffixes (LinkedIn adds random IDs like "a1b2c3")
        if p.isdigit() or (len(p) >= 5 and all(c in "0123456789abcdef" for c in p)):
            break
        if p.isalpha() and len(p) >= 2:
            name_parts.append(p)
    if len(name_parts) < 2:
        return ""
    # Check if the first part matches any variant of the person's first name
    variants = _get_name_variants(first_name)
    if name_parts[0] not in variants:
        return ""
    # Build the full name from the remaining parts
    return " ".join(p.title() for p in name_parts[:3])  # cap at 3 words (first middle last)


def format_phone(raw_phone: str, country: str) -> str:
    """Normalize and strictly validate phone number.
    Returns '+' prefixed digits (e.g. '+61XXXXXXXXXX') or '' if invalid.
    The '+' prefix prevents Excel from converting to scientific notation.
    AU: +61 + 10 digits = 12 digit body.  USA: +1 + 10 = 11.
    UK: +44 + 10 = 12.  India: +91 + 10 = 12.
    V5.27: Preserve valid international E.164 numbers with a DIFFERENT country code
    (e.g. a South African +27 number for a contact at an AU company).
    """
    if not raw_phone:
        return ""
    config = COUNTRY_CONFIG.get(country)
    if not config:
        return ""
    code_digits = config["phone_code"].replace("+", "")  # e.g. "61"
    expected_len = config["phone_digits"]  # e.g. 11

    # Strip ALL non-digit characters (removes letters, +, spaces, dashes, etc.)
    digits = re.sub(r"[^\d]", "", str(raw_phone))
    if not digits or len(digits) < 8:
        return ""

    # V5.27: If the original phone string starts with '+' and uses a DIFFERENT country code,
    # it's a valid international E.164 number from another country (e.g. +27 South Africa).
    # Preserve it as-is rather than mangling it with the target country code.
    raw_stripped = str(raw_phone).strip()
    if raw_stripped.startswith("+") and not digits.startswith(code_digits):
        # Accept any E.164-format international number (8–15 digits total)
        if 8 <= len(digits) <= 15:
            return f"+{digits}"
        return ""

    # Strip leading 0 (local format) and prepend country code
    if digits.startswith("0"):
        digits = code_digits + digits[1:]
    # If doesn't start with country code, prepend it
    if not digits.startswith(code_digits):
        digits = code_digits + digits

    # Strict validation: exact length required
    if len(digits) != expected_len:
        return ""
    return f"+{digits}"


def is_valid_email(email: str) -> bool:
    """Basic email validation — filters out obvious non-emails."""
    if not email or "@" not in email:
        return False
    pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    if not re.match(pattern, email):
        return False
    bad_patterns = [
        "example.com", "test.com", "sentry.io", "wixpress.com",
        ".png", ".jpg", ".gif", ".svg", ".webp", ".css", ".js",
        "noreply", "no-reply", "mailer-daemon", "postmaster",
        "schema.org", "sentry", "w3.org", "googleapis",
    ]
    email_lower = email.lower()
    return not any(bp in email_lower for bp in bad_patterns)


# Generic/company email prefixes that indicate a shared inbox, NOT a personal email
GENERIC_EMAIL_PREFIXES = {
    "info", "enquiries", "enquiry", "contact", "reception", "admin",
    "office", "hello", "hi", "help", "support", "sales", "marketing",
    "billing", "accounts", "finance", "hr", "careers", "jobs",
    "team", "general", "mail", "service", "services", "bookings",
    "booking", "appointments", "appointment", "feedback", "media",
    "press", "news", "newsletter", "subscribe", "unsubscribe",
    "webmaster", "postmaster", "abuse", "security", "legal",
    "compliance", "privacy", "orders", "order", "returns", "shipping",
    "dispatch", "warehouse", "operations", "customerservice",
    "customer.service", "customer-service", "customercare",
    "reception", "frontdesk", "front.desk", "front-desk",
    "practice", "clinic", "surgery", "studio", "salon", "shop",
    "store", "manager", "management",
    # City/region inboxes (office contact points, NOT personal)
    "london", "newyork", "new-york", "austin", "paris", "berlin",
    "sydney", "perth", "melbourne", "brisbane", "adelaide",
    "asia", "global", "international", "national", "regional",
    # Business action prefixes
    "newbusiness", "new-business", "enquire", "enq",
    "quote", "quotes", "estimates", "estimate",
    "noreply", "no-reply", "donotreply", "do-not-reply",
    # Department/function prefixes
    "design", "creative", "digital", "agency",
    "partnerships", "partnership", "solutions", "projects",
    "careers", "internship", "volunteer", "work",
    "discovery", "business", "pr",
}


def is_personal_email(email: str) -> bool:
    """V5.11: True ONLY for emails at known personal/consumer domains (gmail, yahoo, hotmail etc.).
    firstname@company.com is NOT personal — it's a work email.
    """
    if not email or "@" not in email:
        return False
    domain = email.lower().split("@")[-1].strip()
    return domain in PERSONAL_EMAIL_DOMAINS


def is_work_email(email: str) -> bool:
    """V5.11: True for first-name-style work emails (firstname@company.com).
    Distinct from generic inbox (info@, contact@) and personal (gmail, yahoo).
    """
    if not email or "@" not in email:
        return False
    domain = email.lower().split("@")[-1].strip()
    if domain in PERSONAL_EMAIL_DOMAINS:
        return False  # personal domain
    local = email.lower().split("@")[0].strip()
    return local not in GENERIC_EMAIL_PREFIXES  # not a generic inbox prefix


def classify_email_smart(email: str, person_name: str = "", company_name: str = "") -> str:
    """V5.4: Smart email classifier — uses name + company cross-referencing.
    Returns 'Personal', 'Generic', or 'Unknown'.

    Rules:
    1. If local part is in GENERIC_EMAIL_PREFIXES → Generic
    2. If local part contains any word from the person's name → Personal
    3. If local part contains any word from the company name → likely Generic (company email)
    4. Otherwise → Unknown (let OpenAI decide)
    """
    if not email or "@" not in email:
        return "Unknown"
    local = email.lower().split("@")[0].strip()

    # Rule 1: Known generic prefixes
    if local in GENERIC_EMAIL_PREFIXES:
        return "Generic"

    # Rule 2: Check if email contains words from the person's name
    if person_name:
        name_words = [w.lower() for w in person_name.split() if len(w) >= 2]
        for nw in name_words:
            # Check if name word appears in local part (handles first.last, firstlast, etc.)
            clean_local = local.replace(".", "").replace("-", "").replace("_", "")
            if nw in clean_local or nw in local:
                return "Personal"

    # Rule 3: Check if email contains words from the company name → likely company/generic email
    if company_name:
        company_words = [w.lower() for w in company_name.split()
                         if len(w) >= 3 and w.lower() not in _BUSINESS_SUFFIXES
                         and w.lower() not in {"the", "and", "of", "by", "for", "at", "in"}]
        for cw in company_words:
            if cw in local:
                return "Generic"

    # Rule 4: Passes generic prefix check → likely personal (heuristic fallback)
    return "Personal" if local not in GENERIC_EMAIL_PREFIXES else "Generic"


def _email_contains_person_name(email_addr: str, first_name: str = "", last_name: str = "") -> bool:
    """V5.20: Check if an email's local part contains the person's first or last name.
    Used to distinguish name-based business emails from generic company emails.
    Case-insensitive. Requires name parts >= 2 chars to avoid false matches.
    """
    if not email_addr or "@" not in email_addr:
        return False
    local = email_addr.lower().split("@")[0]
    clean_local = local.replace(".", "").replace("-", "").replace("_", "")
    first_lower = first_name.lower().strip() if first_name else ""
    last_lower = last_name.lower().strip() if last_name else ""
    if first_lower and len(first_lower) >= 2 and (first_lower in local or first_lower in clean_local):
        return True
    if last_lower and len(last_lower) >= 2 and (last_lower in local or last_lower in clean_local):
        return True
    return False


def _pick_best_email_from_apollo(person: dict, first_name: str = "", last_name: str = "") -> tuple:
    """V5.20: Smart email selection from Apollo person data.
    Returns (best_email, is_from_personal_list).

    Priority (per Apollo Knowledge Base — business emails first):
    1. Primary-tagged business email containing person's name
    2. Any business-tagged email containing person's name
    3. Primary-tagged personal email (gmail/yahoo)
    4. Any personal email (gmail/yahoo)
    5. Work email containing person's name (non-generic)
    6. Fallback to first available non-generic email

    Ensures business emails aren't generic company emails by verifying
    the person's first or last name appears in the local part.
    """
    structured_emails = person.get("emails") or []
    flat_personal = person.get("personal_emails") or []
    org_email = safe_str(person.get("email"))
    contact_email = safe_str(person.get("contact_email"))

    # --- Structured emails with type/tag metadata ---
    if structured_emails:
        business_primary = []
        business_other = []
        personal_primary = []
        personal_other = []

        for em_obj in structured_emails:
            if not isinstance(em_obj, dict):
                continue
            email_addr = em_obj.get("email", "")
            if not email_addr:
                continue
            email_type = (em_obj.get("email_type") or em_obj.get("type") or "").lower()
            email_tag = (em_obj.get("email_tag") or em_obj.get("tag") or em_obj.get("label") or "").lower()
            email_status = (em_obj.get("email_status") or em_obj.get("status") or "").lower()
            is_primary = ("primary" in email_tag or "primary" in email_status
                          or "primary" in email_type
                          or em_obj.get("position") == 0)
            is_business = ("business" in email_type or "professional" in email_type
                           or "work" in email_type or "primary" in email_type)
            is_personal_type = "personal" in email_type

            # V5.22: Business-tagged emails are preferred regardless of name containment.
            # Previously required name in address — too restrictive (e.g. j.smith@co omitted).
            if is_business and not is_personal_email(email_addr):
                (business_primary if is_primary else business_other).append(email_addr)
            elif is_personal_type or is_personal_email(email_addr):
                (personal_primary if is_primary else personal_other).append(email_addr)

        for bucket in [business_primary, business_other, personal_primary, personal_other]:
            if bucket:
                return bucket[0], True

    # --- V5.22: Check org/contact email for BUSINESS format BEFORE flat_personal consumer emails.
    # Apollo's `email` field is the primary work/org email (company domain).
    # Prefer it over personal_emails (gmail/hotmail) which are stored in flat_personal.
    for em in [org_email, contact_email]:
        if em and not is_personal_email(em):  # Company-domain email (not gmail/yahoo/hotmail)
            return em, False  # Business email found — return before checking consumer emails

    # --- Flat personal_emails: prefer name-based business over consumer ---
    if flat_personal:
        name_based = []
        consumer = []
        for em in flat_personal:
            if not em:
                continue
            if is_personal_email(em):                          # gmail/yahoo
                consumer.append(em)
            elif _email_contains_person_name(em, first_name, last_name):
                name_based.append(em)                          # riley@24hrpower.net.au
            else:
                name_based.append(em)                          # non-consumer, non-name — still preferable
        if name_based:
            return name_based[0], True
        if consumer:
            return consumer[0], True

    # --- Org/contact email final fallback (consumer domain) ---
    for em in [org_email, contact_email]:
        if em:
            return em, False

    return "", False


def _pick_best_phone_from_apollo(person: dict, company_phone: str = "") -> tuple:
    """V5.22: Smart phone selection from Apollo person data.
    Returns (best_phone, quality_score). quality_score=0 if no phone found.
    Priority: mobile/direct personal phones OVER company HQ numbers.
    Type priority: mobile > direct > work_direct > personal > home > other > work > work_hq
    This ensures we get the decision-maker's personal/direct number,
    NOT the generic company switchboard number.
    V5.25: Accepts company_phone to exclude company-level numbers. Also checks singular
    phone_number/direct_phone fields as fallback when phone_numbers array is empty.
    """
    _co_digits = re.sub(r'\D', '', company_phone) if company_phone else ""
    phone_numbers = person.get("phone_numbers") or []
    if not phone_numbers:
        # V5.25: Check singular phone fields as fallback — these CAN be personal for revealed contacts
        for field in ["phone_number", "direct_phone_number", "sanitized_phone", "phone"]:
            singular = safe_str(person.get(field))
            if singular:
                singular_digits = re.sub(r'\D', '', singular)
                # Skip if it matches the company phone (generic switchboard)
                if _co_digits and singular_digits == _co_digits:
                    continue
                # Treat singular field as medium quality (25) — not verified as mobile
                return singular, 25
        return "", 0

    # Score each phone by type — higher = more personal/direct
    # Threshold >= 30 means genuinely personal (mobile/direct/personal/home)
    _TYPE_SCORES = {
        "mobile": 50,
        "direct": 40,         # V5.22: Added — Apollo uses "direct" for direct lines
        "work_direct": 40,
        "direct_dial": 40,
        "personal": 35,
        "home": 30,
        "other": 15,
        "work": 15,           # V5.22: Added — "work" is company-level, not personal
        "work_hq": 5,         # company switchboard — least preferred
        "company_hq": 5,
        "corporate": 5,
        "headquarters": 5,
        "main": 5,
    }

    scored_phones = []
    for pn in phone_numbers:
        if not isinstance(pn, dict):
            continue
        number = safe_str(pn.get("sanitized_number") or pn.get("number")
                          or pn.get("raw_number", ""))
        if not number:
            continue

        # V5.26: Handle both standard Apollo format (type) and webhook format (type_cd)
        pn_type = (pn.get("type") or pn.get("type_cd") or "").lower().strip()
        pn_status = (pn.get("status") or pn.get("status_cd") or "").lower()
        pn_label = (pn.get("label") or pn.get("tag") or "").lower()
        is_primary = pn.get("is_primary") or pn.get("position") == 0
        is_default = ("default" in pn_status or "default" in pn_label
                      or "default" in pn_type)

        # Base score from phone type
        score = _TYPE_SCORES.get(pn_type, 20)  # unknown type gets 20

        # Bonus for primary/default flags
        if is_primary:
            score += 3
        if is_default:
            # V5.23: Default phone = Apollo's primary contact number for this person.
            # Boost to 35 for "other/work" defaults so they are treated as _direct_phone
            # (quality >= 30) and cannot be overwritten by enrich_person HQ phones (quality 5).
            # work_hq (5+20=25) remains non-direct. Mobile (50+20=70) stays highest.
            score += 20

        # V5.25: Penalize if this number matches the known company phone
        if _co_digits:
            num_digits = re.sub(r'\D', '', number)
            if num_digits == _co_digits:
                score = min(score, 5)  # Force to lowest tier (company HQ level)

        scored_phones.append((score, number))

    if not scored_phones:
        return "", 0  # V5.24: no scoreable phones — don't fall back to company-level singular field

    # Sort by score descending — highest-scored (most personal) phone wins
    scored_phones.sort(key=lambda x: x[0], reverse=True)
    return scored_phones[0][1], scored_phones[0][0]


def match_email_to_name(email: str, first_name: str, last_name: str) -> bool:
    """Check if an email's local part matches patterns for a person's name."""
    if not email or not first_name:
        return False
    local = email.lower().split("@")[0]
    f = first_name.lower().strip()
    l = last_name.lower().strip() if last_name else ""
    if f and len(f) > 1 and f in local:
        return True
    if l and len(l) > 1 and l in local:
        return True
    return False


def generate_email_candidates(first_name: str, last_name: str, domain: str) -> list:
    """V5.7: Generate likely email addresses from name + domain. No API calls."""
    if not first_name or not domain:
        return []
    f = first_name.lower().strip()
    l = last_name.lower().strip() if last_name else ""
    if l:
        return [
            f"{f}.{l}@{domain}",
            f"{f}{l}@{domain}",
            f"{f[0]}.{l}@{domain}",
            f"{f[0]}{l}@{domain}",
            f"{f}@{domain}",
            f"{l}.{f}@{domain}",
        ]
    return [f"{f}@{domain}"]


def _is_news_domain_heuristic(domain: str) -> bool:
    """Check if a domain looks like a news/media site by common name patterns.
    Only matches whole segments to avoid false positives (e.g. 'newscastle-dental' won't match).
    """
    d = domain.lower().strip()
    if d.startswith("www."):
        d = d[4:]
    # Split domain into segments (e.g. 'newcastle-herald.com.au' -> ['newcastle-herald', 'com', 'au'])
    base = d.split(".")[0]  # just the main domain name part
    # Split by hyphens too for compound names
    parts = set(base.replace("-", " ").replace("_", " ").split())
    news_keywords = {
        "news", "herald", "gazette", "journal", "tribune",
        "chronicle", "telegraph", "observer", "courier",
        "examiner", "mercury", "sentinel", "dispatch", "bulletin",
        "recorder", "advertiser", "times", "post", "press",
        "daily", "morning", "evening", "weekly", "media",
    }
    # Must match a whole word in the domain (not substring)
    return bool(parts & news_keywords)


def is_platform_domain(domain: str) -> bool:
    """Check if a domain is a known platform/directory/non-SMB to skip."""
    d = domain.lower().strip()
    if d.startswith("www."):
        d = d[4:]
    # Exact match or subdomain match against blocklist
    for pd in PLATFORM_DOMAINS:
        if d == pd or d.endswith(f".{pd}"):
            return True
    # Filter educational and government domains globally
    edu_gov_patterns = [".edu.", ".edu", ".gov.", ".gov", ".ac.uk", ".ac.au"]
    for pattern in edu_gov_patterns:
        if pattern in d or d.endswith(pattern):
            return True
    # Filter .org domains (covers .org, .org.au, .org.uk, etc.)
    if ".org" in d:
        return True
    # Heuristic news domain detection (catches domains not in the explicit blocklist)
    if _is_news_domain_heuristic(d):
        return True
    return False


# ══════════════════════════════════════════════════════════════════════════════
# API CLIENTS
# ══════════════════════════════════════════════════════════════════════════════


class SemrushClient:
    """SEMrush API client for keyword expansion AND domain discovery."""

    BASE_URL = "https://api.semrush.com/"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.limiter = RateLimiter(0.8)  # V5.1: Optimized from 1.2
        self._counter = {}  # V5.7: Per-run API call counter (set by pipeline)

    def _request(self, params: dict) -> str:
        """Make a rate-limited request and return raw text."""
        self.limiter.wait()
        params["key"] = self.api_key
        try:
            resp = requests.get(self.BASE_URL, params=params, timeout=30)
            if resp.status_code == 200:
                if "ERROR" in resp.text[:80]:
                    # Log SEMrush API errors (once per unique message to avoid flooding)
                    err_snippet = resp.text[:120].strip()
                    if not hasattr(self, "_last_error") or self._last_error != err_snippet:
                        self._last_error = err_snippet
                        self._counter["semrush_errors"] = self._counter.get("semrush_errors", 0) + 1
                        print(f"[SEMrush] API error: {err_snippet}", flush=True)
                    return ""
                self._counter["semrush"] = self._counter.get("semrush", 0) + 1
                return resp.text
            else:
                print(f"[SEMrush] HTTP {resp.status_code}", flush=True)
        except Exception as exc:
            print(f"[SEMrush] Request exception: {exc}", flush=True)
        return ""

    def get_related_keywords(self, phrase: str, database: str, display_limit: int = 15) -> list[dict]:
        """Get related keywords for a seed phrase."""
        text = self._request({
            "type": "phrase_related",
            "phrase": phrase,
            "database": database,
            "display_limit": display_limit,
            "export_columns": "Ph,Nq,Cp",
        })
        return self._parse_keyword_csv(text)

    def get_organic_domains(self, phrase: str, database: str, limit: int = 10) -> list[dict]:
        """Find domains ranking organically for a keyword.
        Returns list of {'domain': ..., 'url': ...}
        """
        text = self._request({
            "type": "phrase_organic",
            "phrase": phrase,
            "database": database,
            "display_limit": limit,
            "export_columns": "Dn,Ur",
        })
        return self._parse_domain_csv(text)

    def get_adwords_domains(self, phrase: str, database: str, limit: int = 10) -> list[dict]:
        """Find domains running ads for a keyword (high-intent prospects).
        Returns list of {'domain': ..., 'url': ...}
        """
        text = self._request({
            "type": "phrase_adwords",
            "phrase": phrase,
            "database": database,
            "display_limit": limit,
            "export_columns": "Dn,Ur",
        })
        return self._parse_domain_csv(text)

    def _parse_keyword_csv(self, text: str) -> list[dict]:
        results = []
        lines = text.strip().split("\n")
        if len(lines) < 2:
            return results
        for line in lines[1:]:
            parts = line.split(";")
            if len(parts) >= 3:
                try:
                    keyword = parts[0].strip()
                    volume = int(parts[1].strip().replace(",", "") or "0")
                    cpc = float(parts[2].strip().replace(",", "") or "0")
                    results.append({"keyword": keyword, "volume": volume, "cpc": cpc})
                except (ValueError, IndexError):
                    continue
        return results

    def _parse_domain_csv(self, text: str) -> list[dict]:
        results = []
        lines = text.strip().split("\n")
        if len(lines) < 2:
            return results
        for line in lines[1:]:
            parts = line.split(";")
            if len(parts) >= 2:
                domain = parts[0].strip()
                url = parts[1].strip() if len(parts) > 1 else ""
                # Clean domain
                d = extract_domain(domain) or extract_domain(url)
                if d and not is_platform_domain(d):
                    results.append({"domain": d, "url": url})
        return results

    def has_paid_traffic(self, domain: str, database: str) -> bool:
        """V5.10: Check if a domain runs Google Ads (paid traffic != 0)."""
        text = self._request({
            "type": "domain_adwords",
            "domain": domain,
            "database": database,
            "display_limit": 1,
            "export_columns": "Ph,Po,Nq,Cp",
        })
        lines = [ln.strip() for ln in text.strip().split("\n") if ln.strip()]
        return len(lines) >= 2

    def get_domain_traffic_metrics(self, domain: str, database: str) -> dict:
        """V5.13: Get both organic AND paid traffic metrics for a domain.
        Returns dict with paid_keywords, organic_keywords, paid_traffic, organic_traffic.
        """
        result = {
            "paid_keywords": 0, "organic_keywords": 0,
            "paid_traffic": 0, "organic_traffic": 0,
        }
        # Paid metrics
        paid_text = self._request({
            "type": "domain_adwords",
            "domain": domain,
            "database": database,
            "display_limit": 5,
            "export_columns": "Ph,Po,Nq,Cp",
        })
        paid_lines = [l for l in paid_text.strip().split("\n") if l.strip()]
        result["paid_keywords"] = max(0, len(paid_lines) - 1)
        for line in paid_lines[1:]:
            parts = line.split(";")
            if len(parts) >= 3:
                try:
                    result["paid_traffic"] += int(parts[2].strip().replace(",", "") or "0")
                except ValueError:
                    pass
        # Organic metrics
        org_text = self._request({
            "type": "domain_organic",
            "domain": domain,
            "database": database,
            "display_limit": 5,
            "export_columns": "Ph,Po,Nq",
        })
        org_lines = [l for l in org_text.strip().split("\n") if l.strip()]
        result["organic_keywords"] = max(0, len(org_lines) - 1)
        for line in org_lines[1:]:
            parts = line.split(";")
            if len(parts) >= 3:
                try:
                    result["organic_traffic"] += int(parts[2].strip().replace(",", "") or "0")
                except ValueError:
                    pass
        return result

    def get_domain_competitors(self, domain: str, database: str, limit: int = 5) -> list[str]:
        """V5.13: Get paid traffic competitors for a domain.
        Returns list of competitor domain strings.
        """
        # Paid competitors
        text = self._request({
            "type": "domain_adwords_adwords",
            "domain": domain,
            "database": database,
            "display_limit": limit,
            "export_columns": "Dn,Ad,At,Ac",
        })
        competitors = []
        lines = text.strip().split("\n")
        for line in lines[1:]:
            parts = line.split(";")
            if parts:
                d = extract_domain(parts[0].strip())
                if d and not is_platform_domain(d) and d != domain:
                    competitors.append(d)
        # If paid competitors insufficient, try organic competitors
        if len(competitors) < limit:
            org_text = self._request({
                "type": "domain_organic_organic",
                "domain": domain,
                "database": database,
                "display_limit": limit,
                "export_columns": "Dn,Cr,Or,Ot",
            })
            for line in org_text.strip().split("\n")[1:]:
                parts = line.split(";")
                if parts:
                    d = extract_domain(parts[0].strip())
                    if d and not is_platform_domain(d) and d != domain and d not in competitors:
                        competitors.append(d)
        return competitors[:limit]


class SerpApiClient:
    """SerpApi client — optional fallback for domain discovery."""

    BASE_URL = "https://serpapi.com/search.json"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.limiter = RateLimiter(0.8)  # V5.1: Optimized from 1.2
        self._available = True  # Track if API credits remain
        self._counter = {}  # V5.7: Per-run API call counter (set by pipeline)

    def search_keyword(self, query: str, country_gl: str, num: int = 20) -> list[str]:
        """Search Google and return discovered domains."""
        if not self._available:
            return []
        self.limiter.wait()
        params = {
            "q": query, "gl": country_gl, "api_key": self.api_key,
            "num": num, "output": "json",
        }
        try:
            resp = requests.get(self.BASE_URL, params=params, timeout=30)
            if resp.status_code == 429 or "run out of searches" in resp.text:
                self._available = False
                return []
            if resp.status_code != 200:
                return []
            data = resp.json()
            if "error" in data:
                self._available = False
                return []
            self._counter["serpapi"] = self._counter.get("serpapi", 0) + 1
            return self._extract_domains(data)
        except Exception:
            return []

    def _raw_search(self, query: str, country_gl: str, num: int = 5) -> dict:
        """V5.15: Return raw SerpAPI JSON (for snippet/title extraction).
        Used by Step 5e for full-name resolution from result snippets."""
        if not self._available:
            return {}
        self.limiter.wait()
        params = {
            "q": query, "gl": country_gl, "api_key": self.api_key,
            "num": num, "output": "json",
        }
        try:
            resp = requests.get(self.BASE_URL, params=params, timeout=30)
            if resp.status_code == 429 or "run out of searches" in resp.text:
                self._available = False
                return {}
            if resp.status_code != 200:
                return {}
            data = resp.json()
            if "error" in data:
                return {}
            self._counter["serpapi"] = self._counter.get("serpapi", 0) + 1
            return data
        except Exception:
            return {}

    def search_business_info(self, company_name: str, country_gl: str) -> dict:
        """Search for a company's phone/email via Google knowledge graph."""
        if not self._available:
            return {}
        self.limiter.wait()
        query = f'"{company_name}" phone number email contact'
        params = {
            "q": query, "gl": country_gl, "api_key": self.api_key,
            "num": 5, "output": "json",
        }
        try:
            resp = requests.get(self.BASE_URL, params=params, timeout=30)
            if resp.status_code != 200:
                return {}
            self._counter["serpapi"] = self._counter.get("serpapi", 0) + 1
            data = resp.json()
            info = {}
            kg = data.get("knowledge_graph", {})
            if kg.get("phone"):
                info["phone"] = kg["phone"]
            if kg.get("email"):
                info["email"] = kg["email"]
            for local in data.get("local_results", {}).get("places", []):
                if not info.get("phone") and local.get("phone"):
                    info["phone"] = local["phone"]
            return info
        except Exception:
            return {}

    def find_business_phone(self, domain: str, company_name: str, country_gl: str) -> str:
        """V5.16: Multi-strategy phone finder — tries 3 query patterns to find a business phone."""
        if not self._available:
            return ""
        phone_re = re.compile(r'\+?[\d][\d\s\-\(\)\.]{6,14}[\d]')
        queries = [
            f'"{domain}" phone number contact',
            f'site:{domain} contact phone',
            f'"{company_name}" phone',
        ]
        for q in queries:
            self.limiter.wait()
            params = {"q": q, "gl": country_gl, "api_key": self.api_key, "num": 5, "output": "json"}
            try:
                resp = requests.get(self.BASE_URL, params=params, timeout=20)
                if resp.status_code != 200:
                    continue
                self._counter["serpapi"] = self._counter.get("serpapi", 0) + 1
                data = resp.json()
                if "error" in data:
                    self._available = False
                    return ""
                # Knowledge graph
                kg = data.get("knowledge_graph", {})
                if kg.get("phone"):
                    return kg["phone"]
                # Local results
                for place in data.get("local_results", {}).get("places", []):
                    if place.get("phone"):
                        return place["phone"]
                # Answer box
                ab = data.get("answer_box", {})
                if ab.get("phone"):
                    return ab["phone"]
                # Organic snippets
                for r in data.get("organic_results", []):
                    snippet = r.get("snippet", "") + " " + r.get("title", "")
                    m = phone_re.search(snippet)
                    if m:
                        candidate = m.group().strip()
                        if len(re.sub(r'\D', '', candidate)) >= 7:
                            return candidate
            except Exception:
                continue
        return ""

    def find_person_phone(self, person_name: str, domain: str,
                          company: str, country_gl: str) -> str:
        """V5.23: Search for a specific person's phone number via Google.
        Searches trade directories, company team pages, and Google snippets.
        Returns first candidate phone found. Used as Phase 4b fallback when
        Apollo/Lusha have no phone data for an individual."""
        if not self._available or not person_name:
            return ""
        phone_re = re.compile(r'\+?[\d][\d\s\-\(\)\.]{6,14}[\d]')
        queries = [
            f'"{person_name}" site:{domain}',
            f'"{person_name}" "{company}" phone mobile contact',
        ]
        for q in queries:
            data = self._raw_search(q, country_gl, num=5)
            if not data:
                continue
            # Check knowledge graph
            kg = data.get("knowledge_graph", {})
            if kg.get("phone"):
                return kg["phone"]
            # Search organic snippets for phone pattern
            for r in data.get("organic_results", []):
                snippet = r.get("snippet", "") + " " + r.get("title", "")
                m = phone_re.search(snippet)
                if m:
                    candidate = m.group().strip()
                    digits = re.sub(r'\D', '', candidate)
                    if len(digits) >= 7:
                        return candidate
        return ""

    def _extract_domains(self, data: dict) -> list[str]:
        domains = set()
        for result in data.get("organic_results", []):
            d = extract_domain(result.get("link", ""))
            if d and not is_platform_domain(d):
                domains.add(d)
        for ad in data.get("ads", []):
            d = extract_domain(ad.get("link", "") or ad.get("tracking_link", ""))
            if d and not is_platform_domain(d):
                domains.add(d)
        for place in data.get("local_results", {}).get("places", []):
            d = extract_domain(place.get("website", "") or place.get("link", ""))
            if d and not is_platform_domain(d):
                domains.add(d)
        return list(domains)

    def find_person_full_name(self, first_name: str, company_name: str,
                               domain: str, country_gl: str) -> str:
        """V5.2: Try to find a person's full name by searching Google for
        'FirstName CompanyName site:domain OR linkedin.com/in'."""
        if not self._available or not first_name:
            return ""
        self.limiter.wait()
        query = f'"{first_name}" "{company_name}" site:{domain} OR site:linkedin.com'
        params = {
            "q": query, "gl": country_gl,
            "api_key": self.api_key, "num": 5, "output": "json",
        }
        try:
            resp = requests.get(self.BASE_URL, params=params, timeout=20)
            if resp.status_code == 429 or "run out of searches" in resp.text:
                self._available = False
                return ""
            if resp.status_code != 200:
                return ""
            data = resp.json()
            if "error" in data:
                self._available = False
                return ""
            self._counter["serpapi"] = self._counter.get("serpapi", 0) + 1
            # Search snippets for "FirstName LastName" patterns
            name_pattern = re.compile(
                rf"\b{re.escape(first_name)}\s+([A-Z][a-z]{{1,20}})\b"
            )
            text_to_search = " ".join(
                r.get("snippet", "") + " " + r.get("title", "")
                for r in data.get("organic_results", [])
            )
            match = name_pattern.search(text_to_search)
            if match:
                return f"{first_name} {match.group(1)}"
            return ""
        except Exception:
            return ""

    def find_person_on_linkedin(self, first_name: str, company_name: str) -> str:
        """V5.13: LinkedIn-targeted SerpApi query to extract full name from title snippet.
        Query: '{first_name} {company_name} site:linkedin.com'
        LinkedIn titles are typically: 'FirstName LastName - Title at Company | LinkedIn'
        """
        if not self._available or not first_name:
            return ""
        self.limiter.wait()
        query = f"{first_name} {company_name} site:linkedin.com/in"
        params = {
            "q": query, "api_key": self.api_key, "num": 5, "output": "json",
        }
        try:
            resp = requests.get(self.BASE_URL, params=params, timeout=20)
            if resp.status_code == 429 or "run out of searches" in resp.text:
                self._available = False
                return ""
            if resp.status_code != 200:
                return ""
            data = resp.json()
            if "error" in data:
                self._available = False
                return ""
            self._counter["serpapi"] = self._counter.get("serpapi", 0) + 1
            # LinkedIn titles: "FirstName LastName - Title | LinkedIn"
            # Parse full name from the title (most reliable field)
            name_pattern = re.compile(
                rf"\b({re.escape(first_name)}\s+[A-Z][a-zA-Z\-']{{1,25}})\b"
            )
            for result in data.get("organic_results", []):
                title = result.get("title", "")
                snippet = result.get("snippet", "")
                for text in (title, snippet):
                    m = name_pattern.search(text)
                    if m:
                        candidate = m.group(1).strip()
                        parts = candidate.split()
                        if len(parts) == 2 and _is_valid_person_name(candidate):
                            return candidate
            return ""
        except Exception:
            return ""


class HunterClient:
    """Hunter.io API client — domain-level email search for personal contact discovery.
    Free plan: 25 requests/month. Set HUNTER_API_KEY env var to enable.
    """

    BASE_URL = "https://api.hunter.io/v2"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.limiter = RateLimiter(1.5)  # Conservative — avoid rate limiting
        self._available = bool(api_key and len(api_key) > 5)
        self._counter = {}

    def domain_search(self, domain: str, limit: int = 10) -> list[dict]:
        """Search all emails at a domain via Hunter.io domain-search API.
        Returns list of dicts: {first_name, last_name, email, position, confidence}.
        Priority: higher confidence score = more reliable email.
        """
        if not self._available:
            return []
        self.limiter.wait()
        try:
            resp = requests.get(
                f"{self.BASE_URL}/domain-search",
                params={"domain": domain, "api_key": self.api_key, "limit": limit},
                timeout=15,
                headers={"User-Agent": _get_random_ua()},
            )
            if resp.status_code == 200:
                self._counter["hunter"] = self._counter.get("hunter", 0) + 1
                raw = resp.json().get("data", {}).get("emails", [])
                # Sort by confidence descending so best results come first
                raw.sort(key=lambda x: x.get("confidence", 0), reverse=True)
                return raw
            if resp.status_code == 429:
                self._available = False
            return []
        except Exception:
            return []


class ApolloClient:
    """Apollo.io API client for people search and organization enrichment."""

    BASE_URL = "https://api.apollo.io/api/v1"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.limiter = RateLimiter(0.25)  # V5.1: Optimized from 0.4 (was 0.6 in V4)
        self._counter = {}  # V5.7: Per-run API call counter (set by pipeline)

    def _headers(self) -> dict:
        return {
            "Content-Type": "application/json",
            "Cache-Control": "no-cache",
            "X-Api-Key": self.api_key,
        }

    def search_people_by_domain(self, domain: str, per_page: int = 10) -> list[dict]:
        """Search for people at a domain using the new api_search endpoint.
        V5.19: Added reveal_personal_emails to capture any emails Apollo returns in search results.
        V5.26: Removed reveal_phone_number — mixed_people/api_search does NOT support it
        (silently ignored or causes 400). Phone reveal happens in enrich_person instead."""
        self.limiter.wait()
        url = f"{self.BASE_URL}/mixed_people/api_search"
        payload = {
            "q_organization_domains": domain,
            "per_page": per_page,
            "reveal_personal_emails": True,   # V5.19: reveal any available personal emails
        }
        try:
            resp = requests.post(url, json=payload, headers=self._headers(), timeout=30)
            if resp.status_code == 200:
                self._counter["apollo"] = self._counter.get("apollo", 0) + 1
                people = resp.json().get("people", [])
                return people
            return []
        except Exception:
            return []

    def enrich_organization(self, domain: str) -> dict:
        """Get organization-level data including phone number."""
        self.limiter.wait()
        url = f"{self.BASE_URL}/organizations/enrich"
        try:
            resp = requests.get(
                url, params={"domain": domain},
                headers=self._headers(), timeout=30
            )
            if resp.status_code == 200:
                self._counter["apollo"] = self._counter.get("apollo", 0) + 1
                org = resp.json().get("organization", {})
                return {
                    "company_name": org.get("name", ""),
                    "phone": org.get("phone", ""),
                    "website": org.get("website_url", ""),
                    "industry": org.get("industry", ""),
                    "employees": org.get("estimated_num_employees", ""),
                    "city": org.get("city", ""),
                    "linkedin": org.get("linkedin_url", ""),
                }
            return {}
        except Exception:
            return {}

    def enrich_person(self, first_name: str, last_name: str, domain: str,
                      linkedin_url: str = "", organization_name: str = "",
                      apollo_id: str = "", company_phone: str = "") -> dict:
        """Try to enrich a person with email/phone. Uses LinkedIn URL for precise matching when available.
        V5.10+: organization_name improves Apollo matching when last_name is absent.
        V5.19: apollo_id enables exact record lookup, bypassing fuzzy name matching.
        V5.26: Webhook-based phone reveal — sends reveal request with webhook_url, phone data
        arrives async at /api/apollo-phone-callback. Falls back to non-reveal if no webhook URL."""
        self.limiter.wait()
        url = f"{self.BASE_URL}/people/match"
        payload = {
            "reveal_personal_emails": True,
        }

        # V5.26: Include phone reveal with webhook URL
        _webhook_url = _get_webhook_url()
        if _webhook_url:
            payload["reveal_phone_number"] = True
            payload["webhook_url"] = _webhook_url

        # V5.19: If we have the Apollo person ID from the initial search, use it for exact lookup.
        if apollo_id:
            payload["id"] = apollo_id
        if first_name:
            payload["first_name"] = first_name
        # V5.19: Don't pass an obfuscated initial as last_name
        if last_name and not (len(last_name.rstrip(".")) == 1 and last_name.rstrip(".").isalpha()):
            payload["last_name"] = last_name
        if domain:
            payload["domain"] = domain
        if linkedin_url:
            payload["linkedin_url"] = linkedin_url
        if organization_name:
            payload["organization_name"] = organization_name
        try:
            resp = requests.post(url, json=payload, headers=self._headers(), timeout=30)
            # V5.26: If reveal with webhook causes 400/422, retry without reveal
            if resp.status_code in (400, 422):
                payload.pop("reveal_phone_number", None)
                payload.pop("webhook_url", None)
                resp = requests.post(url, json=payload, headers=self._headers(), timeout=30)
            if resp.status_code == 200:
                self._counter["apollo"] = self._counter.get("apollo", 0) + 1
                person = resp.json().get("person", {})
                if person:
                    _person_id = person.get("id", "")

                    # V5.26: Register this person for async phone collection
                    if _person_id and _webhook_url:
                        _register_phone_reveal(_person_id)

                    # V5.26: Check if webhook already delivered phone data (fast turnaround)
                    if _person_id and not person.get("phone_numbers"):
                        webhook_phones = _collect_phone_reveal(_person_id)
                        if webhook_phones:
                            person["phone_numbers"] = webhook_phones

                    first = safe_str(person.get('first_name'))
                    last = safe_str(person.get('last_name'))
                    email, _ = _pick_best_email_from_apollo(person, first, last)
                    phone, phone_quality = _pick_best_phone_from_apollo(person, company_phone)
                    result = {
                        "name": f"{first} {last}".strip() if last else first,
                        "role": safe_str(person.get("title")),
                        "email": email,
                        "phone": phone,
                        "_phone_quality": phone_quality,
                        "_apollo_id": _person_id,  # V5.26: Store for later phone collection
                        "company": safe_str((person.get("organization") or {}).get("name")),
                    }
                    return result
            return {}
        except Exception:
            return {}

    def search_email_verified_people(self, domain: str, per_page: int = 5) -> list:
        """V5.10+: Find people at domain who Apollo has contact emails for (any status).
        V5.14: Expanded to include 'unverified' status so more contacts with work emails
        are returned for enrichment — previously 'verified'+'likely_to_engage' only.
        Used in Step 2e as a last-resort email recovery pass."""
        self.limiter.wait()
        url = f"{self.BASE_URL}/mixed_people/api_search"
        payload = {
            "q_organization_domains": domain,
            "contact_email_status": ["verified", "likely_to_engage", "unverified"],
            "per_page": per_page,
        }
        try:
            resp = requests.post(url, json=payload, headers=self._headers(), timeout=30)
            if resp.status_code == 200:
                self._counter["apollo"] = self._counter.get("apollo", 0) + 1
                return resp.json().get("people", [])
            return []
        except Exception:
            return []


class LushaClient:
    """Lusha API client — company enrichment and person lookup."""

    BASE_URL = "https://api.lusha.com"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.limiter = RateLimiter(0.15)  # V5.1: Optimized from 0.2 (was 0.3 in V4)
        self._counter = {}  # V5.7: Per-run API call counter (set by pipeline)

    def _headers(self) -> dict:
        return {"api_key": self.api_key, "Content-Type": "application/json"}

    def get_company_info(self, domain: str) -> dict:
        """Get company information from Lusha company API v2."""
        self.limiter.wait()
        url = f"{self.BASE_URL}/v2/company"
        try:
            resp = requests.get(
                url, params={"domain": domain},
                headers=self._headers(), timeout=30
            )
            if resp.status_code == 200:
                self._counter["lusha"] = self._counter.get("lusha", 0) + 1
                global _lusha_calls_total
                _lusha_calls_total += 1
                data = resp.json().get("data", {})
                if data:
                    return {
                        "company_name": data.get("name", ""),
                        "description": data.get("description", ""),
                        "domain": data.get("domain", ""),
                        "employees": data.get("employees", ""),
                        "industry": data.get("mainIndustry", ""),
                        "sub_industry": data.get("subIndustry", ""),
                        "linkedin": data.get("social", {}).get("linkedin", {}).get("url", ""),
                        "city": data.get("location", {}).get("city", ""),
                        "country": data.get("location", {}).get("country", ""),
                        "website": data.get("website", ""),
                    }
            return {}
        except Exception:
            return {}

    def enrich_person(self, first_name: str, last_name: str, company_domain: str) -> dict:
        """Enrich a person via Lusha Person API v2."""
        self.limiter.wait()
        url = f"{self.BASE_URL}/v2/person"
        try:
            resp = requests.get(
                url,
                params={
                    "firstName": first_name,
                    "lastName": last_name,
                    "companyDomain": company_domain,
                },
                headers=self._headers(),
                timeout=30,
            )
            if resp.status_code == 200:
                self._counter["lusha"] = self._counter.get("lusha", 0) + 1
                global _lusha_calls_total
                _lusha_calls_total += 1
                data = resp.json()
                contact = data.get("contact", {})
                if contact and contact.get("data"):
                    person_data = contact["data"]
                    first = safe_str(person_data.get('firstName'))
                    last = safe_str(person_data.get('lastName'))
                    result = {
                        "name": f"{first} {last}".strip() if last else first,
                        "role": safe_str(person_data.get("jobTitle")),
                        "email": "",
                        "phone": "",
                        "company": safe_str((person_data.get("company") or {}).get("name")),
                    }
                    if person_data.get("emails"):
                        # V5.22: Pick BUSINESS email first — prefer company domain over consumer.
                        # Priority: business/primary tagged > work tagged > company domain > consumer.
                        _business_email = ""
                        _work_email = ""
                        _company_email = ""
                        _consumer_email = ""
                        _first_email = ""
                        for em in person_data["emails"]:
                            addr = em.get("email", "")
                            if not addr:
                                continue
                            if not _first_email:
                                _first_email = addr
                            em_type = (em.get("type") or "").lower()
                            if "business" in em_type or "primary" in em_type:
                                if not _business_email:
                                    _business_email = addr
                            elif "work" in em_type:
                                if not _work_email:
                                    _work_email = addr
                            elif is_personal_email(addr):  # gmail/yahoo/hotmail
                                if not _consumer_email:
                                    _consumer_email = addr
                            else:
                                if not _company_email:
                                    _company_email = addr
                        result["email"] = (_business_email or _work_email or
                                           _company_email or _consumer_email or _first_email)
                    if person_data.get("phoneNumbers"):
                        # V5.22: Pick most personal phone from Lusha — prefer mobile/direct over landline/HQ
                        _lusha_type_scores = {"mobile": 50, "direct": 40, "personal": 35, "work": 15, "landline": 10, "other": 20}
                        _lusha_phones = []
                        for pn in person_data["phoneNumbers"]:
                            num = pn.get("number", "")
                            if not num:
                                continue
                            ptype = (pn.get("type") or "other").lower()
                            score = _lusha_type_scores.get(ptype, 20)
                            _lusha_phones.append((score, num))
                        if _lusha_phones:
                            _lusha_phones.sort(key=lambda x: x[0], reverse=True)
                            result["phone"] = _lusha_phones[0][1]
                            result["_phone_quality"] = _lusha_phones[0][0]  # V5.22: track quality
                        else:
                            result["phone"] = person_data["phoneNumbers"][0].get("number", "")
                            result["_phone_quality"] = 20
                    return result
            return {}
        except Exception:
            return {}


class OpenAIEmailVerifier:
    """OpenAI-powered email classification — determines if email is personal or generic."""

    API_URL = "https://api.openai.com/v1/chat/completions"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.limiter = RateLimiter(0.5)
        self._available = bool(api_key and len(api_key) > 10)
        self._counter = {}  # V5.7: Per-run API call counter (set by pipeline)

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def is_personal_email_ai(self, email: str, person_name: str = "", company_name: str = "") -> bool | None:
        """Use OpenAI to classify an email as personal or generic.
        Returns True (personal), False (generic), or None (API error/unavailable).
        """
        if not self._available or not email:
            return None
        self.limiter.wait()
        prompt = (
            f"Classify this email address as 'personal' or 'generic'.\n"
            f"Email: {email}\n"
            f"Person name: {person_name or 'unknown'}\n"
            f"Company: {company_name or 'unknown'}\n\n"
            f"CLASSIFICATION RULES:\n"
            f"1. If the email local part (before @) contains ANY word from the person's name "
            f"(case-insensitive) → PERSONAL (e.g. matt.cornell@ for 'Matt Cornell')\n"
            f"2. If the email local part contains a company name word (not a person's name) → GENERIC "
            f"(e.g. smithdental@ for company 'Smith Dental' but person 'John Doe')\n"
            f"3. Role-based prefixes (info, admin, sales, contact, hello, support, bookings, "
            f"enquiries, reception, practice, studio, office) → GENERIC\n"
            f"4. Unique non-role, non-company words → likely PERSONAL\n\n"
            f"Reply with ONLY one word: 'personal' or 'generic'."
        )
        try:
            resp = requests.post(
                self.API_URL,
                headers=self._headers(),
                json={
                    "model": "gpt-4o-mini",
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 10,
                    "temperature": 0,
                },
                timeout=15,
            )
            if resp.status_code == 401 or resp.status_code == 403:
                self._available = False
                return None
            if resp.status_code == 429:
                return None  # Rate limited, skip but don't disable
            if resp.status_code == 200:
                self._counter["openai"] = self._counter.get("openai", 0) + 1
                answer = resp.json()["choices"][0]["message"]["content"].strip().lower()
                return "personal" in answer
            return None
        except Exception:
            return None

    def infer_personal_email(self, first_name: str, last_name: str,
                              domain: str, company_name: str = "") -> list:
        """V5.7: Use OpenAI to generate likely personal email patterns for a person."""
        if not self._available or not first_name or not domain:
            return []
        self.limiter.wait()
        prompt = (
            f"Given a person's name and their company domain, generate the most likely "
            f"personal email addresses they would use at that domain.\n\n"
            f"Person: {first_name} {last_name}\n"
            f"Domain: {domain}\n"
            f"Company: {company_name or 'unknown'}\n\n"
            f"Common patterns: firstname@domain, firstname.lastname@domain, "
            f"firstnamelastname@domain, f.lastname@domain, flastname@domain\n\n"
            f"Reply with ONLY the email addresses, one per line, most likely first. "
            f"No explanations. Maximum 5 emails."
        )
        try:
            resp = requests.post(
                self.API_URL, headers=self._headers(),
                json={
                    "model": "gpt-4o-mini",
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 100,
                    "temperature": 0.2,
                },
                timeout=15,
            )
            if resp.status_code in (401, 403):
                self._available = False
                return []
            if resp.status_code == 200:
                self._counter["openai"] = self._counter.get("openai", 0) + 1
                text = resp.json()["choices"][0]["message"]["content"].strip()
                candidates = []
                for line in text.split("\n"):
                    line = line.strip().strip("-").strip("*").strip().strip("0123456789.").strip()
                    if "@" in line and "." in line.split("@")[-1]:
                        candidates.append(line.lower())
                return candidates[:5]
            return []
        except Exception:
            return []

    def verify_leads_batch(self, leads: list) -> None:
        """V5.4: Verify emails using 3-tier classification:
        1. OpenAI AI agent (best quality, uses enhanced prompt with name/company rules)
        2. Smart classifier (name-matching + company-matching heuristic)
        3. Basic generic prefix check (last resort)
        """
        for lead in leads:
            email = lead.get("email", "")
            if not email:
                lead["_email_type"] = ""
                continue

            # V5.7: Skip classification for inferred emails — they're name-based patterns
            if lead.get("_email_inferred"):
                lead["_email_type"] = "Inferred"
                continue

            person_name = lead.get("name", "")
            company_name = lead.get("company", "")

            # Tier 1: Try OpenAI AI agent first (most accurate)
            ai_result = self.is_personal_email_ai(email, person_name, company_name)

            if ai_result is not None:
                lead["_email_type"] = "Personal" if ai_result else "Generic"
            else:
                # Tier 2: Smart classifier with name/company cross-referencing
                smart_result = classify_email_smart(email, person_name, company_name)
                lead["_email_type"] = smart_result if smart_result != "Unknown" else (
                    "Personal" if is_personal_email(email) else "Generic"
                )


class WebScraper:
    """Free web scraper for extracting contact info from company websites."""

    CONTACT_PATHS = [
        "", "/contact",  # V5.1: Optimized to 2 paths (was 3 in V5, 12 in V4)
    ]

    # V5.13: Expanded team/about page paths for full-name extraction
    TEAM_PATHS = [
        "/about", "/about-us", "/team", "/our-team",
        "/staff", "/people", "/meet-the-team", "/meet-us",
        "/who-we-are", "/company", "/management", "/leadership",
        "/our-people", "/team-members", "/our-staff",
    ]

    def __init__(self, country_code: str = "AU"):
        self.country_code = country_code
        self.phone_regex = COUNTRY_CONFIG.get(country_code, COUNTRY_CONFIG["AU"])["phone_regex"]
        self.limiter = RateLimiter(0.3)
        self._headers = {"User-Agent": _get_random_ua()}

    def scrape_domain(self, domain: str) -> dict:
        """Scrape a domain for contact information."""
        result = {"emails": [], "phones": [], "company_name": "", "name_email_pairs": []}
        for path in self.CONTACT_PATHS:
            url = f"https://{domain}{path}"
            page_data = self._scrape_page(url)
            if page_data:
                result["emails"].extend(page_data.get("emails", []))
                result["phones"].extend(page_data.get("phones", []))
                result["name_email_pairs"].extend(page_data.get("name_email_pairs", []))
                if not result["company_name"] and page_data.get("company_name"):
                    result["company_name"] = page_data["company_name"]
        # Deduplicate
        result["emails"] = list(dict.fromkeys(e for e in result["emails"] if is_valid_email(e)))
        result["phones"] = list(dict.fromkeys(result["phones"]))
        # Deduplicate name_email_pairs by email
        seen_pair_emails = set()
        unique_pairs = []
        for pair in result["name_email_pairs"]:
            if pair["email"] not in seen_pair_emails:
                seen_pair_emails.add(pair["email"])
                unique_pairs.append(pair)
        result["name_email_pairs"] = unique_pairs
        return result

    def scrape_team_names(self, domain: str) -> list[dict]:
        """V5.13: Scrape team/about pages to extract staff full names.
        Returns list of {'name': str, 'email': str} dicts.
        Tries first 6 team paths. Uses _is_valid_person_name() guard.
        """
        found = []
        seen_names = set()
        for path in self.TEAM_PATHS[:6]:
            url = f"https://{domain}{path}"
            try:
                self.limiter.wait()
                self._headers["User-Agent"] = _get_random_ua()
                time.sleep(random.uniform(0.5, 1.5))
                resp = requests.get(url, headers=self._headers, timeout=8, allow_redirects=True)
                if resp.status_code != 200:
                    continue
                soup = BeautifulSoup(resp.text, "html.parser")
                containers = soup.find_all(
                    ["div", "li", "article", "section"],
                    class_=re.compile(r"team|staff|member|person|profile|card|employee|director|partner|bio|people", re.I),
                )
                for container in containers:
                    container_text = container.get_text(separator=" ", strip=True)
                    name_matches = re.findall(r"\b([A-Z][a-z]{1,20}(?:\s+[A-Z][a-z]{1,20}){1,2})\b", container_text)
                    emails = re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", container_text)
                    for nm in name_matches:
                        words = nm.split()
                        if all(w[0].isupper() and w.replace("'", "").replace("-", "").isalpha() for w in words):
                            # V5.13: Apply name validation guard
                            if _is_valid_person_name(nm):
                                key = nm.lower()
                                if key not in seen_names:
                                    seen_names.add(key)
                                    found.append({
                                        "name": nm,
                                        "email": emails[0] if emails else "",
                                    })
                            break  # One name per container
                # V5.15: Schema.org Person markup parsing
                schema_blocks = soup.find_all(
                    True,
                    attrs={"itemtype": re.compile(r"schema\.org/Person", re.I)}
                )
                for block in schema_blocks:
                    name_tag = block.find(attrs={"itemprop": "name"})
                    nm = name_tag.get_text(strip=True) if name_tag else ""
                    if nm and " " in nm and _is_valid_person_name(nm):
                        key = nm.lower()
                        if key not in seen_names:
                            seen_names.add(key)
                            found.append({"name": nm, "email": ""})

                # V5.15: JSON-LD Person parsing
                for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
                    try:
                        import json as _json
                        data = _json.loads(script.string or "")
                        items = data if isinstance(data, list) else [data]
                        for item in items:
                            if item.get("@type") in ("Person", "Employee"):
                                nm = item.get("name", "")
                                if nm and " " in nm and _is_valid_person_name(nm):
                                    key = nm.lower()
                                    if key not in seen_names:
                                        seen_names.add(key)
                                        found.append({"name": nm, "email": item.get("email", "")})
                    except Exception:
                        pass

                # V5.15: <meta name="author"> tags
                for meta in soup.find_all("meta", attrs={"name": "author"}):
                    nm = meta.get("content", "").strip()
                    if nm and " " in nm and _is_valid_person_name(nm):
                        key = nm.lower()
                        if key not in seen_names:
                            seen_names.add(key)
                            found.append({"name": nm, "email": ""})

                if len(found) >= 20:
                    break
            except Exception:
                continue
        return found[:20]

    def _extract_obfuscated_emails(self, soup: BeautifulSoup, text: str) -> list:
        """V5.13: Extract emails hidden behind common obfuscation techniques."""
        found = []
        # Pattern 1: [at] / (at) substitution
        deobf = re.sub(r'\s*\[at\]\s*|\s*\(at\)\s*|\s+AT\s+', '@', text, flags=re.I)
        deobf = re.sub(r'\s*\[dot\]\s*|\s*\(dot\)\s*|\s+DOT\s+', '.', deobf, flags=re.I)
        found.extend(re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", deobf))
        # Pattern 2: HTML entity encoding (&#64; = @, &#46; = .)
        decoded = html_mod.unescape(text)
        found.extend(re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", decoded))
        # Pattern 3: data-email + data-domain split attributes
        for tag in soup.find_all(attrs={"data-email": True}):
            user = tag.get("data-email", "")
            domain = tag.get("data-domain", "")
            if user and domain:
                found.append(f"{user}@{domain}")
        # Pattern 4: mailto in javascript hrefs
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]
            if "mailto:" in href.lower():
                email = re.sub(r"mailto:", "", href, flags=re.I).split("?")[0].strip()
                if email:
                    found.append(email)
        # Pattern 5: Simple JS string concatenation in inline <script>
        for script in soup.find_all("script"):
            script_text = script.get_text()
            concat_pattern = re.findall(
                r"""['"]([a-zA-Z0-9._%+-]+)['"]\s*\+\s*['"]?@['"]?\s*\+\s*['"]([a-zA-Z0-9.-]+\.[a-zA-Z]{2,})['"]""",
                script_text
            )
            for user, dom in concat_pattern:
                found.append(f"{user}@{dom}")
        return [e for e in found if is_valid_email(e)]

    def _scrape_page(self, url: str) -> dict | None:
        self.limiter.wait()
        try:
            self._headers["User-Agent"] = _get_random_ua()
            resp = requests.get(url, headers=self._headers, timeout=10, allow_redirects=True)
            if resp.status_code == 429 or resp.status_code == 503:
                # V5.13: Exponential backoff for rate limiting
                for attempt in range(3):
                    time.sleep(2 ** (attempt + 1))
                    resp = requests.get(url, headers=self._headers, timeout=10, allow_redirects=True)
                    if resp.status_code == 200:
                        break
            if resp.status_code != 200:
                return None
            soup = BeautifulSoup(resp.text, "html.parser")
            text = soup.get_text(separator=" ", strip=True)

            # Emails from text + mailto links
            emails = re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", text)
            for a_tag in soup.find_all("a", href=True):
                if a_tag["href"].startswith("mailto:"):
                    email = a_tag["href"].replace("mailto:", "").split("?")[0].strip()
                    if email:
                        emails.append(email)
            # V5.13: Extract obfuscated emails
            emails.extend(self._extract_obfuscated_emails(soup, text))
            emails = list(dict.fromkeys(emails))  # deduplicate preserving order

            # Phones from text + tel links
            phones = re.findall(self.phone_regex, text)
            for a_tag in soup.find_all("a", href=True):
                if a_tag["href"].startswith("tel:"):
                    phone = a_tag["href"].replace("tel:", "").strip()
                    if phone:
                        phones.append(phone)

            # Company name
            company_name = ""
            og_name = soup.find("meta", property="og:site_name")
            if og_name and og_name.get("content"):
                company_name = og_name["content"].strip()
            elif soup.title and soup.title.string:
                title_text = soup.title.string.strip()
                for sep in [" | ", " - ", " – ", " — ", " :: ", " : "]:
                    if sep in title_text:
                        company_name = title_text.split(sep)[0].strip()
                        break
                if not company_name:
                    company_name = title_text[:60]

            # Try to find name-email associations from structured HTML
            name_email_pairs = []
            for container in soup.find_all(
                ["div", "li", "article", "section"],
                class_=re.compile(r"team|staff|member|person|profile|card|employee|director|partner", re.I),
            ):
                container_text = container.get_text(separator=" ", strip=True)
                container_emails = re.findall(
                    r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", container_text)
                name_matches = re.findall(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})\b", container_text)
                if container_emails and name_matches:
                    for ce in container_emails:
                        # V5.13: Apply name validation guard
                        if is_valid_email(ce) and _is_valid_person_name(name_matches[0]):
                            name_email_pairs.append({"name": name_matches[0], "email": ce})

            return {
                "emails": emails[:10], "phones": phones[:10],
                "company_name": company_name, "name_email_pairs": name_email_pairs[:10],
            }
        except Exception:
            return None


# ══════════════════════════════════════════════════════════════════════════════
# V5.13: WHOIS FOUNDER VERIFICATION
# ══════════════════════════════════════════════════════════════════════════════

class WhoisFounderClient:
    """V5.13: WHOIS registrant lookup for founder identification."""

    def __init__(self):
        self._session = requests.Session()

    def get_registrant_name(self, domain: str) -> str:
        """Scrape whois.com for registrant/admin contact name."""
        try:
            time.sleep(random.uniform(1.5, 3.0))
            url = f"https://www.whois.com/whois/{domain}"
            r = self._session.get(url, headers={"User-Agent": _get_random_ua()}, timeout=15)
            if r.status_code != 200:
                return ""
            text = r.text
            for pattern in [
                r"Registrant Name:\s*(.+)",
                r"Admin Name:\s*(.+)",
                r"Tech Name:\s*(.+)",
                r"Registrant Contact Name:\s*(.+)",
            ]:
                m = re.search(pattern, text, re.IGNORECASE)
                if m:
                    name = m.group(1).strip()
                    if any(w in name.lower() for w in [
                        "privacy", "redacted", "proxy", "domain", "whois",
                        "protected", "not disclosed", "n/a", "identity",
                        "registration", "private", "domains by",
                    ]):
                        continue
                    if _is_valid_person_name(name):
                        return name
            return ""
        except Exception:
            return ""

    @staticmethod
    def find_founder_in_leads(whois_name: str, leads: list):
        """Fuzzy-match WHOIS registrant name against existing leads."""
        if not whois_name or not leads:
            return None
        wn_lower = whois_name.lower().split()
        wn_first = wn_lower[0] if wn_lower else ""
        wn_last = wn_lower[-1] if len(wn_lower) > 1 else ""
        for ld in leads:
            ld_name = (ld.get("name") or "").lower().split()
            if not ld_name:
                continue
            ld_first = ld_name[0]
            ld_last = ld_name[-1] if len(ld_name) > 1 else ""
            if ld_first == wn_first and (ld_last == wn_last or wn_last in ld_last or ld_last in wn_last):
                return ld
        return None


# ══════════════════════════════════════════════════════════════════════════════
# V5.8: SMART LEAD RELEVANCE SCORING
# ══════════════════════════════════════════════════════════════════════════════

def _calculate_lead_relevance_score(title: str, has_email: bool = False) -> float:
    """V5.11: Trade-aware decision-maker relevance scoring.

    Score tiers:
    - 95  = HARD decision-maker (owner, CEO, director, founder) — always DM
    - 75  = SOFT decision-maker (manager, head) without trade context
    - 40  = Trade professional (plumber, electrician etc.) — industry worker, NOT DM
    - 20  = Support/admin staff — skip expensive enrichment
    - 30  = No title — neutral

    The trade-override rule: if title contains a TRADE_ROLE_WORD AND no HARD_DM keyword,
    the person is a tradesperson, not a decision maker.
    Example: "Lead Plumber" → score 40 (not DM); "Owner / Plumber" → score 95 (DM).
    """
    if not title:
        return 30

    title_lower = title.lower()

    # Tier 0: Low-relevance admin/support → skip enrichment immediately
    for keyword in LOW_RELEVANCE_KEYWORDS:
        if keyword in title_lower:
            return 20

    # Tier 1: HARD DM keywords → always a genuine decision maker
    for keyword in HARD_DM_KEYWORDS:
        if keyword in title_lower:
            return 95 + (5 if has_email else 0)

    # Check if title contains a trade/craft word
    is_trade_role = any(trade in title_lower for trade in TRADE_ROLE_WORDS)

    if is_trade_role:
        # Trade professional — this is an industry worker, NOT a decision maker
        # Even "Senior Plumber" or "Lead Electrician" is not a DM
        return 40

    # Tier 2: SOFT DM keywords (manager, head, supervisor) — valid DM only without trade context
    for keyword in SOFT_DM_KEYWORDS:
        if keyword in title_lower:
            return 75

    # Default: regular professional
    return 55


def _filter_people_by_relevance(people: list, max_leads: int) -> list:
    """V5.8: Filter and sort people by relevance to reduce API calls.

    - Calculate relevance score for each person
    - Keep top N*2 people (accounts for failures/partial data)
    - Skip low-relevance people entirely (don't make expensive API calls)

    Example: max_leads=20 → keep top ~40-50 people to account for incomplete data
    """
    if not max_leads or max_leads <= 0:
        return people  # No filtering if max_leads not set

    # Score each person
    scored = []
    for person in people:
        title = safe_str(person.get("title", ""))
        has_email = bool(person.get("personal_emails") or person.get("email"))
        score = _calculate_lead_relevance_score(title, has_email)
        # V5.11: Hard DM threshold — only enrich people scoring >= 55 (skip pure trade workers)
        scored.append((score, person))

    # Sort by score descending, keep top N*2.5 (with buffer for failures)
    scored.sort(key=lambda x: x[0], reverse=True)
    keep_count = max(10, int(max_leads * 2.5))  # Keep at least 10, up to 2.5x max_leads

    return [person for _, person in scored[:keep_count]]


# ══════════════════════════════════════════════════════════════════════════════
# LEAD GENERATION PIPELINE
# ══════════════════════════════════════════════════════════════════════════════


class LeadGenerationPipeline:
    """Orchestrates the complete 6-phase lead generation pipeline."""

    def __init__(
        self,
        industry: str,
        country: str,
        min_volume: int,
        min_cpc: float,
        output_folder: str,
        progress_callback=None,
        log_callback=None,
        max_leads: int = 0,
    ):
        self.industry = industry
        self.country = country
        self.min_volume = min_volume
        self.min_cpc = min_cpc
        self.output_folder = output_folder
        self.progress_callback = progress_callback or (lambda *a: None)
        self.log_callback = log_callback or (lambda *a: None)
        self.max_leads = max_leads
        self._cancelled = False
        self._phone_leads_count = 0      # V5.10: phone-bearing leads counter (credit gate)
        self._complete_leads_lock = threading.Lock()  # shared lock for both counters
        self._adwords_domains: set = set()   # V5.10+: domains confirmed directly via adwords
        self._organic_domains: set = set()   # V5.12: PAID-ONLY mode (empty set, no organic)
        self._organic_fallback_domains: set = set()  # V5.12: Fallback domains from Apollo org search (organic)
        self._email_credits_used = 0         # V5.10+: API calls that yielded personal email
        self._phone_credits_used = 0         # V5.10+: API calls that yielded direct phone
        self._log_lock = threading.Lock()  # V5.1: Thread-safe logging

        self.config = COUNTRY_CONFIG[country]

        # V5.7: Per-run API call counter
        self._api_counter = {"apollo": 0, "lusha": 0, "semrush": 0, "serpapi": 0, "openai": 0, "hunter": 0}

        # API clients
        self.semrush = SemrushClient(API_KEYS["semrush"])
        self.serpapi = SerpApiClient(API_KEYS["serpapi"])
        self.apollo = ApolloClient(API_KEYS["apollo"])
        self.lusha = LushaClient(API_KEYS["lusha"])
        self.hunter = HunterClient(API_KEYS.get("hunter", ""))  # V5.13: Hunter.io email enrichment
        self.openai_verifier = OpenAIEmailVerifier(API_KEYS.get("openai", ""))
        self.scraper = WebScraper(country)
        self.whois_client = WhoisFounderClient()  # V5.13: WHOIS founder verification
        self._competitor_domains_added = 0  # V5.13: Competitor expansion counter

        # Wire counter reference to each client
        self.semrush._counter = self._api_counter
        self.serpapi._counter = self._api_counter
        self.apollo._counter = self._api_counter
        self.lusha._counter = self._api_counter
        self.hunter._counter = self._api_counter
        self.openai_verifier._counter = self._api_counter

        # Data stores
        self.keywords: list[str] = []
        self.domains: list[str] = []
        self.leads: list[dict] = []

    def cancel(self):
        self._cancelled = True

    def _log(self, msg: str):
        timestamp = datetime.now().strftime("%H:%M:%S")
        try:
            self.log_callback(f"[{timestamp}] {msg}")
        except Exception as e:
            print(f"[LOG ERROR] {e}", flush=True)

    def _progress(self, pct: int, status: str = ""):
        try:
            self.progress_callback(pct, status)
        except Exception:
            pass  # Ignore progress callback errors

    def _has_enough_leads(self) -> bool:
        """V5.10: True when we have max_leads phone-bearing leads (credit gate).
        Phone is the expensive signal — Apollo/Lusha enrich_person reveals phone.
        Gate triggers at exactly max_leads phones (+ 20% buffer for cleanup losses).
        Returns False when max_leads=0 (unlimited mode).
        """
        if self.max_leads <= 0:
            return False
        with self._complete_leads_lock:
            # 1.5x buffer: e.g. max_leads=10 → stop at 15 phone leads (more buffer for cleanup losses)
            return self._phone_leads_count >= int(self.max_leads * 1.5)

    def run(self) -> str:
        """Execute the full pipeline. Returns path to output CSV."""
        try:
            self._phase1_seed_keywords()
            if self._cancelled:
                return ""
            self._phase2_semrush_expansion()
            if self._cancelled:
                return ""
            self._phase3_domain_discovery()
            if self._cancelled:
                return ""
            if not self.domains:
                self._log("No prospect domains found. Try different industry/settings.")
                return ""
            self._phase4_enrichment()
            if self._cancelled:
                return ""
            self._phase4b_targeted_completion()  # V5.16: fills phone for top N outside credit gate
            if self._cancelled:
                return ""
            self._phase5_cleanup()
            if self._cancelled:
                return ""
            self._phase5b_openai_verify()
            if self._cancelled:
                return ""
            return self._phase6_export()
        except Exception as e:
            self._log(f"Pipeline error: {e}")
            return ""

    # ── Phase 1: Seed Keywords ──────────────────────────────────────────────

    def _phase1_seed_keywords(self):
        self._progress(1, "Phase 1: Generating seed keywords...")
        self._log(f"[Phase 1] START: Generating seed keywords for '{self.industry}'")

        seeds = INDUSTRY_KEYWORDS.get(self.industry, [])
        if not seeds:
            base = self.industry.lower()
            seeds = [
                f"{base} near me", f"best {base}", f"{base} services",
                f"{base} {self.config['location_suffix']}",
                f"professional {base}", f"local {base}",
                f"affordable {base}", f"top {base}",
            ]

        self.keywords = seeds[:]
        self._log(f"   Generated {len(self.keywords)} seed keywords")

        # API key status check — surface missing keys early so failures are diagnosable
        missing_keys = [name.upper() for name, val in API_KEYS.items() if not val]
        if missing_keys:
            self._log(f"   WARNING: Missing API keys: {', '.join(missing_keys)}. Discovery will be limited.")
        if not API_KEYS.get("semrush"):
            self._log("   WARNING: No SEMRUSH_API_KEY — domain discovery will rely on SerpApi + Apollo fallback.")
        if not API_KEYS.get("serpapi") and not API_KEYS.get("semrush"):
            self._log("   WARNING: Both SEMRUSH_API_KEY and SERPAPI_API_KEY are missing — only Apollo org search available.")

        self._progress(5, f"{len(self.keywords)} seed keywords ready")

    # ── Phase 2: SEMrush Keyword Expansion ──────────────────────────────────

    def _phase2_semrush_expansion(self):
        self._progress(6, "Expanding keywords via SEMrush...")
        self._log("Phase 2: SEMrush keyword expansion")

        db = self.config["semrush_db"]
        expanded = set(self.keywords)
        seeds_to_expand = self.keywords[:12]

        for i, seed in enumerate(seeds_to_expand):
            if self._cancelled:
                return
            self._log(f"   Expanding: '{seed}'")
            results = self.semrush.get_related_keywords(seed, db, display_limit=25)

            added = 0
            for kw_data in results:
                kw = kw_data["keyword"]
                vol = kw_data["volume"]
                cpc = kw_data["cpc"]
                if vol >= self.min_volume and cpc >= self.min_cpc and kw not in expanded:
                    expanded.add(kw)
                    added += 1
                    if len(expanded) >= 80:
                        break

            self._log(f"   -> +{added} keywords (total: {len(expanded)})")
            pct = 6 + int((i + 1) / len(seeds_to_expand) * 14)
            self._progress(pct, f"Keyword expansion: {len(expanded)} keywords")
            if len(expanded) >= 80:
                break

        self.keywords = list(expanded)
        self._log(f"   Total unique keywords: {len(self.keywords)}")
        self._progress(20, f"{len(self.keywords)} keywords ready for search")

    # ── Phase 3: Domain Discovery ───────────────────────────────────────────

    def _phase3_domain_discovery(self):
        self._progress(21, "Discovering business domains...")
        self._log("Phase 3: Domain discovery via SEMrush + SerpApi (V5.10: paid-traffic filter)")

        db = self.config["semrush_db"]
        gl = self.config["serpapi_gl"]

        # V5.17: Calculate optimal domain cap based on max_leads — wider net = more phone-bearing leads
        if self.max_leads > 0:
            optimal_domain_cap = max(50, int(self.max_leads * 5))
            self._log(f"   V5.17 Smart mode: max_leads={self.max_leads}, domain cap={optimal_domain_cap}")
        else:
            optimal_domain_cap = 200
            self._log("   V5.17: max_leads not set, using standard domain discovery (200 cap)")

        # V5.10: Separate paid-confirmed (from adwords endpoint) from organic/SERP candidates
        paid_domains: set = set()   # confirmed running paid ads
        organic_candidates: set = set()  # from organic search / SerpApi — need paid check

        keywords_to_search = self.keywords[:30]
        total_steps = len(keywords_to_search)

        for i, kw in enumerate(keywords_to_search):
            if self._cancelled:
                return

            if not self.serpapi._available and i % 5 == 0:
                self.serpapi._available = True

            # V5.12: PAID-ONLY MODE — Only fetch domains with active Google Ads (paid_traffic != 0)
            # Removed: get_organic_domains() and organic verification entirely
            ad_results = self.semrush.get_adwords_domains(kw, db, limit=15)  # Increased limit to 15 for paid only
            for r in ad_results:
                d = r["domain"]
                paid_domains.add(d)

            if (i + 1) % 5 == 0 or i == total_steps - 1:
                self._log(f"   Searched {i + 1}/{total_steps} keywords -> "
                          f"{len(paid_domains)} PAID (Google Ads) domains found")

            pct = 21 + int((i + 1) / total_steps * 20)
            self._progress(pct, f"Found {len(paid_domains)} paid domains")

            if len(paid_domains) >= optimal_domain_cap:
                self._log(f"   Reached paid domain cap ({optimal_domain_cap}). Stopping search.")
                break

        # V5.13: Competitor expansion — for top paid domains, find competitors via SEMrush
        competitor_domains_added = 0
        if len(paid_domains) >= 5:
            sample = list(paid_domains)[:20]
            comp_calls = 0
            for seed_domain in sample:
                if comp_calls >= 40 or self._cancelled:
                    break
                competitors = self.semrush.get_domain_competitors(seed_domain, db, limit=3)
                comp_calls += 1
                for cd in competitors:
                    if cd not in paid_domains and not is_platform_domain(cd):
                        paid_domains.add(cd)
                        competitor_domains_added += 1
            if competitor_domains_added:
                self._log(f"   V5.13: Competitor expansion added {competitor_domains_added} new domains")
        self._competitor_domains_added = competitor_domains_added

        # V5.12: All domains are PAID — no organic candidates
        self._adwords_domains = set(paid_domains)
        self._organic_domains = set()  # V5.12: Empty (paid-only mode)

        pct_after = 41
        self._progress(pct_after, f"Paid traffic check done: {len(paid_domains)} qualified domains")

        # V5.15: Always collect organic domains alongside paid — no longer a fallback-only
        # Organic leads appear with Traffic Source = "Organic" in CSV
        if len(paid_domains) == 0:
            self._log("   SEMrush returned 0 paid domains. Checking organic sources...")
        organic_supp: set = set()
        if self.serpapi._available:
            for kw in self.keywords[:15]:
                if len(organic_supp) >= 20:
                    break
                serp_domains = self.serpapi.search_keyword(
                    f"{kw} {self.config['location_suffix']}", gl, num=10
                )
                for d in serp_domains:
                    if d not in paid_domains and not is_platform_domain(d):
                        organic_supp.add(d)
            if organic_supp:
                self._organic_domains = organic_supp
                self._log(f"   V5.15 Organic: {len(organic_supp)} organic domains added (Traffic Source = Organic)")
        self.domains = list(paid_domains)[:optimal_domain_cap] + list(organic_supp)[:20]

        self._organic_fallback_domains = set()  # kept for compat
        self._log(f"   Total prospect domains: {len(self.domains)} ({len(paid_domains)} paid)")
        self._progress(45, f"{len(self.domains)} domains ready for enrichment")

    # ── V5.2: Email-based name inference utility ────────────────────────────

    @staticmethod
    def _infer_name_from_email(lead: dict) -> str:
        """V5.3: Try to infer the last name from a personal email address.
        Handles abbreviated names (matt→matthew, chris→christopher) via _NAME_ABBREVIATIONS.
        e.g. matt@matthewcornell.com.au → domain=matthewcornell → "Matthew Cornell"
             sarah.jones@company.com   → local=sarah.jones → "Sarah Jones"
             chris@christopherbrown.com → "Christopher Brown"
        Returns the full name string if inferred, else empty string.
        """
        email = lead.get("email", "")
        name = lead.get("name", "")
        if not email or not name or " " in name:
            return ""
        first = name.lower()
        local = email.lower().split("@")[0]
        email_domain = email.lower().split("@")[1] if "@" in email else ""
        variants = _get_name_variants(first)

        # Pattern 1: local part is "first.last" or "variant.last"
        if "." in local:
            parts = local.split(".")
            if parts[0] in variants and len(parts) > 1 and len(parts[1]) > 1:
                best_first = parts[0].title()  # Use the form from the email
                return f"{best_first} {parts[1].title()}"

        # Pattern 2: local part is "firstlast" or "variantlast" (concatenated)
        for variant in variants:    
            if local.startswith(variant) and len(local) > len(variant) + 1:
                suffix = local[len(variant):]
                if suffix.isalpha() and len(suffix) >= 2:
                    return f"{variant.title()} {suffix.title()}"

        # Pattern 3: email domain contains name (e.g. matthewcornell.com.au)
        clean_domain = email_domain
        for tld in [".com.au", ".co.uk", ".org.au", ".net.au", ".co.nz",
                    ".com", ".org", ".net", ".io", ".co", ".au", ".uk"]:
            if clean_domain.endswith(tld):
                clean_domain = clean_domain[:-len(tld)]
                break
        for variant in variants:
            if clean_domain.startswith(variant) and len(clean_domain) > len(variant) + 1:
                suffix = clean_domain[len(variant):]
                if suffix.isalpha() and 2 <= len(suffix) <= 15:
                    return f"{variant.title()} {suffix.title()}"

        return ""

    # ── Phase 4: Lead Enrichment ────────────────────────────────────────────

    # ── V5.2: Skip-if-complete helper (requires FULL two-word name) ──
    @staticmethod
    def _lead_is_complete(lead):
        """V5.11: A lead is complete when it has FULL name + TRUE personal email (gmail/yahoo) + phone.
        Work emails (firstname@company.com) do NOT stop enrichment — we keep trying for a real
        personal email. Generic emails (info@, contact@) also don't stop enrichment.
        """
        name = lead.get("name", "")
        has_full_name = bool(name) and " " in name
        email = lead.get("email", "")
        has_real_personal_email = bool(email) and is_personal_email(email)  # gmail/yahoo only
        has_phone = bool(lead.get("phone"))
        return has_full_name and has_real_personal_email and has_phone

    def _scrape_contact_pages(self, domain: str) -> str:
        """V5.16: Scrape additional contact/about pages for phone numbers beyond what scrape_domain() tries."""
        extra_paths = ["/contact-us", "/about-us", "/find-us", "/get-in-touch", "/locations", "/our-team"]
        for path in extra_paths:
            url = f"https://{domain}{path}"
            try:
                page_data = self.scraper._scrape_page(url)
                if page_data and page_data.get("phones"):
                    phone = format_phone(page_data["phones"][0], self.country)
                    if phone:
                        return phone
            except Exception:
                continue
        return ""

    def _enrich_single_domain(self, domain, index, total):
        """V5.1: Enrich a single domain. Returns list of leads for this domain.
        V5.8: Smart filtering to reduce API calls for low-relevance leads.
        V5.9: Credit gate — skips domain entirely when enough complete leads collected.
        Thread-safe: does not mutate self.leads, returns results instead."""
        if self._cancelled:
            return []

        # V5.9: Credit gate — if we already have enough complete leads, skip this domain
        if self._has_enough_leads():
            self._log(f"   [{index + 1}/{total}] {domain}: skipped (credit quota reached)")
            return []

        domain_leads = []
        company_name = ""
        company_phone = ""

        # V5.13: Snapshot API counter for per-domain credit tracking
        _counter_snapshot = {k: v for k, v in self._api_counter.items()}

        # Step 1: Apollo organization enrichment — get company name + phone
        org_data = self.apollo.enrich_organization(domain)
        if org_data:
            company_name = org_data.get("company_name", "")
            company_phone = org_data.get("phone", "")
            # V5.10: Skip large enterprises (>500 employees) — not SMB targets
            employees = org_data.get("employees") or 0
            try:
                employees = int(employees)
            except (ValueError, TypeError):
                employees = 0
            if employees > 500:
                self._log(f"   [{index + 1}/{total}] {domain}: skipped (large company, {employees} employees)")
                return []
            if company_name:
                self._log(f"   [{index + 1}/{total}] {company_name} ({domain})")
            else:
                self._log(f"   [{index + 1}/{total}] {domain}")
        else:
            self._log(f"   [{index + 1}/{total}] {domain}")

        # V5.13: SEMrush traffic metrics for this domain
        db = self.config["semrush_db"]
        traffic_metrics = self.semrush.get_domain_traffic_metrics(domain, db)

        # Step 2: Apollo people search — get names and roles (V5.18: per_page=25 for wider coverage)
        people = self.apollo.search_people_by_domain(domain, per_page=25)

        # V5.8: Smart filtering — reduce people list by relevance before expensive enrichment
        # This prevents wasting credits on low-relevance leads (interns, support staff, etc.)
        original_count = len(people)
        if self.max_leads > 0 and len(people) > 10:
            people = _filter_people_by_relevance(people, self.max_leads)
            self._log(f"   V5.8: Filtered {original_count} people → {len(people)} high-relevance (max_leads={self.max_leads})")
        for person in people:
            first = safe_str(person.get("first_name"))
            # V5.13 Bug Fix: Reject company names / possessives used as first name
            if first and ("'" in first or any(c.isdigit() for c in first) or len(first) < 2):
                first = ""
            last = safe_str(person.get("last_name"))
            title = safe_str(person.get("title"))
            # V5.20: Smart email selection — business-tagged > personal, with name check
            email, email_from_apollo_personal_list = _pick_best_email_from_apollo(
                person, first, last
            )
            # Store generic org email as fallback (will be used only if no personal found)
            org_email = safe_str(person.get("email"))
            generic_email = org_email if (org_email and not is_personal_email(org_email)) else ""
            # V5.25: Smart phone selection — pass company_phone to exclude HQ numbers
            person_phone, phone_quality = _pick_best_phone_from_apollo(person, company_phone)
            if first:
                full_name = get_full_name(person) or (f"{first} {last}".strip() if last else first)
                # V5.18: Only reject if last name looks like a CLEAR title (multi-word role like
                # "managing director") — trust Apollo's last_name field for single-word surnames.
                # Single words like "Director" as a last name are extremely rare and rejecting hurts
                # coverage more than it prevents errors.
                if last and " " in full_name:
                    _last_lower = last.lower()
                    _multi_word_roles = {kw for kw in _NAME_FORBIDDEN_WORDS if " " in kw}
                    _single_bad = {"ceo", "cfo", "cto", "coo", "managing", "executive"}
                    if _last_lower in _multi_word_roles or _last_lower in _single_bad:
                        full_name = first  # Only fall back for obvious title words

                # V5.19: Strip Apollo's obfuscated "FirstName I." format (e.g. "Matt M." → "Matt")
                # Apollo returns this for locked/unrevealed contacts. Stripping the initial ensures
                # all downstream name-resolution guards (which check " " not in name) trigger correctly.
                if _is_obfuscated_name(full_name):
                    full_name = full_name.split()[0]

                # V5.3: Also check Apollo's `name` field directly (may have full name)
                apollo_name = safe_str(person.get("name"))
                if apollo_name and " " in apollo_name and " " not in full_name:
                    # V5.19: Skip if the apollo name itself is obfuscated ("Matt M." format)
                    if not _is_obfuscated_name(apollo_name):
                        # V5.13 Bug Fix: Reject if any word after first is a title/role word
                        _role_words = HARD_DM_KEYWORDS | SOFT_DM_KEYWORDS | TRADE_ROLE_WORDS
                        _name_words = apollo_name.lower().split()
                        if not any(w in _role_words for w in _name_words[1:]):
                            full_name = apollo_name

                # V5.3: Extract LinkedIn URL for later name resolution
                person_linkedin = safe_str(person.get("linkedin_url"))
                lead_company = company_name or safe_str((person.get("organization") or {}).get("name"))

                # V5.9: LinkedIn URL name resolution (reliable — Apollo actual profile URL)
                if " " not in full_name and person_linkedin:
                    resolved = _extract_name_from_linkedin_url(first, person_linkedin)
                    if resolved and " " in resolved:
                        full_name = resolved

                # V5.13: Re-enable company + domain name extraction
                # _extract_name_from_company/_extract_name_from_domain have built-in
                # business-suffix guards — they only match when name is clearly encoded
                if " " not in full_name and first and lead_company:
                    resolved = _extract_name_from_company(first, lead_company)
                    if resolved and " " in resolved:
                        full_name = resolved

                if " " not in full_name and first and domain:
                    resolved = _extract_name_from_domain(first, domain)
                    if resolved and " " in resolved:
                        full_name = resolved

                lead = {
                    "name": full_name,
                    "domain": domain,
                    "company": lead_company,
                    "role": title,
                    "email": email or "",
                    "phone": person_phone or "",
                    "source": "Apollo",
                    "_generic_email": generic_email,  # internal: fallback only
                    "_needs_full_name": " " not in full_name,  # V5.2: track single-name leads
                    "_linkedin_url": person_linkedin,  # V5.3: store for later resolution
                    "_apollo_id": safe_str(person.get("id")),  # V5.19: exact record ID for people/match
                    "_email_verified": email_from_apollo_personal_list,  # V5.10: Apollo-authoritative personal email
                    "_direct_phone": bool(person_phone) and phone_quality >= 30,  # V5.22: True only for mobile/direct/personal/home (not company HQ)
                    "_phone_quality": phone_quality if person_phone else 0,  # V5.22: track quality for comparison
                    "_company_phone": company_phone,  # V5.26: store for async phone collection filtering
                    "_domain_source": "paid" if domain in self._adwords_domains else ("organic" if domain in self._organic_domains else "paid"),  # V5.13
                    "_paid_keywords": traffic_metrics.get("paid_keywords", 0),      # V5.13
                    "_organic_keywords": traffic_metrics.get("organic_keywords", 0),  # V5.13
                    "_paid_traffic": traffic_metrics.get("paid_traffic", 0),          # V5.13
                    "_organic_traffic": traffic_metrics.get("organic_traffic", 0),    # V5.13
                }
                domain_leads.append(lead)

        # Step 2a.5: V5.13 — Multi-source pre-enrichment for single-name leads
        # Runs BEFORE Apollo enrich_person (Step 2b) to resolve first-only names.
        # Layer A: SerpAPI LinkedIn query (fast, no export credits)
        # Layer B: Apollo people/search by first_name + domain (gets LinkedIn URL + last name)
        # V5.14: Removed _has_enough_leads() gate — name resolution uses no Apollo export credits
        #        and must run for ALL single-name leads regardless of quota state.
        for ld in domain_leads:
            name_str = ld.get("name", "")
            if not name_str or " " in name_str or ld.get("_linkedin_url"):
                continue  # skip: already has full name or LinkedIn URL

            co_name = ld.get("company") or company_name or domain

            # Layer A: SerpAPI LinkedIn-targeted name lookup (no export credits used)
            if self.serpapi._available:
                linkedin_name = self.serpapi.find_person_on_linkedin(name_str, co_name)
                if linkedin_name and " " in linkedin_name:
                    ld["name"] = linkedin_name
                    ld["_needs_full_name"] = False
                    ld["source"] += "+SerpLI"
                    self._log(f"   [Name] SerpAPI LinkedIn → '{name_str}' resolved to '{linkedin_name}'")
                    continue  # Name resolved, skip Layer B

            # Layer B: Apollo people/search by first_name + domain (no export credits)
            # V5.14: Also searches without email_status filter so unverified contacts are included
            try:
                search_url = f"{self.apollo.BASE_URL}/mixed_people/api_search"
                payload = {
                    "q_person_name": name_str,
                    "q_organization_domains": [domain],
                    "per_page": 5,
                }
                resp = requests.post(
                    search_url, json=payload,
                    headers=self.apollo._headers(), timeout=20
                )
                if resp.status_code == 200:
                    people_results = resp.json().get("people", [])
                    for candidate in people_results:
                        c_first = safe_str(candidate.get("first_name")).lower().strip()
                        if c_first == name_str.lower():
                            c_last = safe_str(candidate.get("last_name")).strip()
                            c_linkedin = safe_str(candidate.get("linkedin_url")).strip()
                            c_apollo_id = safe_str(candidate.get("id"))
                            # V5.19: Always save Apollo ID — enables exact people/match in Step 2b
                            if c_apollo_id and not ld.get("_apollo_id"):
                                ld["_apollo_id"] = c_apollo_id
                            # V5.19: Reject obfuscated last names (single letter like "M." or "L.")
                            last_clean = c_last.rstrip(".")
                            is_obfuscated_last = len(last_clean) == 1 and last_clean.isalpha()
                            if c_last and not is_obfuscated_last and _is_valid_person_name(f"{name_str} {c_last}"):
                                ld["name"] = f"{name_str} {c_last}"
                                ld["_needs_full_name"] = False
                                ld["source"] += "+ApolloSearch"
                                self._log(f"   [Name] Apollo search → '{name_str}' resolved to '{ld['name']}'")
                            if c_linkedin:
                                ld["_linkedin_url"] = c_linkedin
                            break
            except Exception:
                pass

        # Step 2b: V5.7 — Apollo enrich for leads missing personal email
        # V5.18: Phone quota no longer blocks name/email resolution within a domain.
        # Only skip a lead if quota is reached AND it already has full name + any email.
        # This ensures single-name leads and leads missing personal email still get enriched.
        for ld in domain_leads:
            if self._has_enough_leads():
                # At quota — only skip if lead has a full name AND already has some email
                has_full_name = bool(ld.get("name")) and " " in ld.get("name", "")
                has_any_email = bool(ld.get("email"))
                if has_full_name and has_any_email:
                    continue  # Lead is reasonably complete — skip at quota
                # Fall through: still resolve name or find first email even at quota
            if self._lead_is_complete(ld):
                continue

            # V5.8: Skip enrichment for low-relevance leads when max_leads is set (credit saving)
            if self.max_leads > 0:
                role = ld.get("role", "").lower()
                is_low_relevance = any(kw in role for kw in LOW_RELEVANCE_KEYWORDS)
                if is_low_relevance:
                    continue  # Skip expensive enrichment for interns, support staff, etc.

            name = ld.get("name", "")
            needs_name = name and " " not in name
            needs_email = not ld.get("email") or not is_personal_email(ld.get("email", ""))
            if needs_name or needs_email:
                parts = name.split() if name else [""]
                first_n = parts[0] if parts else ""
                last_n = parts[-1] if len(parts) > 1 else ""
                linkedin_url = ld.get("_linkedin_url", "")
                # V5.7: Call for ALL leads with at least a first name
                if first_n:
                    enriched = self.apollo.enrich_person(
                        first_n, last_n, domain, linkedin_url,
                        organization_name=company_name,  # V5.10+: improves Apollo matching
                        apollo_id=ld.get("_apollo_id", ""),  # V5.19: exact record lookup
                        company_phone=company_phone,  # V5.25: exclude company HQ phone
                    )
                    if enriched:
                        # V5.18: Always update name from Apollo match when better data available
                        enriched_name = enriched.get("name", "")
                        # V5.19: Never accept an obfuscated name ("Matt M.") from people/match
                        if enriched_name and _is_obfuscated_name(enriched_name):
                            enriched_name = enriched_name.split()[0]  # Strip initial, keep first name only
                        if enriched_name and " " in enriched_name:
                            if not (ld.get("name") and " " in ld.get("name", "")):
                                # Single-name lead — upgrade to full name
                                ld["name"] = enriched_name
                                ld["_needs_full_name"] = False
                                ld["source"] += "+ApolloName"
                        _enriched_email = enriched.get("email", "")
                        if _enriched_email:
                            _current_email = ld.get("email", "")
                            _enriched_is_consumer = is_personal_email(_enriched_email)
                            _current_is_consumer = is_personal_email(_current_email) if _current_email else False
                            _current_is_business = bool(_current_email) and not _current_is_consumer
                            _current_local = _current_email.split("@")[0].lower() if _current_email else ""
                            _current_is_generic = _current_local in GENERIC_EMAIL_PREFIXES

                            if not _current_email:
                                # No email yet — use whatever enrichment returned
                                ld["email"] = _enriched_email
                                ld["source"] += "+ApolloEmail"
                                if not ld.get("_email_verified"):
                                    ld["_email_verified"] = True
                                    with self._complete_leads_lock:
                                        self._email_credits_used += 1
                            elif not _enriched_is_consumer and _current_is_consumer:
                                # V5.22: Enriched has BUSINESS email, current has consumer — upgrade
                                ld["email"] = _enriched_email
                                ld["source"] += "+ApolloEmail"
                                if not ld.get("_email_verified"):
                                    ld["_email_verified"] = True
                                    with self._complete_leads_lock:
                                        self._email_credits_used += 1
                            elif not _enriched_is_consumer and _current_is_generic:
                                # V5.22: Enriched has business email, current is generic inbox — upgrade
                                ld["email"] = _enriched_email
                                ld["source"] += "+ApolloEmail"
                            elif not _enriched_is_consumer and not _current_is_business:
                                # V5.19: Prefer name-based work email over generic inbox email
                                _local = _enriched_email.split("@")[0].lower()
                                _is_name_based = first_n and first_n.lower() in _local
                                if _is_name_based:
                                    ld["email"] = _enriched_email
                                    ld["source"] += "+ApolloEmail"
                            # V5.22: Never replace a business email with a consumer (gmail/hotmail) email
                        if not ld.get("role") and enriched.get("role"):
                            ld["role"] = enriched["role"]
                        # V5.26: Store apollo_id from enrichment for async phone collection
                        if enriched.get("_apollo_id") and not ld.get("_apollo_id"):
                            ld["_apollo_id"] = enriched["_apollo_id"]
                        # V5.22: Replace phone only if enriched phone has higher quality score
                        if enriched.get("phone"):
                            new_quality = enriched.get("_phone_quality", 20)
                            existing_quality = ld.get("_phone_quality", 0)
                            if not ld.get("phone") or new_quality > existing_quality:
                                ld["phone"] = enriched["phone"]
                                ld["_direct_phone"] = new_quality >= 30
                                ld["_phone_quality"] = new_quality
                                with self._complete_leads_lock:
                                    self._phone_credits_used += 1  # V5.10+

        # Step 2c: V5.6 — LinkedIn-URL-targeted enrichment for remaining single-name leads
        # V5.18: Same quota logic as 2b — still resolve names/emails even at quota.
        for ld in domain_leads:
            if self._has_enough_leads():
                has_full_name = bool(ld.get("name")) and " " in ld.get("name", "")
                has_any_email = bool(ld.get("email"))
                if has_full_name and has_any_email:
                    continue
            if self._lead_is_complete(ld):
                continue

            # V5.8: Skip enrichment for low-relevance leads when max_leads is set
            if self.max_leads > 0:
                role = ld.get("role", "").lower()
                is_low_relevance = any(kw in role for kw in LOW_RELEVANCE_KEYWORDS)
                if is_low_relevance:
                    continue  # Skip expensive enrichment

            name = ld.get("name", "")
            linkedin_url = ld.get("_linkedin_url", "")
            if not linkedin_url:
                continue  # No LinkedIn URL = can't do precise match
            if name and " " in name and ld.get("email") and is_personal_email(ld["email"]):
                continue  # Already fully enriched
            first_n = name.split()[0] if name else ""
            last_n = name.split()[-1] if name and " " in name else ""
            enriched = self.apollo.enrich_person(
                first_n, last_n, domain, linkedin_url,
                organization_name=company_name,  # V5.10+
                apollo_id=ld.get("_apollo_id", ""),  # V5.19: exact record lookup
                company_phone=company_phone,  # V5.25: exclude company HQ phone
            )
            if enriched:
                # V5.18: Update name from LinkedIn-matched Apollo record
                if enriched.get("name") and " " in enriched["name"] and " " not in ld.get("name", ""):
                    ld["name"] = enriched["name"]
                    ld["_needs_full_name"] = False
                    ld["source"] += "+ApolloLI"
                _2c_email = enriched.get("email", "")
                if _2c_email:
                    _2c_current = ld.get("email", "")
                    _2c_enriched_is_consumer = is_personal_email(_2c_email)
                    _2c_current_is_consumer = is_personal_email(_2c_current) if _2c_current else False
                    _2c_current_is_business = bool(_2c_current) and not _2c_current_is_consumer
                    if not _2c_current:
                        ld["email"] = _2c_email
                        if not ld.get("_email_verified"):
                            ld["_email_verified"] = True
                            with self._complete_leads_lock:
                                self._email_credits_used += 1
                    elif not _2c_enriched_is_consumer and _2c_current_is_consumer:
                        # V5.22: Business email upgrades consumer email
                        ld["email"] = _2c_email
                        if not ld.get("_email_verified"):
                            ld["_email_verified"] = True
                            with self._complete_leads_lock:
                                self._email_credits_used += 1
                    # V5.22: Never replace business with consumer email
                # V5.26: Store apollo_id from enrichment for async phone collection
                if enriched.get("_apollo_id") and not ld.get("_apollo_id"):
                    ld["_apollo_id"] = enriched["_apollo_id"]
                # V5.22: Replace phone only if enriched phone has higher quality score
                if enriched.get("phone"):
                    new_quality = enriched.get("_phone_quality", 20)
                    existing_quality = ld.get("_phone_quality", 0)
                    if not ld.get("phone") or new_quality > existing_quality:
                        ld["phone"] = enriched["phone"]
                        ld["_direct_phone"] = new_quality >= 30
                        ld["_phone_quality"] = new_quality
                        with self._complete_leads_lock:
                            self._phone_credits_used += 1  # V5.10+

        # Step 2d removed (V5.9): Email pattern inference was generating fake emails
        # (firstname.lastname@domain fabrications). Only real emails from Apollo/Lusha/scraping kept.

        # Step 2e: V5.10+ — Personal email second pass via Apollo email-status filter
        # V5.18: Removed phone-quota gate — always run this pass. Increased per_page to 10.
        # Threshold raised: run until we have at least max_leads personal emails (or 3 minimum).
        personal_so_far = sum(
            1 for ld in domain_leads if is_personal_email(ld.get("email", ""))
        )
        personal_target = max(3, self.max_leads) if self.max_leads > 0 else 5
        if personal_so_far < personal_target:
            email_candidates = self.apollo.search_email_verified_people(domain, per_page=10)
            for person in email_candidates:
                # V5.18: No quota break — process all candidates for personal emails
                first = safe_str(person.get("first_name"))
                if not first:
                    continue
                last = safe_str(person.get("last_name"))
                li_url = safe_str(person.get("linkedin_url"))
                # Find existing lead for this person
                matching_lead = None
                for ld in domain_leads:
                    ld_first = (ld.get("name") or "").split()[0].lower()
                    if ld_first and ld_first == first.lower():
                        matching_lead = ld
                        break
                if matching_lead and is_personal_email(matching_lead.get("email", "")):
                    continue  # already has personal email
                # V5.22: Skip if matching lead already has a business email (don't downgrade to consumer)
                if matching_lead and matching_lead.get("email") and not is_personal_email(matching_lead.get("email", "")):
                    _ml_local = matching_lead["email"].split("@")[0].lower()
                    if _ml_local not in GENERIC_EMAIL_PREFIXES:
                        continue  # Has a business email — skip consumer email search
                # Run targeted enrich_person for this Apollo-verified-email person
                enriched = self.apollo.enrich_person(
                    first, last, domain, li_url, organization_name=company_name
                )
                if enriched and is_personal_email(enriched.get("email", "")):
                    personal_email = enriched["email"]
                    if matching_lead:
                        _ml_current = matching_lead.get("email", "")
                        _ml_is_business = bool(_ml_current) and not is_personal_email(_ml_current)
                        _ml_local = _ml_current.split("@")[0].lower() if _ml_current else ""
                        _ml_is_generic = _ml_local in GENERIC_EMAIL_PREFIXES
                        # V5.22: Only use consumer email if no business email exists OR current is generic prefix
                        if not _ml_current or _ml_is_generic:
                            matching_lead["email"] = personal_email
                            matching_lead["_email_verified"] = True
                            matching_lead["source"] += "+Apollo2E"
                            with self._complete_leads_lock:
                                self._email_credits_used += 1
                    else:
                        # New person Apollo knows about — add as a lead
                        title = safe_str(person.get("title"))
                        new_ld = {
                            "name": enriched.get("name") or f"{first} {last}".strip(),
                            "domain": domain,
                            "company": company_name or domain_to_company_name(domain),
                            "role": enriched.get("role") or title,
                            "email": personal_email,
                            "phone": enriched.get("phone") or "",
                            "source": "Apollo+Apollo2E",
                            "_email_verified": True,
                            "_direct_phone": bool(enriched.get("phone")) and enriched.get("_phone_quality", 0) >= 30,
                            "_phone_quality": enriched.get("_phone_quality", 0),
                            "_domain_source": "paid" if domain in self._adwords_domains else ("organic" if domain in self._organic_domains else "paid"),  # V5.13
                            "_needs_full_name": False,
                            "_linkedin_url": li_url,
                            "_generic_email": "",
                        }
                        domain_leads.append(new_ld)
                        with self._complete_leads_lock:
                            self._email_credits_used += 1
            self._log(f"   V5.18 Step2e: {sum(1 for ld in domain_leads if is_personal_email(ld.get('email', '')))} "
                      f"personal emails after email-status pass")

        # Step 3: Lusha company data — V5: ALWAYS call Lusha for company info
        # V5.8: Skip if no high-relevance leads found (credit optimization)
        has_high_relevance_leads = any(
            not any(kw in ld.get("role", "").lower() for kw in LOW_RELEVANCE_KEYWORDS)
            for ld in domain_leads
        )
        if self.max_leads > 0 and not has_high_relevance_leads:
            # No high-relevance leads found, skip Lusha call to save credits
            pass
        else:
            lusha_company = self.lusha.get_company_info(domain)
            if lusha_company:
                lusha_co_name = lusha_company.get("company_name", "")
                if lusha_co_name:
                    company_name = lusha_co_name
                    for ld in domain_leads:
                        if not ld.get("company"):
                            ld["company"] = lusha_co_name
                            ld["source"] += "+Lusha"

        # V5.24: Deduplication guard — shared phones are company-level, not personal
        # If 2+ leads from this domain have the same phone, strip it and let Lusha fill.
        _phone_freq: dict = {}
        for ld in domain_leads:
            p = ld.get("phone", "")
            if p:
                _phone_freq[p] = _phone_freq.get(p, 0) + 1
        for ld in domain_leads:
            p = ld.get("phone", "")
            if p and _phone_freq.get(p, 0) >= 2:
                ld["_dedup_stripped_phone"] = p  # V5.24: remember what was stripped
                ld["phone"] = ""
                ld["_phone_quality"] = 0
                ld["_direct_phone"] = False

        # Step 4: Lusha person enrichment — try for leads with a name
        # V5.18: Same quota logic — still enrich single-name leads and email-less leads at quota.
        for ld in domain_leads:
            if self._has_enough_leads():
                has_full_name = bool(ld.get("name")) and " " in ld.get("name", "")
                has_any_email = bool(ld.get("email"))
                if has_full_name and has_any_email:
                    continue
            if self._lead_is_complete(ld):  # V5.1: Skip if already complete
                continue
            if not ld.get("name"):
                continue

            # V5.8: Skip Lusha enrichment for low-relevance leads when max_leads is set (major credit save)
            if self.max_leads > 0:
                role = ld.get("role", "").lower()
                is_low_relevance = any(kw in role for kw in LOW_RELEVANCE_KEYWORDS)
                if is_low_relevance:
                    continue  # Skip expensive Lusha enrichment for non-decision-makers

            parts = ld["name"].split()
            first_n = parts[0]
            last_n = parts[-1] if len(parts) > 1 else ""
            lusha_person = self.lusha.enrich_person(first_n, last_n, domain)
            if lusha_person:
                lusha_name = lusha_person.get("name", "")
                if lusha_name and " " in lusha_name:
                    if " " not in ld.get("name", ""):
                        ld["name"] = lusha_name
                lusha_email = lusha_person.get("email", "")
                if lusha_email:
                    _l4_current = ld.get("email", "")
                    _l4_lusha_is_consumer = is_personal_email(lusha_email)
                    _l4_current_is_consumer = is_personal_email(_l4_current) if _l4_current else False
                    _l4_current_is_business = bool(_l4_current) and not _l4_current_is_consumer
                    _l4_current_local = _l4_current.split("@")[0].lower() if _l4_current else ""
                    _l4_current_is_generic = _l4_current_local in GENERIC_EMAIL_PREFIXES

                    if not _l4_current:
                        # No email yet — use Lusha email
                        ld["email"] = lusha_email
                        if not _l4_lusha_is_consumer:
                            ld["_email_verified"] = True
                    elif not _l4_lusha_is_consumer and (_l4_current_is_consumer or _l4_current_is_generic):
                        # V5.22: Lusha has BUSINESS email, current has consumer/generic — upgrade
                        ld["email"] = lusha_email
                        ld["_email_verified"] = True
                        with self._complete_leads_lock:
                            self._email_credits_used += 1  # V5.10+
                    elif _l4_lusha_is_consumer and not _l4_current_is_business and not _l4_current:
                        # Lusha has consumer, no current business email
                        ld["email"] = lusha_email
                        ld["_email_verified"] = True
                        with self._complete_leads_lock:
                            self._email_credits_used += 1
                    # V5.22: Never replace business email with consumer (gmail/hotmail) email
                # V5.22: Replace phone using quality comparison — prefer higher quality (mobile over HQ)
                if lusha_person.get("phone"):
                    lusha_quality = lusha_person.get("_phone_quality", 50)  # default 50 (mobile) if not tracked
                    existing_quality = ld.get("_phone_quality", 0)
                    if not ld.get("phone") or lusha_quality > existing_quality or (lusha_quality >= 30 and existing_quality < 30):
                        ld["phone"] = lusha_person["phone"]
                        ld["_direct_phone"] = lusha_quality >= 30
                        ld["_phone_quality"] = lusha_quality
                        with self._complete_leads_lock:
                            self._phone_credits_used += 1  # V5.10+
                if not ld.get("role") and lusha_person.get("role"):
                    ld["role"] = lusha_person["role"]
                ld["source"] += "+Lusha"

        # Step 4b: V5.2 — SerpApi full-name fallback for remaining single-name leads
        for ld in domain_leads:
            if ld.get("name") and " " in ld["name"]:
                continue  # already has full name
            first_only = ld.get("name", "")
            if not first_only:
                continue
            co = ld.get("company") or company_name or domain
            full_name = self.serpapi.find_person_full_name(
                first_only, co, domain, self.config["serpapi_gl"]
            )
            if full_name and " " in full_name:
                ld["name"] = full_name
                ld["_needs_full_name"] = False
                ld["source"] += "+SerpApiName"

        # Step 4b.5: V5.13 — DuckDuckGo instant-answer fallback for still-single-name leads
        for ld in domain_leads:
            if ld.get("name") and " " in ld["name"]:
                continue  # already has full name
            first_only = ld.get("name", "")
            if not first_only:
                continue
            co_name = ld.get("company") or company_name or domain
            try:
                ddg_resp = requests.get(
                    "https://api.duckduckgo.com/",
                    params={"q": f"{first_only} {co_name}", "format": "json", "no_redirect": "1"},
                    timeout=8,
                )
                if ddg_resp.status_code == 200:
                    ddg_data = ddg_resp.json()
                    ddg_text = ddg_data.get("AbstractText", "") + " " + str(ddg_data.get("RelatedTopics", ""))
                    name_pat = re.compile(rf"\b{re.escape(first_only)}\s+([A-Z][a-z]{{2,20}})\b")
                    m = name_pat.search(ddg_text)
                    if m:
                        candidate = f"{first_only} {m.group(1)}"
                        if _is_valid_person_name(candidate):
                            ld["name"] = candidate
                            ld["_needs_full_name"] = False
                            ld["source"] += "+DDG"
            except Exception:
                pass

        # Step 4c: V5.13 — Apollo bulk_match for leads still missing personal email
        missing_email_leads = [
            ld for ld in domain_leads
            if not ld.get("email") or not is_personal_email(ld.get("email", ""))
        ]
        if missing_email_leads:  # V5.18: removed quota gate — always bulk-match for personal emails
            batch_url = f"{self.apollo.BASE_URL}/people/bulk_match"
            match_records = []
            for ld in missing_email_leads[:10]:  # Apollo limit per batch
                parts = ld.get("name", "").split()
                record = {"reveal_personal_emails": True, "reveal_phone_number": True}
                if parts:
                    record["first_name"] = parts[0]
                if len(parts) > 1:
                    record["last_name"] = parts[-1]
                if domain:
                    record["domain"] = domain
                if ld.get("_linkedin_url"):
                    record["linkedin_url"] = ld["_linkedin_url"]
                match_records.append(record)
            if match_records:
                try:
                    resp = requests.post(
                        batch_url,
                        json={"details": match_records},
                        headers=self.apollo._headers(),
                        timeout=30,
                    )
                    if resp.status_code == 200:
                        self.apollo._counter["apollo"] = self.apollo._counter.get("apollo", 0) + 1
                        matches = resp.json().get("matches", [])
                        for i, match in enumerate(matches):
                            if i >= len(missing_email_leads) or not match:
                                continue
                            ld = missing_email_leads[i]
                            person = match.get("person") or match
                            m_first = safe_str(person.get("first_name"))
                            m_last = safe_str(person.get("last_name"))
                            # V5.18: Trust Apollo bulk_match name data — less restrictive validation
                            if m_last and " " not in ld.get("name", ""):
                                candidate = f"{m_first} {m_last}".strip()
                                _last_lower = m_last.lower()
                                _bad_last = {"ceo", "cfo", "cto", "coo", "managing", "executive"}
                                if _last_lower not in _bad_last:
                                    ld["name"] = candidate
                                    ld["_needs_full_name"] = False
                                    ld["source"] += "+BulkName"
                            # V5.22: Prefer business emails over consumer — collect all candidates
                            _bm_current = ld.get("email", "")
                            _bm_current_is_business = bool(_bm_current) and not is_personal_email(_bm_current)
                            _bm_current_local = _bm_current.split("@")[0].lower() if _bm_current else ""
                            _bm_current_is_generic = _bm_current_local in GENERIC_EMAIL_PREFIXES
                            if not _bm_current or _bm_current_is_generic:
                                # No email or generic prefix — find best email from match
                                _match_emails = []
                                for em_field in ["email", "contact_email"]:
                                    em_val = person.get(em_field)
                                    if em_val:
                                        _match_emails.append(em_val)
                                _pe = person.get("personal_emails")
                                if isinstance(_pe, list):
                                    _match_emails.extend([e for e in _pe if e])
                                elif _pe:
                                    _match_emails.append(_pe)
                                # Pick best: business email first, then consumer as fallback
                                _picked = ""
                                for em_val in _match_emails:
                                    if em_val and is_valid_email(str(em_val)) and not is_personal_email(str(em_val)):
                                        _local_val = str(em_val).split("@")[0].lower()
                                        if _local_val not in GENERIC_EMAIL_PREFIXES:
                                            _picked = str(em_val)
                                            break
                                if not _picked:
                                    for em_val in _match_emails:
                                        if em_val and is_valid_email(str(em_val)):
                                            _picked = str(em_val)
                                            break
                                if _picked:
                                    ld["email"] = _picked
                                    ld["_email_verified"] = not is_personal_email(_picked)
                                    ld["source"] += "+BulkEmail"
                except Exception:
                    pass

        # Step 4d: V5.13 — Hunter.io domain-search for emails (second-best after Apollo personal)
        # Hunter.io returns real emails with confidence scores. Match to leads by first+last name.
        # EMAIL PRIORITY: 1=Apollo personal_emails, 2=Hunter.io, 3=Apollo verified search,
        #                 4=Inferred pattern, 5=Scraped company email (last resort)
        if self.hunter._available:  # V5.18: removed quota gate — Hunter provides full names
            hunter_contacts = self.hunter.domain_search(domain, limit=10)
            if hunter_contacts:
                self._log(f"   [Hunter] Found {len(hunter_contacts)} contacts at {domain}")
            for contact in hunter_contacts:
                h_first = safe_str(contact.get("first_name")).lower()
                h_last = safe_str(contact.get("last_name")).lower()
                h_email = safe_str(contact.get("value") or contact.get("email"))
                h_role = safe_str(contact.get("position") or contact.get("type", ""))
                if not h_email or not is_valid_email(h_email):
                    continue
                # Match Hunter contact to an existing lead by first and/or last name
                for ld in domain_leads:
                    if ld.get("_email_verified") and is_personal_email(ld.get("email", "")):
                        continue  # already has best-quality email
                    lead_name = (ld.get("name") or "").lower()
                    lead_parts = lead_name.split()
                    lead_first = lead_parts[0] if lead_parts else ""
                    lead_last = lead_parts[-1] if len(lead_parts) > 1 else ""
                    matched = False
                    if h_first and lead_first and h_first == lead_first:
                        matched = True
                    if h_last and lead_last and h_last == lead_last and h_first == lead_first:
                        matched = True  # strong match
                    # Also update full name if Hunter knows last name and we only have first
                    if matched:
                        _h_current = ld.get("email", "")
                        _h_current_is_business = bool(_h_current) and not is_personal_email(_h_current)
                        _h_current_local = _h_current.split("@")[0].lower() if _h_current else ""
                        _h_current_is_generic = _h_current_local in GENERIC_EMAIL_PREFIXES
                        # V5.22: Only update email if no email or current is generic prefix
                        # Don't overwrite a good business email with Hunter's email
                        if not _h_current or _h_current_is_generic:
                            ld["email"] = h_email
                            ld["_email_verified"] = not is_personal_email(h_email)
                            ld["source"] += "+Hunter"
                            self._log(f"   [Hunter] Email → '{ld.get('name')}': {h_email}")
                        if h_last and lead_first and " " not in (ld.get("name") or ""):
                            candidate = f"{lead_parts[0].capitalize()} {h_last.capitalize()}"
                            # V5.18: Trust Hunter.io name data — only reject obvious title words
                            _bad_last = {"ceo", "cfo", "cto", "coo", "managing", "executive"}
                            if h_last.lower() not in _bad_last and len(h_last) >= 2:
                                ld["name"] = candidate
                                ld["_needs_full_name"] = False
                                self._log(f"   [Hunter] Full name → '{lead_first}' resolved to '{candidate}'")
                        if not ld.get("role") and h_role:
                            ld["role"] = h_role
                        break  # matched to this lead, move to next Hunter contact

        # Step 4e: V5.15 — Hunter.io email-verifier for leads with full name but no email
        # Generates the most likely email candidate and verifies it for free via Hunter.io
        if self.hunter._available:  # V5.18: removed quota gate — Hunter email verification always runs
            for ld in domain_leads:
                if ld.get("email"):
                    continue
                if not ld.get("name") or " " not in ld["name"]:
                    continue
                parts = ld["name"].split()
                candidates = generate_email_candidates(parts[0], parts[-1], domain)
                if not candidates:
                    continue
                try:
                    r = requests.get(
                        f"{self.hunter.BASE_URL}/email-verifier",
                        params={"email": candidates[0], "api_key": API_KEYS.get("hunter", "")},
                        timeout=12,
                    )
                    if r.status_code == 200:
                        result = r.json().get("data", {})
                        if result.get("status") == "valid":
                            ld["email"] = candidates[0]
                            ld["_email_verified"] = True
                            ld["source"] += "+HunterVerify"
                            self._api_counter["hunter"] = self._api_counter.get("hunter", 0) + 1
                            self._log(f"   [Hunter✓] Verified email for {ld.get('name')}: {candidates[0]}")
                except Exception:
                    pass

        # Step 5: Web scraping — always scrape for emails and phones
        scraped = self.scraper.scrape_domain(domain)
        scraped_company = scraped.get("company_name", "")
        scraped_emails = scraped.get("emails", [])
        scraped_phones = scraped.get("phones", [])
        scraped_pairs = scraped.get("name_email_pairs", [])

        if not company_name and scraped_company:
            company_name = scraped_company

        scraped_personal = [e for e in scraped_emails if is_personal_email(e)]
        scraped_generic = [e for e in scraped_emails if not is_personal_email(e)]

        # Step 5b: Try to match scraped emails to specific leads by name
        for ld in domain_leads:
            lead_name = ld.get("name", "")
            if not lead_name or " " not in lead_name:
                continue
            if ld.get("email") and is_personal_email(ld.get("email", "")):
                continue
            parts = lead_name.split()
            first_n = parts[0]
            last_n = parts[-1] if len(parts) > 1 else ""
            matched = False
            for pair in scraped_pairs:
                if match_email_to_name(pair["email"], first_n, last_n):
                    ld["email"] = pair["email"]
                    ld["source"] += "+NameMatch"
                    matched = True
                    break
            if not matched:
                for se in scraped_personal:
                    if match_email_to_name(se, first_n, last_n):
                        ld["email"] = se
                        ld["source"] += "+NameMatch"
                        break

        # Step 5c: V5.2 — Fill missing last names from scraped name-email pairs
        for ld in domain_leads:
            if ld.get("name") and " " in ld["name"]:
                continue  # already has full name
            lead_first = (ld.get("name") or "").split()[0].lower() if ld.get("name") else ""
            if not lead_first:
                continue
            for pair in scraped_pairs:
                scraped_name = pair.get("name", "")
                if (scraped_name and " " in scraped_name and
                        scraped_name.split()[0].lower() == lead_first
                        and _is_valid_person_name(scraped_name)):  # V5.13: name guard
                    ld["name"] = scraped_name
                    ld["_needs_full_name"] = False
                    ld["source"] += "+ScrapeName"
                    break

        # Step 5d: V5.10 — Team/about page name lookup for remaining single-name leads
        # Scrapes /about, /team, /our-team etc. for staff full names — free, no API credits
        single_name_leads = [ld for ld in domain_leads if ld.get("name") and " " not in ld["name"]]
        if single_name_leads:
            team_entries = self.scraper.scrape_team_names(domain)
            if team_entries:
                for ld in single_name_leads:
                    if ld.get("name") and " " in ld["name"]:
                        continue  # already resolved by another step running in parallel
                    first_only = (ld.get("name") or "").split()[0].lower()
                    # V5.13: Use name abbreviation variants (matt → matthew, etc.)
                    first_variants = set(_get_name_variants(first_only))
                    for entry in team_entries:
                        entry_name = entry.get("name", "")
                        if (entry_name and " " in entry_name and
                                entry_name.split()[0].lower() in first_variants
                                and _is_valid_person_name(entry_name)):  # V5.13: name guard
                            ld["name"] = entry_name
                            ld["_needs_full_name"] = False
                            ld["source"] += "+TeamPage"
                            # Grab scraped email if lead still needs one
                            entry_email = entry.get("email", "")
                            if entry_email and is_valid_email(entry_email) and not ld.get("email"):
                                ld["email"] = entry_email
                            break

        # Step 5e: V5.15 — SerpAPI site-search snippet for persistent single-name leads
        # Queries company's own website for first name, parses snippets for "FirstName LastName"
        final_singles = [ld for ld in domain_leads if ld.get("name") and " " not in ld["name"]]
        if final_singles and self.serpapi._available:  # V5.18: removed quota gate — always resolve names
            co_name = company_name or domain_to_company_name(domain)
            for ld in final_singles:
                if ld.get("name") and " " in ld["name"]:
                    continue
                first_only = (ld.get("name") or "").split()[0]
                if not first_only:
                    continue
                try:
                    # Query 1: site-specific search on company domain
                    q = f'site:{domain} "{first_only}"'
                    results = self.serpapi.search_keyword(q, self.config["serpapi_gl"], num=5)
                    # SerpAPI search_keyword returns domains only — use raw API for snippets
                    raw = self.serpapi._raw_search(q, self.config["serpapi_gl"], num=5) if hasattr(self.serpapi, "_raw_search") else {}
                    snippets = []
                    for r in raw.get("organic_results", []):
                        snippets.append(r.get("title", "") + " " + r.get("snippet", ""))
                    # Query 2: LinkedIn-targeted
                    q2 = f'"{first_only}" "{co_name}" site:linkedin.com/in'
                    raw2 = self.serpapi._raw_search(q2, self.config["serpapi_gl"], num=5) if hasattr(self.serpapi, "_raw_search") else {}
                    for r in raw2.get("organic_results", []):
                        snippets.append(r.get("title", "") + " " + r.get("snippet", ""))
                    # Parse snippets for "FirstName LastName" pattern
                    first_cap = first_only.capitalize()
                    pattern = re.compile(
                        r"\b" + re.escape(first_cap) + r"\s+([A-Z][a-z]{2,20})\b"
                    )
                    for text in snippets:
                        m = pattern.search(text)
                        if m:
                            candidate = f"{first_cap} {m.group(1)}"
                            if _is_valid_person_name(candidate):
                                ld["name"] = candidate
                                ld["_needs_full_name"] = False
                                ld["source"] += "+SerpSite"
                                break
                except Exception:
                    pass

        # V5.6 FIX: Assign scraped emails by NAME MATCH only — never round-robin
        # This prevents assigning person A's email to person B
        for ld in domain_leads:
            _sc_current = ld.get("email", "")
            if _sc_current and not is_personal_email(_sc_current):
                _sc_local = _sc_current.split("@")[0].lower()
                if _sc_local not in GENERIC_EMAIL_PREFIXES:
                    continue  # V5.22: Already has a business email — don't overwrite with scraped
            elif _sc_current and is_personal_email(_sc_current):
                continue  # already has a personal email
            lead_name = ld.get("name", "")
            parts = lead_name.split() if lead_name else []
            first_n = parts[0] if parts else ""
            last_n = parts[-1] if len(parts) > 1 else ""
            matched = False
            if first_n:
                for se in scraped_emails:
                    if match_email_to_name(se, first_n, last_n):
                        ld["email"] = se
                        ld["source"] += "+Scrape"
                        matched = True
                        break
            # Only assign a purely generic inbox email if lead has NO email at all
            if not matched and not ld.get("email"):
                for se in scraped_generic:
                    local = se.lower().split("@")[0]
                    if local in GENERIC_EMAIL_PREFIXES:
                        ld["email"] = se
                        ld["source"] += "+Scrape"
                        break
                if not ld.get("email") and ld.get("_generic_email"):
                    ld["email"] = ld["_generic_email"]

        # V5.25: Business email upgrade — when lead has consumer email (gmail/hotmail) and their
        # name appears in it, generate business email from name + company domain.
        # This handles the case where Apollo only reveals consumer emails but the lead actually
        # has a business email (first.last@companydomain) visible on their Apollo profile.
        for ld in domain_leads:
            current_email = ld.get("email", "")
            if not current_email or not is_personal_email(current_email):
                continue  # No email or already has business email — skip
            lead_name = ld.get("name", "")
            if not lead_name or " " not in lead_name:
                continue  # Need full name to generate business email pattern
            parts = lead_name.split()
            first_n = parts[0].lower()
            last_n = parts[-1].lower() if len(parts) > 1 else ""
            local_part = current_email.lower().split("@")[0]
            # Check if ANY of the lead's names appear in the consumer email local part
            name_in_email = False
            for name_word in parts:
                if len(name_word) >= 2 and name_word.lower() in local_part:
                    name_in_email = True
                    break
            if not name_in_email:
                continue  # Name not in email — can't confirm this is truly their personal email
            # Generate business email candidates from name + company domain
            if domain and last_n:
                candidates = [
                    f"{first_n}.{last_n}@{domain}",
                    f"{first_n}@{domain}",
                    f"{first_n}{last_n}@{domain}",
                    f"{first_n[0]}.{last_n}@{domain}",
                ]
                # Use the first.last@domain pattern as the most common business email format
                business_email = candidates[0]
                ld["email"] = business_email
                ld["_email_inferred"] = True
                ld["_email_verified"] = False
                ld["source"] += "+BizEmailGen"
                self._log(f"   V5.25: Business email upgrade for {lead_name}: "
                          f"{current_email} → {business_email}")

        # Phone and company fallback
        # V5.21: Company phone is stored separately — NOT assigned to individual leads.
        # We want personal/mobile phones only. Company HQ number is a last-resort fallback
        # and must never overwrite or prevent personal phone discovery.
        # V5.25: Filter scraped phones to exclude company phone
        _co_digits_for_scrape = re.sub(r'\D', '', company_phone) if company_phone else ""
        _scraped_personal_phones = []
        for sp in scraped_phones:
            sp_digits = re.sub(r'\D', '', sp)
            if _co_digits_for_scrape and sp_digits == _co_digits_for_scrape:
                continue  # Skip company phone from scraped list
            _scraped_personal_phones.append(sp)
        for ld in domain_leads:
            if not ld.get("phone"):
                if _scraped_personal_phones:
                    ld["phone"] = _scraped_personal_phones[0]
                # V5.21: Do NOT assign company_phone here — it's the generic switchboard.
                # Personal phones come from Apollo/Lusha enrich_person (Steps 2c/2d/4b).
            if not ld.get("company"):
                ld["company"] = company_name or domain_to_company_name(domain)

        # Step 6: SerpApi business info fallback (V5.1: skip if ALL leads have phones)
        # V5.25: SerpAPI phone is company-level (Google Knowledge Panel) — mark as non-direct
        needs_phone = any(not ld.get("phone") for ld in domain_leads)
        if needs_phone:
            for ld in domain_leads:
                if not ld.get("phone") and ld.get("company"):
                    info = self.serpapi.search_business_info(ld["company"], self.config["serpapi_gl"])
                    if info.get("phone"):
                        ld["phone"] = info["phone"]
                        ld["_direct_phone"] = False  # V5.25: SerpAPI phone = company level
                        ld["_phone_quality"] = 5  # V5.25: Lowest quality — company number
                        ld["source"] += "+SerpApi"
                    if info.get("email"):
                        serp_email = info["email"]
                        _serp_current = ld.get("email", "")
                        _serp_current_is_business = bool(_serp_current) and not is_personal_email(_serp_current)
                        _serp_current_local = _serp_current.split("@")[0].lower() if _serp_current else ""
                        _serp_current_is_generic = _serp_current_local in GENERIC_EMAIL_PREFIXES
                        # V5.22: Only assign SerpAPI email if no business email exists
                        if not _serp_current or _serp_current_is_generic:
                            ld["email"] = serp_email

        # V5.21: Last-resort company phone fallback — only for leads that got NO phone from any source.
        # This is the generic company number (from org enrichment) and is clearly marked as non-direct.
        # V5.24: Don't re-add a phone we already identified as a shared/company-level number.
        if company_phone:
            _co_digits = re.sub(r'\D', '', company_phone)
            for ld in domain_leads:
                if not ld.get("phone"):
                    _stripped = ld.get("_dedup_stripped_phone", "")
                    if _stripped and re.sub(r'\D', '', _stripped) == _co_digits:
                        continue  # V5.24: company_phone == the shared phone we stripped — skip
                    ld["phone"] = company_phone
                    # NOT marked as _direct_phone — this is the company switchboard

        # Clean up internal fields
        for ld in domain_leads:
            ld.pop("_generic_email", None)

        # Step 6b: V5.9 — Final name resolution via LinkedIn URL only (real data, no fabrication)
        for ld in domain_leads:
            if ld.get("name") and " " in ld["name"]:
                continue  # already has full name
            first_only = ld.get("name", "")
            li_url = ld.get("_linkedin_url", "")
            if first_only and li_url:
                resolved = _extract_name_from_linkedin_url(first_only, li_url)
                if resolved and " " in resolved:
                    ld["name"] = resolved
                    ld["_needs_full_name"] = False
                    ld["source"] += "+LinkedIn"

        # V5.10: Update phone-leads credit gate counter
        # Counts ONLY direct personal phones from Apollo/Lusha enrich_person (not company/scraped phones).
        # Once _phone_leads_count >= max_leads * 1.2, no more enrich_person calls are made.
        if self.max_leads > 0:
            direct_phone_here = sum(
                1 for ld in domain_leads
                if ld.get("_direct_phone") and format_phone(ld.get("phone", ""), self.country) != ""
            )
            if direct_phone_here > 0:
                with self._complete_leads_lock:
                    self._phone_leads_count += direct_phone_here
                self._log(f"   V5.10: +{direct_phone_here} direct phone leads "
                          f"(total: {self._phone_leads_count}/{int(self.max_leads * 1.2)} target)")

        # V5.13: WHOIS founder verification
        if domain_leads:
            self._step_whois_verify(domain, domain_leads)

        # V5.13: Per-domain credit tracking
        domain_credit_cost = sum(
            (self._api_counter.get(svc, 0) - _counter_snapshot.get(svc, 0)) * API_CREDIT_COSTS.get(svc, 0)
            for svc in API_CREDIT_COSTS
        )
        per_lead_cost = domain_credit_cost / max(len(domain_leads), 1)
        for ld in domain_leads:
            ld["_api_credits_used"] = per_lead_cost

        # If Apollo found people, return them
        if domain_leads:
            return domain_leads
        else:
            # Fallback: create a domain-level lead from scraped/org data
            fallback_email = ""
            if scraped_personal:
                fallback_email = scraped_personal[0]
            elif scraped_emails:
                fallback_email = scraped_emails[0]
            fallback = {
                "name": "",
                "domain": domain,
                "company": company_name or domain_to_company_name(domain),
                "role": "",
                "email": fallback_email,
                "phone": company_phone or (scraped_phones[0] if scraped_phones else ""),
                "source": "Org+Scrape",
                "_domain_source": "paid" if domain in self._adwords_domains else ("organic" if domain in self._organic_domains else "paid"),  # V5.13
                "_direct_phone": False,  # V5.12: Scraped phone, not from Apollo search
                "_email_verified": False,  # V5.12: Scraped email, not verified
                "_paid_keywords": traffic_metrics.get("paid_keywords", 0),
                "_organic_keywords": traffic_metrics.get("organic_keywords", 0),
                "_paid_traffic": traffic_metrics.get("paid_traffic", 0),
                "_organic_traffic": traffic_metrics.get("organic_traffic", 0),
                "_api_credits_used": per_lead_cost,
            }
            if fallback["email"] or fallback["phone"]:
                return [fallback]
            return []

    # ── Apollo org search fallback for domain discovery ─────────────────────

    def _apollo_org_discovery_fallback(self) -> set:
        """Use Apollo's organization search as a last-resort domain discovery source.
        Called when both SEMrush and SerpApi return 0 domains.
        Searches Apollo for companies matching the industry in the target country.
        """
        domains = set()
        try:
            url = f"{self.apollo.BASE_URL}/mixed_companies/search"
            # Map country code to Apollo location string
            location_map = {
                "AU": "Australia", "USA": "United States",
                "UK": "United Kingdom", "India": "India",
            }
            location = location_map.get(self.country, self.country)
            # Use first few industry keywords as search tags
            industry_tags = [self.industry.lower().split("/")[0].strip()]
            payload = {
                "q_organization_keyword_tags": industry_tags,
                "organization_locations": [location],
                "per_page": 25,
                "page": 1,
            }
            resp = requests.post(
                url, json=payload,
                headers=self.apollo._headers(), timeout=30
            )
            if resp.status_code == 200:
                self._api_counter["apollo"] = self._api_counter.get("apollo", 0) + 1
                orgs = resp.json().get("organizations", [])
                for org in orgs:
                    d = org.get("primary_domain", "") or extract_domain(org.get("website_url", ""))
                    if d and not is_platform_domain(d):
                        domains.add(d)
                self._log(f"   Apollo org search returned {len(orgs)} orgs → {len(domains)} valid domains")
            else:
                self._log(f"   Apollo org search failed: HTTP {resp.status_code}")
        except Exception as exc:
            self._log(f"   Apollo org search error: {exc}")
        return domains

    def _fetch_retry_domains(self, already_processed: set, max_new: int = 15) -> list:
        """V5.10: Fetch additional paid domains for the retry loop.
        Used when Phase 4 finishes but phone-leads count < max_leads.
        Pulls from keywords beyond the first 30 (not used in Phase 3).
        """
        db = self.config["semrush_db"]
        gl = self.config["serpapi_gl"]
        new_domains = []

        # Try keywords beyond the first 30 (already used in Phase 3)
        retry_keywords = self.keywords[30:] if len(self.keywords) > 30 else self.keywords[:10]
        for kw in retry_keywords:
            if len(new_domains) >= max_new:
                break
            ad_results = self.semrush.get_adwords_domains(kw, db, limit=10)
            for r in ad_results:
                d = r["domain"]
                if d not in already_processed and d not in new_domains:
                    new_domains.append(d)

        # Supplement with SerpApi if needed
        if len(new_domains) < max_new and self.serpapi._available:
            serp_domains = self.serpapi.search_keyword(
                f"best {self.industry} {self.config['location_suffix']}", gl, num=20
            )
            for d in serp_domains:
                if d not in already_processed and d not in new_domains:
                    # Quick paid traffic check
                    if self.semrush.has_paid_traffic(d, db):
                        new_domains.append(d)
                    if len(new_domains) >= max_new:
                        break

        return new_domains[:max_new]

    def _phase4_enrichment(self):
        """V5.1: Parallel domain enrichment using ThreadPoolExecutor (8 workers).
        V5.10: Retry loop — if phone-leads count < max_leads after first pass, fetch more domains.
        """
        self._progress(46, "Enriching leads (V5.10: phone-targeted credit gate)...")
        self._log("Phase 4: Multi-source lead enrichment (V5.1: ThreadPoolExecutor, 8 workers)")
        self._log("   V5.10: Credits spent only until max_leads phone-bearing leads are found")

        total = len(self.domains)
        all_domain_leads = []
        completed_count = 0
        BATCH_SIZE = 12  # V5.17: Increased from 8 → 12 workers for faster throughput

        # V5.10: Submit domains in batches so the credit gate can stop between batches.
        # Previously all domains were submitted at once — gate couldn't stop mid-inflight workers.
        with ThreadPoolExecutor(max_workers=BATCH_SIZE) as executor:
            for batch_start in range(0, total, BATCH_SIZE):
                if self._has_enough_leads() or self._cancelled:
                    self._log(f"   Gate reached — stopping domain submission at {batch_start}/{total}")
                    break
                batch = self.domains[batch_start: batch_start + BATCH_SIZE]
                futures = {
                    executor.submit(self._enrich_single_domain, domain, batch_start + i, total): domain
                    for i, domain in enumerate(batch)
                }
                for future in as_completed(futures):
                    if self._cancelled:
                        executor.shutdown(wait=False, cancel_futures=True)
                        return
                    try:
                        result = future.result()
                        if result:
                            all_domain_leads.extend(result)
                    except Exception as e:
                        self._log(f"   ERROR enriching {futures[future]}: {e}")
                    completed_count += 1
                    pct = 46 + int(completed_count / total * 40)
                    self._progress(pct, f"Enriched {completed_count}/{total} domains ({len(all_domain_leads)} leads)")

        # V5.10: Retry loop — if we don't have enough phone-bearing leads, fetch more domains
        if (self.max_leads > 0 and not self._cancelled
                and self._phone_leads_count < self.max_leads):
            deficit = self.max_leads - self._phone_leads_count
            self._log(f"   V5.10: Only {self._phone_leads_count}/{self.max_leads} phone leads found. "
                      f"Retry loop: fetching more domains to fill {deficit} gap...")
            self._progress(86, f"Retry: need {deficit} more phone leads...")

            already_processed = set(self.domains)
            retry_count = min(deficit * 3, 20)  # fetch up to 3x deficit, cap at 20
            retry_domains = self._fetch_retry_domains(already_processed, max_new=retry_count)

            if retry_domains:
                self._log(f"   V5.10 Retry: Processing {len(retry_domains)} additional domains...")
                retry_total = total + len(retry_domains)
                for ri, domain in enumerate(retry_domains):
                    if self._has_enough_leads() or self._cancelled:
                        break
                    try:
                        result = self._enrich_single_domain(domain, total + ri, retry_total)
                        if result:
                            all_domain_leads.extend(result)
                    except Exception as e:
                        self._log(f"   Retry ERROR enriching {domain}: {e}")
                self._log(f"   V5.10 Retry complete: {self._phone_leads_count}/{self.max_leads} phone leads")
            else:
                self._log("   V5.10 Retry: No additional domains found in retry pass.")

        self.leads = all_domain_leads
        self._log(f"   Total raw leads: {len(self.leads)} "
                  f"(phone-bearing: {self._phone_leads_count})")
        self._progress(90, f"{len(self.leads)} raw leads collected")

    # ── V5.13: WHOIS Founder Verification ──────────────────────────────────

    def _step_whois_verify(self, domain: str, domain_leads: list) -> None:
        """V5.13: WHOIS founder verification. Marks verified founder + promotes to front."""
        try:
            whois_name = self.whois_client.get_registrant_name(domain)
            if not whois_name:
                return
            self._log(f"   WHOIS: {domain} registrant = {whois_name}")
            match = WhoisFounderClient.find_founder_in_leads(whois_name, domain_leads)
            if match:
                match["_whois_verified"] = True
                match["_founder_verified"] = True
                if not match.get("role") or not any(kw in match["role"].lower() for kw in HARD_DM_KEYWORDS):
                    match["role"] = "Founder (WHOIS Verified)"
                match["source"] += "+WHOIS"
                self._log(f"   WHOIS: Matched founder '{match.get('name')}' at {domain}")
        except Exception:
            pass

    # ── Phase 4b: Targeted Completion ──────────────────────────────────────

    def _phase4b_targeted_completion(self):
        """V5.16: Targeted completion pass — fills phone for top N leads outside the credit gate.
        Runs after Phase 4 regardless of _has_enough_leads() state.
        V5.23: Also processes leads with quality=0 phones (company HQ fallback) to find
        personal/direct numbers via SerpAPI person search.
        Priority: (A) Apollo re-enrich via LinkedIn URL, (B) SerpAPI find_business_phone,
        (B2) SerpAPI find_person_phone, (C) contact page scraping.
        """
        if self.max_leads <= 0 or self._cancelled:
            return
        self._progress(88, "V5.16: Targeted completion pass for top leads...")
        self._log("Phase 4b: Targeted completion — filling phone for top-N leads outside credit gate")

        gl = self.config.get("serpapi_gl", "au")
        # V5.23: Include leads with quality=0 (company HQ fallback) — try to find personal phone
        need_phone = [
            ld for ld in self.leads
            if not ld.get("phone") or ld.get("_phone_quality", 0) == 0
        ][:self.max_leads * 2]
        filled = 0

        for ld in need_phone:
            if self._cancelled:
                break
            domain = ld.get("domain", "")
            company = ld.get("company") or domain_to_company_name(domain)
            name = ld.get("name", "")
            _existing_phone = ld.get("phone", "")

            # (A) Apollo enrich retry — only for leads with LinkedIn URL not yet Apollo-enriched
            if name and ld.get("_linkedin_url") and "apollo" not in ld.get("source", "").lower():
                try:
                    _parts4b = name.split()
                    enriched = self.apollo.enrich_person(
                        _parts4b[0] if _parts4b else "",
                        _parts4b[-1] if len(_parts4b) > 1 else "",
                        domain,
                        linkedin_url=ld.get("_linkedin_url", ""),
                    )
                    if enriched.get("phone"):
                        ld["phone"] = enriched["phone"]
                        ld["source"] = ld.get("source", "") + "+Apollo4b"
                        with self._complete_leads_lock:
                            self._phone_leads_count += 1
                        filled += 1
                        if enriched.get("email") and not ld.get("email"):
                            ld["email"] = enriched["email"]
                        continue
                except Exception:
                    pass

            # (B) SerpAPI find_business_phone — only for truly empty phone
            if not ld.get("phone") and self.serpapi._available and domain:
                phone = self.serpapi.find_business_phone(domain, company, gl)
                if phone:
                    ld["phone"] = phone
                    ld["source"] = ld.get("source", "") + "+SerpPhone"
                    with self._complete_leads_lock:
                        self._phone_leads_count += 1
                    filled += 1
                    continue

            # (C) Contact page scraping — only for truly empty phone
            if not ld.get("phone") and domain:
                phone = self._scrape_contact_pages(domain)
                if phone:
                    ld["phone"] = phone
                    ld["source"] = ld.get("source", "") + "+ContactPage"
                    with self._complete_leads_lock:
                        self._phone_leads_count += 1
                    filled += 1

            # (D) V5.23: SerpAPI person phone search — for leads with low-quality phone (company HQ)
            # Searches Google for "[person name] [domain/company] phone mobile" to find personal number.
            # Only runs when current phone quality=0 (company fallback) and a full name is known.
            # Only replaces if a DIFFERENT phone is found (avoids redundant same-number replacement).
            if (name and " " in name and self.serpapi._available and domain
                    and ld.get("_phone_quality", 0) < 30):  # V5.24: was == 0; now catches landlines (quality 5-15) too
                person_phone = self.serpapi.find_person_phone(name, domain, company, gl)
                if person_phone:
                    _found_digits = re.sub(r'\D', '', person_phone)
                    _existing_digits = re.sub(r'\D', '', _existing_phone) if _existing_phone else ""
                    if _found_digits != _existing_digits:  # Only update if different number
                        ld["phone"] = person_phone
                        ld["_phone_quality"] = 20   # Unknown type — score as non-HQ
                        ld["_direct_phone"] = False  # Can't confirm it's direct until validated
                        ld["source"] = ld.get("source", "") + "+SerpPersonPhone"
                        filled += 1
                        self._log(f"   [SerpPerson] Found phone for {name}: {person_phone}")

        self._log(f"   Phase 4b: Filled phone for {filled} additional leads "
                  f"(total phone-bearing: {self._phone_leads_count})")

        # V5.17: Phase 4b.5 — fill email for phone-bearing leads that still have no email
        # Uses inferred pattern (first.last@domain) for leads with full name + phone.
        # This ensures the top N leads meet the minimum: name + phone + email.
        email_filled = 0
        for ld in self.leads:
            if not ld.get("phone"):
                continue  # only care about phone-bearing leads
            if ld.get("email"):
                continue  # already has email
            ld_name = ld.get("name", "")
            ld_domain = ld.get("domain", "")
            if not ld_name or " " not in ld_name or not ld_domain:
                continue
            parts = ld_name.split()
            candidates = generate_email_candidates(parts[0], parts[-1], ld_domain)
            if candidates:
                ld["email"] = candidates[0]
                ld["_email_inferred"] = True
                ld["source"] = ld.get("source", "") + "+InferredEmail"
                email_filled += 1
        if email_filled:
            self._log(f"   Phase 4b.5: Inferred email assigned to {email_filled} phone-bearing leads")

        self._progress(90, f"Phase 4b done: {self._phone_leads_count} phone-bearing leads")

        # V5.26: Phase 4c — Async phone collection from Apollo webhook
        # By now, Apollo has had time to process phone reveals submitted during enrichment.
        # Collect phone data from webhook store (self-hosted) or poll webhook.site (relay).
        _wh_url = _get_webhook_url()
        if _wh_url:
            self._log("Phase 4c: Collecting async phone reveals from Apollo...")
            import time as _time

            # V5.27: Multi-round polling with increasing waits.
            # Apollo's webhook delivery is async; some contacts arrive within 5s, others take 15-25s.
            # Poll 3 times (at 5s, 10s, 15s delays) to catch late deliveries (e.g. small companies
            # where Apollo's phone reveal queue is slower).
            _poll_waits = [5, 10, 15]  # seconds between each poll attempt
            for _poll_wait in _poll_waits:
                _time.sleep(_poll_wait)
                # If using webhook.site relay, poll for received data
                if _webhook_site_token:
                    self._log(f"    Polling webhook.site (wait={_poll_wait}s)...")
                    _poll_webhook_site_phones(_webhook_site_token)

            phones_collected = 0
            leads_checked = 0
            for ld in self.leads:
                apollo_id = ld.get("_apollo_id", "")
                if not apollo_id:
                    continue
                # Skip leads that already have a high-quality personal/mobile phone (q >= 30)
                existing_quality = ld.get("_phone_quality", 0)
                if existing_quality >= 30:
                    continue  # Already has a personal/mobile phone — no need to check webhook
                leads_checked += 1
                # Check webhook store for delivered phone data
                webhook_phones = _collect_phone_reveal(apollo_id)
                if webhook_phones:
                    company_phone = ld.get("_company_phone", "")
                    _fake_person = {"phone_numbers": webhook_phones}
                    phone, quality = _pick_best_phone_from_apollo(_fake_person, company_phone)
                    if phone and quality > existing_quality:
                        old_phone = ld.get("phone", "")
                        ld["phone"] = phone
                        ld["_phone_quality"] = quality
                        ld["_direct_phone"] = quality >= 30
                        phones_collected += 1
                        if old_phone:
                            self._log(f"    V5.26 Phone upgrade: {ld.get('name', '?')} {old_phone} -> {phone} (q={existing_quality}->{quality})")
                        else:
                            self._log(f"    V5.26 Phone reveal: {ld.get('name', '?')} -> {phone} (q={quality})")
            self._log(f"    Phase 4c: {phones_collected}/{leads_checked} phones collected from Apollo reveals")

            # V5.27: Post-Phase-4c dedup guard — strip phones shared across 2+ leads from same domain.
            # When Apollo's phone reveal credits are exhausted, the webhook delivers a shared company
            # placeholder (e.g. +61485857016) for multiple contacts. This bypasses the earlier dedup
            # guard (which runs before Phase 4c). Only strip non-personal phones (quality < 30) to
            # avoid incorrectly stripping two people who genuinely share a family/office line.
            import re as _re_dedup
            _4c_domain_phone_counts: dict = {}
            for ld in self.leads:
                p = ld.get("phone", "")
                domain = ld.get("domain", "")
                q = ld.get("_phone_quality", 0)
                if p and domain and q < 30:
                    key = (domain, _re_dedup.sub(r'\D', '', p))
                    _4c_domain_phone_counts[key] = _4c_domain_phone_counts.get(key, 0) + 1
            _4c_stripped = 0
            for ld in self.leads:
                p = ld.get("phone", "")
                domain = ld.get("domain", "")
                q = ld.get("_phone_quality", 0)
                if p and domain and q < 30:
                    key = (domain, _re_dedup.sub(r'\D', '', p))
                    if _4c_domain_phone_counts.get(key, 0) >= 2:
                        self._log(f"    V5.27 Post-4c dedup: {ld.get('name', '?')} stripped shared phone {p} (q={q})")
                        ld["_dedup_stripped_phone"] = p
                        ld["phone"] = ""
                        ld["_phone_quality"] = 0
                        ld["_direct_phone"] = False
                        _4c_stripped += 1
            if _4c_stripped:
                self._log(f"    V5.27 Post-4c dedup: stripped {_4c_stripped} shared phone(s) across leads")

            # Cleanup
            _cleanup_phone_reveals()
            if _webhook_site_token:
                _delete_webhook_site_token(_webhook_site_token)

    # ── Phase 5: Data Cleanup ───────────────────────────────────────────────

    def _phase5_cleanup(self):
        self._progress(91, "Cleaning and deduplicating leads...")
        self._log("Phase 5: Data cleanup")

        cleaned = []
        seen = set()

        for lead in self.leads:
            # Filter .org domains UNLESS lead has email or phone
            if lead.get("domain") and ".org" in lead["domain"].lower():
                if not lead.get("email") and not lead.get("phone"):
                    continue

            # Format and strictly validate phone number
            if lead.get("phone"):
                lead["phone"] = format_phone(lead["phone"], self.country)

            # Clean company name
            if not lead.get("company") or lead["company"] == lead.get("domain", ""):
                lead["company"] = domain_to_company_name(lead.get("domain", ""))

            # V5.13: Clear name if it exactly matches company name (name contamination guard)
            # e.g. "Le Tooth West End" appearing in both Name and Company Name
            if lead.get("name") and lead.get("company"):
                if lead["name"].strip().lower() == lead["company"].strip().lower():
                    lead["name"] = ""

            # Validate email
            if lead.get("email") and not is_valid_email(lead["email"]):
                lead["email"] = ""

            # V5.25: REMOVE leads whose role is purely a practitioner/trade role matching the industry
            # e.g. searching for "dentist" → remove leads with role "Dentist" or "General Dentist"
            # but KEEP "Dentist & Owner" (has DM keyword)
            if lead.get("role"):
                _role_lower = lead["role"].lower().strip()
                _has_dm_keyword = any(kw in _role_lower for kw in HARD_DM_KEYWORDS | SOFT_DM_KEYWORDS)
                _is_trade = any(trade in _role_lower for trade in TRADE_ROLE_WORDS)
                _is_non_dm = any(kw in _role_lower for kw in NON_DECISION_MAKER_KEYWORDS)
                # V5.25: Check if role matches the searched industry (pure practitioner)
                _industry_lower = self.industry.lower().strip()
                _industry_words = set(_industry_lower.replace("/", " ").replace("-", " ").split())
                _role_words_set = set(_role_lower.replace("/", " ").replace("-", " ").split())
                # Role is pure industry match if ALL significant role words are industry words
                # e.g. "dentist" matches industry "dentist", "general dentist" matches too
                _generic_prefixes = {"general", "senior", "junior", "lead", "head", "chief", "principal"}
                _role_significant = _role_words_set - _generic_prefixes - {"&", "and", "of", "the", "a"}
                _is_pure_industry = bool(_role_significant) and _role_significant.issubset(
                    _industry_words | {w + "s" for w in _industry_words} |
                    {w.rstrip("s") for w in _industry_words} |
                    TRADE_ROLE_WORDS
                )
                if not _has_dm_keyword and _is_pure_industry:
                    continue  # V5.25: REMOVE lead — practitioner, not decision-maker
                if not _has_dm_keyword and (_is_trade or _is_non_dm):
                    lead["role"] = ""

            # Keep ANY lead that has email or phone, regardless of other fields
            # Only skip if there is no email AND no phone AND no name
            if not lead.get("email") and not lead.get("phone") and not lead.get("name"):
                continue

            # Deduplicate
            dedup_key = ""
            if lead.get("name") and lead.get("domain"):
                dedup_key = f"{lead['name'].lower()}|{lead['domain'].lower()}"
            elif lead.get("email"):
                dedup_key = lead["email"].lower()
            else:
                dedup_key = f"{lead.get('phone', '')}|{lead.get('domain', '')}"

            if dedup_key and dedup_key in seen:
                continue
            if dedup_key:
                seen.add(dedup_key)

            cleaned.append(lead)

        self.leads = cleaned

        # V5.17: Removed secondary scraped-phone dedup — it was DROPPING leads sharing the same
        # company phone (common when Apollo org returns one number for all staff).
        # The primary dedup above (name|domain, email, phone|domain) is sufficient.
        # All leads with phones are kept — shared company numbers are acceptable.

        # V5.2: Log warning for leads that still have only a first name
        single_name_count = sum(
            1 for ld in self.leads
            if ld.get("name") and " " not in ld["name"]
        )
        if single_name_count > 0:
            self._log(f"   WARNING: {single_name_count} leads still have only a first name "
                      f"after all enrichment. These will be included but are lower quality.")

        self._log(f"   Final leads after cleanup: {len(self.leads)}")
        self._progress(95, f"{len(self.leads)} leads cleaned")

    # ── Phase 5b: OpenAI Email Verification ────────────────────────────────

    def _phase5b_openai_verify(self):
        if not self.leads:
            return
        self._progress(95, "Verifying emails with OpenAI...")
        self._log("Phase 5b: OpenAI email verification")

        # Process in batches of 20
        batch_size = 20
        total = len(self.leads)
        verified = 0
        for start in range(0, total, batch_size):
            if self._cancelled:
                return
            batch = self.leads[start:start + batch_size]
            self.openai_verifier.verify_leads_batch(batch)
            verified += len(batch)
            self._log(f"   Verified {verified}/{total} emails")

        personal_count = sum(1 for ld in self.leads if ld.get("_email_type") == "Personal")
        generic_count = sum(1 for ld in self.leads if ld.get("_email_type") == "Generic")
        self._log(f"   Email types: {personal_count} personal, {generic_count} generic")
        self._progress(96, f"Email verification complete")

    # ── Phase 6: CSV Export ─────────────────────────────────────────────────

    def _phase6_export(self) -> str:
        self._progress(97, "Sorting and exporting CSV...")
        self._log("Phase 6: CSV export with decision-maker grouping")

        if not self.leads:
            self._log("   No leads to export.")
            return ""

        # ── V5.13 Scoring: Enhanced partition-based with traffic source priority ──
        def _partition_score(lead):
            """V5.15: New 12-tier scoring per user-specified algorithm.

            PRIMARY SORT KEY — Completeness tier (12 = best, 0 = worst):
              12: Name+Domain+Role+Phone+Email(Personal/Verified)
              11: Name+Domain+Role+Phone+Email(Generic/Work)
              10: Name+Domain+Role+Phone  (no email)
               9: Name+Domain+Role+Email(Personal/Verified)  (no phone)
               8: Name+Domain+Role+Email(Generic/Work)  (no phone)
               7: Name+Domain+Role  (no phone, no email)
               6: Name+Domain+Phone+Email(Personal/Verified)  (no role)
               5: Name+Domain+Phone+Email(Generic/Work)  (no role)
               4: Name+Domain+Phone  (no role, no email)
               3: Name+Domain+Email(Personal/Verified)  (no role, no phone)
               2: Name+Domain+Email(Generic/Work)  (no role, no phone)
               1: Name+Domain only
               0: anything else

            SECONDARY — within same tier:
              Paid > Organic (+100 bonus)
            TERTIARY — DM role quality (+20 hard DM, +10 soft DM)
            QUATERNARY — WHOIS founder verified (+5)
            """
            has_name = bool(lead.get("name"))
            has_domain = bool(lead.get("domain"))
            has_role = bool(lead.get("role"))
            has_phone = bool(lead.get("phone"))
            raw_email = lead.get("email", "")
            has_email = bool(raw_email)
            is_paid = lead.get("_domain_source") == "paid"
            is_personal_em = has_email and (
                lead.get("_email_verified") or is_personal_email(raw_email)
            )
            # Generic/Work = has email but NOT personal
            is_any_em = has_email

            # Determine completeness tier
            if has_name and has_domain and has_role and has_phone and is_personal_em:
                tier = 12
            elif has_name and has_domain and has_role and has_phone and is_any_em:
                tier = 11
            elif has_name and has_domain and has_role and has_phone:
                tier = 10
            elif has_name and has_domain and has_role and not has_phone and is_personal_em:
                tier = 9
            elif has_name and has_domain and has_role and not has_phone and is_any_em:
                tier = 8
            elif has_name and has_domain and has_role:
                tier = 7
            elif has_name and has_domain and not has_role and has_phone and is_personal_em:
                tier = 6
            elif has_name and has_domain and not has_role and has_phone and is_any_em:
                tier = 5
            elif has_name and has_domain and not has_role and has_phone:
                tier = 4
            elif has_name and has_domain and not has_role and not has_phone and is_personal_em:
                tier = 3
            elif has_name and has_domain and not has_role and not has_phone and is_any_em:
                tier = 2
            elif has_name and has_domain:
                tier = 1
            else:
                tier = 0

            # Secondary: Paid vs Organic
            paid_bonus = 100 if is_paid else 0

            # Tertiary: DM role quality
            _role_str = (lead.get("role") or "").lower()
            if any(kw in _role_str for kw in HARD_DM_KEYWORDS):
                dm_bonus = 20
            elif any(kw in _role_str for kw in SOFT_DM_KEYWORDS):
                dm_bonus = 10
            else:
                dm_bonus = 0

            # Quaternary: WHOIS founder verified
            whois_bonus = 5 if lead.get("_founder_verified") else 0

            return tier * 1000 + paid_bonus + dm_bonus + whois_bonus

        # Score all leads and store for CSV output
        for lead in self.leads:
            lead["_score"] = _partition_score(lead)
            lead["lead_score"] = lead["_score"]

        self.leads.sort(key=lambda ld: ld["_score"], reverse=True)

        # Log partition counts
        p1 = sum(1 for ld in self.leads if ld["_score"] >= 6000)
        p2 = sum(1 for ld in self.leads if 4000 <= ld["_score"] < 5000)
        p3 = sum(1 for ld in self.leads if 3000 <= ld["_score"] < 4000)
        p4 = sum(1 for ld in self.leads if 2000 <= ld["_score"] < 3000)
        p5 = sum(1 for ld in self.leads if 1000 <= ld["_score"] < 2000)
        self._log(f"   V5.4 Partitions: {p1} Name+Email+Phone | {p2} Name+Phone | "
                  f"{p3} Name+Email | {p4} Phone-only | {p5} Email-only")

        # ── V5.20: Role-hierarchy domain grouping ──
        # Group by domain, sort within each group by role hierarchy score (owner>founder>c-suite>...),
        # keep top 2 per domain (but keep ALL owners+founders if both exist).
        MAX_PER_DOMAIN = 3  # V5.25: User requested max 3 leads per company
        domain_groups = defaultdict(list)
        no_domain = []
        for lead in self.leads:
            d = (lead.get("domain") or "").strip().lower()
            if d:
                domain_groups[d].append(lead)
            else:
                no_domain.append(lead)

        top_section = []
        rest_section = []
        for d, group in domain_groups.items():
            # V5.20: Score each lead by role hierarchy (owner=100, founder=95, ... intern=10)
            for ld in group:
                ld["_hierarchy_score"] = _role_hierarchy_score(ld.get("role", ""))
            group.sort(key=lambda x: (x["_hierarchy_score"], x.get("_score", 0)), reverse=True)

            # V5.20: If both owner (>=100) and founder (>=95) exist, keep all of them
            keep_count = MAX_PER_DOMAIN
            has_owner = any(ld["_hierarchy_score"] >= 100 for ld in group)
            has_founder = any(95 <= ld["_hierarchy_score"] < 100 for ld in group)
            if has_owner and has_founder:
                owner_founder_count = sum(1 for ld in group if ld["_hierarchy_score"] >= 95)
                keep_count = max(keep_count, owner_founder_count)

            if len(group) <= keep_count:
                top_section.extend(group)
            else:
                top_section.extend(group[:keep_count])
                rest_section.extend(group[keep_count:])

            for ld in group:
                ld.pop("_hierarchy_score", None)

        top_section.extend(no_domain)
        top_section.sort(key=lambda ld: ld.get("_score", 0), reverse=True)
        rest_section.sort(key=lambda ld: ld.get("_score", 0), reverse=True)
        self.leads = top_section + rest_section

        dm_top = len(top_section)
        dm_rest = len(rest_section)
        self._log(f"   V5.25 Domain Sort: {dm_top} top leads (role hierarchy, max {MAX_PER_DOMAIN}/domain) + {dm_rest} remaining")

        # Clean up internal scoring fields (keep lead_score for CSV)
        for lead in self.leads:
            lead.pop("_score", None)

        # ── Write CSV files ──
        os.makedirs(self.output_folder, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        industry_slug = re.sub(r"[^\w]+", "_", self.industry.lower()).strip("_")
        fieldnames = ["Name", "Company Name", "Domain", "Role", "Phone Number",
                      "Email", "Inferred Email", "All Emails", "Email Type", "Traffic Source",
                      "Founder Verified", "LinkedIn URL", "Paid KW", "Organic KW",
                      "Run Timestamp", "Notes"]

        def _write_csv(filepath, leads_subset):
            with open(filepath, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for lead in leads_subset:
                    notes_parts = []
                    if lead.get("source"):
                        notes_parts.append(f"Source: {lead['source']}")
                    # Determine email type for display — V5.14 four-tier classification
                    # Personal = gmail/yahoo/icloud (is_personal_email) OR verified by Apollo OR
                    #            any word from the lead's name appears in the email local part
                    email_type = ""
                    raw_email = lead.get("email", "")
                    if raw_email:
                        if lead.get("_email_inferred"):
                            email_type = "Inferred"
                        elif is_personal_email(raw_email) or lead.get("_email_verified"):
                            email_type = "Personal"
                        else:
                            # V5.14: Check if any name word appears in email local part
                            _name_for_type = lead.get("name", "")
                            _local = raw_email.lower().split("@")[0]
                            _clean_local = _local.replace(".", "").replace("-", "").replace("_", "")
                            _name_words = [w.lower() for w in _name_for_type.split() if len(w) >= 2]
                            if _name_words and any(nw in _clean_local or nw in _local for nw in _name_words):
                                email_type = "Personal"
                            elif is_work_email(raw_email):
                                email_type = "Work"
                            else:
                                email_type = "Generic"
                    # V5.13: Inferred email — generate candidates when no verified email
                    inferred_email = ""
                    lead_name = lead.get("name", "")
                    lead_domain = lead.get("domain", "")
                    if not raw_email and lead_name and " " in lead_name and lead_domain:
                        parts = lead_name.split()
                        candidates = generate_email_candidates(parts[0], parts[-1], lead_domain)
                        if candidates:
                            inferred_email = candidates[0]
                    # V5.13: All emails — collect every email source
                    all_emails_list = []
                    if raw_email:
                        all_emails_list.append(raw_email)
                    if inferred_email and inferred_email not in all_emails_list:
                        all_emails_list.append(inferred_email)
                    generic_email = lead.get("_generic_email", "")
                    if generic_email and generic_email not in all_emails_list:
                        all_emails_list.append(generic_email)
                    row = {
                        "Name": lead.get("name", ""),
                        "Company Name": lead.get("company", ""),
                        "Domain": lead.get("domain", ""),
                        "Role": lead.get("role", ""),
                        "Phone Number": lead.get("phone", ""),
                        "Email": raw_email,
                        "Inferred Email": inferred_email,
                        "All Emails": " | ".join(all_emails_list),
                        "Email Type": email_type,
                        "Traffic Source": "Paid" if lead.get("_domain_source") == "paid" else "Organic",
                        "Founder Verified": "Yes" if lead.get("_founder_verified") else "",
                        "LinkedIn URL": lead.get("_linkedin_url", ""),
                        "Paid KW": lead.get("_paid_keywords", ""),
                        "Organic KW": lead.get("_organic_keywords", ""),
                        "Run Timestamp": timestamp,
                        "Notes": " | ".join(notes_parts),
                    }
                    writer.writerow(row)

        # CSV 1: ALL leads
        all_filename = f"leads_ALL_{industry_slug}_{self.country}_{timestamp}.csv"
        all_filepath = os.path.join(self.output_folder, all_filename)
        _write_csv(all_filepath, self.leads)
        self._log(f"   Saved ALL {len(self.leads)} leads to: {all_filepath}")

        # CSV 2: TOP leads — V5.17: guarantee name+phone+email in top rows
        # Priority tiers (user requirement: name+phone is absolute must, email+role ideally):
        #   T1: full name + phone + email + role  (perfect lead)
        #   T2: full name + phone + email          (no role)
        #   T3: full name + phone + role           (no email)
        #   T4: full name + phone                  (minimum acceptable)
        #   T5: partial name + phone               (fallback)
        #   T6: phone only / email only / name only (last resort)
        if self.max_leads > 0:
            def _has_full_name(ld):
                return bool(ld.get("name")) and " " in ld.get("name", "")
            _t1 = [ld for ld in self.leads if _has_full_name(ld) and ld.get("phone") and ld.get("email") and ld.get("role")]
            _t2 = [ld for ld in self.leads if _has_full_name(ld) and ld.get("phone") and ld.get("email") and not ld.get("role")]
            _t3 = [ld for ld in self.leads if _has_full_name(ld) and ld.get("phone") and not ld.get("email") and ld.get("role")]
            _t4 = [ld for ld in self.leads if _has_full_name(ld) and ld.get("phone") and not ld.get("email") and not ld.get("role")]
            _t5 = [ld for ld in self.leads if not _has_full_name(ld) and ld.get("phone")]
            _t6 = [ld for ld in self.leads if not ld.get("phone")]
            top_leads = (_t1 + _t2 + _t3 + _t4 + _t5 + _t6)[:self.max_leads]
            top_filename = f"leads_TOP_{self.max_leads}_{industry_slug}_{self.country}_{timestamp}.csv"
        else:
            top_leads = self.leads
            top_filename = f"leads_TOP_all_{industry_slug}_{self.country}_{timestamp}.csv"
        top_filepath = os.path.join(self.output_folder, top_filename)
        _write_csv(top_filepath, top_leads)
        # V5.17: Log completeness of top leads (user requirement: name+phone minimum)
        top_with_phone = sum(1 for ld in top_leads if ld.get("phone"))
        top_with_email = sum(1 for ld in top_leads if ld.get("email"))
        top_complete = sum(1 for ld in top_leads if ld.get("phone") and ld.get("email") and " " in ld.get("name",""))
        self._log(f"   Saved TOP {len(top_leads)} leads → {top_with_phone} with phone, "
                  f"{top_with_email} with email, {top_complete} fully complete (name+phone+email)")

        # V5.17: COMPREHENSIVE TOKEN & LEAD SUMMARY
        self._log("\n" + "="*80)
        self._log("V5.17 RUN SUMMARY — TOKEN USAGE & LEAD STATISTICS")
        self._log("="*80)

        # Lead source breakdown (Paid vs Organic)
        paid_leads = sum(1 for ld in self.leads if ld.get("_domain_source") == "paid")
        organic_leads = sum(1 for ld in self.leads if ld.get("_domain_source") == "organic")
        self._log(f"\nLead Sources:")
        self._log(f"  • PAID (Google Ads):     {paid_leads} leads")
        self._log(f"  • ORGANIC:               {organic_leads} leads")
        self._log(f"  • TOTAL:                 {len(self.leads)} leads")

        # Contact data coverage
        with_phone = sum(1 for ld in self.leads if ld.get("phone"))
        with_email = sum(1 for ld in self.leads if ld.get("email"))
        personal_emails = sum(1 for ld in self.leads if ld.get("email") and is_personal_email(ld.get("email", "")))
        work_emails = sum(1 for ld in self.leads if ld.get("email") and is_work_email(ld.get("email", "")))
        generic_emails = with_email - personal_emails - work_emails
        self._log(f"\nContact Data Coverage:")
        self._log(f"  • With Phone:            {with_phone} leads")
        self._log(f"  • With Email:            {with_email} leads")
        self._log(f"    - Personal (Gmail/Yahoo): {personal_emails} leads")
        self._log(f"    - Work (firstname@co):    {work_emails} leads")
        self._log(f"    - Generic (info@):        {generic_emails} leads")

        # API token usage this run
        self._log(f"\nAPI Token Usage This Run:")
        self._log(f"  • SEMrush:               {self._api_counter.get('semrush', 0)} API calls")
        self._log(f"  • Apollo:                {self._api_counter.get('apollo', 0)} API calls")
        self._log(f"  • Lusha:                 {self._api_counter.get('lusha', 0)} API calls")
        self._log(f"  • SerpApi:               {self._api_counter.get('serpapi', 0)} API calls")
        self._log(f"  • OpenAI:                {self._api_counter.get('openai', 0)} API calls")

        # Enrichment efficiency
        leads_enriched = self._api_counter.get('apollo', 0) + self._api_counter.get('lusha', 0)
        leads_with_phone_enrichment = sum(1 for ld in self.leads if ld.get("_direct_phone"))
        leads_with_email_enrichment = sum(1 for ld in self.leads if ld.get("_email_verified"))
        self._log(f"\nEnrichment Efficiency (for top {self.max_leads or len(self.leads)} leads):")
        self._log(f"  • Leads with direct phone (API enriched): {leads_with_phone_enrichment}")
        self._log(f"  • Leads with verified email (API enriched): {leads_with_email_enrichment}")
        self._log(f"  • Total enrichment API calls:         {leads_enriched}")

        # V5.13: Credit cost summary
        total_credits = sum(
            self._api_counter.get(svc, 0) * API_CREDIT_COSTS.get(svc, 0)
            for svc in API_CREDIT_COSTS
        )
        founders_verified = sum(1 for ld in self.leads if ld.get("_founder_verified"))
        scraped_emails = sum(1 for ld in self.leads if "Scrape" in (ld.get("source") or "") and ld.get("email"))
        avg_score = sum(ld.get("lead_score", 0) for ld in self.leads) / max(len(self.leads), 1)

        self._log(f"\nV5.13 Summary:")
        self._log(f"  • Leads generated:          {len(self.leads)}")
        self._log(f"  • Average lead score:       {avg_score:.1f}")
        self._log(f"  • Total credits used:       {total_credits:.0f}")
        self._log(f"  • Founders verified (WHOIS):{founders_verified}")
        self._log(f"  • Emails found via website: {scraped_emails}")
        self._log(f"  • Competitor domains added:  {self._competitor_domains_added}")
        self._log("\n" + "="*80)

        self._progress(100, f"Done! {len(top_leads)} top leads + {len(self.leads)} total exported")
        return top_filepath


# ══════════════════════════════════════════════════════════════════════════════
# GUI APPLICATION — Dark Theme
# ══════════════════════════════════════════════════════════════════════════════

# Lazy-load tkinter — only imported when GUI is actually started (main())
# This allows the pipeline/API classes above to be imported without tkinter
tk = None
ttk = None
filedialog = None
messagebox = None

# Color palette
COLORS = {
    "bg_dark": "#1a1a2e",
    "bg_medium": "#16213e",
    "bg_light": "#0f3460",
    "bg_card": "#1f2b47",
    "accent": "#e94560",
    "accent_hover": "#ff6b81",
    "text_primary": "#eaeaea",
    "text_secondary": "#a0a0b0",
    "text_muted": "#6c6c80",
    "success": "#2ecc71",
    "warning": "#f39c12",
    "error": "#e74c3c",
    "input_bg": "#0d1b2a",
    "input_border": "#1b2838",
    "button_bg": "#e94560",
    "button_fg": "#ffffff",
    "progress_bg": "#0d1b2a",
    "progress_fg": "#e94560",
    "log_bg": "#0a0f1a",
}


class LeadGeneratorApp:
    """Main GUI application with dark theme."""

    def __init__(self, root):
        self.root = root
        self.root.title("Lead Generation Pro V5.4")
        self.root.geometry("820x740")
        self.root.minsize(780, 700)
        self.root.configure(bg=COLORS["bg_dark"])

        self.pipeline = None
        self.pipeline_thread = None

        self._build_ui()
        self._center_window()
        # Show API key status check after window is visible
        self.root.after(500, self._show_api_status_window)

    def _center_window(self):
        self.root.update_idletasks()
        w = self.root.winfo_width()
        h = self.root.winfo_height()
        sw = self.root.winfo_screenwidth()
        sh = self.root.winfo_screenheight()
        x = (sw - w) // 2
        y = (sh - h) // 2
        self.root.geometry(f"+{x}+{y}")

    def _show_api_status_window(self):
        """Check all API keys in background and show a compact status popup."""
        win = tk.Toplevel(self.root)
        win.title("API Key Status")
        win.configure(bg=COLORS["bg_dark"])
        win.resizable(False, False)

        tk.Label(win, text="API Key Status", bg=COLORS["bg_dark"], fg=COLORS["accent"],
                 font=("Segoe UI", 12, "bold")).pack(anchor=tk.W, padx=18, pady=(12, 6))

        # Build status rows
        status_labels = {}
        keys_to_check = [
            ("apollo",   "Apollo"),
            ("lusha",    "Lusha"),
            ("semrush",  "SEMrush"),
            ("serpapi",  "SerpApi"),
            ("openai",   "OpenAI"),
        ]
        for key_id, display_name in keys_to_check:
            fr = tk.Frame(win, bg=COLORS["bg_dark"])
            fr.pack(fill=tk.X, padx=24, pady=2)
            tk.Label(fr, text=display_name, bg=COLORS["bg_dark"], fg=COLORS["text_secondary"],
                     font=("Segoe UI", 9), width=12, anchor=tk.W).pack(side=tk.LEFT)
            lbl = tk.Label(fr, text="Checking…", bg=COLORS["bg_dark"],
                           fg=COLORS["text_muted"], font=("Segoe UI", 9, "bold"))
            lbl.pack(side=tk.LEFT)
            status_labels[key_id] = lbl

        close_btn = tk.Button(
            win, text="  OK  ",
            bg=COLORS["accent"], fg=COLORS["button_fg"],
            activebackground=COLORS["accent_hover"],
            font=("Segoe UI", 9, "bold"), relief=tk.FLAT, padx=16, pady=4,
            command=win.destroy,
        )
        close_btn.pack(pady=(8, 14))

        # Centre
        win.update_idletasks()
        mw = self.root.winfo_width(); mx = self.root.winfo_x()
        mh = self.root.winfo_height(); my = self.root.winfo_y()
        ww = win.winfo_width(); wh = win.winfo_height()
        win.geometry(f"+{mx + (mw - ww)//2}+{my + (mh - wh)//2}")

        def _check_keys():
            """Run key checks in background thread; update labels via after()."""
            results = {}

            # Apollo
            try:
                key = API_KEYS.get("apollo", "")
                if not key:
                    results["apollo"] = ("No key", False)
                else:
                    r = requests.get(
                        "https://api.apollo.io/api/v1/auth/health",
                        headers={"X-Api-Key": key}, timeout=10,
                    )
                    if r.status_code == 200:
                        data = r.json()
                        credits = data.get("data", {}).get("credits_used_this_month", "?")
                        results["apollo"] = (f"OK  (used: {credits})", True)
                    else:
                        results["apollo"] = (f"Error {r.status_code}", False)
            except Exception as e:
                results["apollo"] = (f"Unreachable", False)

            # Lusha
            try:
                key = API_KEYS.get("lusha", "")
                if not key:
                    results["lusha"] = ("No key", False)
                else:
                    r = requests.get(
                        "https://api.lusha.com/account",
                        headers={"api_key": key}, timeout=10,
                    )
                    if r.status_code == 200:
                        results["lusha"] = ("OK", True)
                    else:
                        results["lusha"] = (f"Error {r.status_code}", False)
            except Exception:
                results["lusha"] = ("Unreachable", False)

            # SEMrush
            try:
                key = API_KEYS.get("semrush", "")
                if not key:
                    results["semrush"] = ("No key", False)
                else:
                    r = requests.get(
                        f"https://api.semrush.com/?type=phrase_this&key={key}&phrase=test&export_columns=Ph,Nq&database=au",
                        timeout=10,
                    )
                    if r.status_code == 200 and "ERROR" not in r.text[:30]:
                        results["semrush"] = ("OK", True)
                    elif "CREDITS" in r.text.upper() or "limit" in r.text.lower():
                        results["semrush"] = ("No credits", False)
                    else:
                        results["semrush"] = (f"Error {r.status_code}", False)
            except Exception:
                results["semrush"] = ("Unreachable", False)

            # SerpApi
            try:
                key = API_KEYS.get("serpapi", "")
                if not key:
                    results["serpapi"] = ("No key", False)
                else:
                    r = requests.get(
                        "https://serpapi.com/account",
                        params={"api_key": key}, timeout=10,
                    )
                    if r.status_code == 200:
                        data = r.json()
                        remaining = data.get("total_searches_left", "?")
                        results["serpapi"] = (f"OK  ({remaining} left)", True)
                    else:
                        results["serpapi"] = (f"Error {r.status_code}", False)
            except Exception:
                results["serpapi"] = ("Unreachable", False)

            # OpenAI
            try:
                key = API_KEYS.get("openai", "")
                if not key:
                    results["openai"] = ("No key", False)
                else:
                    r = requests.get(
                        "https://api.openai.com/v1/models",
                        headers={"Authorization": f"Bearer {key}"}, timeout=10,
                    )
                    if r.status_code == 200:
                        results["openai"] = ("OK", True)
                    elif r.status_code == 401:
                        results["openai"] = ("Invalid key", False)
                    else:
                        results["openai"] = (f"Error {r.status_code}", False)
            except Exception:
                results["openai"] = ("Unreachable", False)

            # Update GUI from main thread
            def _apply():
                if not win.winfo_exists():
                    return
                for key_id, (msg, ok) in results.items():
                    lbl = status_labels.get(key_id)
                    if lbl:
                        lbl.configure(
                            text=("✓ " if ok else "✗ ") + msg,
                            fg=COLORS["success"] if ok else COLORS["error"],
                        )
            win.after(0, _apply)

        import threading as _threading
        _threading.Thread(target=_check_keys, daemon=True).start()

    def _build_ui(self):
        style = ttk.Style()
        style.theme_use("clam")

        style.configure("Dark.TFrame", background=COLORS["bg_dark"])
        style.configure("Card.TFrame", background=COLORS["bg_card"])
        style.configure(
            "Dark.TLabel", background=COLORS["bg_dark"],
            foreground=COLORS["text_primary"], font=("Segoe UI", 10),
        )
        style.configure(
            "CardLabel.TLabel", background=COLORS["bg_card"],
            foreground=COLORS["text_primary"], font=("Segoe UI", 10),
        )
        style.configure(
            "Header.TLabel", background=COLORS["bg_dark"],
            foreground=COLORS["accent"], font=("Segoe UI", 22, "bold"),
        )
        style.configure(
            "SubHeader.TLabel", background=COLORS["bg_dark"],
            foreground=COLORS["text_secondary"], font=("Segoe UI", 10),
        )
        style.configure(
            "Dark.TCombobox",
            fieldbackground=COLORS["input_bg"], background=COLORS["bg_light"],
            foreground=COLORS["text_primary"],
            selectbackground=COLORS["accent"], selectforeground=COLORS["button_fg"],
        )
        style.configure(
            "Dark.Horizontal.TProgressbar",
            troughcolor=COLORS["progress_bg"], background=COLORS["progress_fg"], thickness=8,
        )

        # Main container
        main = ttk.Frame(self.root, style="Dark.TFrame", padding=20)
        main.pack(fill=tk.BOTH, expand=True)

        # Header
        ttk.Label(main, text="Lead Generation Pro", style="Header.TLabel").pack(anchor=tk.W, pady=(0, 2))
        ttk.Label(main, text="Discover & enrich B2B leads automatically", style="SubHeader.TLabel").pack(
            anchor=tk.W, pady=(0, 15)
        )

        # Input Card
        card = ttk.Frame(main, style="Card.TFrame", padding=15)
        card.pack(fill=tk.X, pady=(0, 12))

        # Row 1: Industry + Country
        row1 = ttk.Frame(card, style="Card.TFrame")
        row1.pack(fill=tk.X, pady=(0, 10))

        ind_frame = ttk.Frame(row1, style="Card.TFrame")
        ind_frame.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 10))
        ttk.Label(ind_frame, text="Industry", style="CardLabel.TLabel").pack(anchor=tk.W)
        self.industry_var = tk.StringVar(value="Dentist")
        self.industry_combo = ttk.Combobox(
            ind_frame, textvariable=self.industry_var,
            values=sorted(INDUSTRY_KEYWORDS.keys()),
            style="Dark.TCombobox", state="normal", font=("Segoe UI", 10),
        )
        self.industry_combo.pack(fill=tk.X, pady=(3, 0))

        country_frame = ttk.Frame(row1, style="Card.TFrame")
        country_frame.pack(side=tk.LEFT, fill=tk.X, expand=True)
        ttk.Label(country_frame, text="Country", style="CardLabel.TLabel").pack(anchor=tk.W)
        self.country_var = tk.StringVar(value="AU")
        self.country_combo = ttk.Combobox(
            country_frame, textvariable=self.country_var,
            values=["AU", "USA", "UK", "India"],
            style="Dark.TCombobox", state="readonly", font=("Segoe UI", 10),
        )
        self.country_combo.pack(fill=tk.X, pady=(3, 0))

        # Row 2: Volume + CPC
        row2 = ttk.Frame(card, style="Card.TFrame")
        row2.pack(fill=tk.X, pady=(0, 10))

        vol_frame = ttk.Frame(row2, style="Card.TFrame")
        vol_frame.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 10))
        ttk.Label(vol_frame, text="Min Search Volume", style="CardLabel.TLabel").pack(anchor=tk.W)
        self.volume_var = tk.StringVar(value="50")
        tk.Entry(
            vol_frame, textvariable=self.volume_var,
            bg=COLORS["input_bg"], fg=COLORS["text_primary"],
            insertbackground=COLORS["text_primary"], font=("Segoe UI", 10),
            relief=tk.FLAT, bd=5,
        ).pack(fill=tk.X, pady=(3, 0))

        cpc_frame = ttk.Frame(row2, style="Card.TFrame")
        cpc_frame.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 10))
        ttk.Label(cpc_frame, text="Min CPC ($)", style="CardLabel.TLabel").pack(anchor=tk.W)
        self.cpc_var = tk.StringVar(value="1.0")
        tk.Entry(
            cpc_frame, textvariable=self.cpc_var,
            bg=COLORS["input_bg"], fg=COLORS["text_primary"],
            insertbackground=COLORS["text_primary"], font=("Segoe UI", 10),
            relief=tk.FLAT, bd=5,
        ).pack(fill=tk.X, pady=(3, 0))

        max_leads_frame = ttk.Frame(row2, style="Card.TFrame")
        max_leads_frame.pack(side=tk.LEFT, fill=tk.X, expand=True)
        ttk.Label(max_leads_frame, text="Max Leads (0=all)", style="CardLabel.TLabel").pack(anchor=tk.W)
        self.max_leads_var = tk.StringVar(value="50")
        tk.Entry(
            max_leads_frame, textvariable=self.max_leads_var,
            bg=COLORS["input_bg"], fg=COLORS["text_primary"],
            insertbackground=COLORS["text_primary"], font=("Segoe UI", 10),
            relief=tk.FLAT, bd=5,
        ).pack(fill=tk.X, pady=(3, 0))

        # Row 3: Output folder
        row3 = ttk.Frame(card, style="Card.TFrame")
        row3.pack(fill=tk.X)
        ttk.Label(row3, text="Output Folder", style="CardLabel.TLabel").pack(anchor=tk.W)
        folder_row = ttk.Frame(row3, style="Card.TFrame")
        folder_row.pack(fill=tk.X, pady=(3, 0))

        self.folder_var = tk.StringVar(value=self._default_output_folder())
        tk.Entry(
            folder_row, textvariable=self.folder_var,
            bg=COLORS["input_bg"], fg=COLORS["text_primary"],
            insertbackground=COLORS["text_primary"], font=("Segoe UI", 10),
            relief=tk.FLAT, bd=5,
        ).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))

        tk.Button(
            folder_row, text="Browse", bg=COLORS["bg_light"], fg=COLORS["text_primary"],
            font=("Segoe UI", 9), relief=tk.FLAT, padx=10, pady=4,
            command=self._browse_folder,
        ).pack(side=tk.RIGHT)

        # Action Buttons
        btn_frame = ttk.Frame(main, style="Dark.TFrame")
        btn_frame.pack(fill=tk.X, pady=(0, 12))

        self.generate_btn = tk.Button(
            btn_frame, text="   Generate Leads   ",
            bg=COLORS["accent"], fg=COLORS["button_fg"],
            activebackground=COLORS["accent_hover"], activeforeground=COLORS["button_fg"],
            font=("Segoe UI", 12, "bold"), relief=tk.FLAT, padx=25, pady=8,
            cursor="hand2", command=self._on_generate,
        )
        self.generate_btn.pack(side=tk.LEFT)

        self.cancel_btn = tk.Button(
            btn_frame, text="  Cancel  ",
            bg=COLORS["bg_light"], fg=COLORS["text_primary"],
            activebackground=COLORS["bg_medium"],
            font=("Segoe UI", 10), relief=tk.FLAT, padx=15, pady=6,
            state=tk.DISABLED, command=self._on_cancel,
        )
        self.cancel_btn.pack(side=tk.LEFT, padx=(10, 0))

        self.status_var = tk.StringVar(value="Ready")
        tk.Label(
            btn_frame, textvariable=self.status_var,
            bg=COLORS["bg_dark"], fg=COLORS["text_secondary"],
            font=("Segoe UI", 10), anchor=tk.E,
        ).pack(side=tk.RIGHT)

        # Progress Bar
        self.progress_var = tk.DoubleVar(value=0)
        ttk.Progressbar(
            main, variable=self.progress_var, maximum=100, mode="determinate",
            style="Dark.Horizontal.TProgressbar",
        ).pack(fill=tk.X, pady=(0, 12))

        # Run Statistics Box (V5.10+)
        stats_card = tk.Frame(main, bg=COLORS["bg_card"], padx=10, pady=6)
        stats_card.pack(fill=tk.X, pady=(0, 8))
        tk.Label(
            stats_card, text="Run Statistics",
            bg=COLORS["bg_card"], fg=COLORS["text_secondary"],
            font=("Segoe UI", 9, "bold"),
        ).pack(anchor=tk.W)
        # Row 1: Lead source breakdown
        row1 = tk.Frame(stats_card, bg=COLORS["bg_card"])
        row1.pack(fill=tk.X, pady=(3, 0))
        self.stat_total_var = tk.StringVar(value="Total: —")
        self.stat_paid_var = tk.StringVar(value="Paid Ads: —")
        self.stat_organic_var = tk.StringVar(value="Organic: —")
        self.stat_phone_var = tk.StringVar(value="w/ Phone: —")
        self.stat_email_var = tk.StringVar(value="Personal Email: —")
        for var, fg in [
            (self.stat_total_var, COLORS["text_primary"]),
            (self.stat_paid_var, COLORS["success"]),
            (self.stat_organic_var, COLORS["warning"]),
            (self.stat_phone_var, COLORS["accent"]),
            (self.stat_email_var, COLORS["text_secondary"]),
        ]:
            tk.Label(row1, textvariable=var, bg=COLORS["bg_card"], fg=fg,
                     font=("Segoe UI", 9), padx=8).pack(side=tk.LEFT)
        # Row 2: API call and credit counters
        row2 = tk.Frame(stats_card, bg=COLORS["bg_card"])
        row2.pack(fill=tk.X, pady=(2, 0))
        self.stat_apollo_var = tk.StringVar(value="Apollo: 0")
        self.stat_lusha_var = tk.StringVar(value="Lusha: 0")
        self.stat_semrush_var = tk.StringVar(value="SEMrush: 0")
        self.stat_phone_cr_var = tk.StringVar(value="Phone Credits: 0")
        self.stat_email_cr_var = tk.StringVar(value="Email Credits: 0")
        for var in [self.stat_apollo_var, self.stat_lusha_var, self.stat_semrush_var,
                    self.stat_phone_cr_var, self.stat_email_cr_var]:
            tk.Label(row2, textvariable=var, bg=COLORS["bg_card"], fg=COLORS["text_muted"],
                     font=("Segoe UI", 8), padx=8).pack(side=tk.LEFT)

        # Log Panel
        ttk.Label(main, text="Activity Log", style="Dark.TLabel").pack(anchor=tk.W, pady=(0, 3))
        log_frame = tk.Frame(main, bg=COLORS["log_bg"])
        log_frame.pack(fill=tk.BOTH, expand=True)

        self.log_text = tk.Text(
            log_frame, bg=COLORS["log_bg"], fg=COLORS["text_secondary"],
            font=("Consolas", 9), relief=tk.FLAT, wrap=tk.WORD,
            state=tk.DISABLED, padx=10, pady=8, spacing1=2,
        )
        scrollbar = tk.Scrollbar(log_frame, command=self.log_text.yview)
        self.log_text.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        self.log_text.tag_configure("success", foreground=COLORS["success"])
        self.log_text.tag_configure("error", foreground=COLORS["error"])
        self.log_text.tag_configure("warning", foreground=COLORS["warning"])

    def _default_output_folder(self) -> str:
        if platform.system() == "Windows":
            return r"C:\AI LEAD GENERATION AGENT ai code\___LEADS GENERATED___"
        return os.path.join(os.path.expanduser("~"), "LeadGen_Output")

    def _browse_folder(self):
        folder = filedialog.askdirectory(title="Select Output Folder", initialdir=self.folder_var.get())
        if folder:
            self.folder_var.set(folder)

    def _validate_inputs(self) -> bool:
        if not self.industry_var.get().strip():
            messagebox.showwarning("Input Required", "Please enter an industry.")
            return False
        try:
            vol = int(self.volume_var.get())
            if vol < 0:
                raise ValueError
        except ValueError:
            messagebox.showwarning("Invalid Input", "Min Search Volume must be a positive number.")
            return False
        try:
            cpc = float(self.cpc_var.get())
            if cpc < 0:
                raise ValueError
        except ValueError:
            messagebox.showwarning("Invalid Input", "Min CPC must be a positive number.")
            return False
        try:
            max_leads = int(self.max_leads_var.get())
            if max_leads < 0:
                raise ValueError
        except ValueError:
            messagebox.showwarning("Invalid Input", "Max Leads must be a non-negative integer (0 = unlimited).")
            return False
        if not self.folder_var.get().strip():
            messagebox.showwarning("Input Required", "Please specify an output folder.")
            return False
        return True

    def _on_generate(self):
        if not self._validate_inputs():
            return
        self.generate_btn.configure(state=tk.DISABLED)
        self.cancel_btn.configure(state=tk.NORMAL)
        self.progress_var.set(0)
        self._clear_log()

        self.pipeline = LeadGenerationPipeline(
            industry=self.industry_var.get().strip(),
            country=self.country_var.get().strip(),
            min_volume=int(self.volume_var.get()),
            min_cpc=float(self.cpc_var.get()),
            output_folder=self.folder_var.get().strip(),
            progress_callback=self._update_progress_safe,
            log_callback=self._append_log_safe,
            max_leads=int(self.max_leads_var.get()),
        )
        self.pipeline_thread = threading.Thread(target=self._run_pipeline, daemon=True)
        self.pipeline_thread.start()

    def _run_pipeline(self):
        result_path = self.pipeline.run()
        self.root.after(0, self._on_pipeline_done, result_path)

    def _on_pipeline_done(self, result_path: str):
        self.generate_btn.configure(state=tk.NORMAL)
        self.cancel_btn.configure(state=tk.DISABLED)
        if result_path:
            leads = self.pipeline.leads if self.pipeline else []
            count = len(leads)
            personal_count = sum(1 for ld in leads if is_personal_email(ld.get("email", "")) or ld.get("_email_verified"))
            work_count = sum(1 for ld in leads if not is_personal_email(ld.get("email", "")) and is_work_email(ld.get("email", "")))
            generic_count = sum(1 for ld in leads if ld.get("email") and not is_personal_email(ld.get("email", "")) and not is_work_email(ld.get("email", "")))
            no_email_count = sum(1 for ld in leads if not ld.get("email"))
            paid_count = sum(1 for ld in leads if ld.get("_domain_source") == "paid")
            organic_count = sum(1 for ld in leads if ld.get("_domain_source") == "organic")
            phone_count = sum(1 for ld in leads if ld.get("phone"))
            api = self.pipeline._api_counter if self.pipeline else {}
            phone_cr = getattr(self.pipeline, "_phone_credits_used", 0)
            email_cr = getattr(self.pipeline, "_email_credits_used", 0)
            self.status_var.set(f"Done! {count} leads exported")
            self._refresh_stats()
            self._show_run_summary(
                count=count,
                paid_count=paid_count, organic_count=organic_count,
                phone_count=phone_count,
                personal_count=personal_count, work_count=work_count,
                generic_count=generic_count, no_email_count=no_email_count,
                apollo_calls=api.get("apollo", 0), lusha_calls=api.get("lusha", 0),
                semrush_calls=api.get("semrush", 0), serpapi_calls=api.get("serpapi", 0),
                phone_cr=phone_cr, email_cr=email_cr,
                result_path=result_path,
            )
        elif self.pipeline and self.pipeline._cancelled:
            self.status_var.set("Cancelled")
        else:
            self.status_var.set("Completed (no leads found)")
            messagebox.showwarning(
                "No Results",
                "No leads were found. Try a different industry or lower the search volume/CPC thresholds.",
            )

    def _show_run_summary(self, count, paid_count, organic_count, phone_count,
                          personal_count, work_count, generic_count, no_email_count,
                          apollo_calls, lusha_calls, semrush_calls, serpapi_calls,
                          phone_cr, email_cr, result_path):
        """Show a dedicated Toplevel run-summary panel instead of a plain messagebox."""
        win = tk.Toplevel(self.root)
        win.title("Run Summary")
        win.configure(bg=COLORS["bg_dark"])
        win.resizable(False, False)
        win.grab_set()

        pad = dict(padx=18, pady=6)

        def section(parent, title):
            tk.Label(parent, text=title, bg=COLORS["bg_dark"],
                     fg=COLORS["accent"], font=("Segoe UI", 10, "bold")).pack(anchor=tk.W, padx=18, pady=(10, 2))
            sep = tk.Frame(parent, bg=COLORS["bg_light"], height=1)
            sep.pack(fill=tk.X, padx=18, pady=(0, 4))

        def row(parent, label, value, value_color=None):
            fr = tk.Frame(parent, bg=COLORS["bg_dark"])
            fr.pack(fill=tk.X, padx=24, pady=1)
            tk.Label(fr, text=label, bg=COLORS["bg_dark"], fg=COLORS["text_secondary"],
                     font=("Segoe UI", 9), width=28, anchor=tk.W).pack(side=tk.LEFT)
            tk.Label(fr, text=str(value), bg=COLORS["bg_dark"],
                     fg=value_color or COLORS["text_primary"],
                     font=("Segoe UI", 9, "bold")).pack(side=tk.LEFT)

        # Header
        tk.Label(win, text=f"Run Complete — {count} Leads",
                 bg=COLORS["bg_dark"], fg=COLORS["text_primary"],
                 font=("Segoe UI", 14, "bold")).pack(anchor=tk.W, **pad)

        # Lead Sources
        section(win, "Lead Sources")
        row(win, "Total leads generated", count)
        row(win, "Paid Ads domains", paid_count, COLORS["success"])
        row(win, "Organic domains", organic_count, COLORS["warning"])

        # Contact Data Coverage
        section(win, "Contact Data Coverage")
        row(win, "Leads with phone number", phone_count, COLORS["success"] if phone_count == count else COLORS["warning"])
        row(win, "Personal email (gmail/yahoo)", personal_count, COLORS["success"] if personal_count > 0 else COLORS["error"])
        row(win, "Work email (firstname@company)", work_count, COLORS["warning"])
        row(win, "Generic email (info@/contact@)", generic_count, COLORS["text_muted"])
        row(win, "No email found", no_email_count, COLORS["error"] if no_email_count > 0 else COLORS["text_muted"])

        # API Credit Usage
        section(win, "API Credits Used This Run")
        row(win, "Apollo API calls", apollo_calls)
        row(win, "Lusha API calls", lusha_calls)
        row(win, "SEMrush units", semrush_calls)
        row(win, "SerpApi credits", serpapi_calls)
        row(win, "Credits → phone numbers", phone_cr, COLORS["accent"])
        row(win, "Credits → personal emails", email_cr, COLORS["accent"])

        # Output path
        section(win, "Output Files")
        path_fr = tk.Frame(win, bg=COLORS["bg_dark"])
        path_fr.pack(fill=tk.X, padx=24, pady=(2, 6))
        tk.Label(path_fr, text=result_path, bg=COLORS["bg_dark"], fg=COLORS["text_secondary"],
                 font=("Consolas", 8), wraplength=460, anchor=tk.W, justify=tk.LEFT).pack(anchor=tk.W)

        # Close button
        tk.Button(
            win, text="  Close  ",
            bg=COLORS["accent"], fg=COLORS["button_fg"],
            activebackground=COLORS["accent_hover"],
            font=("Segoe UI", 10, "bold"), relief=tk.FLAT, padx=20, pady=6,
            command=win.destroy,
        ).pack(pady=(6, 16))

        # Centre relative to main window
        win.update_idletasks()
        mw = self.root.winfo_width()
        mh = self.root.winfo_height()
        mx = self.root.winfo_x()
        my = self.root.winfo_y()
        ww = win.winfo_width()
        wh = win.winfo_height()
        win.geometry(f"+{mx + (mw - ww)//2}+{my + (mh - wh)//2}")

    def _on_cancel(self):
        if self.pipeline:
            self.pipeline.cancel()
            self.cancel_btn.configure(state=tk.DISABLED)
            self.status_var.set("Cancelling...")

    def _update_progress_safe(self, pct: int, status: str = ""):
        self.root.after(0, self._update_progress, pct, status)

    def _update_progress(self, pct: int, status: str = ""):
        self.progress_var.set(pct)
        if status:
            self.status_var.set(status)
        self._refresh_stats()

    def _refresh_stats(self):
        """V5.10+: Update the Run Statistics panel from current pipeline state."""
        p = self.pipeline
        if not p:
            return
        leads = getattr(p, "leads", None) or []
        total = len(leads)
        paid_count = sum(1 for ld in leads if ld.get("_domain_source") == "paid")
        organic_count = sum(1 for ld in leads if ld.get("_domain_source") == "organic")
        phone_count = sum(1 for ld in leads if ld.get("phone"))
        email_count = sum(1 for ld in leads if is_personal_email(ld.get("email", "")))
        api = getattr(p, "_api_counter", {})

        self.stat_total_var.set(f"Total: {total}")
        self.stat_paid_var.set(f"Paid Ads: {paid_count}")
        self.stat_organic_var.set(f"Organic: {organic_count}")
        self.stat_phone_var.set(f"w/ Phone: {phone_count}")
        self.stat_email_var.set(f"Personal Email: {email_count}")
        self.stat_apollo_var.set(f"Apollo: {api.get('apollo', 0)}")
        self.stat_lusha_var.set(f"Lusha: {api.get('lusha', 0)}")
        self.stat_semrush_var.set(f"SEMrush: {api.get('semrush', 0)}")
        self.stat_phone_cr_var.set(f"Phone Credits: {getattr(p, '_phone_credits_used', 0)}")
        self.stat_email_cr_var.set(f"Email Credits: {getattr(p, '_email_credits_used', 0)}")

    def _append_log_safe(self, message: str):
        self.root.after(0, self._append_log, message)

    def _append_log(self, message: str):
        self.log_text.configure(state=tk.NORMAL)
        tag = ""
        if "Done" in message or "Saved" in message or "Total" in message:
            tag = "success"
        elif "Error" in message or "error" in message:
            tag = "error"
        elif "Warning" in message or "warning" in message:
            tag = "warning"
        self.log_text.insert(tk.END, message + "\n", tag if tag else ())
        self.log_text.see(tk.END)
        self.log_text.configure(state=tk.DISABLED)

    def _clear_log(self):
        self.log_text.configure(state=tk.NORMAL)
        self.log_text.delete("1.0", tk.END)
        self.log_text.configure(state=tk.DISABLED)


# ══════════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def main_gui():
    import tkinter as tk_mod
    from tkinter import filedialog as fd_mod, messagebox as mb_mod, ttk as ttk_mod

    global tk, ttk, filedialog, messagebox
    tk = tk_mod
    ttk = ttk_mod
    filedialog = fd_mod
    messagebox = mb_mod

    root = tk.Tk()
    LeadGeneratorApp(root)
    root.mainloop()


# ══════════════════════════════════════════════════════════════════════════════
# EMBEDDED WEB SERVER (replaces server.py — run with: python V5.py)
# ══════════════════════════════════════════════════════════════════════════════

def main_web():
    """Start the Flask web server and auto-open the browser."""
    import csv as _csv
    import uuid as _uuid
    import webbrowser

    from flask import Flask, jsonify, request as flask_request, send_from_directory
    from flask_cors import CORS

    _DIR = os.path.dirname(os.path.abspath(__file__))
    app = Flask(__name__, static_folder=_DIR)
    CORS(app)

    _jobs = {}
    _credits_cache = {"data": None, "timestamp": 0}
    _CACHE_TTL = 300

    # V5.26: Apollo phone reveal webhook endpoint
    @app.route(_APOLLO_PHONE_CALLBACK_PATH, methods=['POST'])
    def apollo_phone_callback():
        """Receives async phone data from Apollo after a reveal request.
        Apollo payload: {"status":"success","people":[{"id":"...","status":"success","phone_numbers":[...]}]}"""
        data = flask_request.get_json(silent=True)
        if data:
            # Apollo wraps results in a "people" array
            people = data.get("people", [])
            for person in people:
                person_id = person.get("id", "")
                phone_numbers = person.get("phone_numbers") or []
                if person_id and phone_numbers:
                    _receive_phone_reveal(person_id, phone_numbers)
        return jsonify({"status": "ok"}), 200

    # ── Job State ────────────────────────────────────────────────────────────
    class _JobState:
        def __init__(self):
            self.progress = 0
            self.status_text = "Starting..."
            self.state = "running"
            self.logs = []
            self.log_cursor = 0
            self.leads = []
            self.top_csv = ""
            self.all_csv = ""
            self.error = ""
            self.pipeline = None
            self.api_usage = {}  # V5.7: Per-run API call counts
            self.summary = {}   # V5.13: Token usage & lead statistics
            self.start_time = time.time()  # V5.14: Track start for time remaining

    # ── Credit Checkers ──────────────────────────────────────────────────────
    def _check_apollo():
        try:
            r = requests.get(
                "https://api.apollo.io/api/v1/auth/health",
                headers={"X-Api-Key": API_KEYS.get("apollo", "")},
                timeout=10,
            )
            if r.status_code == 200:
                data = r.json()
                plan = data.get("plan", {})
                usage = data.get("usage", {})
                total = plan.get("credits", 10000)
                used = usage.get("credits_used", 0)
                remaining = max(0, total - used)
                return {"service": "Apollo", "status": "ok", "total": total, "used": used,
                        "remaining": remaining, "pct_remaining": round(remaining / max(total, 1) * 100, 1),
                        "searches_remaining": remaining // 2}
            return {"service": "Apollo", "status": "error", "error": f"HTTP {r.status_code}",
                    "total": 0, "used": 0, "remaining": 0, "pct_remaining": 0, "searches_remaining": 0}
        except Exception as e:
            return {"service": "Apollo", "status": "error", "error": str(e),
                    "total": 0, "used": 0, "remaining": 0, "pct_remaining": 0, "searches_remaining": 0}

    def _check_lusha():
        """V5.7: Validate Lusha key + local call tracking (Lusha has no credit balance API)."""
        try:
            r = requests.get(
                "https://api.lusha.com/v2/company",
                headers={"api_key": API_KEYS.get("lusha", "")},
                params={"domain": "example.com"}, timeout=10,
            )
            if r.status_code in (401, 403):
                return {"service": "Lusha", "status": "error", "error": "API key invalid or expired",
                        "total": 0, "used": 0, "remaining": 0, "pct_remaining": 0, "searches_remaining": 0}
            # Key is valid — compute local tracking
            total = LUSHA_PLAN_CREDITS
            used = _lusha_calls_total
            remaining = max(0, total - used)
            pct = round(remaining / max(total, 1) * 100, 1)
            return {"service": "Lusha", "status": "ok", "total": total, "used": used,
                    "remaining": remaining, "pct_remaining": pct,
                    "searches_remaining": remaining // 2,
                    "note": "Locally tracked (resets on server restart)"}
        except Exception as e:
            return {"service": "Lusha", "status": "error", "error": str(e),
                    "total": 0, "used": 0, "remaining": 0, "pct_remaining": 0, "searches_remaining": 0}

    def _check_semrush():
        """V5.7: Use real Semrush API units balance endpoint."""
        try:
            r = requests.get(
                "https://www.semrush.com/users/countapiunits.html",
                params={"key": API_KEYS.get("semrush", "")},
                timeout=10,
            )
            if r.status_code == 200:
                text = r.text.strip()
                try:
                    remaining = int(float(text))
                    total = max(remaining, SEMRUSH_PLAN_TOTAL)
                    used = total - remaining
                    pct = round(remaining / max(total, 1) * 100, 1)
                    return {"service": "Semrush", "status": "ok", "total": total, "used": used,
                            "remaining": remaining, "pct_remaining": pct,
                            "searches_remaining": remaining // 3}
                except ValueError:
                    if "ERROR" in text:
                        return {"service": "Semrush", "status": "error",
                                "error": text[:100],
                                "total": 0, "used": 0, "remaining": 0, "pct_remaining": 0, "searches_remaining": 0}
            return {"service": "Semrush", "status": "error",
                    "error": f"HTTP {r.status_code}",
                    "total": 0, "used": 0, "remaining": 0, "pct_remaining": 0, "searches_remaining": 0}
        except Exception as e:
            return {"service": "Semrush", "status": "error", "error": str(e),
                    "total": 0, "used": 0, "remaining": 0, "pct_remaining": 0, "searches_remaining": 0}

    def _fetch_credits(force=False):
        now = time.time()
        if not force and _credits_cache["data"] and (now - _credits_cache["timestamp"]) < _CACHE_TTL:
            return _credits_cache["data"]
        results = {}
        with ThreadPoolExecutor(max_workers=3) as pool:
            futs = {pool.submit(_check_apollo): "apollo",
                    pool.submit(_check_lusha): "lusha",
                    pool.submit(_check_semrush): "semrush"}
            for f in as_completed(futs):
                k = futs[f]
                try:
                    results[k] = f.result()
                except Exception as e:
                    results[k] = {"service": k.title(), "status": "error", "error": str(e),
                                  "total": 0, "used": 0, "remaining": 0, "pct_remaining": 0, "searches_remaining": 0}
        total_searches = sum(r.get("searches_remaining", 0) for r in results.values())
        alerts = []
        for k, r in results.items():
            pct = r.get("pct_remaining", 0)
            if r.get("status") == "error":
                alerts.append({"level": "error", "service": r["service"],
                               "message": f"{r['service']}: {r.get('error', 'Unknown error')}"})
            elif pct <= 10:
                alerts.append({"level": "critical", "service": r["service"],
                               "message": f"{r['service']} credits critically low ({pct}%)"})
            elif pct <= 25:
                alerts.append({"level": "warning", "service": r["service"],
                               "message": f"{r['service']} credits running low ({pct}%)"})
        payload = {"services": results, "total_searches_remaining": total_searches,
                   "alerts": alerts, "cached": False, "timestamp": datetime.now().isoformat()}
        _credits_cache["data"] = payload
        _credits_cache["timestamp"] = now
        return payload

    # ── Routes ───────────────────────────────────────────────────────────────
    @app.route("/health")
    def health():
        return jsonify({"status": "ok", "version": "V5.7"})

    @app.route("/")
    def serve_index():
        return send_from_directory(_DIR, "index.html")

    @app.route("/<path:filename>")
    def serve_static(filename):
        safe_ext = {".png", ".jpg", ".jpeg", ".gif", ".svg", ".ico", ".css", ".js", ".webp"}
        if os.path.splitext(filename)[1].lower() in safe_ext:
            return send_from_directory(_DIR, filename)
        return "Not found", 404

    @app.route("/industries")
    def get_industries():
        return jsonify({"industries": list(INDUSTRY_KEYWORDS.keys())})

    @app.route("/generate", methods=["POST"])
    def generate():
        data = flask_request.json
        if not data:
            return jsonify({"error": "No data provided"}), 400
        industry = data.get("industry", "")
        country = data.get("country", "AU")
        min_volume = int(data.get("min_volume", 100))
        min_cpc = float(data.get("min_cpc", 1.0))
        max_leads = int(data.get("max_leads", 0))
        if not industry:
            return jsonify({"error": "Industry is required"}), 400
        job_id = str(_uuid.uuid4())[:8]
        job = _JobState()
        _jobs[job_id] = job
        output_folder = os.path.join(_DIR, "output", job_id)
        os.makedirs(output_folder, exist_ok=True)

        def progress_cb(pct, status=""):
            job.progress = pct
            if status:
                job.status_text = status

        def log_cb(message):
            job.logs.append(message)

        pipeline = LeadGenerationPipeline(
            industry=industry, country=country, min_volume=min_volume,
            min_cpc=min_cpc, output_folder=output_folder,
            progress_callback=progress_cb, log_callback=log_cb, max_leads=max_leads,
        )
        job.pipeline = pipeline

        def run():
            try:
                job.progress = 1
                job.status_text = "Initializing pipeline..."
                job.logs.append("[SYSTEM] Pipeline initialized, starting Phase 1...")
                result_path = pipeline.run()
                job.api_usage = pipeline._api_counter.copy()  # V5.7: Capture run cost
                if pipeline._cancelled:
                    job.state = "cancelled"
                    return
                if result_path and os.path.exists(result_path):
                    with open(result_path, "r", encoding="utf-8") as f:
                        job.top_csv = f.read()
                    with open(result_path, "r", encoding="utf-8") as f:
                        reader = _csv.DictReader(f)
                        for row in reader:
                            job.leads.append({
                                "name": row.get("Name", ""),
                                "company": row.get("Company Name", ""),
                                "domain": row.get("Domain", ""),
                                "role": row.get("Role", ""),
                                "email": row.get("Email", ""),
                                "phone": row.get("Phone Number", ""),
                                "email_type": row.get("Email Type", ""),
                            })
                    for fname in os.listdir(output_folder):
                        if fname.startswith("leads_ALL_") and fname.endswith(".csv"):
                            with open(os.path.join(output_folder, fname), "r", encoding="utf-8") as f:
                                job.all_csv = f.read()
                            break

                    # V5.13: Build comprehensive summary
                    pipeline_leads = getattr(pipeline, 'leads', [])
                    paid_count = sum(1 for ld in pipeline_leads if ld.get("_domain_source", "paid") == "paid")
                    organic_count = sum(1 for ld in pipeline_leads if ld.get("_domain_source") == "organic")
                    with_phone = sum(1 for lead in job.leads if lead.get("phone"))
                    with_email = sum(1 for lead in job.leads if lead.get("email"))
                    personal_emails = sum(1 for lead in job.leads if lead.get("email_type") == "Personal")
                    direct_phones = sum(1 for ld in pipeline_leads if ld.get("_direct_phone"))
                    verified_emails = sum(1 for ld in pipeline_leads if ld.get("_email_verified"))
                    founder_verified_count = sum(1 for ld in pipeline_leads if ld.get("_founder_verified"))
                    total_credits = sum(
                        job.api_usage.get(svc, 0) * API_CREDIT_COSTS.get(svc, 0)
                        for svc in API_CREDIT_COSTS
                    )
                    credits_breakdown = {
                        svc: job.api_usage.get(svc, 0) * API_CREDIT_COSTS.get(svc, 0)
                        for svc in API_CREDIT_COSTS
                    }
                    job.summary = {
                        "paid_leads": paid_count,
                        "organic_leads": organic_count,
                        "total_leads": len(job.leads),
                        "with_phone": with_phone,
                        "with_email": with_email,
                        "personal_emails": personal_emails,
                        "semrush_tokens": job.api_usage.get("semrush", 0),
                        "apollo_tokens": job.api_usage.get("apollo", 0),
                        "lusha_tokens": job.api_usage.get("lusha", 0),
                        "direct_phones": direct_phones,
                        "verified_emails": verified_emails,
                        "total_api_calls": sum(job.api_usage.values()),
                        "total_credits_used": total_credits,
                        "avg_credits_per_lead": total_credits / max(len(job.leads), 1),
                        "credits_breakdown": credits_breakdown,
                        "founder_verified_count": founder_verified_count,
                        "competitor_domains_added": getattr(pipeline, '_competitor_domains_added', 0),
                    }
                    job.state = "done"
                else:
                    job.state = "done" if not pipeline._cancelled else "cancelled"
            except Exception as e:
                job.error = str(e)
                job.state = "error"

        threading.Thread(target=run, daemon=True).start()
        return jsonify({"job_id": job_id})

    @app.route("/status/<job_id>")
    def get_status(job_id):
        job = _jobs.get(job_id)
        if not job:
            return jsonify({"error": "Job not found"}), 404
        new_logs = job.logs[job.log_cursor:]
        job.log_cursor = len(job.logs)
        elapsed = time.time() - job.start_time
        progress = max(job.progress, 1)
        remaining = (elapsed / progress) * (100 - progress) if progress < 100 else 0
        result = {"state": job.state, "progress": job.progress,
                  "status_text": job.status_text, "new_logs": new_logs,
                  "elapsed_seconds": round(elapsed),
                  "time_remaining_seconds": round(remaining)}
        if job.state == "done":
            result["leads"] = job.leads
            result["top_csv"] = job.top_csv
            result["all_csv"] = job.all_csv
            result["api_usage"] = job.api_usage
            result["summary"] = job.summary  # V5.13: Token & lead summary
        if job.state == "error":
            result["error"] = job.error
            result["api_usage"] = job.api_usage
        return jsonify(result)

    @app.route("/cancel", methods=["POST"])
    def cancel():
        for jid in reversed(list(_jobs.keys())):
            j = _jobs[jid]
            if j.state == "running" and j.pipeline:
                j.pipeline.cancel()
                return jsonify({"status": "cancelling"})
        return jsonify({"status": "no active job"})

    @app.route("/api/credits")
    def get_credits():
        data = _fetch_credits(force=False)
        data["cached"] = (time.time() - _credits_cache["timestamp"]) > 1
        return jsonify(data)

    @app.route("/api/credits/refresh", methods=["POST"])
    def refresh_credits():
        return jsonify(_fetch_credits(force=True))

    # ── Start ────────────────────────────────────────────────────────────────
    port = int(os.environ.get("PORT", 5000))  # Railway sets PORT env var
    print("=" * 60)
    print("  LeadForge V5.7 — Web Interface")
    print(f"  Server running on port {port}")
    if port == 5000:  # Only auto-open browser on local dev
        print(f"  Opening browser at http://localhost:{port}")
        threading.Timer(1.5, lambda: webbrowser.open(f"http://localhost:{port}")).start()
    print("  Press Ctrl+C to stop")
    print("=" * 60)
    app.run(host="0.0.0.0", port=port, debug=False)

if __name__ == "__main__":
    main_web()