"""WSGI entry point - Flask app defined here"""
import os
import time
import csv as _csv
import uuid as _uuid
import threading
from flask import Flask, jsonify, send_from_directory, request

# Create Flask app
_DIR = os.path.dirname(os.path.abspath(__file__))
app = Flask(__name__, static_folder=_DIR)

# Job management
_jobs = {}

class JobState:
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
        self.api_usage = {}
        self.summary = {}  # V5.12: Token usage & lead statistics
        self.start_time = time.time()  # V5.14: Track start for time remaining

# Configure CORS
@app.after_request
def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    return response

@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Not found"}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({"error": "Internal server error"}), 500

@app.errorhandler(Exception)
def handle_exception(error):
    return jsonify({"error": str(error)}), 500

# Routes
@app.route("/")
def serve_index():
    try:
        return send_from_directory(_DIR, "index.html")
    except Exception as e:
        return f"Error: {e}", 500

@app.route("/health")
def health():
    return jsonify({"status": "ok", "version": "V5.12"})

@app.route("/<path:filename>")
def serve_static(filename):
    safe_ext = {".png", ".jpg", ".jpeg", ".gif", ".svg", ".ico", ".css", ".js", ".webp"}
    if os.path.splitext(filename)[1].lower() in safe_ext:
        try:
            return send_from_directory(_DIR, filename)
        except:
            return "Not found", 404
    return "Not found", 404

@app.route("/industries")
def get_industries():
    # Import only when needed to avoid startup issues
    try:
        from V5 import INDUSTRY_KEYWORDS
        return jsonify({"industries": list(INDUSTRY_KEYWORDS.keys())})
    except:
        return jsonify({"industries": ["Electrician", "Plumber", "Photographer"]})

@app.route("/api/credits")
def get_credits():
    services = {
        "apollo": {
            "service": "Apollo",
            "status": "offline",
            "used": 0,
            "total": 1000,
            "pct_remaining": 100,
            "searches_remaining": 1000
        },
        "lusha": {
            "service": "Lusha",
            "status": "offline",
            "used": 0,
            "total": 1000,
            "pct_remaining": 100,
            "searches_remaining": 1000
        },
        "semrush": {
            "service": "SEMrush",
            "status": "offline",
            "used": 0,
            "total": 50000,
            "pct_remaining": 100,
            "searches_remaining": 50000
        },
        "serpapi": {
            "service": "SerpAPI",
            "status": "offline",
            "used": 0,
            "total": 10000,
            "pct_remaining": 100,
            "searches_remaining": 10000
        },
        "openai": {
            "service": "OpenAI",
            "status": "offline",
            "used": 0,
            "total": 5000,
            "pct_remaining": 100,
            "searches_remaining": 5000
        }
    }
    data = {
        "services": services,
        "total_searches_remaining": 77000,
        "timestamp": time.time(),
        "cached": False,
        "alerts": []
    }
    return jsonify(data)

@app.route("/api/credits/refresh", methods=["POST"])
def refresh_credits():
    services = {
        "apollo": {"service": "Apollo", "status": "offline", "used": 0, "total": 1000, "pct_remaining": 100, "searches_remaining": 1000},
        "lusha": {"service": "Lusha", "status": "offline", "used": 0, "total": 1000, "pct_remaining": 100, "searches_remaining": 1000},
        "semrush": {"service": "SEMrush", "status": "offline", "used": 0, "total": 50000, "pct_remaining": 100, "searches_remaining": 50000},
        "serpapi": {"service": "SerpAPI", "status": "offline", "used": 0, "total": 10000, "pct_remaining": 100, "searches_remaining": 10000},
        "openai": {"service": "OpenAI", "status": "offline", "used": 0, "total": 5000, "pct_remaining": 100, "searches_remaining": 5000}
    }
    data = {
        "services": services,
        "total_searches_remaining": 77000,
        "timestamp": time.time(),
        "cached": False,
        "alerts": []
    }
    return jsonify(data)

@app.route("/generate", methods=["POST"])
def generate():
    try:
        from V5 import LeadGenerationPipeline

        data = request.get_json()
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
        job = JobState()
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
                job.api_usage = pipeline._api_counter.copy()
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
                                "phone": row.get("Phone Number", ""),
                                "email": row.get("Email", ""),
                                "email_type": row.get("Email Type", ""),
                            })
                    for fname in os.listdir(output_folder):
                        if fname.startswith("leads_ALL_") and fname.endswith(".csv"):
                            with open(os.path.join(output_folder, fname), "r", encoding="utf-8") as f:
                                job.all_csv = f.read()
                            break

                    # V5.13: Build comprehensive summary statistics
                    pipeline_leads = getattr(pipeline, 'leads', [])
                    paid_count = sum(1 for ld in pipeline_leads if ld.get("_domain_source", "paid") == "paid")
                    organic_count = sum(1 for ld in pipeline_leads if ld.get("_domain_source") == "organic")
                    with_phone = sum(1 for lead in job.leads if lead.get("phone"))
                    with_email = sum(1 for lead in job.leads if lead.get("email"))
                    personal_emails = sum(1 for lead in job.leads if lead.get("email_type") == "Personal")
                    direct_phones = sum(1 for ld in pipeline_leads if ld.get("_direct_phone"))
                    verified_emails = sum(1 for ld in pipeline_leads if ld.get("_email_verified"))
                    founder_verified_count = sum(1 for ld in pipeline_leads if ld.get("_founder_verified"))

                    # V5.13: Credits tracking
                    _credit_costs = {"semrush": 10, "apollo": 1, "lusha": 1, "serpapi": 1, "openai": 0.01, "hunter": 1}
                    total_credits = sum(
                        job.api_usage.get(svc, 0) * cost
                        for svc, cost in _credit_costs.items()
                    )
                    credits_breakdown = {
                        svc: round(job.api_usage.get(svc, 0) * cost, 2)
                        for svc, cost in _credit_costs.items()
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
                        "total_credits_used": round(total_credits, 2),
                        "avg_credits_per_lead": round(total_credits / max(len(job.leads), 1), 2),
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
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/status/<job_id>")
def get_status(job_id):
    try:
        job = _jobs.get(job_id)
        if not job:
            return jsonify({"error": "Job not found"}), 404
        new_logs = job.logs[job.log_cursor:]
        job.log_cursor = len(job.logs)
        elapsed = time.time() - job.start_time
        progress = max(job.progress, 1)
        remaining = (elapsed / progress) * (100 - progress) if progress < 100 else 0
        result = {
            "state": job.state,
            "progress": job.progress,
            "status_text": job.status_text,
            "new_logs": new_logs,
            "elapsed_seconds": round(elapsed),
            "time_remaining_seconds": round(remaining),
        }
        if job.state == "done":
            result["leads"] = job.leads
            result["top_csv"] = job.top_csv
            result["all_csv"] = job.all_csv
            result["api_usage"] = job.api_usage
            result["summary"] = job.summary  # V5.12: Include token & lead summary
        if job.state == "error":
            result["error"] = job.error
            result["api_usage"] = job.api_usage
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/cancel", methods=["POST"])
def cancel():
    try:
        for jid in reversed(list(_jobs.keys())):
            j = _jobs[jid]
            if j.state == "running" and j.pipeline:
                j.pipeline.cancel()
                return jsonify({"status": "cancelling"})
        return jsonify({"status": "no active job"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
