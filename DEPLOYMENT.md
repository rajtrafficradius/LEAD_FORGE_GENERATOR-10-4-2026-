# LeadForge Deployment Guide

## File Structure & Execution Flow

### Key Files
- **V5.py** — Main application file with Flask routes (requires running first)
- **wsgi.py** — WSGI entry point for production servers (imports from V5.py)
- **index.html** — Frontend UI (served by Flask, communicates via API)
- **Procfile** — Railway deployment configuration
- **requirements.txt** — Python dependencies

## Execution Flow

### Local Development
```bash
cd "c:\Users\Shreyas\Desktop\LEAD FORGE_LEAD GENERATOR"
python V5.py
# Opens browser at http://localhost:5000
# Flask server runs with all routes available
# index.html loads and auto-connects to the Flask server
```

### Railway/Production
1. **Procfile** triggers: `gunicorn -w 1 -b 0.0.0.0:$PORT --timeout 60 wsgi:app`
2. **wsgi.py** imports Flask app from V5.py
3. **V5.py** initializes all routes at module level
4. **Flask app** exposes all endpoints
5. **index.html** loads from "/" route and communicates with API endpoints

## API Endpoints

All endpoints return JSON:

### GET /health
Health check endpoint
```json
{ "status": "ok", "version": "V5.7" }
```

### GET /api/credits
Returns service credits and status
```json
{
  "services": {
    "apollo": { "service": "Apollo", "status": "offline", "used": 0, "total": 1000, "pct_remaining": 100 },
    "lusha": { "service": "Lusha", "status": "offline", "used": 0, "total": 1000, "pct_remaining": 100 },
    ...
  }
}
```

### GET /industries
Returns list of supported industries
```json
{ "industries": ["Electrician", "Plumber", ...] }
```

### POST /generate
Starts lead generation job
```json
{ "job_id": "abc12345" }
```

### GET /status/<job_id>
Gets job status and progress

### POST /cancel
Cancels running job

### GET /<path>
Serves static files (images, CSS, etc.)

## Configuration

### Environment Variables (Railway)
Set these in Railway → Variables:
- `SEMRUSH_API_KEY` — Your Semrush API key
- `SERPAPI_API_KEY` — Your SerpAPI key
- `APOLLO_API_KEY` — Your Apollo key
- `LUSHA_API_KEY` — Your Lusha key
- `OPENAI_API_KEY` — Your OpenAI API key
- `PORT` — (automatically set by Railway, default 8080)

### Local Development (.env file)
Create `.env` from `.env.example` and add your keys:
```
SEMRUSH_API_KEY=your_key_here
SERPAPI_API_KEY=your_key_here
APOLLO_API_KEY=your_key_here
LUSHA_API_KEY=your_key_here
OPENAI_API_KEY=your_key_here
```

## Deployment Steps

1. **Push to GitHub:**
   ```bash
   git add .
   git commit -m "Changes"
   git push origin main
   ```

2. **Railway Auto-Deploy:**
   - Railway automatically redeploys when you push to GitHub
   - Check deployment status in Railway dashboard
   - View logs if deployment fails

3. **Verify Deployment:**
   - Visit: https://leadforgeledgenerator-production.up.railway.app
   - Should see "Traffic Radius" UI
   - Credits section should show "Offline" status
   - Click "Generate Leads" to test the flow

## Troubleshooting

### JSON Parse Error
**Problem:** "JSON.parse: unexpected character at line 1"
**Solution:** This means API endpoints are not returning valid JSON. Check that:
- Flask app is running (not just static HTML)
- All routes in V5.py return `jsonify()` responses
- CORS headers are set correctly

### Connection Failed
**Problem:** "Failed to connect to server"
**Solution:**
- Check Railway logs for errors
- Verify all environment variables are set
- Ensure Procfile is correct

### Routes Not Found
**Problem:** 404 errors on API calls
**Solution:**
- Ensure @app.route decorators are at module level in V5.py
- Verify wsgi.py correctly imports from V5.py
- Check Procfile uses correct entry point: `wsgi:app`

## CORS & Headers

All endpoints have CORS headers enabled:
- `Access-Control-Allow-Origin: *`
- `Access-Control-Allow-Methods: GET, POST, OPTIONS`
- `Access-Control-Allow-Headers: Content-Type`

This allows the frontend to communicate with the backend across domains.

## Testing Locally

```bash
python -c "
from wsgi import app

with app.test_client() as client:
    # Test endpoints
    resp = client.get('/api/credits')
    print('Status:', resp.status_code)
    print('Data:', resp.get_json())
"
```

## Git Workflow

```bash
# Check status
git status

# Stage changes
git add V5.py

# Commit with message
git commit -m "Fix API response format"

# Push to GitHub (triggers Railway deployment)
git push origin main

# View recent commits
git log --oneline -5
```
