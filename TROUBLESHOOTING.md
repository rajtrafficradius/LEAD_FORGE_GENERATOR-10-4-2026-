# Troubleshooting Guide

## Quick Checks

### 1. Test Locally First
Always test locally before pushing to Railway:

```bash
cd "c:\Users\Shreyas\Desktop\LEAD FORGE_LEAD GENERATOR"
python test_api.py
```

Expected output:
```
ALL TESTS PASSED - Ready for deployment!
```

### 2. Test Individual Endpoints

```bash
python -c "
from wsgi import app

with app.test_client() as client:
    # Test health endpoint
    resp = client.get('/health')
    print('Health check:', resp.status_code, resp.get_json())

    # Test credits endpoint
    resp = client.get('/api/credits')
    print('Credits:', resp.status_code, 'Valid JSON' if resp.get_json() else 'Invalid')

    # Test home page
    resp = client.get('/')
    print('Home page:', resp.status_code, 'HTML' if 'text/html' in resp.content_type else resp.content_type)
"
```

## Railway Deployment Issues

### Issue: "JSON.parse: unexpected character at line 1"

This means the API endpoint is not returning valid JSON. Usually caused by:

**1. Flask app not initialized**
- Check Railway logs for `[V5]` messages
- If you don't see them, the V5.py module didn't load

**2. Route returning error page instead of JSON**
- Check logs for `[ERROR]` messages
- Look for Python traceback in logs

**3. Static HTML file being served instead of API response**
- This happens if routes aren't properly defined
- Check logs for `[API] GET /api/credits` message

### Checking Railway Logs

Go to: **Railway Dashboard → Select Project → Select Service → Logs**

Look for these messages (in order):

```
[WSGI] Starting initialization...
[WSGI] Importing V5 module...
[WSGI] V5 module imported successfully
[WSGI] App name: V5
[WSGI] Routes registered: 10
[V5] Loading WSGI entry point...
[V5] Project directory: /app
[V5] Flask app created: V5
[V5] CORS enabled
[V5] Registering routes...
[V5] Routes registered: 10
[V5] WSGI entry point ready!
```

If you see these, the app initialized correctly.

### Testing in Browser

Once deployed, open your browser's **Developer Tools (F12)** → **Network** tab

Try clicking different buttons and watch the network requests:

1. **Test /health endpoint:**
   - Visit: `https://your-app.up.railway.app/health`
   - Should see JSON with `{"status":"ok","version":"V5.7"}`

2. **Test /api/credits:**
   - Visit: `https://your-app.up.railway.app/api/credits`
   - Should see JSON with services object

3. **Test home page:**
   - Visit: `https://your-app.up.railway.app/`
   - Should see "Traffic Radius" UI

## Common Issues & Solutions

### Problem: 404 Not Found
**Cause:** Route not registered
**Solution:**
- Check that decorators (`@app.route()`) are defined at module level in V5.py
- Not inside a function
- Restart Railway deployment

### Problem: 503 Service Unavailable
**Cause:** App crashed or didn't start
**Solution:**
- Check Railway logs for Python errors
- Verify all imports work: `python -c "from V5 import app"`
- Check that index.html exists in the project directory

### Problem: CORS Error in Browser Console
**Cause:** API doesn't have CORS headers
**Solution:**
- Already fixed in code: `CORS(app, ...)`
- Check logs for `[V5] CORS enabled` message
- Clear browser cache and reload

### Problem: Blank Page (No UI)
**Cause:** index.html not being served
**Solution:**
- Check that index.html exists in project root
- Verify "/" route works: visit `/health` first
- Check logs for `[API] GET /` message

## Deployment Checklist

Before each deployment:

- [ ] Run `python test_api.py` locally - all tests pass
- [ ] Check git status: `git status`
- [ ] No uncommitted changes (clean working directory)
- [ ] Commit message is descriptive
- [ ] Run: `git push origin main`
- [ ] Wait for Railway to auto-deploy
- [ ] Check Railway logs for initialization messages
- [ ] Test in browser:
  - [ ] Home page loads (`/`)
  - [ ] Health check works (`/health`)
  - [ ] API credits loads (`/api/credits`)
  - [ ] Industries dropdown populates

## Debug Mode

### Enable More Logging

Add to top of V5.py:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
logging.getLogger('flask').setLevel(logging.DEBUG)
```

### Check File System

Add a test endpoint to V5.py:
```python
@app.route("/debug")
def debug():
    import os
    files = os.listdir(_DIR)
    return jsonify({
        "directory": _DIR,
        "exists": os.path.exists(os.path.join(_DIR, "index.html")),
        "files": files[:20]
    })
```

Then visit: `https://your-app.up.railway.app/debug`

## Performance

### Monitor Resource Usage

Railway Dashboard → Metrics tab:
- CPU usage should be < 20%
- Memory < 200MB
- If higher, there might be a memory leak

### Check Response Times

Browser Dev Tools → Network tab → Check "Time" column
- `/` should be < 100ms
- `/api/credits` should be < 50ms
- If slow, check Railway logs for blocking operations

## Getting Help

1. **Check logs first:** 90% of issues are visible in logs
2. **Run test_api.py locally:** Confirms app works
3. **Look for [ERROR] messages:** Python tracebacks indicate issues
4. **Check browser console:** JavaScript errors indicate frontend issues

## Summary

**The app works locally if:**
- `python test_api.py` returns "ALL TESTS PASSED"
- `python -c "from wsgi import app; print(app)"` shows Flask app object
- No Python syntax errors in V5.py

**The app works on Railway if:**
- Logs show all `[V5]` initialization messages
- `/health` returns `{"status":"ok","version":"V5.7"}`
- `/api/credits` returns services with valid JSON
- Browser UI loads and displays correctly
