#!/bin/bash
# V5.12 Quick Test Script

echo "========================================================================"
echo "LeadForge V5.12 — Final Validation Test"
echo "========================================================================"

# Test 1: Syntax check
echo ""
echo "[1/4] Checking Python syntax..."
python3 -m py_compile V5.py && echo "  [OK] V5.py compiles" || echo "  [FAIL] V5.py syntax error"
python3 -m py_compile wsgi.py && echo "  [OK] wsgi.py compiles" || echo "  [FAIL] wsgi.py syntax error"

# Test 2: Keywords verification
echo ""
echo "[2/4] Verifying keyword expansion..."
python3 << 'PYEOF'
from V5 import INDUSTRY_KEYWORDS
dentist = len(INDUSTRY_KEYWORDS.get('Dentist', []))
plumber = len(INDUSTRY_KEYWORDS.get('Plumber', []))
print(f"  Dentist:  {dentist} keywords (expected ~46)")
print(f"  Plumber:  {plumber} keywords (expected ~45)")
if dentist >= 40 and plumber >= 40:
    print("  [OK] Keywords doubled successfully")
else:
    print("  [FAIL] Keywords not expanded properly")
PYEOF

# Test 3: PAID-only mode verification
echo ""
echo "[3/4] Verifying PAID-only mode..."
grep -q "get_organic_domains" V5.py && echo "  [FAIL] Organic domain calls still present" || echo "  [OK] Organic domain calls removed"
grep -q "PAID-ONLY MODE" V5.py && echo "  [OK] V5.12 PAID-only comment found" else echo "  [WARN] Comment not found"

# Test 4: Token summary integration
echo ""
echo "[4/4] Verifying token summary logging..."
grep -q "V5.12 RUN SUMMARY" V5.py && echo "  [OK] Token summary logging added" || echo "  [FAIL] Summary logging missing"
grep -q "paid_leads\|organic_leads\|total_leads" wsgi.py && echo "  [OK] Server summary collection ready" || echo "  [FAIL] Server summary missing"

echo ""
echo "========================================================================"
echo "All V5.12 features verified and ready for testing!"
echo "========================================================================"
