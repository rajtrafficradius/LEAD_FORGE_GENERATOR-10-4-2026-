#!/usr/bin/env python3
"""
Test script to verify all API endpoints are working correctly.
Run this locally to ensure the Flask app is functioning before deploying to Railway.
"""
import json
import sys

def test_endpoints():
    print("=" * 60)
    print("LEADFORGE API TEST SUITE")
    print("=" * 60)

    try:
        print("\n[1] Importing Flask app...")
        from wsgi import app
        print("    [OK] App imported")
    except Exception as e:
        print(f"    [FAIL] Could not import app: {e}")
        return False

    print("\n[2] Checking Flask app...")
    print(f"    App name: {app.name}")
    print(f"    App type: {type(app)}")

    print("\n[3] Checking routes...")
    routes = [str(r) for r in app.url_map.iter_rules() if not str(r).startswith('/static')]
    print(f"    Total routes: {len(routes)}")
    for route in routes:
        print(f"      - {route}")

    print("\n[4] Testing API endpoints...")

    test_cases = [
        ("GET", "/health", None, {"status": "ok"}),
        ("GET", "/api/credits", None, {"services": dict}),
        ("GET", "/industries", None, {"industries": list}),
        ("GET", "/", None, None),  # HTML response
    ]

    with app.test_client() as client:
        passed = 0
        failed = 0

        for method, endpoint, data, expected in test_cases:
            try:
                if method == "GET":
                    resp = client.get(endpoint)
                else:
                    resp = client.post(endpoint, json=data)

                # Check status
                if resp.status_code != 200:
                    print(f"    [FAIL] {method} {endpoint}: HTTP {resp.status_code}")
                    failed += 1
                    continue

                # Check content type
                is_json = "json" in resp.content_type

                # Check structure
                if expected and is_json:
                    resp_data = resp.get_json()
                    for key, value_type in expected.items():
                        if key not in resp_data:
                            print(f"    [FAIL] {method} {endpoint}: Missing key '{key}'")
                            failed += 1
                            continue
                        if value_type == dict and not isinstance(resp_data[key], dict):
                            print(f"    [FAIL] {method} {endpoint}: Key '{key}' is not a dict")
                            failed += 1
                            continue
                        if value_type == list and not isinstance(resp_data[key], list):
                            print(f"    [FAIL] {method} {endpoint}: Key '{key}' is not a list")
                            failed += 1
                            continue

                print(f"    [OK] {method} {endpoint}")
                passed += 1

            except Exception as e:
                print(f"    [ERROR] {method} {endpoint}: {e}")
                failed += 1

        print(f"\n[5] Results: {passed} passed, {failed} failed")

        return failed == 0

if __name__ == "__main__":
    success = test_endpoints()
    print("\n" + "=" * 60)
    if success:
        print("ALL TESTS PASSED - Ready for deployment!")
        print("=" * 60)
        sys.exit(0)
    else:
        print("SOME TESTS FAILED - Fix issues before deploying")
        print("=" * 60)
        sys.exit(1)
