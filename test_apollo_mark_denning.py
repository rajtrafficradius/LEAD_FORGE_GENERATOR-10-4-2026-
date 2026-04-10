"""Test: what phone data does Apollo actually have for Mark Denning?"""
import sys, json
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

from V5 import ApolloClient, API_KEYS

apollo = ApolloClient(API_KEYS["apollo"])

print("=" * 60)
print("TEST 1: Search for Mark Denning at Fallon Solutions")
print("=" * 60)
people = apollo.search_people_by_domain("fallonsolutions.com.au", per_page=10)
print(f"Found {len(people)} people\n")

for p in people:
    first = p.get('first_name', '')
    last = p.get('last_name', '')
    title = p.get('title', '')
    pid = p.get('id', '')

    if first.lower() == 'mark' and last.lower() == 'denning':
        print(f"FOUND: {first} {last} ({title})")
        print(f"  ID: {pid}")
        print(f"  phone_numbers (search): {p.get('phone_numbers')}")
        print(f"  phone_number (search): {p.get('phone_number')}")
        print(f"  direct_phone_number: {p.get('direct_phone_number')}")

        # Now enrich him
        print(f"\n  Enriching {first} {last}...")
        enriched = apollo.enrich_person("Mark", "Denning", "fallonsolutions.com.au", apollo_id=pid)
        print(f"  Enriched result: {json.dumps(enriched, indent=2)[:500]}")

print("\n" + "=" * 60)
print("TEST 2: Search by name directly")
print("=" * 60)
enriched2 = apollo.enrich_person("Mark", "Denning", "fallonsolutions.com.au")
print(f"Direct enrich: {json.dumps(enriched2, indent=2)[:500]}")
