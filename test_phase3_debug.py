"""Debug Phase 3 crash - isolate domain discovery"""
import os, sys, traceback

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

from V5 import LeadGenerationPipeline

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output_test_phase3")
os.makedirs(OUTPUT_DIR, exist_ok=True)

def log_cb(msg):
    try:
        print(msg)
    except:
        pass

try:
    pipeline = LeadGenerationPipeline(
        industry="Plumber",
        country="AU",
        min_volume=100,
        min_cpc=1.0,
        output_folder=OUTPUT_DIR,
        log_callback=log_cb,
        max_leads=10,
    )
    result = pipeline.run()
    print(f"\nSUCCESS: {result}")
except Exception as e:
    print(f"\nERROR: {e}")
    traceback.print_exc()
