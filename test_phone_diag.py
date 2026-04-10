"""V5.26 Phone reveal test — run pipeline with webhook-based phone reveal."""
import os, sys

sys.stdout.reconfigure(encoding='utf-8', errors='replace')
sys.stderr.reconfigure(encoding='utf-8', errors='replace')
os.environ.setdefault("PYTHONIOENCODING", "utf-8")

from V5 import LeadGenerationPipeline

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output_test_v26")
os.makedirs(OUTPUT_DIR, exist_ok=True)

def log_cb(msg):
    try:
        print(msg)
    except UnicodeEncodeError:
        print(msg.encode('ascii', 'replace').decode())

pipeline = LeadGenerationPipeline(
    industry="Plumber",
    country="AU",
    min_volume=100,
    min_cpc=1.0,
    output_folder=OUTPUT_DIR,
    log_callback=log_cb,
    max_leads=3,
)

result = pipeline.run()
print(f"\n=== RESULT: {result} ===")
