"""Check if pending is actual value or NULL displayed as pending."""
import json
from urllib.request import urlopen

# Get raw API response
resp = urlopen("http://127.0.0.1:8001/api/v1/rdagent/tasks/local?limit=5", timeout=30)
raw = resp.read().decode("utf-8")
print("Raw API response (first 2000 chars):")
print(raw[:2000])
