"""Smoke-test DeletionCandidateService end-to-end on live DB."""
import os, sys
from pathlib import Path

REPO_ROOT = Path(r"F:/Dev/AIstock")
for line in (REPO_ROOT / ".env").read_text(encoding="utf-8").splitlines():
    line = line.strip()
    if not line or line.startswith("#") or "=" not in line:
        continue
    k, v = line.split("=", 1)
    os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

sys.path.insert(0, str(REPO_ROOT))

from backend.services.quantevolver.deletion_candidate_service import deletion_candidate_service

result = deletion_candidate_service.analyze()

print(f"total_factors       = {result['total_factors']}")
print(f"immune_count        = {result['immune_count']}")
print(f"exact_twins         = {len(result['exact_twins'])}")
print(f"pure_noise          = {len(result['pure_noise'])}")
print(f"fuzzy_twins         = {len(result['fuzzy_twins'])}")
print(f"total_candidates    = {result['total_candidates']}")
print(f"remaining_keep      = {result['remaining_keep']}")
print()

for label, bucket in (("EXACT TWINS", "exact_twins"), ("PURE NOISE", "pure_noise"), ("FUZZY TWINS", "fuzzy_twins")):
    rows = result[bucket]
    if not rows:
        continue
    print(f"=== {label} (first 5) ===")
    for r in rows[:5]:
        fn = r.get("factor_name")
        src = r.get("source")
        kept = r.get("twin_kept", "-")
        corr = r.get("twin_corr", "-")
        v2 = r.get("v2_score")
        ric = r.get("rank_ic_mean")
        print(f"  [{src:8s}] {fn:40s} v2={v2}  ric={ric}  kept={kept}  corr={corr}")
    print()
