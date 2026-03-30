\"\"\"Patch train.py: complete oracle_gap early stopping\"\"\"
from pathlib import Path

path = Path('/mnt/f/Dev/AIstock/scripts/rl_execution/train.py')
src = path.read_text()

patches = [
    (
        'log output',
        '            logger.info(\
'
        '                f\"  Validation: avg_pa={val_result[\\'avg_pa\\']:.2f} bps, \"\
'
        '                f\"n_eval={val_result[\\'n_eval\\']}\\