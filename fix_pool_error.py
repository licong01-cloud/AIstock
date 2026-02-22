import os
import psycopg2

file_path = r'F:\Dev\AIstock\rdagent_assets\qe_strategies\enhanced_topk_dropout_v4_copy.py'
with open(file_path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

new_lines = []
skip = False
for line in lines:
    if '"""获取总市值用于选股前过滤，拒绝兜底"""' in line:
        skip = True
    if skip and 'return float(mv)' in line:
        skip = False
        continue
    if not skip:
        new_lines.append(line)

content = ''.join(new_lines)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(content)

workspace_path = r'F:\Dev\RD-Agent-main\qe_workspace\qe_exp_c73611a2\custom_strategy.py'
if os.path.exists(workspace_path):
    with open(workspace_path, 'w', encoding='utf-8') as f:
        f.write(content)
    print('Updated workspace custom_strategy.py')

try:
    conn = psycopg2.connect(host='127.0.0.1', port=5432, user='postgres', password='lc78080808', dbname='aistock')
    cur = conn.cursor()
    cur.execute("UPDATE aistock_strategy_catalog SET source_code = %(code)s WHERE source_code_relpath LIKE %(path)s", 
                {'code': content, 'path': '%enhanced_topk_dropout_v4_copy%'})
    conn.commit()
    print('DB updated rows:', cur.rowcount)
except Exception as e:
    print('DB Error:', e)
