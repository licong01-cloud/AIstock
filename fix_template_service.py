import os

file_path = r'F:\Dev\RD-Agent-main\rdagent\app\scheduler\template_service.py'
with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

new_content = content.replace(
    'ALLOWED_SUFFIXES = {".yaml", ".yml", ".json"}',
    'ALLOWED_SUFFIXES = {".yaml", ".yml", ".json", ".py", ".md", ".txt", ".sh"}'
)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(new_content)

print('Updated ALLOWED_SUFFIXES successfully.')
