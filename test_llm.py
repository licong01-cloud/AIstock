import os
import sys

# Add rdagent to path if needed
sys.path.append('/mnt/f/Dev/RD-Agent-main')

from dotenv import load_dotenv
load_dotenv('/mnt/f/Dev/RD-Agent-main/.env')

from rdagent.core.conf import RD_AGENT_SETTINGS
from rdagent.oai.backend.base import APIBackend

print('Backend:', RD_AGENT_SETTINGS.BACKEND)
print('Map:', RD_AGENT_SETTINGS.LITELLM_CHAT_MODEL_MAP)

try:
    backend = APIBackend()
    resp = backend.build_messages_and_create_chat_completion(
        user_prompt='Hello',
        system_prompt='You are an AI',
        json_mode=False
    )
    print('Success:', resp)
except Exception as e:
    print('Failed:', e)
