import sys
sys.path.append('/mnt/f/Dev/RD-Agent-main')
from dotenv import load_dotenv
load_dotenv('/mnt/f/Dev/RD-Agent-main/.env')

from rdagent.oai.backend.base import APIBackend
import logging
import litellm

logging.basicConfig(level=logging.DEBUG)
litellm.set_verbose = True

try:
    backend = APIBackend()
    print("Backend class:", type(backend).__name__)
    resp = backend.build_messages_and_create_chat_completion(
        user_prompt='Hello, this is a test. Please reply "Test successful" and nothing else.',
        system_prompt='You are a helpful assistant.',
        json_mode=False
    )
    print('Success:', resp)
except Exception as e:
    import traceback
    traceback.print_exc()
