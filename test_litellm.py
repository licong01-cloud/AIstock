import sys
import os
import logging
import traceback

sys.path.append('/mnt/f/Dev/RD-Agent-main')
from dotenv import load_dotenv
load_dotenv('/mnt/f/Dev/RD-Agent-main/.env')

from rdagent.oai.backend.litellm import LiteLLMAPIBackend
import litellm

# Enable detailed LiteLLM debugging
litellm.set_verbose = True
logging.basicConfig(level=logging.DEBUG)

def main():
    try:
        print("Initializing LiteLLMAPIBackend...")
        backend = LiteLLMAPIBackend()
        
        print(f"LITELLM_CHAT_MODEL_MAP: {os.environ.get('LITELLM_CHAT_MODEL_MAP')}")
        print(f"DEEPSEEK_API_BASE: {os.environ.get('DEEPSEEK_API_BASE')}")
        print(f"DEEPSEEK_API_KEY prefix: {os.environ.get('DEEPSEEK_API_KEY', '')[:5]}")

        print("Sending test chat completion request...")
        resp = backend.build_messages_and_create_chat_completion(
            user_prompt='Hello, testing LiteLLM backend.',
            system_prompt='You are a helpful assistant.',
            json_mode=False
        )
        print('\nSUCCESS! Response:')
        print(resp)
    except Exception as e:
        print('\nFAILED with exception:')
        traceback.print_exc()

if __name__ == '__main__':
    main()
