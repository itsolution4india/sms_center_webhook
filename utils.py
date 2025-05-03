import logging
import os, re

LOGS_DIR = 'message_logs'
DASHBOARD_URL = "https://wtsdealnow.com/user_responses/"

if not os.path.exists(LOGS_DIR):
    os.makedirs(LOGS_DIR)

logging.basicConfig(
    filename=os.path.join(LOGS_DIR, 'app.log'),
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
)

def remove_special_chars(text: str) -> str:
    """Remove non-ASCII characters from text."""
    if not text:
        return ""
    return re.sub(r'[^\x00-\x7F]+', '', text)

def clean_interactive_type(interactive_type):
    cleaned_interactive_type = {}
    for key, value in interactive_type.items():
        if key == "flow_token":
            continue
        new_key = "_".join(key.split('_')[:-1]).replace('_', ' ')
        
        if isinstance(value, str) and '_' in value:
            value = value.split('_', 1)[1]
        
        cleaned_interactive_type[new_key] = value

    return 
