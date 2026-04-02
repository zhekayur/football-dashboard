import streamlit as st
import boto3
import json
import time
import uuid
import os
import threading
from botocore.exceptions import ClientError

# Configuration
# Assuming eu-north-1 based on app.py configuration
AWS_REGION = "eu-north-1"
DYNAMODB_TABLE_NAME = "football-dashboard-events"

# We initialize the client globally but use threading for calls so Streamlit isn't blocked.
# Boto3 session setup using the same fallback logic as app.py
try:
    if "aws" in st.secrets:
        # Streamlit Cloud Env
        session = boto3.Session(
            aws_access_key_id=st.secrets["aws"]["access_key_id"],
            aws_secret_access_key=st.secrets["aws"]["secret_access_key"],
            region_name=st.secrets["aws"]["region"]
        )
    else:
        session = boto3.Session(region_name=AWS_REGION)
except Exception:
    session = boto3.Session(region_name=AWS_REGION)

try:
    dynamodb = session.resource('dynamodb')
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)
except Exception as e:
    table = None
    print(f"Failed to initialize DynamoDB resource: {e}")

def get_session_id():
    """Generates and persists a unique session ID per user visit."""
    if 'session_id' not in st.session_state:
        st.session_state['session_id'] = str(uuid.uuid4())
    return st.session_state['session_id']

def get_client_metadata():
    """Extracts IP, User-Agent from Streamlit headers. Uses robust case-insensitive iteration."""
    ip_address = "unknown"
    user_agent = "unknown"
    try:
        if hasattr(st, "context") and hasattr(st.context, "headers"):
            headers = st.context.headers
            # Convert to dictionary safely and do a manual search
            headers_dict = dict(headers)
            
            for k, v in headers_dict.items():
                k_lower = str(k).lower()
                if k_lower == "x-forwarded-for" and v:
                    ip_address = str(v).split(',')[0].strip()
                elif k_lower == "user-agent" and v:
                    user_agent = str(v)
                    
            # Fallback for IP if X-Forwarded-For is missing
            if ip_address == "unknown":
                for k, v in headers_dict.items():
                    if str(k).lower() == "host" and v:
                        ip_address = str(v).split(',')[0].strip()
                        break
                        
    except Exception:
        pass
    
    return ip_address, user_agent

def _send_to_dynamodb(payload):
    """Background task to send payload to AWS DynamoDB."""
    if not table:
        return
    try:
        # Convert floats if any exist to Decimal to prevent DynamoDB errors, though tracking metrics are usually strings
        payload = json.loads(json.dumps(payload), parse_float=str)
        table.put_item(Item=payload)
    except ClientError as e:
        print(f"DynamoDB error: {e}")
    except Exception as e:
        print(f"Unknown tracking error: {e}")

def track_event(event_action, event_details=None, page_context=""):
    """
    Constructs the event payload and fires it off to AWS in a non-blocking thread.
    
    :param event_action: e.g., 'page_view', 'api_call_live_data', 'filter_change'
    :param event_details: Dict of additional info (e.g., {'selected_team': 'Arsenal'})
    :param page_context: Which page generated this event
    """
    if event_details is None:
        event_details = {}

    ip_address, user_agent = get_client_metadata()

    payload = {
        "event_id": str(uuid.uuid4()),
        "timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        "session_id": get_session_id(),
        "ip_address": ip_address,
        "user_agent": user_agent,
        "event_action": event_action,
        "page_context": page_context,
        "event_details": event_details
    }

    # Run in background thread to avoid slowing down UI
    thread = threading.Thread(target=_send_to_dynamodb, args=(payload,))
    thread.daemon = True
    thread.start()
