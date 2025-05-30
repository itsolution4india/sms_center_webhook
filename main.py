from utils import logging, clean_interactive_type, remove_special_chars
import json
import os
import mysql.connector
from mysql.connector import Error
import requests
from typing import Dict, Any, Optional, Tuple
from datetime import datetime

import uvicorn
from fastapi import FastAPI, Request, Response, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware

# Create FastAPI app
app = FastAPI(title="WhatsApp Webhook Service")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Constants
VERIFY_TOKEN = "hello"
LOGS_DIR = 'message_logs'

# Database configuration
DB_CONFIG = {
    'host': "localhost",
    'database': 'smsc_db',
    'user': 'prashanth@itsolution4india.com',
    'password': 'Solution@97',
    'port': 3306
}

def get_db_connection():
    """Create and return a database connection."""
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        if connection.is_connected():
            return connection
    except Error as e:
        logging.error(f"Error connecting to MySQL database: {e}")
        return None

def parse_webhook_response(response: Dict[str, Any]) -> Dict[str, Any]:
    """Parse webhook response into standardized format."""
    report = {}
    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime('%Y-%m-%d %H:%M:%S')

    report['Date'] = formatted_datetime
    
    for entry in response.get('entry', []):
        changes = entry.get('changes', [])
        for change in changes:
            value = change.get('value', {})
            metadata = value.get('metadata', {})
            report['display_phone_number'] = metadata.get('display_phone_number')
            report['phone_number_id'] = metadata.get('phone_number_id')
            
            message_template_id = value.get('message_template_id')
            message_template_name = value.get('message_template_name')

            if message_template_id and message_template_name:
                report['message_template_id'] = message_template_id
                report['message_template_name'] = message_template_name
                
            statuses = value.get('statuses', [])
            for status in statuses:
                report['wamid'] = status.get('id')
                report['status'] = status.get('status')
                report['message_timestamp'] = status.get('timestamp')
                report['contact_wa_id'] = status.get('recipient_id')
                if 'errors' in status:
                    error_details = status['errors'][0]
                    report['error_code'] = error_details.get('code')
                    report['error_title'] = error_details.get('title')
                    report['error_message'] = error_details.get('message')
                    report['error_data'] = error_details.get('error_data', {}).get('details')

            contacts = value.get('contacts', [])
            for contact in contacts:
                contact_name = contact.get('profile', {}).get('name', '')
                try:
                    report['contact_name'] = remove_special_chars(contact_name)
                except:
                    report['contact_name'] = ''
                report['contact_wa_id'] = contact.get('wa_id')

            messages = value.get('messages', [])
            for message in messages:
                report['message_from'] = message.get('from')
                report['status'] = 'reply'
                report['wamid'] = message.get('id')
                report['message_timestamp'] = message.get('timestamp')
                report['message_type'] = message.get('type')
                
                if message.get('type') == 'text':
                    report['message_body'] = message.get('text', {}).get('body')
                elif message.get('type') == 'button':
                    report['message_body'] = message.get('button', {}).get('text')
                elif message.get('type') == 'image':
                    report['message_body'] = message.get('image', {}).get('id')
                elif message.get('type') == 'document':
                    report['message_body'] = message.get('document', {}).get('id')
                elif message.get('type') == 'video':
                    report['message_body'] = message.get('video', {}).get('id')
                elif message.get('type') == 'interactive':
                    interactive_type = message.get('interactive', {}).get('type')
                    if interactive_type == 'button_reply':
                        report['message_body'] = message.get('interactive', {}).get('button_reply', {}).get('title')
                    elif interactive_type == 'list_reply':
                        report['message_body'] = message.get('interactive', {}).get('list_reply', {}).get('title')
                    elif interactive_type == 'nfm_reply':
                        interactive_msg = message.get('interactive', {}).get('nfm_reply', {}).get('response_json')
                        if isinstance(interactive_msg, str):
                            interactive_type_dict = json.loads(interactive_msg)
                        else:
                            interactive_type_dict = interactive_msg
                        report['message_body'] = clean_interactive_type(interactive_type_dict)
                        report['message_body'] = json.dumps(report['message_body'])
                else:
                    report['message_body'] = ""
    
    return report

import random

def update_database_status(wamid: str, status: str, message_timestamp: str, 
                          error_code: Optional[int] = None, 
                          error_message: Optional[str] = None,
                          contact_name: Optional[str] = None,
                          message_body: Optional[str] = None,
                          phone_id: Optional[str] = None) -> Tuple[bool, Optional[Dict]]:
    """
    Update the database record if wamid exists, else insert a new record.
    If error_code is 131048, deactivate current number and activate a random inactive number for same user.
    """
    conn = get_db_connection()
    if not conn:
        return False, None

    cursor = None
    try:
        cursor = conn.cursor(dictionary=True, buffered=True)

        # Step 1: Check if the wamid exists
        cursor.execute("SELECT id FROM smsc_responses WHERE wamid = %s", (wamid,))
        record = cursor.fetchone()

        # Determine dlr_status based on status
        dlr_status = "skip" if status.lower() == "reply" else "pending"

        if record:
            dlr_status = None
            # Step 2: Perform update
            update_query = """
            UPDATE smsc_responses
            SET status = %s,
                message_timestamp = %s,
                error_code = %s,
                error_message = %s,
                message_body = %s
            """
            update_params = [status, message_timestamp, error_code, error_message, message_body]

            if contact_name:
                update_query += ", contact_name = %s"
                update_params.append(contact_name)

            if dlr_status:
                update_query += ", dlr_status = %s"
                update_params.append(dlr_status)

            update_query += " WHERE wamid = %s"
            update_params.append(wamid)

            cursor.execute(update_query, update_params)
        else:
            # Step 3: Perform insert
            insert_query = """
            INSERT INTO smsc_responses 
                (wamid, status, message_timestamp, error_code, error_message, contact_name, message_body, dlr_status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            insert_params = [wamid, status, message_timestamp, error_code, error_message, contact_name, message_body, dlr_status]
            cursor.execute(insert_query, insert_params)

        # Step 4: Handle error_code 131048 - deactivate and switch number
        if error_code == 131048 and phone_id:
            # Check if the current number is active
            cursor.execute("""
                SELECT number_status FROM whatsapp_numbers WHERE phone_id = %s
            """, (phone_id,))
            status_result = cursor.fetchone()
            if status_result and status_result['number_status'] == 'active':
                # Deactivate current number
                cursor.execute("""
                    UPDATE whatsapp_numbers 
                    SET number_status = 'inactive' 
                    WHERE phone_id = %s
                """, (phone_id,))
                
                # Get username for that phone_id
                cursor.execute("""
                    SELECT username FROM whatsapp_numbers WHERE phone_id = %s
                """, (phone_id,))
                user = cursor.fetchone()
                if user:
                    username = user["username"]
                    # Get all other inactive numbers for this user
                    cursor.execute("""
                        SELECT phone_id FROM whatsapp_numbers 
                        WHERE username = %s AND number_status = 'inactive' AND phone_id != %s
                    """, (username, phone_id))
                    options = cursor.fetchall()
                    if options:
                        new_phone = random.choice(options)['phone_id']
                        # Activate one of them
                        cursor.execute("""
                            UPDATE whatsapp_numbers 
                            SET number_status = 'active' 
                            WHERE phone_id = %s
                        """, (new_phone,))
                        logging.info(f"Switched active number to {new_phone} for user {username}")
                    else:
                        logging.warning(f"No other inactive number found for user {username} to activate.")
                        
            else:
                logging.info(f"Skip {phone_id} is already inactived")

        conn.commit()
        logging.info(f"SUCCESS, {wamid, status, message_timestamp, error_code, error_message, contact_name, message_body}")
        return True, True

    except Exception as e:
        logging.error(f"Database error: {e}")
        if conn.is_connected():
            conn.rollback()
        return False, None
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()

# def update_dlr_status(message_id: str, status: str) -> None:
#     """Update the dlr_status for a given message_id."""
#     conn = get_db_connection()
#     if not conn:
#         return
    
#     cursor = None
#     try:
#         cursor = conn.cursor()
#         query = """
#         UPDATE smsc_responses
#         SET dlr_status = %s
#         WHERE message_id = %s
#         """
#         cursor.execute(query, (status, message_id))
#         conn.commit()
#     except Error as e:
#         logging.error(f"Failed to update dlr_status: {e}")
#         if conn.is_connected():
#             conn.rollback()
#     finally:
#         if cursor:
#             cursor.close()
#         if conn and conn.is_connected():
#             conn.close()


# def call_dlr_webhook(data: Dict[str, Any]):
#     """Call the DLR webhook with the provided data."""
#     payload = {
#         "username": data.get("username"),
#         "source_addr": data.get("source_addr"),
#         "destination_addr": data.get("destination_addr"),
#         "message_id": data.get("message_id"),
#         "status": data.get("status")
#     }
    
#     try:
#         response = requests.post(DLR_WEBHOOK_URL, json=payload, timeout=10)
#         dlr_status = 'sent' if response.status_code == 200 else 'failed'
#         update_dlr_status(data.get("message_id"), dlr_status)
#         if response.status_code == 200:
#             logging.info(f"DLR webhook response: Status={response.status_code}, Body={response.text}")
#             return True
#         else:
#             logging.error(
#                 f"DLR webhook failed: Status={response.status_code}, Body={response.text}, Payload={payload}"
#             )
#             return False
#     except Exception as e:
#         logging.error(f"Error calling DLR webhook: {e}")
#     return True

# Background task to handle webhook data
async def process_webhook(body: Dict[str, Any], account_id: str):
    """Process webhook data asynchronously."""
    try:
        # Parse webhook response
        data = parse_webhook_response(body)
        
        # Check if we have the necessary data to update the database
        if 'wamid' in data:
            wamid = data.get('wamid')
            status = data.get('status')
            message_timestamp = data.get('message_timestamp')
            phone_number_id = data.get('phone_number_id')
            error_code = data.get('error_code')
            error_message = data.get('error_message')
            contact_name = data.get('contact_name')
            contact_wa_id = data.get('contact_wa_id')
            reply_msg = data.get('message_body')
            
            message_body = f"{contact_wa_id}: {reply_msg}"
            
            # Update database and get required data for DLR webhook
            success, record = update_database_status(
                wamid, status, message_timestamp, error_code, error_message, contact_name, message_body, phone_number_id
            )
            
            # If database update was successful and we have the required data, call DLR webhook
            # if success and record:
            #     call_dlr_webhook(record)
            # else:
            #     logging.warning(f"Could not update database or fetch required data for wamid: {wamid}")
        else:
            logging.warning("No wamid found in webhook data")
        
    except Exception as e:
        logging.exception(f"Error in background processing: {e}")

# Endpoints
@app.get("/")
async def root():
    """Root endpoint."""
    return {"message": "WhatsApp Webhook Service is running"}

@app.get("/{account_id}/")
async def verify_webhook(account_id: str, request: Request):
    """Verify webhook endpoint for specific account."""
    mode = request.query_params.get("hub.mode")
    token = request.query_params.get("hub.verify_token")
    challenge = request.query_params.get("hub.challenge")

    logging.debug(f"Verification for account {account_id} - Mode: {mode}, Token: {token}, Challenge: {challenge}")

    if mode and token:
        if mode == 'subscribe' and token == VERIFY_TOKEN:
            return Response(content=challenge, media_type="text/plain")
        else:
            logging.warning(f"Webhook verification failed for account {account_id}: Invalid token.")
            return Response(content="Verification token mismatch", status_code=403)
    else:
        logging.warning(f"Webhook verification failed for account {account_id}: Missing parameters.")
        return Response(content="Bad request parameters", status_code=400)

@app.post("/{account_id}/")
async def handle_webhook(account_id: str, request: Request, background_tasks: BackgroundTasks):
    """Handle webhook for specific account."""
    try:
        body = await request.json()
        
        if not body:
            logging.warning(f"Received empty JSON payload for account {account_id}.")
            return {"status": "ok"}

        if 'entry' not in body or not body['entry']:
            logging.warning(f"Invalid webhook payload for account {account_id}: {body}")
            return {"status": "ok"}

        # Process webhook in background
        background_tasks.add_task(process_webhook, body, account_id)
        
        return {"status": "ok"}

    except Exception as e:
        logging.exception(f"Error processing message for account {account_id}: {e}")
        return {"status": "error", "message": str(e)}

@app.get("/status")
async def check_status():
    """Health check endpoint."""
    return {"status": "online", "version": "1.0", "database": "connected" if get_db_connection() else "disconnected"}

if __name__ == "__main__":
    logging.info("Starting FastAPI application")
    uvicorn.run(app, host="0.0.0.0", port=8000)