from utils import logging, clean_interactive_type, remove_special_chars
import json
import os
import mysql.connector
from mysql.connector import Error
from mysql.connector.pooling import MySQLConnectionPool
import requests
from typing import Dict, Any, Optional, Tuple, List
from datetime import datetime
import asyncio
import time
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, Request, Response, BackgroundTasks, Depends
from fastapi.middleware.cors import CORSMiddleware
from starlette.concurrency import run_in_threadpool
from starlette.responses import JSONResponse

# Constants
VERIFY_TOKEN = "hello"
LOGS_DIR = 'message_logs'
DASHBOARD_URL = "https://wtsdealnow.com/user_responses/"
DLR_WEBHOOK_URL = "http://46.202.130.143:1401/dlr_webhook"

# Database configuration
DB_CONFIG = {
    'host': "localhost",
    'database': 'smsc_table',
    'user': 'prashanth@itsolution4india.com',
    'password': 'Solution@97',
    'port': 3306,
    'pool_name': 'mypool',
    'pool_size': 20,  # Adjust based on your server capacity
    'pool_reset_session': True,
    'autocommit': True
}

# Connection Pool
connection_pool = None

def get_connection_pool():
    """Create and return a database connection pool."""
    global connection_pool
    if connection_pool is None:
        try:
            connection_pool = MySQLConnectionPool(**DB_CONFIG)
            logging.info("Database connection pool established successfully")
        except Error as e:
            logging.error(f"Error creating MySQL connection pool: {e}")
    return connection_pool

# Semaphore to limit concurrent database operations
DB_SEMAPHORE = asyncio.Semaphore(20)  # Adjust based on your DB capacity

# Rate limiting settings
MAX_REQUESTS_PER_MINUTE = 1000  # Adjust based on your needs
request_count = 0
request_reset_time = time.time()

# DLR processing queue
dlr_queue = asyncio.Queue(maxsize=10000)  # Adjust size based on expected traffic
DLR_WORKERS = 5  # Number of workers processing DLR webhooks

# Background worker
async def dlr_worker():
    """Worker to process DLR webhook requests from queue."""
    while True:
        try:
            data = await dlr_queue.get()
            success = await run_in_threadpool(call_dlr_webhook, data)
            if not success:
                # If failed, wait and retry once
                await asyncio.sleep(2)
                await run_in_threadpool(call_dlr_webhook, data)
            dlr_queue.task_done()
        except Exception as e:
            logging.error(f"Error in DLR worker: {e}")
            dlr_queue.task_done()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle manager for FastAPI app."""
    # Start connection pool
    get_connection_pool()
    
    # Start DLR workers
    worker_tasks = []
    for _ in range(DLR_WORKERS):
        task = asyncio.create_task(dlr_worker())
        worker_tasks.append(task)
    
    logging.info(f"Started {DLR_WORKERS} DLR workers")
    yield
    
    # Shutdown logic
    for task in worker_tasks:
        task.cancel()
    
    logging.info("Shutting down application")

# Create FastAPI app
app = FastAPI(title="WhatsApp Webhook Service", lifespan=lifespan)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add response compression middleware
try:
    from fastapi.middleware.gzip import GZipMiddleware
    app.add_middleware(GZipMiddleware, minimum_size=1000)
except ImportError:
    logging.warning("GZipMiddleware not available. Consider installing it for better performance.")

async def get_db_connection():
    """Get a connection from the pool with async context management."""
    async with DB_SEMAPHORE:
        pool = get_connection_pool()
        if not pool:
            return None
        try:
            connection = pool.get_connection()
            return connection
        except Error as e:
            logging.error(f"Error getting connection from pool: {e}")
            return None

def parse_webhook_response(response: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Parse webhook response into standardized format with support for multiple entries."""
    reports = []
    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime('%Y-%m-%d %H:%M:%S')
    
    for entry in response.get('entry', []):
        changes = entry.get('changes', [])
        for change in changes:
            report = {'Date': formatted_datetime}
            value = change.get('value', {})
            metadata = value.get('metadata', {})
            report['display_phone_number'] = metadata.get('display_phone_number')
            report['phone_number_id'] = metadata.get('phone_number_id')
            
            message_template_id = value.get('message_template_id')
            message_template_name = value.get('message_template_name')

            if message_template_id and message_template_name:
                report['message_template_id'] = message_template_id
                report['message_template_name'] = message_template_name
            
            # Process status updates
            statuses = value.get('statuses', [])
            for status in statuses:
                status_report = report.copy()  # Create a copy for each status
                status_report['wamid'] = status.get('id')
                status_report['status'] = status.get('status')
                status_report['message_timestamp'] = status.get('timestamp')
                status_report['contact_wa_id'] = status.get('recipient_id')
                if 'errors' in status:
                    error_details = status['errors'][0]
                    status_report['error_code'] = error_details.get('code')
                    status_report['error_title'] = error_details.get('title')
                    status_report['error_message'] = error_details.get('message')
                    status_report['error_data'] = error_details.get('error_data', {}).get('details')
                reports.append(status_report)

            # Process contacts
            contacts = value.get('contacts', [])
            for contact in contacts:
                contact_name = contact.get('profile', {}).get('name', '')
                try:
                    report['contact_name'] = remove_special_chars(contact_name)
                except:
                    report['contact_name'] = ''
                report['contact_wa_id'] = contact.get('wa_id')

            # Process messages
            messages = value.get('messages', [])
            for message in messages:
                message_report = report.copy()  # Create a copy for each message
                message_report['message_from'] = message.get('from')
                message_report['status'] = 'reply'
                message_report['wamid'] = message.get('id')
                message_report['message_timestamp'] = message.get('timestamp')
                message_report['message_type'] = message.get('type')
                
                # Process different message types
                msg_type = message.get('type')
                if msg_type == 'text':
                    message_report['message_body'] = message.get('text', {}).get('body')
                elif msg_type == 'button':
                    message_report['message_body'] = message.get('button', {}).get('text')
                elif msg_type in ('image', 'document', 'video'):
                    message_report['message_body'] = message.get(msg_type, {}).get('id')
                elif msg_type == 'interactive':
                    interactive_type = message.get('interactive', {}).get('type')
                    if interactive_type == 'button_reply':
                        message_report['message_body'] = message.get('interactive', {}).get('button_reply', {}).get('title')
                    elif interactive_type == 'list_reply':
                        message_report['message_body'] = message.get('interactive', {}).get('list_reply', {}).get('title')
                    elif interactive_type == 'nfm_reply':
                        interactive_msg = message.get('interactive', {}).get('nfm_reply', {}).get('response_json')
                        if isinstance(interactive_msg, str):
                            try:
                                interactive_type_dict = json.loads(interactive_msg)
                            except json.JSONDecodeError:
                                interactive_type_dict = {"raw": interactive_msg}
                        else:
                            interactive_type_dict = interactive_msg
                        message_report['message_body'] = clean_interactive_type(interactive_type_dict)
                        message_report['message_body'] = json.dumps(message_report['message_body'])
                else:
                    message_report['message_body'] = ""
                
                reports.append(message_report)
    
    return reports

async def update_database_status(wamid: str, status: str, message_timestamp: str, 
                          error_code: Optional[int] = None, 
                          error_message: Optional[str] = None,
                          contact_name: Optional[str] = None) -> Tuple[bool, Optional[Dict]]:
    """
    Update the database record with new status information and fetch related data
    unless the dlr_status is 'sent'.
    
    Returns:
        Tuple[bool, Dict]: Success flag and dictionary containing fetched data if applicable
    """
    conn = await get_db_connection()
    if not conn:
        return False, None
    
    cursor = None
    try:
        cursor = conn.cursor(dictionary=True)
        
        # Step 1: Check current dlr_status
        check_query = "SELECT dlr_status FROM smsc_responses WHERE wamid = %s"
        cursor.execute(check_query, (wamid,))
        result = cursor.fetchone()

        if not result:
            logging.warning(f"No record found with wamid: {wamid}")
            return False, None

        if result["dlr_status"] == "sent":
            logging.info(f"DLR already sent for wamid: {wamid}, skipping fetch")
            return True, None  # No need to fetch/send again

        # Step 2: Update the record with transaction
        conn.start_transaction()
        update_query = """
        UPDATE smsc_responses
        SET status = %s, 
            message_timestamp = %s,
            error_code = %s,
            error_message = %s
        """
        update_params = [status, message_timestamp, error_code, error_message]

        if contact_name:
            update_query += ", contact_name = %s"
            update_params.append(contact_name)
        
        update_query += " WHERE wamid = %s"
        update_params.append(wamid)

        cursor.execute(update_query, update_params)
        
        # Step 3: Fetch DLR webhook data if update was successful
        if cursor.rowcount > 0:
            fetch_query = """
            SELECT username, source_addr, destination_addr, message_id, status
            FROM smsc_responses
            WHERE wamid = %s
            """
            cursor.execute(fetch_query, (wamid,))
            record = cursor.fetchone()
            conn.commit()
            return True, record
        else:
            logging.warning(f"Update failed or no rows affected for wamid: {wamid}")
            conn.commit()
            return False, None

    except Error as e:
        logging.error(f"Database error: {e}")
        if conn.is_connected():
            conn.rollback()
        return False, None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

async def update_dlr_status(message_id: str, status: str) -> None:
    """Update the dlr_status for a given message_id."""
    conn = await get_db_connection()
    if not conn:
        return
    
    cursor = None
    try:
        cursor = conn.cursor()
        query = """
        UPDATE smsc_responses
        SET dlr_status = %s
        WHERE message_id = %s
        """
        cursor.execute(query, (status, message_id))
        conn.commit()
    except Error as e:
        logging.error(f"Failed to update dlr_status: {e}")
        if conn.is_connected():
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def call_dlr_webhook(data: Dict[str, Any]) -> bool:
    """Call the DLR webhook with the provided data."""
    payload = {
        "username": data.get("username"),
        "source_addr": data.get("source_addr"),
        "destination_addr": data.get("destination_addr"),
        "message_id": data.get("message_id"),
        "status": data.get("status")
    }
    
    # Create a session with connection pooling
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=20,
        pool_maxsize=100,
        max_retries=3
    )
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    
    try:
        # Use session for better connection reuse
        response = session.post(DLR_WEBHOOK_URL, json=payload, timeout=15)
        dlr_status = 'sent' if response.status_code == 200 else 'failed'
        
        # Run in executor to avoid blocking
        asyncio.create_task(update_dlr_status(data.get("message_id"), dlr_status))
        
        if response.status_code == 200:
            logging.info(f"DLR webhook success for message_id={data.get('message_id')}")
            return True
        else:
            logging.error(
                f"DLR webhook failed: Status={response.status_code}, Message ID={data.get('message_id')}"
            )
            return False
    except Exception as e:
        logging.error(f"Error calling DLR webhook for message_id={data.get('message_id')}: {e}")
        return False
    finally:
        session.close()

async def process_webhook_entry(entry_data: Dict[str, Any]):
    """Process a single webhook entry data."""
    try:
        # Parse webhook response into multiple records if needed
        data_records = parse_webhook_response({"entry": [entry_data]})
        
        for data in data_records:
            # Check if we have the necessary data to update the database
            if 'wamid' in data:
                wamid = data.get('wamid')
                status = data.get('status')
                message_timestamp = data.get('message_timestamp')
                error_code = data.get('error_code')
                error_message = data.get('error_message')
                contact_name = data.get('contact_name')
                
                # Update database and get required data for DLR webhook
                success, record = await update_database_status(
                    wamid, status, message_timestamp, error_code, error_message, contact_name
                )
                
                # If database update was successful and we have data, queue DLR webhook call
                if success and record:
                    try:
                        await dlr_queue.put(record)
                    except asyncio.QueueFull:
                        logging.error("DLR queue full, dropping webhook call")
                        
    except Exception as e:
        logging.exception(f"Error processing webhook entry: {e}")

# Background task to handle webhook data
async def process_webhook(body: Dict[str, Any], account_id: str):
    """Process webhook data asynchronously with parallel processing for multiple entries."""
    try:
        entries = body.get('entry', [])
        
        # Process each entry in parallel with concurrency control
        # Limit concurrent tasks to prevent resource exhaustion
        tasks = []
        for entry in entries:
            task = asyncio.create_task(process_webhook_entry(entry))
            tasks.append(task)
            
            # If we have too many tasks, wait for some to complete
            if len(tasks) >= 10:  # Adjust based on your server capacity
                await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                tasks = [t for t in tasks if not t.done()]
        
        # Wait for remaining tasks
        if tasks:
            await asyncio.gather(*tasks)
            
    except Exception as e:
        logging.exception(f"Error in webhook processing: {e}")

async def check_rate_limit():
    """Check if the current request exceeds rate limits."""
    global request_count, request_reset_time
    current_time = time.time()
    
    # Reset counter if a minute has passed
    if current_time - request_reset_time >= 60:
        request_count = 0
        request_reset_time = current_time
    
    # Check if we're over the limit
    if request_count >= MAX_REQUESTS_PER_MINUTE:
        return False
    
    request_count += 1
    return True

# Health check dependency
async def check_db_health():
    """Check database connectivity for health check endpoint."""
    conn = await get_db_connection()
    is_connected = conn is not None
    if conn:
        conn.close()
    return is_connected

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

    logging.debug(f"Verification for account {account_id} - Mode: {mode}, Token: {token}")

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
    """Handle webhook for specific account with rate limiting."""
    # Check rate limit
    if not await check_rate_limit():
        return JSONResponse(
            status_code=429,
            content={"status": "error", "message": "Too many requests. Please try again later."}
        )
    
    try:
        # Efficiently read the request body
        body_bytes = await request.body()
        
        # Fast return to acknowledge webhook
        background_tasks.add_task(process_request_body, body_bytes, account_id)
        
        # Return immediately to acknowledge the webhook
        return {"status": "ok"}

    except Exception as e:
        logging.exception(f"Error handling webhook for account {account_id}: {e}")
        return {"status": "error", "message": str(e)}

async def process_request_body(body_bytes: bytes, account_id: str):
    """Process the webhook body after sending the response."""
    try:
        body = json.loads(body_bytes)
        
        if not body or 'entry' not in body or not body['entry']:
            logging.warning(f"Invalid webhook payload for account {account_id}")
            return
        
        # Process webhook
        await process_webhook(body, account_id)
        
    except Exception as e:
        logging.exception(f"Error processing message body for account {account_id}: {e}")

@app.get("/status")
async def check_status(db_health: bool = Depends(check_db_health)):
    """Health check endpoint."""
    queue_size = dlr_queue.qsize()
    return {
        "status": "online", 
        "version": "1.0", 
        "database": "connected" if db_health else "disconnected",
        "queue_size": queue_size,
        "workers": DLR_WORKERS,
        "request_rate": f"{request_count}/{MAX_REQUESTS_PER_MINUTE} per minute"
    }

@app.get("/metrics")
async def metrics():
    """Metrics endpoint for monitoring."""
    db_health = await check_db_health()
    queue_size = dlr_queue.qsize()
    queue_percentage = (queue_size / dlr_queue.maxsize) * 100 if dlr_queue.maxsize > 0 else 0
    
    return {
        "database_connected": db_health,
        "queue_size": queue_size,
        "queue_percentage": queue_percentage,
        "requests_per_minute": request_count,
        "workers_active": DLR_WORKERS
    }

if __name__ == "__main__":
    logging.info("Starting FastAPI application")
    uvicorn.run(app, host="0.0.0.0", port=8000)