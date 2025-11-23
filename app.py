from flask import Flask, render_template, request, jsonify, send_file, session, send_from_directory, url_for
import logging
from flask_session import Session
import os
import base64
import requests
import json
import re
from werkzeug.utils import secure_filename
import uuid
from datetime import datetime, timedelta
import shutil
import time
from threading import Thread
import threading
import uuid as uuid_module

app = Flask(__name__)

# In-memory storage for scraping events/status (for real-time preview)
scraping_status = {}
scraping_status_lock = threading.Lock()
app.config['SECRET_KEY'] = 'your-secret-key-here'
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['OUTPUT_FOLDER'] = 'outputs'
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB max file size
app.config['SESSION_TYPE'] = 'filesystem'
Session(app)

# Basic logging - configure to log to both file and console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('server.log', encoding='utf-8'),
        logging.StreamHandler()  # Also log to console
    ]
)
logger = logging.getLogger(__name__)

# PP-StructureV3 API Configuration
# Using API URL from official documentation (PP-StructureV3_API_en documentation.txt)
API_URL = "https://wfk3ide9lcd0x0k9.aistudio-hub.baidu.com/layout-parsing"
TOKEN = "031c87b3c44d16aa4adf6928bcfa132e23393afc"

ALLOWED_EXTENSIONS = {'pdf', 'png', 'jpg', 'jpeg', 'xls', 'xlsx'}

# Create necessary directories
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['OUTPUT_FOLDER'], exist_ok=True)
os.makedirs('flask_session', exist_ok=True)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def cleanup_session_files():
    """Clean up all uploaded and extracted files for current session"""
    session_id = session.get('session_id')
    if session_id:
        session_upload_dir = os.path.join(app.config['UPLOAD_FOLDER'], session_id)
        session_output_dir = os.path.join(app.config['OUTPUT_FOLDER'], session_id)
        
        if os.path.exists(session_upload_dir):
            shutil.rmtree(session_upload_dir)
        if os.path.exists(session_output_dir):
            shutil.rmtree(session_output_dir)

def cleanup_old_files(hours=24):
    """Clean up files and directories older than specified hours"""
    cutoff_time = time.time() - (hours * 3600)
    cleaned = {'uploads': 0, 'outputs': 0, 'sessions': 0}
    
    # Clean old upload directories
    for session_dir in os.listdir(app.config['UPLOAD_FOLDER']):
        dir_path = os.path.join(app.config['UPLOAD_FOLDER'], session_dir)
        if os.path.isdir(dir_path) and os.path.getmtime(dir_path) < cutoff_time:
            try:
                shutil.rmtree(dir_path)
                cleaned['uploads'] += 1
                logger.info(f"Cleaned old upload directory: {session_dir}")
            except Exception as e:
                logger.error(f"Error cleaning upload directory {session_dir}: {e}")
    
    # Clean old output directories
    for session_dir in os.listdir(app.config['OUTPUT_FOLDER']):
        dir_path = os.path.join(app.config['OUTPUT_FOLDER'], session_dir)
        if os.path.isdir(dir_path) and os.path.getmtime(dir_path) < cutoff_time:
            try:
                shutil.rmtree(dir_path)
                cleaned['outputs'] += 1
                logger.info(f"Cleaned old output directory: {session_dir}")
            except Exception as e:
                logger.error(f"Error cleaning output directory {session_dir}: {e}")
    
    # Clean old flask session files
    session_dir = 'flask_session'
    if os.path.exists(session_dir):
        for session_file in os.listdir(session_dir):
            file_path = os.path.join(session_dir, session_file)
            if os.path.isfile(file_path) and os.path.getmtime(file_path) < cutoff_time:
                try:
                    os.remove(file_path)
                    cleaned['sessions'] += 1
                    logger.info(f"Cleaned old session file: {session_file}")
                except Exception as e:
                    logger.error(f"Error cleaning session file {session_file}: {e}")
    
    return cleaned

def cleanup_other_sessions(current_session_id):
    """Clean up all sessions EXCEPT the current one"""
    cleaned = {'uploads': 0, 'outputs': 0, 'sessions': 0}
    
    # Clean other session upload directories
    if os.path.exists(app.config['UPLOAD_FOLDER']):
        for session_dir in os.listdir(app.config['UPLOAD_FOLDER']):
            if session_dir != current_session_id:
                dir_path = os.path.join(app.config['UPLOAD_FOLDER'], session_dir)
                if os.path.isdir(dir_path):
                    try:
                        shutil.rmtree(dir_path)
                        cleaned['uploads'] += 1
                        logger.info(f"Cleaned other session upload directory: {session_dir}")
                    except Exception as e:
                        logger.error(f"Error cleaning upload directory {session_dir}: {e}")
    
    # Clean other session output directories
    if os.path.exists(app.config['OUTPUT_FOLDER']):
        for session_dir in os.listdir(app.config['OUTPUT_FOLDER']):
            if session_dir != current_session_id:
                dir_path = os.path.join(app.config['OUTPUT_FOLDER'], session_dir)
                if os.path.isdir(dir_path):
                    try:
                        shutil.rmtree(dir_path)
                        cleaned['outputs'] += 1
                        logger.info(f"Cleaned other session output directory: {session_dir}")
                    except Exception as e:
                        logger.error(f"Error cleaning output directory {session_dir}: {e}")
    
    # Keep flask session files - they are needed for active sessions
    # Only clean files older than 24 hours
    session_dir = 'flask_session'
    if os.path.exists(session_dir):
        cutoff_time = time.time() - (24 * 3600)
        for session_file in os.listdir(session_dir):
            file_path = os.path.join(session_dir, session_file)
            if os.path.isfile(file_path) and os.path.getmtime(file_path) < cutoff_time:
                try:
                    os.remove(file_path)
                    cleaned['sessions'] += 1
                    logger.info(f"Cleaned old session file: {session_file}")
                except Exception as e:
                    logger.error(f"Error cleaning session file {session_file}: {e}")
    
    return cleaned

def cleanup_all_sessions():
    """Clean up ALL session data (aggressive cleanup on startup)"""
    cleaned = {'uploads': 0, 'outputs': 0, 'sessions': 0}
    
    # Clean ALL upload directories
    if os.path.exists(app.config['UPLOAD_FOLDER']):
        for session_dir in os.listdir(app.config['UPLOAD_FOLDER']):
            dir_path = os.path.join(app.config['UPLOAD_FOLDER'], session_dir)
            if os.path.isdir(dir_path):
                try:
                    shutil.rmtree(dir_path)
                    cleaned['uploads'] += 1
                    logger.info(f"Cleaned upload directory: {session_dir}")
                except Exception as e:
                    logger.error(f"Error cleaning upload directory {session_dir}: {e}")
    
    # Clean ALL output directories
    if os.path.exists(app.config['OUTPUT_FOLDER']):
        for item in os.listdir(app.config['OUTPUT_FOLDER']):
            dir_path = os.path.join(app.config['OUTPUT_FOLDER'], item)
            if os.path.isdir(dir_path):
                try:
                    shutil.rmtree(dir_path)
                    cleaned['outputs'] += 1
                    logger.info(f"Cleaned output directory: {item}")
                except Exception as e:
                    logger.error(f"Error cleaning output directory {item}: {e}")
    
    # Clean ALL flask session files
    session_dir = 'flask_session'
    if os.path.exists(session_dir):
        for session_file in os.listdir(session_dir):
            file_path = os.path.join(session_dir, session_file)
            if os.path.isfile(file_path):
                try:
                    os.remove(file_path)
                    cleaned['sessions'] += 1
                    logger.info(f"Cleaned session file: {session_file}")
                except Exception as e:
                    logger.error(f"Error cleaning session file {session_file}: {e}")
    
    return cleaned

def periodic_cleanup():
    """Run cleanup periodically in background"""
    while True:
        time.sleep(3600)  # Run every hour
        try:
            cleaned = cleanup_old_files(hours=24)
            logger.info(f"Periodic cleanup: {cleaned}")
        except Exception as e:
            logger.error(f"Error in periodic cleanup: {e}")

@app.before_request
def before_request():
    """Initialize session and ensure session directories exist"""
    if 'session_id' not in session:
        session['session_id'] = str(uuid.uuid4())
        # Only initialize uploaded_files if it doesn't exist
        if 'uploaded_files' not in session:
            session['uploaded_files'] = []
        logger.info(f"New session created: {session['session_id']}, Files in session: {len(session.get('uploaded_files', []))}")
        
        # Don't clean up on every new session - let periodic cleanup handle it
        # This prevents deleting files that users are still working with
        logger.info(f"New session started. Periodic cleanup will handle old files (runs every hour).")
    else:
        # For existing sessions, ensure uploaded_files exists
        if 'uploaded_files' not in session:
            session['uploaded_files'] = []
            logger.info(f"Initialized uploaded_files for existing session: {session['session_id']}")
        else:
            logger.debug(f"Existing session: {session['session_id']}, Files: {len(session['uploaded_files'])}")
    
    # Create session-specific directories
    session_id = session['session_id']
    os.makedirs(os.path.join(app.config['UPLOAD_FOLDER'], session_id), exist_ok=True)
    os.makedirs(os.path.join(app.config['OUTPUT_FOLDER'], session_id), exist_ok=True)

@app.route('/fix-session')
def fix_session_page():
    """Serve the session fix page"""
    return send_file('fix_session.html')

@app.route('/landing')
def landing():
    """Modern landing page"""
    return render_template('landing.html')

@app.route('/app')
def main_app():
    """Main application page"""
    # Clean up other sessions on every page load
    try:
        session_id = session.get('session_id')
        if session_id:
            cleaned = cleanup_other_sessions(session_id)
            if any(cleaned.values()):
                logger.info(f"Page load cleanup (removed other sessions): {cleaned}")
    except Exception as e:
        logger.error(f"Error in page load cleanup: {e}")
    
    return render_template('index.html')

@app.route('/')
def index():
    """Home page - show landing page by default"""
    # Check if user wants to go directly to app
    if request.args.get('workflow') or request.args.get('app'):
        return render_template('index.html')
    # Otherwise show landing page
    return render_template('landing.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    """Handle file upload"""
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400
    
    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        session_id = session['session_id']
        
        # Create unique filename
        unique_filename = f"{uuid.uuid4()}_{filename}"
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], session_id, unique_filename)
        
        file.save(filepath)
        
        # Add to uploaded files list
        uploaded_files = session.get('uploaded_files', [])
        file_info = {
            'id': str(uuid.uuid4()),
            'original_name': filename,
            'unique_name': unique_filename,
            'filepath': filepath,
            'upload_time': datetime.now().isoformat(),
            'status': 'uploaded'
        }
        uploaded_files.append(file_info)
        session['uploaded_files'] = uploaded_files
        session.modified = True
        
        logger.info(f"File uploaded: {filename}, Session ID: {session_id}, Total files in session: {len(uploaded_files)}")
        
        return jsonify({
            'success': True,
            'file_id': file_info['id'],
            'filename': filename,
            'message': 'File uploaded successfully'
        })
    
    return jsonify({'error': 'Invalid file type'}), 400

@app.route('/upload-and-extract', methods=['POST'])
def upload_and_extract():
    """Handle file upload and automatically extract"""
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400
    
    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        session_id = session['session_id']
        
        # Create unique filename
        unique_filename = f"{uuid.uuid4()}_{filename}"
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], session_id, unique_filename)
        
        file.save(filepath)
        
        # Get extraction settings from form data
        extraction_settings = {}
        if 'extraction_settings' in request.form:
            try:
                extraction_settings = json.loads(request.form['extraction_settings'])
            except:
                pass
        
        # Add to uploaded files list
        uploaded_files = session.get('uploaded_files', [])
        
        # Check for duplicate uploads (same filename within last 5 seconds)
        current_time = datetime.now()
        is_duplicate = False
        for existing_file in uploaded_files:
            if existing_file.get('original_name') == filename:
                upload_time_str = existing_file.get('upload_time', '')
                try:
                    upload_time = datetime.fromisoformat(upload_time_str)
                    time_diff = (current_time - upload_time).total_seconds()
                    if time_diff < 5:  # Within 5 seconds
                        is_duplicate = True
                        file_info = existing_file  # Reuse existing file info
                        logger.warning(f"Duplicate upload detected for {filename}, reusing file_id: {file_info['id']}")
                        break
                except:
                    pass
        
        if not is_duplicate:
            file_info = {
                'id': str(uuid.uuid4()),
                'original_name': filename,
                'unique_name': unique_filename,
                'filepath': filepath,
                'upload_time': datetime.now().isoformat(),
                'status': 'uploaded'
            }
            uploaded_files.append(file_info)
            session['uploaded_files'] = uploaded_files
            session.modified = True
            logger.info(f"File uploaded: {filename}, Session ID: {session_id}, Total files in session: {len(uploaded_files)}")
        else:
            logger.info(f"Duplicate upload handled for: {filename}, Session ID: {session_id}")
        
        # Automatically extract the file
        extraction_result = None
        extraction_error = None
        try:
            logger.info(f"Starting automatic extraction for file: {filename}")
            
            # Read file and encode to base64
            file_size = os.path.getsize(filepath)
            with open(filepath, 'rb') as f:
                file_bytes = f.read()
                file_data = base64.b64encode(file_bytes).decode('ascii')
            
            # Determine file type
            file_extension = filename.rsplit('.', 1)[1].lower()
            file_type = 0 if file_extension == 'pdf' else 1
            
            headers = {
                "Authorization": f"token {TOKEN}",
                "Content-Type": "application/json"
            }
            
            # Use extraction settings from form or defaults
            payload = {
                "file": file_data,
                "fileType": file_type,
                "useDocPreprocessor": extraction_settings.get("useDocPreprocessor", False),
                "useSealRecognition": extraction_settings.get("useSealRecognition", True),
                "useTableRecognition": extraction_settings.get("useTableRecognition", True),
                "useFormulaRecognition": extraction_settings.get("useFormulaRecognition", False),
                "useChartRecognition": extraction_settings.get("useChartRecognition", False),
                "useRegionDetection": extraction_settings.get("useRegionDetection", True),
                "formatBlockContent": extraction_settings.get("formatBlockContent", True),
                "useTextlineOrientation": extraction_settings.get("useTextlineOrientation", False),
                "useDocOrientationClassify": extraction_settings.get("useDocOrientationClassify", False),
                "visualize": extraction_settings.get("visualize", False)
            }
            
            # Call extraction API - increased timeout for large files
            timeout = 300 if file_size > 5 * 1024 * 1024 else 180  # 5 min for >5MB, 3 min for smaller
            response = requests.post(API_URL, json=payload, headers=headers, timeout=timeout)
            logger.info(f'API request sent with timeout: {timeout}s for file size: {file_size / (1024*1024):.2f}MB')
            response.raise_for_status()
            api_response = response.json()
            
            # Extract the result - API might wrap it in 'result' key
            if 'result' in api_response:
                extraction_result = api_response['result']
            else:
                extraction_result = api_response
            
            logger.info(f"Extraction result structure - keys: {list(extraction_result.keys()) if isinstance(extraction_result, dict) else 'Not a dict'}")
            
            # Download and save images from API response (same as regular extract endpoint)
            session_id = session['session_id']
            output_dir = os.path.join(app.config['OUTPUT_FOLDER'], session_id, file_info['id'])
            images_dir = os.path.join(output_dir, 'imgs')
            os.makedirs(output_dir, exist_ok=True)
            os.makedirs(images_dir, exist_ok=True)
            
            # Process images from extraction result
            for i, res in enumerate(extraction_result.get("layoutParsingResults", [])):
                # Check for images in markdown
                markdown_data = res.get("markdown", {})
                markdown_text = markdown_data.get("text", "")
                images_dict = markdown_data.get("images", {})
                
                # Download images and replace URLs with local paths
                for img_path, img_url in images_dict.items():
                    try:
                        img_response = requests.get(img_url, timeout=30)
                        if img_response.status_code == 200:
                            # Save image locally
                            local_img_path = os.path.join(images_dir, os.path.basename(img_path))
                            with open(local_img_path, 'wb') as img_file:
                                img_file.write(img_response.content)
                            
                            # Create URL-safe path for serving
                            relative_img_path = f"imgs/{os.path.basename(img_path)}"
                            local_url = url_for('serve_output', session_id=session_id, filename=f"{file_info['id']}/{relative_img_path}")
                            
                            # Replace remote URL with local URL in markdown
                            markdown_text = markdown_text.replace(img_path, local_url)
                            
                            logger.info(f'Downloaded image: {img_path} -> {local_img_path}')
                        else:
                            logger.warning(f'Failed to download image {img_url}: HTTP {img_response.status_code}')
                    except requests.exceptions.Timeout:
                        logger.warning(f'Image download timeout for {img_url}')
                    except requests.exceptions.ConnectionError:
                        logger.warning(f'Image download connection error for {img_url}')
                    except Exception as e:
                            logger.warning(f'Error downloading image {img_url}: {e}')
                
                # Also update block_content in prunedResult if it exists
                pruned_result = res.get("prunedResult", {})
                parsing_res_list = pruned_result.get("parsing_res_list", [])
                for block in parsing_res_list:
                    if block.get("block_content"):
                        block_content = block["block_content"]
                        # Replace image paths in block content
                        for img_path, img_url in images_dict.items():
                            relative_img_path = f"imgs/{os.path.basename(img_path)}"
                            local_url = url_for('serve_output', session_id=session_id, filename=f"{file_info['id']}/{relative_img_path}")
                            block_content = block_content.replace(img_path, local_url)
                        block["block_content"] = block_content
                
                # Update the markdown text with local image URLs
                res['markdown']['text'] = markdown_text
            
            # Store output directory for later use
            file_info['output_dir'] = output_dir
            
            # Update file info with extraction result
            file_info['status'] = 'extracted'
            file_info['extraction_result'] = extraction_result
            
            # Update in session
            for i, f in enumerate(uploaded_files):
                if f['id'] == file_info['id']:
                    uploaded_files[i] = file_info
                    break
            session['uploaded_files'] = uploaded_files
            session.modified = True
            
            logger.info(f"Extraction successful for file: {filename}")
            
        except Exception as e:
            logger.exception(f"Error during automatic extraction for {filename}")
            extraction_error = str(e)
            # File is still uploaded, just extraction failed
            file_info['status'] = 'uploaded'
            file_info['extraction_error'] = extraction_error
        
        return jsonify({
            'success': True,
            'file_id': file_info['id'],
            'filename': filename,
            'message': 'File uploaded successfully',
            'extraction_success': extraction_result is not None,
            'extraction_error': extraction_error
        })
    
    return jsonify({'error': 'Invalid file type'}), 400

def convert_costed_data_to_html(costed_data):
    """Convert costed_data (headers/rows format) to HTML table format"""
    try:
        from bs4 import BeautifulSoup
        
        tables = costed_data.get('tables', [])
        if not tables:
            return ''
        
        # Use first table (or combine all tables)
        table_data = tables[0] if tables else {}
        headers = table_data.get('headers', [])
        rows = table_data.get('rows', [])
        
        if not headers or not rows:
            return ''
        
        # Build HTML table
        html = '<table border="1" style="width: 100%; border-collapse: collapse; background: white;">'
        html += '<thead><tr>'
        
        for header in headers:
            # Skip Action column
            if header.lower() in ['action', 'actions', 'product selection', 'productselection']:
                continue
            html += f'<th style="background: #1a365d; color: #d4af37; padding: 12px; border: 1px solid #d4af37; text-align: center;">{header}</th>'
        
        html += '</tr></thead><tbody>'
        
        for row in rows:
            html += '<tr>'
            for header in headers:
                # Skip Action column
                if header.lower() in ['action', 'actions', 'product selection', 'productselection']:
                    continue
                cell_value = row.get(header, '')
                # If cell value contains HTML (like images), use it directly, otherwise escape
                if '<img' in str(cell_value) or '<' in str(cell_value):
                    html += f'<td style="padding: 8px; border: 1px solid #ddd;">{cell_value}</td>'
                else:
                    # Escape HTML
                    escaped_value = str(cell_value).replace('<', '&lt;').replace('>', '&gt;').replace('&', '&amp;')
                    html += f'<td style="padding: 8px; border: 1px solid #ddd;">{escaped_value}</td>'
            html += '</tr>'
        
        html += '</tbody></table>'
        return html
    except Exception as e:
        logger.exception('Error converting costed_data to HTML')
        return ''

@app.route('/api/files/list', methods=['GET'])
def list_files():
    """List uploaded files for current session (for multi-budget feature)"""
    try:
        uploaded_files = session.get('uploaded_files', [])
        # Filter out sensitive data and only return file metadata with stitched_table
        # Exclude multibudget export files (they are generated, not uploaded)
        files_list = []
        for f in uploaded_files:
            # Skip multibudget export files - these are generated by the app, not user uploads
            if f.get('multibudget', False):
                continue
            
            file_data = {
                'id': f.get('id'),
                'original_name': f.get('original_name'),
                'upload_time': f.get('upload_time'),
                'has_extraction': 'extraction_result' in f,
                'has_stitched': 'stitched_table' in f,
                'has_costed': 'costed_data' in f
            }
            # Include stitched_table HTML if available
            if 'stitched_table' in f:
                file_data['stitched_table'] = {
                    'html': f['stitched_table'].get('html', '')
                }
            # Include costed_data with HTML conversion if available
            if 'costed_data' in f:
                costed_data = f['costed_data']
                # Convert costed_data (headers/rows format) to HTML table
                costed_html = convert_costed_data_to_html(costed_data)
                file_data['costed_table'] = {
                    'html': costed_html
                }
            files_list.append(file_data)
        
        return jsonify({
            'success': True,
            'files': files_list
        })
    except Exception as e:
        logger.exception('Error listing files')
        return jsonify({'error': str(e)}), 500

@app.route('/files', methods=['GET'])
def get_uploaded_files():
    """Get list of uploaded files (excludes multibudget export files)"""
    try:
        uploaded_files = session.get('uploaded_files', [])
        logger.info(f"Session ID: {session.get('session_id', 'N/A')}, Found {len(uploaded_files)} files in session")
        
        # Filter out multibudget export files - these are generated, not uploaded
        user_uploaded_files = [f for f in uploaded_files if not f.get('multibudget', False)]
        
        # Also check if files still exist on disk and validate them
        validated_files = []
        session_id = session.get('session_id')
        if session_id:
            upload_dir = os.path.join(app.config['UPLOAD_FOLDER'], session_id)
            if os.path.exists(upload_dir):
                for file_info in user_uploaded_files:
                    filepath = file_info.get('filepath')
                    if filepath and os.path.exists(filepath):
                        validated_files.append(file_info)
                    else:
                        logger.warning(f"File not found on disk: {filepath}")
        
        logger.info(f"Returning {len(validated_files)} validated files")
        return jsonify({'files': validated_files})
    except Exception as e:
        logger.exception('Error getting uploaded files')
        return jsonify({'files': [], 'error': str(e)}), 500

@app.route('/delete-file/<file_id>', methods=['DELETE'])
def delete_file(file_id):
    """Delete a specific uploaded file"""
    uploaded_files = session.get('uploaded_files', [])
    file_to_delete = None
    
    for file_info in uploaded_files:
        if file_info['id'] == file_id:
            file_to_delete = file_info
            break
    
    if file_to_delete:
        # Delete physical file
        if os.path.exists(file_to_delete['filepath']):
            os.remove(file_to_delete['filepath'])
        
        # Remove from session
        uploaded_files.remove(file_to_delete)
        session['uploaded_files'] = uploaded_files
        
        return jsonify({'success': True, 'message': 'File deleted'})
    
    return jsonify({'error': 'File not found'}), 404

@app.route('/cleanup', methods=['POST'])
def cleanup():
    """Clean up all session files"""
    cleanup_session_files()
    session['uploaded_files'] = []
    return jsonify({'success': True, 'message': 'All files cleaned up'})

@app.route('/clear-session', methods=['POST'])
def clear_session():
    """Force clear all session data - useful after cleanup issues"""
    try:
        session_id = session.get('session_id')
        if session_id:
            cleanup_session_files()
        session.clear()
        # Generate new session ID
        session['session_id'] = str(uuid.uuid4())
        logger.info(f'Session cleared and reset. New session ID: {session["session_id"]}')
        return jsonify({'success': True, 'message': 'Session cleared successfully', 'new_session_id': session['session_id']})
    except Exception as e:
        logger.exception('Error clearing session')
        return jsonify({'error': str(e)}), 500

@app.route('/preprocess/<file_id>', methods=['POST'])
def preprocess_file(file_id):
    """Preprocess PDF to detect and stitch tables"""
    from utils.pdf_processor import PDFProcessor
    
    uploaded_files = session.get('uploaded_files', [])
    file_info = None
    
    for f in uploaded_files:
        if f['id'] == file_id:
            file_info = f
            break
    
    if not file_info:
        return jsonify({'error': 'File not found'}), 404
    
    try:
        processor = PDFProcessor()
        result = processor.preprocess_pdf(file_info['filepath'], session['session_id'])

        # Convert local output paths to URLs that the frontend can fetch
        session_id = session['session_id']
        # stitched_image
        stitched_local = result.get('stitched_image')
        if stitched_local:
            # stitched_local is like outputs/<session_id>/preprocessing/stitched_table.jpg
            stitched_rel = os.path.relpath(stitched_local, os.path.join(app.config['OUTPUT_FOLDER'], session_id))
            result['stitched_image_url'] = url_for('serve_output', session_id=session_id, filename=stitched_rel)

        # thumbnails
        thumbs = result.get('thumbnails', [])
        thumb_urls = []
        for t in thumbs:
            thumb_local = t.get('path')
            if thumb_local:
                thumb_rel = os.path.relpath(thumb_local, os.path.join(app.config['OUTPUT_FOLDER'], session_id))
                t['path_url'] = url_for('serve_output', session_id=session_id, filename=thumb_rel)
            thumb_urls.append(t)

        result['thumbnails'] = thumb_urls

        # Update file status
        file_info['status'] = 'preprocessed'
        file_info['preprocessed_data'] = result
        session.modified = True

        return jsonify({
            'success': True,
            'result': result,
            'message': 'Preprocessing completed'
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/outputs/<session_id>/<path:filename>')
def serve_output(session_id, filename):
    """Serve files from the outputs directory for the given session."""
    base_dir = os.path.join(app.config['OUTPUT_FOLDER'], session_id)
    # Security: prevent path traversal by resolving real path
    full_path = os.path.realpath(os.path.join(base_dir, filename))
    if not full_path.startswith(os.path.realpath(base_dir)):
        return jsonify({'error': 'Invalid file path'}), 400
    if not os.path.exists(full_path):
        return jsonify({'error': 'File not found'}), 404
    # send_from_directory expects directory and filename relative to it
    rel_dir = os.path.dirname(filename)
    rel_file = os.path.basename(filename)
    return send_from_directory(os.path.join(app.config['OUTPUT_FOLDER'], session_id, rel_dir), rel_file)

@app.route('/extract/<file_id>', methods=['POST'])
def extract_table(file_id):
    """Extract table using PP-StructureV3 API"""
    logger.info(f'Extract request received for file_id: {file_id}')
    
    try:
        uploaded_files = session.get('uploaded_files', [])
        logger.info(f'Found {len(uploaded_files)} files in session')
        
        file_info = None
        for f in uploaded_files:
            if f['id'] == file_id:
                file_info = f
                logger.info(f'File found: {file_info.get("original_name", "unknown")}')
                break
        
        if not file_info:
            logger.error(f'File not found in session: {file_id}')
            return jsonify({'error': 'File not found'}), 404
        
        if not os.path.exists(file_info['filepath']):
            logger.error(f'File path does not exist: {file_info["filepath"]}')
            return jsonify({'error': 'File not found on disk'}), 404
        
        logger.info(f'Starting extraction for file: {file_info["original_name"]} at {file_info["filepath"]}')
        
        # Read file and encode to base64
        file_size = os.path.getsize(file_info['filepath'])
        file_size_mb = file_size / (1024 * 1024)
        
        # Check file size - base64 encoding increases size by ~33%
        # Warn if file is larger than 30MB (base64 encoded will be ~40MB)
        if file_size > 30 * 1024 * 1024:
            logger.warning(f'Large file detected: {file_size_mb:.2f}MB')
        
        with open(file_info['filepath'], 'rb') as file:
            file_bytes = file.read()
            file_data = base64.b64encode(file_bytes).decode('ascii')
        
        # Log payload size for debugging
        payload_size = len(file_data.encode('utf-8'))
        payload_size_mb = payload_size / (1024 * 1024)
        logger.info(f'File size: {file_size_mb:.2f}MB, Payload size: {payload_size_mb:.2f}MB')
        
        # Determine file type
        file_extension = file_info['original_name'].rsplit('.', 1)[1].lower()
        file_type = 0 if file_extension == 'pdf' else 1
        
        headers = {
            "Authorization": f"token {TOKEN}",
            "Content-Type": "application/json"
        }
        
        # Get extraction settings from request or use defaults from example JSON
        # Default settings from borderless.png_by_PP-StructrueV3.json example
        settings = request.json if request.is_json else {}
        payload = {
            "file": file_data,
            "fileType": file_type,
            # Default settings from example JSON that produced clean extraction
            "useDocPreprocessor": settings.get("useDocPreprocessor", False),
            "useSealRecognition": settings.get("useSealRecognition", True),
            "useTableRecognition": settings.get("useTableRecognition", True),
            "useFormulaRecognition": settings.get("useFormulaRecognition", True),
            "useChartRecognition": settings.get("useChartRecognition", False),
            "useRegionDetection": settings.get("useRegionDetection", True),
            "formatBlockContent": settings.get("formatBlockContent", True),
            "useTextlineOrientation": settings.get("useTextlineOrientation", False),
            "useDocOrientationClassify": settings.get("useDocOrientationClassify", False),
            "visualize": settings.get("visualize", True)
        }
        
        logger.info(f'Extraction settings: {json.dumps({k: v for k, v in payload.items() if k != "file"}, indent=2)}')
        
        # Retry logic with exponential backoff
        max_retries = 3
        retry_delay = 2  # seconds
        last_error = None
        
        for attempt in range(max_retries):
            try:
                # Increase timeout significantly for large files
                if file_size > 15 * 1024 * 1024:  # > 15MB
                    timeout = 360  # 6 minutes
                elif file_size > 10 * 1024 * 1024:  # > 10MB
                    timeout = 300  # 5 minutes
                elif file_size > 5 * 1024 * 1024:  # > 5MB
                    timeout = 240  # 4 minutes
                else:
                    timeout = 180  # 3 minutes for smaller files
                
                logger.info(f'Using timeout: {timeout}s for file size: {file_size / (1024*1024):.2f}MB')
                
                # Use simple post request matching the API example
                # Avoid session pooling which might cause connection resets
                logger.info(f'Attempting API request (attempt {attempt + 1}/{max_retries})...')
                logger.info(f'Payload size: {len(json.dumps(payload)) / 1024:.2f}KB')
                
                response = requests.post(
                    API_URL, 
                    json=payload, 
                    headers=headers, 
                    timeout=timeout,
                    stream=False  # Don't stream for JSON requests
                )
                
                # If we get a response, break out of retry loop
                break
                
            except requests.exceptions.ConnectionError as ce:
                last_error = ce
                error_str = str(ce)
                
                # Check if it's a connection reset error
                if 'ConnectionResetError' in error_str or 'forcibly closed' in error_str or '10054' in error_str:
                    if attempt < max_retries - 1:
                        wait_time = retry_delay * (2 ** attempt)
                        logger.warning(f'Connection reset by server. Retrying in {wait_time} seconds... (attempt {attempt + 1}/{max_retries})')
                        time.sleep(wait_time)
                        continue
                    else:
                        # Last attempt failed
                        logger.error(f'PP-StructureV3 API connection reset after {max_retries} attempts')
                        return jsonify({
                            'error': 'Connection reset by server',
                            'message': f'The API server closed the connection. This may be due to file size ({file_size_mb:.2f}MB) or server limits.',
                            'details': error_str,
                            'file_size_mb': round(file_size_mb, 2),
                            'payload_size_mb': round(payload_size_mb, 2),
                            'api_url': API_URL,
                            'hint': 'Try splitting large files or reducing file size. The API may have size limits.',
                            'attempts': max_retries
                        }), 502
                else:
                    # Other connection errors
                    raise
            except requests.exceptions.Timeout:
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (2 ** attempt)
                    logger.warning(f'Request timeout. Retrying in {wait_time} seconds... (attempt {attempt + 1}/{max_retries})')
                    time.sleep(wait_time)
                    continue
                else:
                    raise
        else:
            # If we exhausted all retries
            if last_error:
                logger.error(f'All retry attempts failed. Last error: {last_error}')
                raise last_error
        
        # Check if we have a response object
        if 'response' not in locals() or response is None:
            logger.error('No response received from API after retries')
            return jsonify({
                'error': 'No response from API',
                'message': 'The API server did not respond after multiple attempts.',
                'hint': 'Please check your internet connection and API status.'
            }), 502
        
        # If we get here, we have a response - continue processing

        # Log status and small preview of response for debugging
        logger.info('PP-StructureV3 response status: %s', response.status_code)
        logger.info('PP-StructureV3 response content-type: %s', response.headers.get('Content-Type', 'unknown'))
        resp_text = None
        try:
            resp_text = response.text[:2000]
            logger.info('PP-StructureV3 response body (truncated): %s', resp_text)
        except Exception as e:
            logger.warning(f'Could not read response text: {e}')

        # Check if response is HTML (error page)
        content_type = response.headers.get('Content-Type', '')
        if 'text/html' in content_type:
            logger.error('API returned HTML instead of JSON - likely an error page or invalid endpoint')
            return jsonify({
                'error': 'API returned HTML instead of JSON',
                'message': 'The API server returned an HTML page instead of JSON. This may indicate an authentication error or invalid endpoint.',
                'status_code': response.status_code,
                'content_type': content_type,
                'body_preview': resp_text[:500] if resp_text else 'No response body',
                'hint': 'Check API_URL and TOKEN are correct. The API may be down or the endpoint may have changed.'
            }), 502

        if response.status_code == 200:
            try:
                result = response.json().get("result")
            except Exception as je:
                logger.exception('Failed to decode JSON from PP-StructureV3 response')
                return jsonify({'error': 'Invalid JSON from API', 'status_code': response.status_code, 'body': resp_text}), 502

            # Save results
            session_id = session['session_id']
            output_dir = os.path.join(app.config['OUTPUT_FOLDER'], session_id, file_id)
            images_dir = os.path.join(output_dir, 'imgs')
            os.makedirs(output_dir, exist_ok=True)
            os.makedirs(images_dir, exist_ok=True)

            # Download and save images from API response
            for i, res in enumerate(result.get("layoutParsingResults", [])):
                # Save markdown
                md_filename = os.path.join(output_dir, f"doc_{i}.md")
                
                # Check for images in markdown
                markdown_data = res.get("markdown", {})
                markdown_text = markdown_data.get("text", "")
                images_dict = markdown_data.get("images", {})
                
                # Download images and replace URLs with local paths
                for img_path, img_url in images_dict.items():
                    try:
                        img_response = requests.get(img_url, timeout=30)
                        if img_response.status_code == 200:
                            # Save image locally
                            local_img_path = os.path.join(images_dir, os.path.basename(img_path))
                            with open(local_img_path, 'wb') as img_file:
                                img_file.write(img_response.content)
                            
                            # Create URL-safe path for serving
                            relative_img_path = f"imgs/{os.path.basename(img_path)}"
                            local_url = url_for('serve_output', session_id=session_id, filename=f"{file_id}/{relative_img_path}")
                            
                            # Replace remote URL with local URL in markdown
                            markdown_text = markdown_text.replace(img_path, local_url)
                            
                            logger.info(f'Downloaded image: {img_path} -> {local_img_path}')
                        else:
                            logger.warning(f'Failed to download image {img_url}: HTTP {img_response.status_code}')
                    except requests.exceptions.Timeout:
                        logger.warning(f'Image download timeout for {img_url}')
                    except requests.exceptions.ConnectionError:
                        logger.warning(f'Image download connection error for {img_url}')
                    except Exception as e:
                        logger.error(f'Failed to download image {img_url}: {str(e)}')
                
                # Also update block_content in prunedResult if it exists
                pruned_result = res.get("prunedResult", {})
                parsing_res_list = pruned_result.get("parsing_res_list", [])
                for block in parsing_res_list:
                    if block.get("block_content"):
                        block_content = block["block_content"]
                        # Replace image paths in block content
                        for img_path, img_url in images_dict.items():
                            relative_img_path = f"imgs/{os.path.basename(img_path)}"
                            local_url = url_for('serve_output', session_id=session_id, filename=f"{file_id}/{relative_img_path}")
                            block_content = block_content.replace(img_path, local_url)
                        block["block_content"] = block_content
                
                # Save updated markdown with UTF-8 encoding to handle Unicode characters
                with open(md_filename, "w", encoding='utf-8') as md_file:
                    md_file.write(markdown_text)

            # Update file status
            file_info['status'] = 'extracted'
            file_info['extraction_result'] = result
            file_info['output_dir'] = output_dir
            session.modified = True

            return jsonify({
                'success': True,
                'result': result,
                'message': 'Extraction completed successfully'
            })
        else:
            # Try to parse body for helpful details
            err_body = None
            try:
                err_body = response.json()
                logger.error('PP-StructureV3 API returned error %s: %s', response.status_code, err_body)
            except Exception as je:
                err_body = resp_text
                logger.error('PP-StructureV3 API returned error %s. Could not parse JSON. Body: %s', response.status_code, resp_text[:500] if resp_text else 'No response body')

            return jsonify({
                'error': 'API error',
                'message': f'The API server returned an error status code: {response.status_code}',
                'status_code': response.status_code,
                'body': err_body,
                'hint': 'The API request failed. Check the API status and your request parameters.'
            }), 502
        
    except requests.exceptions.Timeout:
        logger.error('PP-StructureV3 API request timed out after retries')
        return jsonify({
            'error': 'Request timeout',
            'message': 'The API request took too long. Please try again or check your file size.',
            'details': 'Timeout after retries'
        }), 504
    except requests.exceptions.ConnectionError as ce:
        error_str = str(ce)
        # Connection errors should already be handled in retry logic, but catch any that escape
        logger.error(f'PP-StructureV3 API connection error: {ce}')
        file_size_mb = os.path.getsize(file_info['filepath']) / (1024 * 1024) if 'file_info' in locals() else 0
        return jsonify({
            'error': 'Connection error',
            'message': 'Unable to connect to the API server. Please check your internet connection.',
            'details': error_str,
            'api_url': API_URL,
            'file_size_mb': round(file_size_mb, 2) if file_size_mb > 0 else None
        }), 502
    except requests.exceptions.HTTPError as he:
        logger.error(f'PP-StructureV3 API HTTP error: {he}')
        return jsonify({
            'error': 'HTTP error',
            'message': 'The API server returned an error.',
            'details': str(he),
            'status_code': getattr(he.response, 'status_code', None) if hasattr(he, 'response') else None
        }), 502
    except requests.exceptions.RequestException as re:
        logger.exception('Request to PP-StructureV3 API failed')
        error_type = type(re).__name__
        return jsonify({
            'error': 'Request error',
            'message': f'An error occurred while contacting the API: {error_type}',
            'details': str(re),
            'api_url': API_URL,
            'hint': 'Please check your internet connection and API configuration.'
        }), 502
    except Exception as e:
        logger.exception(f'Unexpected error during extraction: {type(e).__name__}: {str(e)}')
        import traceback
        error_trace = traceback.format_exc()
        logger.error(f'Full traceback:\n{error_trace}')
        return jsonify({
            'error': 'Internal server error',
            'message': f'An unexpected error occurred: {str(e)}',
            'error_type': type(e).__name__,
            'hint': 'Please check the server logs for more details.'
        }), 500

@app.route('/stitch-tables/<file_id>', methods=['POST'])
def stitch_tables(file_id):
    """Stitch tables from multiple pages, keeping only one header and removing duplicates"""
    uploaded_files = session.get('uploaded_files', [])
    file_info = None
    
    # Debug logging
    logger.info(f"Stitch request for file_id: {file_id}")
    logger.info(f"Session ID: {session.get('session_id')}")
    logger.info(f"Total files in session: {len(uploaded_files)}")
    if uploaded_files:
        logger.info(f"Available file IDs: {[f.get('id') for f in uploaded_files]}")
    
    for f in uploaded_files:
        if f['id'] == file_id:
            file_info = f
            break
    
    if not file_info:
        logger.error(f"File not found: {file_id}. Available IDs: {[f.get('id') for f in uploaded_files]}")
        return jsonify({'error': 'File not found'}), 404
    
    if 'extraction_result' not in file_info:
        return jsonify({'error': 'Please extract the tables first'}), 400
    
    try:
        result = file_info['extraction_result']
        
        # Log the structure we received for debugging
        logger.info(f"Extraction result type: {type(result)}")
        if isinstance(result, dict):
            logger.info(f"Extraction result keys: {list(result.keys())}")
        elif isinstance(result, list):
            logger.info(f"Extraction result is a list with {len(result)} items")
        
        # The API might return result wrapped in 'result' key, or directly
        if isinstance(result, dict) and 'result' in result:
            result = result['result']
            logger.info("Unwrapped 'result' key from extraction_result")
            if isinstance(result, dict):
                logger.info(f"After unwrapping, keys: {list(result.keys())}")
        
        # Try to get layoutParsingResults
        layout_parsing_results = []
        if isinstance(result, dict):
            layout_parsing_results = result.get('layoutParsingResults', [])
            # Also try alternative key names
            if not layout_parsing_results:
                layout_parsing_results = result.get('layout_parsing_results', [])
            if not layout_parsing_results:
                layout_parsing_results = result.get('pages', [])
            if not layout_parsing_results:
                layout_parsing_results = result.get('results', [])
        elif isinstance(result, list):
            # Result might be directly a list of layout parsing results
            layout_parsing_results = result
            logger.info("Using result as direct list of layout parsing results")
        
        if not layout_parsing_results:
            logger.error(f"No layoutParsingResults found. Result type: {type(result)}")
            if isinstance(result, dict):
                logger.error(f"Available keys: {list(result.keys())}")
                # Log a sample of the structure for debugging
                for key, value in list(result.items())[:5]:
                    logger.error(f"  {key}: {type(value)} - {str(value)[:100] if not isinstance(value, (dict, list)) else '...'}")
            return jsonify({
                'error': 'No tables found to stitch',
                'details': f'No layoutParsingResults found in extraction result. Result type: {type(result).__name__}',
                'available_keys': list(result.keys()) if isinstance(result, dict) else None,
                'hint': 'The extraction may not have found any tables, or the result structure is different than expected. Please check the extraction was successful and try extracting again.'
            }), 400
        
        # Extract all tables from all pages
        all_tables = []
        main_header = None
        
        logger.info(f'Processing {len(layout_parsing_results)} pages for stitching')
        
        for page_idx, layout_result in enumerate(layout_parsing_results):
            if not isinstance(layout_result, dict):
                logger.warning(f'Page {page_idx + 1} layout_result is not a dict: {type(layout_result)}')
                continue
            
            # Also check markdown for tables (some APIs return tables in markdown format)
            markdown_data = layout_result.get('markdown', {})
            markdown_text = markdown_data.get('text', '') if isinstance(markdown_data, dict) else ''
            
            # Try to extract HTML tables from markdown first (PP-StructureV3 may embed HTML tables in markdown)
            if markdown_text:
                import re
                # Look for HTML tables in markdown
                html_table_pattern = r'<table[^>]*>(.*?)</table>'
                html_tables = re.findall(html_table_pattern, markdown_text, re.DOTALL | re.IGNORECASE)
                
                if html_tables:
                    logger.info(f'Page {page_idx + 1} has {len(html_tables)} HTML table(s) in markdown')
                    for table_idx, table_content in enumerate(html_tables):
                        # Extract rows from HTML table
                        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', table_content, re.DOTALL | re.IGNORECASE)
                        if rows:
                            logger.info(f'Found HTML table {table_idx + 1} with {len(rows)} rows in markdown')
                            # Check if first row is header
                            first_row = rows[0]
                            is_header = '<th' in first_row.lower() or is_header_row(first_row)
                            
                            if main_header is None:
                                if is_header:
                                    main_header = f'<tr>{first_row}</tr>'
                                    logger.info(f'Set main header from HTML table in markdown (page {page_idx + 1})')
                                    all_tables.extend([f'<tr>{row}</tr>' for row in rows[1:]])
                                else:
                                    all_tables.extend([f'<tr>{row}</tr>' for row in rows])
                                logger.info(f'Added {len(rows)} rows from HTML table in markdown')
                            else:
                                # Skip header if present
                                start_idx = 1 if is_header and len(rows) > 1 else 0
                                all_tables.extend([f'<tr>{row}</tr>' for row in rows[start_idx:]])
                                logger.info(f'Added {len(rows) - start_idx} data rows from HTML table in markdown')
                
                # Also try markdown table format (| separators)
                if '|' in markdown_text:
                    logger.info(f'Page {page_idx + 1} has markdown text with potential markdown tables')
                    # Extract markdown tables - improved logic
                    lines = markdown_text.split('\n')
                    markdown_table_lines = []
                    in_table = False
                    
                    for line in lines:
                        line = line.strip()
                        # Check if line looks like a markdown table row
                        if '|' in line and line.count('|') >= 2:
                            # Skip separator rows (like |---|---|)
                            if not re.match(r'^[\|\s\-:]+$', line):
                                markdown_table_lines.append(line)
                                in_table = True
                        elif in_table and markdown_table_lines:
                            # End of table - process it
                            if len(markdown_table_lines) > 1:
                                logger.info(f'Found markdown table on page {page_idx + 1} with {len(markdown_table_lines)} rows')
                                try:
                                    from utils.costing_engine import CostingEngine
                                    engine = CostingEngine()
                                    table_data = engine.markdown_table_to_dict('\n'.join(markdown_table_lines))
                                    if table_data and 'rows' in table_data:
                                        # Convert to HTML rows
                                        rows_html = []
                                        # Header row
                                        if 'headers' in table_data:
                                            header_cells = ''.join([f'<th>{h}</th>' for h in table_data['headers']])
                                            rows_html.append(f'<tr>{header_cells}</tr>')
                                        # Data rows
                                        for row in table_data.get('rows', []):
                                            if isinstance(row, dict):
                                                row_cells = ''.join([f'<td>{row.get(h, "")}</td>' for h in table_data.get('headers', [])])
                                            else:
                                                row_cells = ''.join([f'<td>{cell}</td>' for cell in row])
                                            rows_html.append(f'<tr>{row_cells}</tr>')
                                        
                                        if rows_html:
                                            if main_header is None and 'headers' in table_data:
                                                main_header = rows_html[0]
                                            all_tables.extend(rows_html[1:] if len(rows_html) > 1 else rows_html)
                                            logger.info(f'Added {len(rows_html)} rows from markdown table on page {page_idx + 1}')
                                except Exception as e:
                                    logger.warning(f'Error parsing markdown table: {e}')
                            markdown_table_lines = []  # Reset for next table
                            in_table = False
                    
                    # Process last table if file ends with table
                    if in_table and markdown_table_lines and len(markdown_table_lines) > 1:
                        try:
                            from utils.costing_engine import CostingEngine
                            engine = CostingEngine()
                            table_data = engine.markdown_table_to_dict('\n'.join(markdown_table_lines))
                            if table_data and 'rows' in table_data:
                                rows_html = []
                                if 'headers' in table_data:
                                    header_cells = ''.join([f'<th>{h}</th>' for h in table_data['headers']])
                                    rows_html.append(f'<tr>{header_cells}</tr>')
                                for row in table_data.get('rows', []):
                                    if isinstance(row, dict):
                                        row_cells = ''.join([f'<td>{row.get(h, "")}</td>' for h in table_data.get('headers', [])])
                                    else:
                                        row_cells = ''.join([f'<td>{cell}</td>' for cell in row])
                                    rows_html.append(f'<tr>{row_cells}</tr>')
                                
                                if rows_html:
                                    if main_header is None and 'headers' in table_data:
                                        main_header = rows_html[0]
                                    all_tables.extend(rows_html[1:] if len(rows_html) > 1 else rows_html)
                                    logger.info(f'Added {len(rows_html)} rows from final markdown table on page {page_idx + 1}')
                        except Exception as e:
                            logger.warning(f'Error parsing final markdown table: {e}')
                
            pruned_result = layout_result.get('prunedResult', {})
            if not pruned_result:
                logger.warning(f'Page {page_idx + 1} has no prunedResult. Keys: {list(layout_result.keys())}')
                # Continue to next page even if no prunedResult (we might have found tables in markdown)
                continue
                
            parsing_res_list = pruned_result.get('parsing_res_list', [])
            
            logger.info(f'Processing page {page_idx + 1} with {len(parsing_res_list)} blocks')
            
            if not parsing_res_list:
                logger.warning(f'Page {page_idx + 1} has no parsing_res_list')
                continue
            
            # Log all block labels found on this page for debugging
            block_labels_found = []
            block_types_found = []
            for block in parsing_res_list:
                if isinstance(block, dict):
                    block_labels_found.append(block.get('block_label', 'unknown'))
                    block_types_found.append(block.get('block_type', 'unknown'))
            logger.info(f'Page {page_idx + 1} block labels: {block_labels_found}')
            logger.info(f'Page {page_idx + 1} block types: {block_types_found}')
            
            # Also log a sample of markdown text to see what's there
            if markdown_text:
                markdown_preview = markdown_text[:500].replace('\n', '\\n')
                logger.info(f'Page {page_idx + 1} markdown preview (first 500 chars): {markdown_preview}')
            
            for block_idx, block in enumerate(parsing_res_list):
                if not isinstance(block, dict):
                    logger.warning(f'Page {page_idx + 1}, block {block_idx} is not a dict')
                    continue
                    
                block_label = block.get('block_label', '')
                block_content = block.get('block_content', '')
                
                logger.debug(f'Page {page_idx + 1}, block {block_idx}: label={block_label}, has_content={bool(block_content)}')
                
                # Try multiple ways to detect tables
                is_table = False
                block_type = block.get('block_type', '')
                
                # Check block_label variations
                if block_label.lower() in ['table', 'table_block', 'table_cell'] and block_content:
                    is_table = True
                    logger.info(f'Found table with label: {block_label}')
                # Check block_type field (some APIs use this instead of block_label)
                elif block_type.lower() in ['table', 'table_block'] and block_content:
                    is_table = True
                    logger.info(f'Found table with type: {block_type}')
                # Check for HTML table content
                elif block_content and ('<table' in block_content.lower() or '<tr>' in block_content.lower() or '<td>' in block_content.lower()):
                    is_table = True
                    logger.info(f'Found table-like HTML content with label: {block_label}, type: {block_type}')
                # Check for markdown table content
                elif block_content and '|' in block_content and block_content.count('|') > 2:
                    is_table = True
                    logger.info(f'Found markdown table content with label: {block_label}, type: {block_type}')
                # Check if block has table-related keys
                elif 'table' in str(block).lower() and block_content:
                    # Last resort: check if any key contains "table"
                    table_keys = [k for k in block.keys() if 'table' in k.lower()]
                    if table_keys:
                        is_table = True
                        logger.info(f'Found table-related keys {table_keys} with label: {block_label}')
                
                if is_table:
                    table_html = block_content
                    
                    # Parse the table to extract header and rows
                    import re
                    from html.parser import HTMLParser
                    
                    # Extract table rows - try multiple patterns
                    rows = []
                    
                    # Try tbody pattern first
                    tbody_match = re.search(r'<tbody>(.*?)</tbody>', table_html, re.DOTALL)
                    if tbody_match:
                        tbody_content = tbody_match.group(1)
                        rows = re.findall(r'<tr>(.*?)</tr>', tbody_content, re.DOTALL)
                    else:
                        # Try direct tr pattern (no tbody)
                        rows = re.findall(r'<tr>(.*?)</tr>', table_html, re.DOTALL)
                        
                        logger.info(f'Found {len(rows)} rows in table on page {page_idx + 1}')
                        
                        if rows:
                            # First row is typically the header
                            first_row = rows[0]
                            
                            # Check if this is a header row (contains <th> or looks like a header)
                            is_header = '<th>' in first_row or is_header_row(first_row)
                            
                            if main_header is None:
                                # First table - keep header and all data rows
                                if is_header:
                                    main_header = first_row
                                    logger.info(f'Set main header from page {page_idx + 1}')
                                # Add all rows from first table
                                all_tables.extend(rows)
                                logger.info(f'Added {len(rows)} rows from first table (page {page_idx + 1})')
                            else:
                                # Subsequent tables - skip header, add only data rows
                                start_idx = 0
                                if is_header and len(rows) > 1:
                                    # Skip the header row
                                    start_idx = 1
                                    logger.info(f'Skipping header row on page {page_idx + 1}')
                                
                                # Add data rows
                                data_rows = rows[start_idx:]
                                all_tables.extend(data_rows)
                                logger.info(f'Added {len(data_rows)} data rows from page {page_idx + 1}')
        
        if not all_tables:
            logger.error(f'No tables found after processing {len(layout_parsing_results)} pages')
            return jsonify({
                'error': 'No tables found to stitch',
                'details': f'Processed {len(layout_parsing_results)} pages but found no table blocks with content',
                'hint': 'The document may not contain tables, or tables were not detected by the extraction API. Try checking the extraction settings or the document format.'
            }), 400
        
        logger.info(f'Total rows before filtering: {len(all_tables)}')
        
        # Filter out empty rows (rows with only whitespace or empty cells)
        import re
        filtered_tables = []
        for row in all_tables:
            # Remove HTML tags and check if there's actual content
            row_text = re.sub(r'<[^>]+>', '', row).strip()
            # Remove common whitespace characters and check if anything remains
            row_text_clean = re.sub(r'[\s\xa0\u00a0\u200b\u200c\u200d\ufeff]+', '', row_text)
            
            # Only include rows that have actual content
            if row_text_clean:
                filtered_tables.append(row)
            else:
                logger.debug(f'Filtered out empty row: {row[:100]}...')
        
        logger.info(f'Total rows after filtering empty rows: {len(filtered_tables)} (removed {len(all_tables) - len(filtered_tables)} empty rows)')
        
        # Build the stitched table HTML
        stitched_html = '''
<div style="text-align: center;">
    <html>
        <body>
            <table border="1">
'''
        
        # Add header row if available
        if main_header:
            stitched_html += '                <thead>\n'
            stitched_html += f'                    {main_header}\n'
            stitched_html += '                </thead>\n'
            logger.info('Added header row to stitched table')
        
        stitched_html += '                <tbody>\n'
        
        for row in filtered_tables:
            stitched_html += f'                    <tr>{row}</tr>\n'
        
        stitched_html += '''                </tbody>
            </table>
        </body>
    </html>
</div>
'''
        
        # Save stitched table
        session_id = session['session_id']
        output_dir = file_info.get('output_dir', os.path.join(app.config['OUTPUT_FOLDER'], session_id, file_id))
        os.makedirs(output_dir, exist_ok=True)
        
        stitched_filename = os.path.join(output_dir, 'stitched_table.html')
        with open(stitched_filename, 'w', encoding='utf-8') as f:
            f.write(stitched_html)
        
        # Update file info
        file_info['stitched_table'] = {
            'html': stitched_html,
            'filepath': stitched_filename,
            'row_count': len(filtered_tables)
        }
        
        # Also store in global shared session for cross-app access
        if 'shared_tables' not in session:
            session['shared_tables'] = {}
        session['shared_tables']['stitched_table'] = {
            'file_id': file_id,
            'html': stitched_html,
            'filepath': stitched_filename,
            'row_count': len(filtered_tables),
            'timestamp': datetime.now().isoformat()
        }
        
        session.modified = True
        
        logger.info(f'Stitched {len(filtered_tables)} rows from {len(layout_parsing_results)} pages')
        
        return jsonify({
            'success': True,
            'stitched_html': stitched_html,
            'row_count': len(filtered_tables),
            'page_count': len(layout_parsing_results),
            'message': f'Successfully stitched {len(filtered_tables)} rows from {len(layout_parsing_results)} pages'
        })
        
    except Exception as e:
        logger.exception('Error stitching tables')
        return jsonify({'error': str(e)}), 500

def is_header_row(row_html):
    """Check if a row is likely a header row"""
    row_text = re.sub(r'<[^>]+>', '', row_html).strip().lower()
    header_keywords = ['si.no', 'item', 'description', 'qty', 'unit', 'rate', 'amount', 'price', 'total', 'image', 'ref']
    return any(keyword in row_text for keyword in header_keywords)

@app.route('/costing', methods=['GET', 'POST'])
def costing():
    """Costing card functionality"""
    if request.method == 'GET':
        return render_template('costing.html')
    
    # Apply costing factors
    data = request.json
    file_id = data.get('file_id')
    factors = data.get('factors', {})
    table_data = data.get('table_data')  # Get table data from DOM
    
    try:
        from utils.costing_engine import CostingEngine
        engine = CostingEngine()
        result = engine.apply_factors(file_id, factors, session, table_data)
        
        return jsonify({
            'success': True,
            'result': result,
            'message': 'Costing applied successfully'
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/generate-offer/<file_id>', methods=['POST'])
def generate_offer(file_id):
    """Generate offer with costing factors"""
    try:
        from utils.offer_generator import OfferGenerator
        generator = OfferGenerator()
        result = generator.generate(file_id, session)
        
        return jsonify({
            'success': True,
            'file_path': result,
            'message': 'Offer generated successfully'
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/multibudget/store-table', methods=['POST'])
def store_multibudget_table():
    """Store multi-budget table data for export (excludes Product Selection and Actions columns)"""
    try:
        data = request.get_json()
        tier = data.get('tier', 'budgetary')
        table_html = data.get('table_html', '')
        product_selections_data = data.get('product_selections', [])  # Optional: product selections from frontend
        
        if not table_html:
            return jsonify({'error': 'Table HTML is required'}), 400
        
        # Parse HTML and extract product selections before removing columns
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(table_html, 'html.parser')
        table = soup.find('table')
        
        if not table:
            return jsonify({'error': 'Invalid table HTML'}), 400
        
        # Extract product selections and image URLs before removing Product Selection column
        product_selections = []
        
        # Use provided product selections if available (from frontend)
        if product_selections_data:
            for idx, selection in enumerate(product_selections_data):
                if selection.get('brand') and selection.get('category') and selection.get('model'):
                    product_info = {
                        'row_index': idx,
                        'brand': selection.get('brand'),
                        'category': selection.get('category'),
                        'subcategory': selection.get('subcategory', 'general'),
                        'model': selection.get('model')
                    }
                    # Get image URL from brand data
                    from utils.image_helper import get_product_image_url
                    image_url = get_product_image_url(
                        product_info['brand'], 
                        product_info['category'], 
                        product_info['subcategory'], 
                        product_info['model'], 
                        tier
                    )
                    if image_url:
                        product_info['image_url'] = image_url
                    product_selections.append(product_info)
        else:
            # Fallback: try to extract from HTML
            rows = table.find_all('tr')[1:]  # Skip header row
            for row_idx, row in enumerate(rows):
                cells = row.find_all('td')
                product_info = {}
                
                # Find Product Selection cell
                for cell in cells:
                    product_dropdowns = cell.find(class_='product-selection-dropdowns')
                    if product_dropdowns:
                        # Extract selected values from dropdowns
                        brand_select = product_dropdowns.find('select', class_='brand-dropdown')
                        category_select = product_dropdowns.find('select', class_='category-dropdown')
                        subcategory_select = product_dropdowns.find('select', class_='subcategory-dropdown')
                        model_select = product_dropdowns.find('select', class_='model-dropdown')
                        
                        if brand_select and category_select and model_select:
                            # Get selected values - check value attribute first, then selected option
                            brand = brand_select.get('value', '').strip()
                            if not brand:
                                selected_option = brand_select.find('option', selected=True) or \
                                                brand_select.find('option', {'selected': 'selected'}) or \
                                                brand_select.find('option', value=lambda v: v and v.strip())
                                if selected_option:
                                    brand = selected_option.get('value', '').strip() or selected_option.get_text(strip=True)
                            
                            category = None
                            if category_select:
                                category = category_select.get('value', '').strip()
                                if not category:
                                    selected_option = category_select.find('option', selected=True) or \
                                                    category_select.find('option', {'selected': 'selected'}) or \
                                                    category_select.find('option', value=lambda v: v and v.strip())
                                    if selected_option:
                                        category = selected_option.get('value', '').strip() or selected_option.get_text(strip=True)
                            
                            subcategory = 'general'
                            if subcategory_select:
                                subcategory = subcategory_select.get('value', '').strip() or 'general'
                                if not subcategory or subcategory == 'Select Sub-Category':
                                    selected_option = subcategory_select.find('option', selected=True) or \
                                                    subcategory_select.find('option', {'selected': 'selected'}) or \
                                                    subcategory_select.find('option', value=lambda v: v and v.strip())
                                    if selected_option:
                                        subcategory = selected_option.get('value', '').strip() or selected_option.get_text(strip=True) or 'general'
                            
                            model = None
                            if model_select:
                                model = model_select.get('value', '').strip()
                                if not model or model == 'Select Model':
                                    selected_option = model_select.find('option', selected=True) or \
                                                    model_select.find('option', {'selected': 'selected'}) or \
                                                    model_select.find('option', value=lambda v: v and v.strip())
                                    if selected_option:
                                        model_text = selected_option.get('value', '').strip() or selected_option.get_text(strip=True)
                                        # Extract model name (remove price info like "(Contact for price)")
                                        model = re.sub(r'\s*\([^)]+\)\s*$', '', model_text).strip()
                            
                            if brand and category and model:
                                product_info = {
                                    'row_index': row_idx,
                                    'brand': brand,
                                    'category': category,
                                    'subcategory': subcategory,
                                    'model': model
                                }
                                
                                # Get image URL from brand data
                                from utils.image_helper import get_product_image_url
                                image_url = get_product_image_url(brand, category, subcategory, model, tier)
                                if image_url:
                                    product_info['image_url'] = image_url
                                
                                product_selections.append(product_info)
                        break
        
        # Remove Product Selection and Actions columns
        for row in table.find_all('tr'):
            cells = row.find_all(['th', 'td'])
            cells_to_remove = []
            
            for cell in cells:
                text = cell.get_text(strip=True).lower()
                # Check if cell contains Product Selection or Actions
                if 'product selection' in text or 'actions' in text:
                    cells_to_remove.append(cell)
                # Check if cell has dropdowns or buttons
                elif cell.find(class_='product-selection-dropdowns') or cell.find('button'):
                    cells_to_remove.append(cell)
            
            for cell in cells_to_remove:
                cell.decompose()
        
        # Add image column if we have product selections with images
        if product_selections:
            # Find Image column or add it
            header_row = table.find('tr')
            if header_row:
                headers = [th.get_text(strip=True).lower() for th in header_row.find_all(['th', 'td'])]
                has_image_col = any('image' in h for h in headers)
                
                if not has_image_col:
                    # Add Image header
                    image_header = soup.new_tag('th')
                    image_header.string = 'Image'
                    header_row.insert(1, image_header)  # Insert after Sl.No
                
                # Add image cells to data rows
                rows = table.find_all('tr')[1:]
                for row_idx, row in enumerate(rows):
                    product_info = next((p for p in product_selections if p['row_index'] == row_idx), None)
                    if product_info and product_info.get('image_url'):
                        # Check if image cell already exists
                        cells = row.find_all('td')
                        has_image_cell = False
                        for cell in cells:
                            if cell.find('img'):
                                has_image_cell = True
                                break
                        
                        if not has_image_cell:
                            # Create image cell
                            image_cell = soup.new_tag('td')
                            img_tag = soup.new_tag('img', src=product_info['image_url'], style='width: 80px; height: 80px; object-fit: cover;')
                            image_cell.append(img_tag)
                            row.insert(1, image_cell)  # Insert after Sl.No
        
        # Store filtered table in session
        if 'multibudget_tables' not in session:
            session['multibudget_tables'] = {}
        
        session['multibudget_tables'][tier] = {
            'html': str(table),
            'timestamp': datetime.now().isoformat(),
            'product_selections': product_selections  # Store for export generators
        }
        session.modified = True
        
        # Create a temporary file_id for this table
        import uuid
        file_id = str(uuid.uuid4())
        
        # Store in uploaded_files for compatibility with existing export functions
        if 'uploaded_files' not in session:
            session['uploaded_files'] = []
        
        session['uploaded_files'].append({
            'id': file_id,
            'original_name': f'multibudget_{tier}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.html',
            'stitched_table': {
                'html': str(table)
            },
            'multibudget': True,
            'tier': tier
        })
        session.modified = True
        
        return jsonify({
            'success': True,
            'file_id': file_id,
            'message': 'Table stored successfully for export'
        })
    except Exception as e:
        logger.exception('Error storing multibudget table')
        return jsonify({'error': str(e)}), 500

@app.route('/api/multibudget/export/<tier>', methods=['POST'])
def export_multibudget(tier):
    """Export multi-budget table (excludes Product Selection and Actions columns)"""
    try:
        data = request.get_json()
        export_type = data.get('type', 'offer')  # offer, presentation, mas
        format_type = data.get('format', 'pdf')  # pdf, excel, pptx
        
        # Get stored table from session
        if 'multibudget_tables' not in session or tier not in session['multibudget_tables']:
            return jsonify({'error': 'No table data found for this tier. Please generate a table first.'}), 404
        
        table_html = session['multibudget_tables'][tier]['html']
        
        # Create temporary file entry for export
        import uuid
        file_id = str(uuid.uuid4())
        
        # Store in session for export functions
        if 'uploaded_files' not in session:
            session['uploaded_files'] = []
        
        session['uploaded_files'].append({
            'id': file_id,
            'original_name': f'multibudget_{tier}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.html',
            'stitched_table': {
                'html': table_html
            },
            'multibudget': True,
            'tier': tier
        })
        session.modified = True
        
        # Route to appropriate export function
        if export_type == 'offer':
            if format_type == 'pdf':
                from utils.offer_generator import OfferGenerator
                generator = OfferGenerator()
                result = generator.generate(file_id, session)
                return send_file(result, as_attachment=True, download_name=f'offer_{tier}.pdf')
            elif format_type in ['excel', 'xlsx']:
                from utils.download_manager import DownloadManager
                manager = DownloadManager()
                file_path = manager.prepare_download(file_id, 'offer', format_type, session)
                return send_file(file_path, as_attachment=True, download_name=f'offer_{tier}.xlsx')
        elif export_type == 'presentation':
            if format_type == 'pdf':
                from utils.presentation_generator import PresentationGenerator
                generator = PresentationGenerator()
                result = generator.generate(file_id, session, 'pdf')
                return send_file(result, as_attachment=True, download_name=f'presentation_{tier}.pdf')
            elif format_type == 'pptx':
                from utils.presentation_generator import PresentationGenerator
                generator = PresentationGenerator()
                result = generator.generate(file_id, session, 'pptx')
                return send_file(result, as_attachment=True, download_name=f'presentation_{tier}.pptx')
        elif export_type == 'mas':
            from utils.mas_generator import MASGenerator
            generator = MASGenerator()
            result = generator.generate(file_id, session)
            return send_file(result, as_attachment=True, download_name=f'mas_{tier}.pdf')
        
        return jsonify({'error': 'Invalid export type or format'}), 400
        
    except Exception as e:
        logger.exception('Error exporting multibudget table')
        return jsonify({'error': str(e)}), 500

@app.route('/generate-presentation/<file_id>', methods=['POST'])
def generate_presentation(file_id):
    """Generate technical presentation"""
    try:
        data = request.json or {}
        format_type = data.get('format', 'pdf')
        
        from utils.presentation_generator import PresentationGenerator
        generator = PresentationGenerator()
        result = generator.generate(file_id, session, format_type)
        
        return jsonify({
            'success': True,
            'file_path': result,
            'message': f'Presentation generated successfully as {format_type.upper()}'
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/generate-mas/<file_id>', methods=['POST'])
def generate_mas(file_id):
    """Generate Material Approval Sheets"""
    try:
        from utils.mas_generator import MASGenerator
        generator = MASGenerator()
        result = generator.generate(file_id, session)
        
        return jsonify({
            'success': True,
            'file_path': result,
            'message': 'MAS generated successfully'
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/value-engineering/<file_id>', methods=['POST'])
def value_engineering(file_id):
    """Generate value-engineered alternatives"""
    data = request.json
    budget_option = data.get('budget_option', 'mid_range')
    
    try:
        from utils.value_engineering import ValueEngineer
        engineer = ValueEngineer()
        result = engineer.generate_alternatives(file_id, budget_option, session)
        
        return jsonify({
            'success': True,
            'alternatives': result,
            'message': 'Alternatives generated successfully'
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/brands/tiers', methods=['GET'])
def get_tiers():
    """Get available budget tiers"""
    try:
        from utils.value_engineering import ValueEngineer
        engineer = ValueEngineer()
        tiers = engineer.get_tiers()
        
        return jsonify({
            'success': True,
            'tiers': tiers
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/brands/list', methods=['GET'])
def get_brands_list():
    """Get brands for a specific tier (loads from brands_data folder)"""
    tier = request.args.get('tier', 'mid_range')
    category = request.args.get('category', None)  # Optional category filter
    
    try:
        # Normalize tier name
        tier_map = {
            'budgetary': 'budgetary',
            'mid-range': 'mid_range',
            'mid_range': 'mid_range',
            'high-end': 'high_end',
            'high_end': 'high_end'
        }
        tier = tier_map.get(tier.lower(), tier.lower())
        
        brands = []
        brands_data_dir = 'brands_data'
        
        if not os.path.exists(brands_data_dir):
            return jsonify({
                'success': True,
                'brands': [],
                'tier': tier,
                'category': category
            })
        
        
        # Load all brand files for this tier
        import json
        import re
        pattern = re.compile(rf'^.+_{re.escape(tier)}\.json$', re.I)
        
        for filename in os.listdir(brands_data_dir):
            if pattern.match(filename):
                try:
                    filepath = os.path.join(brands_data_dir, filename)
                    with open(filepath, 'r', encoding='utf-8') as f:
                        brand_data = json.load(f)
                    
                    brand_name = brand_data.get('brand', 'Unknown')
                    brand_categories = brand_data.get('categories', {})
                    
                    # Filter by category if specified
                    if category:
                        category_lower = category.lower()
                        has_category = any(
                            cat.lower() == category_lower or 
                            any(subcat.lower() == category_lower for subcat in cats.keys())
                            for cat, cats in brand_categories.items()
                        )
                        if not has_category:
                            continue
                    
                    brands.append({
                        'name': brand_name,
                        'website': brand_data.get('website', ''),
                        'country': brand_data.get('country', 'Unknown'),
                        'tier': tier,
                        'categories': list(brand_categories.keys())
                    })
                except Exception as e:
                    logger.warning(f"Error loading brand file {filename}: {e}")
                    continue
        
        return jsonify({
            'success': True,
            'brands': brands,
            'tier': tier,
            'category': category
        })
    except Exception as e:
        logger.exception('Error getting brands list')
        return jsonify({'error': str(e)}), 500

@app.route('/api/brands/models', methods=['GET'])
def get_brand_models_api():
    """Get models for a specific brand (loads from brands_data folder)"""
    tier = request.args.get('tier', 'mid_range')
    category = request.args.get('category', None)
    brand = request.args.get('brand', '')
    subcategory = request.args.get('subcategory', None)
    
    if not brand:
        return jsonify({
            'success': True,
            'models': []
        })
    
    try:
        # Normalize tier name
        tier_map = {
            'budgetary': 'budgetary',
            'mid-range': 'mid_range',
            'mid_range': 'mid_range',
            'high-end': 'high_end',
            'high_end': 'high_end'
        }
        tier = tier_map.get(tier.lower(), tier.lower())
        
        # Load brand data from brands_data folder
        import json
        import re
        safe_brand_name = re.sub(r'[^\w\-_]', '', brand.replace(' ', '_'))
        filename = f"{safe_brand_name}_{tier}.json"
        filepath = os.path.join('brands_data', filename)
        
        if not os.path.exists(filepath):
            # Try case-insensitive search
            brands_data_dir = 'brands_data'
            if os.path.exists(brands_data_dir):
                for f in os.listdir(brands_data_dir):
                    if f.lower() == filename.lower():
                        filepath = os.path.join(brands_data_dir, f)
                        break
        
        if not os.path.exists(filepath):
            return jsonify({
                'success': True,
                'models': []
            })
        
        with open(filepath, 'r', encoding='utf-8') as f:
            brand_data = json.load(f)
        
        models = []
        categories_data = brand_data.get('categories', {})
        
        # Try category_tree format (from old scrapers)
        if not categories_data and 'category_tree' in brand_data:
            categories_data = brand_data.get('category_tree', {})
        
        # Handle collections format - convert to categories format (from Firecrawl)
        if not categories_data and 'collections' in brand_data:
            categories_data = {}
            for collection_name, collection_data in brand_data.get('collections', {}).items():
                # Handle "Category > Subcategory" format
                parts = collection_name.split('>')
                if len(parts) == 2:
                    cat = parts[0].strip()
                    subcat = parts[1].strip()
                    if cat not in categories_data:
                        categories_data[cat] = {'subcategories': {}}
                    if 'subcategories' not in categories_data[cat]:
                        categories_data[cat]['subcategories'] = {}
                    
                    # Convert products to list
                    products_list = []
                    for product in collection_data.get('products', []):
                        model_entry = {
                            'model': product.get('name', 'Unknown'),
                            'image_url': product.get('image_url', ''),
                            'source_url': product.get('source_url', ''),
                            'description': product.get('description', ''),
                            'price': product.get('price'),
                            'price_range': product.get('price_range', 'Contact for price'),
                            'features': product.get('features', [])
                        }
                        products_list.append(model_entry)
                    
                    categories_data[cat]['subcategories'][subcat] = {'products': products_list}
                else:
                    # Single-level category - use 'general' subcategory
                    clean_name = collection_name.split('\n')[0].strip()
                    if clean_name not in categories_data:
                        categories_data[clean_name] = {'subcategories': {}}
                    
                    # Add 'general' subcategory with products
                    products_list = []
                    for product in collection_data.get('products', []):
                        model_entry = {
                            'model': product.get('name', 'Unknown'),
                            'image_url': product.get('image_url', ''),
                            'source_url': product.get('source_url', ''),
                            'description': product.get('description', ''),
                            'price': product.get('price'),
                            'price_range': product.get('price_range', 'Contact for price'),
                            'features': product.get('features', [])
                        }
                        products_list.append(model_entry)
                    
                    categories_data[clean_name]['subcategories']['general'] = {'products': products_list}
        
        # Filter by category if specified
        if category:
            category_lower = category.lower()
            matching_categories = {
                cat: cats for cat, cats in categories_data.items()
                if cat.lower() == category_lower or category_lower in cat.lower()
            }
        else:
            matching_categories = categories_data
        
        # Get models from matching categories and subcategories
        for cat_name, cat_data in matching_categories.items():
            # Extract subcategories - handle both old and new formats
            if isinstance(cat_data, dict) and 'subcategories' in cat_data:
                subcategories = cat_data['subcategories']
            else:
                subcategories = cat_data if isinstance(cat_data, dict) else {}
            
            if subcategory:
                subcategory_lower = subcategory.lower()
                matching_subcats = {
                    subcat: subcat_data 
                    for subcat, subcat_data in subcategories.items()
                    if subcat.lower() == subcategory_lower or subcategory_lower in subcat.lower()
                }
            else:
                matching_subcats = subcategories
            
            for subcat_name, subcat_data in matching_subcats.items():
                # Extract products list - handle nested structure
                if isinstance(subcat_data, dict) and 'products' in subcat_data:
                    models_list = subcat_data['products']
                elif isinstance(subcat_data, list):
                    models_list = subcat_data
                else:
                    continue
                
                for model in models_list:
                    # Handle both model dict format and product format
                    model_name = model.get('model') or model.get('name', 'Unknown')
                    models.append({
                        'model': model_name,
                        'price': model.get('price'),
                        'price_range': model.get('price_range', 'Contact for price'),
                        'description': model.get('description') or model.get('name', ''),
                        'image_url': model.get('image_url', ''),
                        'features': model.get('features', []),
                        'source_url': model.get('source_url') or model.get('url', ''),
                        'category': cat_name,
                        'subcategory': subcat_name
                    })
        
        # Optional: Enrich products with missing images/descriptions
        enrich = request.args.get('enrich', 'false').lower() == 'true'
        if enrich and models:
            try:
                from utils.product_enricher import ProductEnricher
                enricher = ProductEnricher()
                logger.info(f"Enriching {len(models)} products for {brand}...")
                models = enricher.enrich_product_selection_data(models, use_selenium=False)
                logger.info(f"Enrichment complete for {brand}")
            except Exception as e:
                logger.error(f"Error enriching products: {e}")
                # Continue without enrichment
        
        return jsonify({
            'success': True,
            'models': models,
            'brand': brand,
            'tier': tier,
            'category': category,
            'subcategory': subcategory,
            'enriched': enrich
        })
    except Exception as e:
        logger.exception('Error getting models')
        return jsonify({'error': str(e)}), 500

@app.route('/api/products/enrich', methods=['POST'])
def enrich_products():
    """Enrich product data with images and descriptions"""
    try:
        data = request.get_json()
        products = data.get('products', [])
        use_selenium = data.get('use_selenium', False)
        
        if not products:
            return jsonify({'error': 'No products provided'}), 400
        
        from utils.product_enricher import ProductEnricher
        enricher = ProductEnricher()
        
        logger.info(f"Enriching {len(products)} products...")
        enriched_products = enricher.enrich_product_selection_data(products, use_selenium)
        logger.info(f"Enrichment complete")
        
        return jsonify({
            'success': True,
            'products': enriched_products,
            'count': len(enriched_products)
        })
    except Exception as e:
        logger.exception('Error enriching products')
        return jsonify({'error': str(e)}), 500


@app.route('/api/brands/categories', methods=['GET'])
def get_brands_categories():
    """Get categories for a specific brand and tier (loads from brands_data folder)"""
    tier = request.args.get('tier', 'mid_range')
    brand = request.args.get('brand', None)
    
    if not brand:
        # Return all categories across all brands in this tier
        brands_data_dir = 'brands_data'
        all_categories = set()
        
        if os.path.exists(brands_data_dir):
            import json
            import re
            tier_map = {
                'budgetary': 'budgetary',
                'mid-range': 'mid_range',
                'mid_range': 'mid_range',
                'high-end': 'high_end',
                'high_end': 'high_end'
            }
            tier = tier_map.get(tier.lower(), tier.lower())
            pattern = re.compile(rf'^.+_{re.escape(tier)}\.json$', re.I)
            
            for filename in os.listdir(brands_data_dir):
                if pattern.match(filename):
                    try:
                        filepath = os.path.join(brands_data_dir, filename)
                        with open(filepath, 'r', encoding='utf-8') as f:
                            brand_data = json.load(f)
                        # Handle both collections and categories formats
                        cats = brand_data.get('categories', {})
                        if not cats and 'collections' in brand_data:
                            # Extract collection names as categories
                            for collection_name in brand_data.get('collections', {}).keys():
                                clean_name = collection_name.split('\n')[0].strip()
                                all_categories.add(clean_name)
                        else:
                            all_categories.update(cats.keys())
                    except:
                        continue
        
        return jsonify({
            'success': True,
            'categories': sorted(list(all_categories))
        })
    
    # Load specific brand's categories
    try:
        import json
        import re
        tier_map = {
            'budgetary': 'budgetary',
            'mid-range': 'mid_range',
            'mid_range': 'mid_range',
            'high-end': 'high_end',
            'high_end': 'high_end'
        }
        tier = tier_map.get(tier.lower(), tier.lower())
        
        safe_brand_name = re.sub(r'[^\w\-_]', '', brand.replace(' ', '_'))
        filename = f"{safe_brand_name}_{tier}.json"
        filepath = os.path.join('brands_data', filename)
        
        if not os.path.exists(filepath):
            # Try case-insensitive
            brands_data_dir = 'brands_data'
            if os.path.exists(brands_data_dir):
                for f in os.listdir(brands_data_dir):
                    if f.lower() == filename.lower():
                        filepath = os.path.join(brands_data_dir, f)
                        break
        
        if not os.path.exists(filepath):
            return jsonify({
                'success': True,
                'categories': []
            })
        
        with open(filepath, 'r', encoding='utf-8') as f:
            brand_data = json.load(f)
        
        # Handle multiple data formats: categories, collections, category_tree
        categories_data = brand_data.get('categories', {})
        
        # Try category_tree format (from old scrapers)
        if not categories_data and 'category_tree' in brand_data:
            categories_data = brand_data.get('category_tree', {})
        
        # Try collections format (from Firecrawl)
        if not categories_data and 'collections' in brand_data:
            # Convert collections to categories format
            categories_data = {}
            for collection_name, collection_data in brand_data.get('collections', {}).items():
                # Handle "Category > Subcategory" format
                parts = collection_name.split('>')
                if len(parts) == 2:
                    cat = parts[0].strip()
                    subcat = parts[1].strip()
                    if cat not in categories_data:
                        categories_data[cat] = {}
                    if 'subcategories' not in categories_data[cat]:
                        categories_data[cat]['subcategories'] = {}
                    categories_data[cat]['subcategories'][subcat] = collection_data
                else:
                    # Single-level category
                    clean_name = collection_name.split('\n')[0].strip()
                    if clean_name not in categories_data:
                        categories_data[clean_name] = {}
        
        categories = list(categories_data.keys()) if categories_data else []
        
        return jsonify({
            'success': True,
            'categories': categories
        })
    except Exception as e:
        logger.exception('Error getting categories')
        return jsonify({'error': str(e)}), 500

@app.route('/api/brands/subcategories', methods=['GET'])
def get_subcategories_api():
    """Get subcategories for a specific brand and category (loads from brands_data folder)"""
    tier = request.args.get('tier', 'mid_range')
    brand = request.args.get('brand', None)
    category = request.args.get('category', None)
    
    if not brand or not category:
        return jsonify({
            'success': True,
            'subcategories': []
        })
    
    try:
        # Load brand data
        import json
        import re
        tier_map = {
            'budgetary': 'budgetary',
            'mid-range': 'mid_range',
            'mid_range': 'mid_range',
            'high-end': 'high_end',
            'high_end': 'high_end'
        }
        tier = tier_map.get(tier.lower(), tier.lower())
        
        safe_brand_name = re.sub(r'[^\w\-_]', '', brand.replace(' ', '_'))
        filename = f"{safe_brand_name}_{tier}.json"
        filepath = os.path.join('brands_data', filename)
        
        if not os.path.exists(filepath):
            # Try case-insensitive
            brands_data_dir = 'brands_data'
            if os.path.exists(brands_data_dir):
                for f in os.listdir(brands_data_dir):
                    if f.lower() == filename.lower():
                        filepath = os.path.join(brands_data_dir, f)
                        break
        
        if not os.path.exists(filepath):
            return jsonify({
                'success': True,
                'subcategories': []
            })
        
        with open(filepath, 'r', encoding='utf-8') as f:
            brand_data = json.load(f)
        
        # Handle multiple data formats: categories, collections, category_tree
        categories_data = brand_data.get('categories', {})
        
        # Try category_tree format (from old scrapers)
        if not categories_data and 'category_tree' in brand_data:
            categories_data = brand_data.get('category_tree', {})
        
        # Try collections format (from Firecrawl)
        if not categories_data and 'collections' in brand_data:
            # Convert collections to categories format
            categories_data = {}
            for collection_name, collection_data in brand_data.get('collections', {}).items():
                # Handle "Category > Subcategory" format
                parts = collection_name.split('>')
                if len(parts) == 2:
                    cat = parts[0].strip()
                    subcat = parts[1].strip()
                    if cat not in categories_data:
                        categories_data[cat] = {'subcategories': {}}
                    if 'subcategories' not in categories_data[cat]:
                        categories_data[cat]['subcategories'] = {}
                    categories_data[cat]['subcategories'][subcat] = collection_data
                else:
                    # Single-level category
                    clean_name = collection_name.split('\n')[0].strip()
                    if clean_name not in categories_data:
                        categories_data[clean_name] = {}
        
        # Find matching category (case-insensitive)
        matching_category = None
        category_lower = category.lower()
        for cat_name, cat_data in categories_data.items():
            if cat_name.lower() == category_lower or category_lower in cat_name.lower():
                # Check if it has subcategories key (category_tree format)
                if isinstance(cat_data, dict) and 'subcategories' in cat_data:
                    matching_category = cat_data['subcategories']
                else:
                    matching_category = cat_data
                break
        
        if not matching_category:
            return jsonify({
                'success': True,
                'subcategories': []
            })
        
        subcategories = list(matching_category.keys()) if matching_category else []
        
        return jsonify({
            'success': True,
            'subcategories': subcategories,
            'category': category
        })
    except Exception as e:
        logger.exception('Error getting subcategories')
        return jsonify({'error': str(e)}), 500

@app.route('/api/brands/scrape', methods=['POST'])
def scrape_brand():
    """Scrape a brand's website to discover products"""
    try:
        data = request.get_json()
        brand_name = data.get('brand_name')
        website = data.get('website')
        
        if not brand_name or not website:
            return jsonify({'error': 'Brand name and website are required'}), 400
        
        from utils.universal_brand_scraper import UniversalBrandScraper
        scraper = UniversalBrandScraper()
        
        logger.info(f"Starting scrape for {brand_name} ({website}) using UniversalBrandScraper")
        scraped_data = scraper.scrape_brand_website(website, brand_name)
        
        if 'error' in scraped_data:
            return jsonify({'error': scraped_data['error']}), 400
        
        return jsonify({
            'success': True,
            'data': scraped_data,
            'message': f'Successfully scraped {len(scraped_data.get("products", []))} products'
        })
    except Exception as e:
        logger.exception('Error in brand scraping')
        return jsonify({'error': str(e)}), 500

@app.route('/api/brands/add', methods=['POST'])
def add_brand():
    """Add a new brand to the database"""
    try:
        data = request.get_json()
        brand_name = data.get('brand_name')
        website = data.get('website')
        country = data.get('country', 'Unknown')
        tier = data.get('tier', 'mid_range')
        categories = data.get('categories', {})
        
        if not brand_name or not website:
            return jsonify({'error': 'Brand name and website are required'}), 400
        
        # Load existing brands
        from utils.brand_database import BRAND_DATABASE
        import json
        
        # Check if brand already exists
        for t in BRAND_DATABASE:
            for cat in BRAND_DATABASE[t]:
                for brand in BRAND_DATABASE[t][cat]:
                    if brand['name'].lower() == brand_name.lower():
                        return jsonify({'error': 'Brand already exists'}), 400
        
        # Create new brand entry
        new_brand = {
            'name': brand_name,
            'website': website,
            'country': country,
            'models': categories
        }
        
        # Add to database (in memory for now)
        if tier not in BRAND_DATABASE:
            return jsonify({'error': f'Invalid tier: {tier}'}), 400
        
        # Add to first available category or create new one
        if categories:
            for category in categories:
                if category not in BRAND_DATABASE[tier]:
                    BRAND_DATABASE[tier][category] = []
                
                # Check if brand already in this category
                brand_exists = False
                for brand in BRAND_DATABASE[tier][category]:
                    if brand['name'].lower() == brand_name.lower():
                        brand_exists = True
                        break
                
                if not brand_exists:
                    BRAND_DATABASE[tier][category].append(new_brand)
        else:
            # Default to general category
            if 'general' not in BRAND_DATABASE[tier]:
                BRAND_DATABASE[tier]['general'] = []
            BRAND_DATABASE[tier]['general'].append(new_brand)
        
        # Save to file
        save_brand_database(BRAND_DATABASE)
        
        return jsonify({
            'success': True,
            'message': f'Brand {brand_name} added successfully',
            'brand': new_brand
        })
    except Exception as e:
        logger.exception('Error adding brand')
        return jsonify({'error': str(e)}), 500

@app.route('/api/brands/scrape-and-add', methods=['POST'])
def scrape_and_add_brand():
    """Scrape a brand's website and add it to the database (supports Universal and Firecrawl)"""
    try:
        data = request.get_json()
        brand_name = data.get('brand_name')
        website = data.get('website')
        country = data.get('country', 'Unknown')
        tier = data.get('tier', 'mid_range')
        scraping_method = data.get('scraping_method', 'universal')
        crawl_limit = data.get('crawl_limit', 50)
        
        logger.info(f"Received scrape request data: {data}")
        
        if not brand_name or not website:
            return jsonify({'error': 'Brand name and website are required'}), 400
        
        logger.info(f"🔍 SCRAPING METHOD: {scraping_method.upper()}")
        logger.info(f"Starting {scraping_method} scrape for {brand_name} ({website})")
        
        # Check if this is an Architonic URL first (use specialized scraper)
        from utils.architonic_scraper import ArchitonicScraper
        from utils.italian_furniture_scraper import ItalianFurnitureScraper
        
        architonic_scraper = ArchitonicScraper()
        italian_scraper = ItalianFurnitureScraper()
        
        if architonic_scraper.is_architonic_url(website):
            logger.info(f"🏛️ Detected Architonic URL - Using specialized ArchitonicScraper")
            scraped_data = architonic_scraper.scrape_collection(
                url=website,
                brand_name=brand_name
            )
            # Convert collections to category_tree format
            if 'collections' in scraped_data and not scraped_data.get('category_tree'):
                scraped_data['category_tree'] = architonic_scraper._convert_collections_to_category_tree(scraped_data['collections'])
        # Check if this is an Italian furniture manufacturer website
        elif italian_scraper.is_italian_furniture_site(website):
            logger.info(f"🇮🇹 Detected Italian Furniture Site - Using specialized ItalianFurnitureScraper")
            # Don't limit Italian scraper - we want all categories and products for complete coverage
            scraped_data = italian_scraper.scrape_brand_website(
                website=website,
                brand_name=brand_name,
                limit=None  # No limit to ensure we get all 8 categories
            )
        # Choose scraper based on method
        elif scraping_method == 'requests':
            logger.info(f"🚀 Using RequestsBrandScraper (Recommended - no API limits)")
            from utils.requests_brand_scraper import RequestsBrandScraper
            scraper = RequestsBrandScraper(delay=0.5)
            scraped_data = scraper.scrape_brand_website(
                website=website,
                brand_name=brand_name,
                limit=crawl_limit
            )
        elif scraping_method == 'firecrawl':
            logger.info(f"🔥 Using FirecrawlBrandScraper (AI-powered, API limits)")
            from utils.firecrawl_brand_scraper import FirecrawlBrandScraper
            scraper = FirecrawlBrandScraper()
            scraped_data = scraper.scrape_brand_website(
                website=website,
                brand_name=brand_name,
                limit=crawl_limit
            )
        else:  # universal (legacy)
            logger.info(f"⚡ Using UniversalBrandScraper (Legacy)")
            from utils.universal_brand_scraper import UniversalBrandScraper
            scraper = UniversalBrandScraper()
            scraped_data = scraper.scrape_brand_website(website, brand_name)
        
        if 'error' in scraped_data:
            return jsonify({'error': scraped_data['error']}), 400
        
        # Load existing brands from brands_dynamic.json
        brands_file = os.path.join('brands_data', 'brands_dynamic.json')
        if os.path.exists(brands_file):
            with open(brands_file, 'r', encoding='utf-8') as f:
                brands_data = json.load(f)
        else:
            brands_data = {'brands': []}
        
        # Handle both collections (old format) and category_tree (new format)
        categories_data = scraped_data.get('collections', {}) or scraped_data.get('category_tree', {})
        
        # Check if brand already exists
        brand_exists = False
        for brand in brands_data['brands']:
            if brand['name'].lower() == brand_name.lower():
                brand_exists = True
                # Update existing brand
                brand['website'] = website
                brand['country'] = country
                brand['tier'] = tier
                brand['categories'] = scraped_data.get('collections', {})
                brand['category_tree'] = scraped_data.get('category_tree', {})
                brand['last_scraped_at'] = datetime.now().isoformat()
                logger.info(f"Updated existing brand: {brand_name}")
                break
        
        if not brand_exists:
            # Create new brand entry
            new_brand = {
                'name': brand_name,
                'website': website,
                'country': country,
                'tier': tier,
                'categories': scraped_data.get('collections', {}),
                'category_tree': scraped_data.get('category_tree', {}),
                'added_date': datetime.now().isoformat(),
                'last_scraped_at': datetime.now().isoformat()
            }
            brands_data['brands'].append(new_brand)
            logger.info(f"Added new brand: {brand_name}")
        
        # Save to brands_dynamic.json
        os.makedirs('brands_data', exist_ok=True)
        with open(brands_file, 'w', encoding='utf-8') as f:
            json.dump(brands_data, f, indent=2, ensure_ascii=False)
        
        # Also save to individual brand file (e.g., OTTIMO_budgetary.json)
        save_individual_brand_file(brand_name, website, country, tier, scraped_data)
        
        # Extract categories for response
        categories = list(categories_data.keys())
        products_count = scraped_data.get('total_products', 0)
        
        return jsonify({
            'success': True,
            'message': f'Successfully scraped and added {brand_name}',
            'products_count': products_count,
            'categories': categories,
            'scraping_method': scraping_method
        })
        
    except Exception as e:
        logger.exception('Error in scrape_and_add_brand')
        return jsonify({'error': str(e)}), 500

def _scrape_single_brand(brand_info):
    """
    Helper function to scrape a single brand (used for parallel scraping)
    Args:
        brand_info: Dict with 'brand_name', 'website', 'country', 'tier'
    Returns:
        Dict with 'success', 'brand_name', 'data' or 'error'
    """
    try:
        brand_name = brand_info['brand_name']
        website = brand_info['website']
        country = brand_info.get('country', 'Unknown')
        tier = brand_info.get('tier', 'mid_range')
        
        # Normalize tier names
        tier_map = {
            'budgetary': 'budgetary',
            'mid-range': 'mid_range',
            'mid_range': 'mid_range',
            'high-end': 'high_end',
            'high_end': 'high_end'
        }
        tier = tier_map.get(tier.lower(), tier.lower())
        
        from utils.brand_scraper import BrandScraper
        scraper = BrandScraper()
        
        logger.info(f"[Parallel] Scraping {brand_name} ({website}) for tier {tier}")
        
        # Simplified retry logic - stop after first successful scrape with any products
        max_retries = 2
        retry_count = 0
        scraped_data = None
        last_error = None
        
        while retry_count < max_retries:
            retry_count += 1
            logger.info(f"[Parallel] Scraping attempt {retry_count}/{max_retries} for {brand_name}")
            
            try:
                scraped_data = scraper.scrape_brand_website(website, brand_name, use_selenium=True)
                
                if 'error' in scraped_data:
                    last_error = scraped_data['error']
                    if retry_count >= max_retries:
                        logger.error(f"[Parallel] Scraping failed after {max_retries} attempts for {brand_name}: {last_error}")
                        return {
                            'success': False,
                            'brand_name': brand_name,
                            'error': f'Scraping failed after {max_retries} attempts. Last error: {last_error}',
                            'details': last_error
                        }
                    logger.warning(f"[Parallel] Scraping error on attempt {retry_count} for {brand_name}: {last_error}. Retrying...")
                    time.sleep(2)
                    continue
            except Exception as e:
                last_error = str(e)
                logger.exception(f"[Parallel] Exception during scraping attempt {retry_count} for {brand_name}: {e}")
                if retry_count >= max_retries:
                    return {
                        'success': False,
                        'brand_name': brand_name,
                        'error': f'Scraping failed after {max_retries} attempts due to exception: {last_error}',
                        'details': last_error
                    }
                time.sleep(2)
                continue
            
            # Count total products - handle both collections format and regular format
            total_products = 0
            is_collections_format = 'collections' in scraped_data and 'total_products' in scraped_data
            
            # Check if it's collections format (Architonic)
            if is_collections_format:
                total_products = scraped_data.get('total_products', 0)
                total_collections = scraped_data.get('total_collections', 0)
                logger.info(f"[Parallel] Found {total_products} products in {total_collections} collections for {brand_name} on attempt {retry_count}")
                # Accept if we got any products - no minimum requirement
                if total_products > 0:
                    logger.info(f"[Parallel] Successfully scraped collections with {total_products} products for {brand_name}. Accepting result.")
                    break
                elif retry_count < max_retries:
                    logger.warning(f"[Parallel] No products found in collections format for {brand_name}. Retrying...")
                    time.sleep(2)
                    continue
                else:
                    logger.error(f"[Parallel] No products found after {max_retries} attempts for {brand_name}")
                    break
            else:
                # Regular format - count from products and categories
                total_products = len(scraped_data.get('products', []))
                for category_products in scraped_data.get('categories', {}).values():
                    if isinstance(category_products, list):
                        total_products += len(category_products)
                    elif isinstance(category_products, dict):
                        for subcat_products in category_products.values():
                            if isinstance(subcat_products, list):
                                total_products += len(subcat_products)
                logger.info(f"[Parallel] Found {total_products} products for {brand_name} on attempt {retry_count}")
                
                # Accept if we got any products - no minimum requirement, stop immediately
                if total_products > 0:
                    logger.info(f"[Parallel] Successfully scraped {total_products} products for {brand_name}. Accepting result.")
                    break
                elif retry_count < max_retries:
                    logger.warning(f"[Parallel] No products found for {brand_name}. Retrying...")
                    time.sleep(2)
                    continue
                else:
                    logger.error(f"[Parallel] No products found after {max_retries} attempts for {brand_name}")
                    break
        
        if not scraped_data:
            update_scrape_status(job_id, 'failed', 'Scraping failed: No data returned', 100)
            return {
                'success': False,
                'brand_name': brand_name,
                'error': 'Scraping failed: No data returned',
                'job_id': job_id
            }
        
        if 'error' in scraped_data:
            update_scrape_status(job_id, 'failed', f'Error: {scraped_data["error"]}', 100)
            return {
                'success': False,
                'brand_name': brand_name,
                'error': scraped_data['error'],
                'job_id': job_id
            }
        
        # Check if data is in collections format (from Architonic)
        is_collections_format = 'collections' in scraped_data and 'total_collections' in scraped_data
        
        update_scrape_status(job_id, 'organizing', 'Organizing product data by categories...', 75)
        
        # Organize data by categories and subcategories
        organized_data = {
            'brand': brand_name,
            'website': website,
            'country': country,
            'tier': tier,
            'categories': {}
        }
        
        # If collections format, preserve it and also convert to categories format
        if is_collections_format:
            # Preserve original collections structure
            organized_data['collections'] = scraped_data.get('collections', {})
            organized_data['all_products'] = scraped_data.get('all_products', [])
            organized_data['source'] = scraped_data.get('source', 'Architonic Collections')
            organized_data['scraped_at'] = scraped_data.get('scraped_at')
            organized_data['total_products'] = scraped_data.get('total_products', 0)
            organized_data['total_collections'] = scraped_data.get('total_collections', 0)
            
            # Convert collections to categories format with intelligent hierarchy and deduplication
            
            # First pass: Identify all products in specific subcategories
            products_by_cat_subcat = {} # {Category: {Subcategory: {model_name: product_data}}}
            
            for collection_name, collection_data in scraped_data.get('collections', {}).items():
                if not collection_data or not collection_data.get('products'):
                    continue
                
                clean_name = collection_name.split('\n')[0].strip()
                
                # Determine Category and Subcategory
                if ' > ' in clean_name:
                    parts = clean_name.split(' > ')
                    category = parts[0].strip()
                    subcategory = parts[1].strip()
                else:
                    category = clean_name
                    subcategory = 'General' # Default, will be filtered later
                
                if category not in products_by_cat_subcat:
                    products_by_cat_subcat[category] = {}
                if subcategory not in products_by_cat_subcat[category]:
                    products_by_cat_subcat[category][subcategory] = {}
                
                for product in collection_data.get('products', []):
                    model_name = product.get('model') or product.get('title') or product.get('name')
                    if not model_name: 
                        continue
                    
                    # Clean model name
                    model_name = model_name.strip()
                    
                    # Skip garbage
                    if model_name.lower() in ['select options', 'contact for price', 'read more', 'add to cart']:
                        continue
                    if 'contact for price' in model_name.lower():
                        model_name = model_name.replace('(Contact for price)', '').replace('Contact for price', '').strip()
                    
                    # Store product
                    products_by_cat_subcat[category][subcategory][model_name] = product

            # Second pass: Build organized_data, handling deduplication
            # If a product exists in a specific subcategory, remove it from 'General'
            
            for category, subcats in products_by_cat_subcat.items():
                organized_data['categories'][category] = {}
                
                # Get all models in specific subcategories (excluding General)
                specific_models = set()
                for subcat, models in subcats.items():
                    if subcat != 'General':
                        specific_models.update(models.keys())
                
                for subcat, models in subcats.items():
                    final_models_list = []
                    
                    for model_name, product in models.items():
                        # If we are in General, and this model exists in a specific subcategory, SKIP it
                        if subcat == 'General' and model_name in specific_models:
                            continue
                        
                        # Convert to app's model format
                        model_entry = {
                            'model': model_name,
                            'price': product.get('price'),
                            'price_range': product.get('price_range', "Contact for price"),
                            'features': product.get('features', []),
                            'image_url': product.get('image_url'),
                            'description': product.get('description', ''),
                            'source_url': product.get('url', product.get('source_url', website)),
                            'product_id': product.get('product_id', ''),
                            'collection': category + (' > ' + subcat if subcat != 'General' else '')
                        }
                        final_models_list.append(model_entry)
                    
                    # Only add subcategory if it has products
                    if final_models_list:
                        organized_data['categories'][category][subcat] = final_models_list
        
        # Process category-based products (unlimited) - for non-collections format
        elif 'categories' in scraped_data:
            for category_name, products in scraped_data.get('categories', {}).items():
                if not products:
                    continue
                
                # Organize by subcategories
                subcategories = {}
                for product in products:
                    # Determine subcategory
                    product_text = (product.get('model', '') + ' ' + product.get('description', '')).lower()
                    subcategory = 'general'
                    
                    if any(word in product_text for word in ['chair', 'seating', 'sofa', 'bench', 'stool']):
                        subcategory = 'seating'
                    elif any(word in product_text for word in ['desk', 'table', 'workstation']):
                        subcategory = 'desking'
                    elif any(word in product_text for word in ['cabinet', 'storage', 'shelf', 'drawer']):
                        subcategory = 'storage'
                    elif any(word in product_text for word in ['lamp', 'light']):
                        subcategory = 'lighting'
                    
                    if subcategory not in subcategories:
                        subcategories[subcategory] = []
                    
                    model_entry = {
                        'model': product.get('model', 'Unknown Model'),
                        'price': product.get('price'),
                        'price_range': f"{int(product.get('price', 0))}-{int(product.get('price', 0) * 1.5)}" if product.get('price') else "Contact for price",
                        'features': product.get('features', []),
                        'image_url': product.get('image_url'),
                        'description': product.get('description', ''),
                        'source_url': product.get('source_url', website)
                    }
                    subcategories[subcategory].append(model_entry)
                
                organized_data['categories'][category_name] = subcategories
        
        # Process standalone products (unlimited) - for non-collections format
        if not is_collections_format:
            for product in scraped_data.get('products', []):
                product_text = (product.get('model', '') + ' ' + product.get('description', '')).lower()
                category = 'General'
                subcategory = 'general'
                
                if any(word in product_text for word in ['chair', 'seating', 'sofa']):
                    category = 'Seating'
                    subcategory = 'seating'
                elif any(word in product_text for word in ['desk', 'table']):
                    category = 'Desking'
                    subcategory = 'desking'
                
                if category not in organized_data['categories']:
                    organized_data['categories'][category] = {}
                
                if subcategory not in organized_data['categories'][category]:
                    organized_data['categories'][category][subcategory] = []
                
                model_entry = {
                    'model': product.get('model', 'Unknown Model'),
                    'price': product.get('price'),
                    'price_range': f"{int(product.get('price', 0))}-{int(product.get('price', 0) * 1.5)}" if product.get('price') else "Contact for price",
                    'features': product.get('features', []),
                    'image_url': product.get('image_url'),
                    'description': product.get('description', ''),
                    'source_url': product.get('source_url', website)
                }
                organized_data['categories'][category][subcategory].append(model_entry)
        
        # Save to brands_data folder as separate JSON file
        filepath = scraper.save_brand_data(organized_data, tier, output_dir='brands_data')
        
        # Count total products
        if is_collections_format:
            total_products = organized_data.get('total_products', 0)
            total_collections = organized_data.get('total_collections', 0)
            categories_list = list(organized_data['categories'].keys())
        else:
            total_products = 0
            for category_data in organized_data['categories'].values():
                for subcat_data in category_data.values():
                    total_products += len(subcat_data)
            total_collections = 0
            categories_list = list(organized_data['categories'].keys())
        
        logger.info(f"[Parallel] Brand {brand_name} saved with {total_products} products. File: {filepath}")
        
        # Update brands_dynamic.json
        scraped_at_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        update_brands_dynamic_json(
            brand_name=brand_name,
            website=website,
            country=country,
            tier=tier,
            categories=organized_data.get('categories', {}),
            source='brand_website',
            scraped_at=scraped_at_str
        )
        
        result = {
            'success': True,
            'brand_name': brand_name,
            'message': f'Successfully scraped and saved {brand_name} with {total_products} products. Existing data has been replaced.',
            'products_count': total_products,
            'categories': categories_list,
            'filepath': filepath
        }
        
        if is_collections_format:
            result['collections_count'] = total_collections
            result['message'] = f'Successfully scraped and saved {brand_name} with {total_products} products in {total_collections} collections. Existing data has been replaced.'
        
        return result
        
    except Exception as e:
        logger.exception(f"[Parallel] Error scraping brand {brand_info.get('brand_name', 'Unknown')}: {e}")
        return {
            'success': False,
            'brand_name': brand_info.get('brand_name', 'Unknown'),
            'error': str(e)
        }


@app.route('/api/brands/scrape-multiple', methods=['POST'])
def scrape_multiple_brands():
    """Scrape multiple brands in parallel"""
    try:
        data = request.get_json()
        brands = data.get('brands', [])  # List of brand objects: [{brand_name, website, country, tier}, ...]
        max_workers = data.get('max_workers', 3)  # Number of parallel workers (default: 3)
        
        if not brands or not isinstance(brands, list):
            return jsonify({'error': 'Brands list is required and must be a list'}), 400
        
        if len(brands) == 0:
            return jsonify({'error': 'At least one brand is required'}), 400
        
        # Validate each brand has required fields
        for brand in brands:
            if not brand.get('brand_name') or not brand.get('website'):
                return jsonify({'error': 'Each brand must have brand_name and website'}), 400
        
        logger.info(f"Starting parallel scraping of {len(brands)} brands with {max_workers} workers")
        
        # Use ThreadPoolExecutor for parallel scraping
        from concurrent.futures import ThreadPoolExecutor, as_completed
        import threading
        
        results = []
        completed_count = 0
        total_brands = len(brands)
        
        # Create a lock for thread-safe logging
        log_lock = threading.Lock()
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all scraping tasks
            future_to_brand = {
                executor.submit(_scrape_single_brand, brand): brand['brand_name'] 
                for brand in brands
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_brand):
                brand_name = future_to_brand[future]
                try:
                    result = future.result()
                    results.append(result)
                    completed_count += 1
                    
                    with log_lock:
                        status = "✅ SUCCESS" if result.get('success') else "❌ FAILED"
                        logger.info(f"[Parallel] [{completed_count}/{total_brands}] {brand_name}: {status}")
                        if result.get('success'):
                            logger.info(f"[Parallel] {brand_name}: {result.get('products_count', 0)} products scraped")
                        else:
                            logger.error(f"[Parallel] {brand_name}: {result.get('error', 'Unknown error')}")
                            
                except Exception as e:
                    logger.exception(f"[Parallel] Exception for {brand_name}: {e}")
                    results.append({
                        'success': False,
                        'brand_name': brand_name,
                        'error': str(e)
                    })
                    completed_count += 1
        
        # Count successes and failures
        successful = [r for r in results if r.get('success')]
        failed = [r for r in results if not r.get('success')]
        
        total_products = sum(r.get('products_count', 0) for r in successful)
        
        logger.info(f"[Parallel] Completed: {len(successful)} successful, {len(failed)} failed, {total_products} total products")
        
        return jsonify({
            'success': True,
            'message': f'Parallel scraping completed: {len(successful)} successful, {len(failed)} failed',
            'total_brands': total_brands,
            'successful_count': len(successful),
            'failed_count': len(failed),
            'total_products': total_products,
            'results': results
        })
        
    except Exception as e:
        logger.exception('Error in parallel scraping')
        return jsonify({'error': str(e)}), 500


@app.route('/api/brands/scrape-status/<job_id>', methods=['GET'])
def get_scrape_status(job_id):
    """Get current scraping status and events for a job"""
    try:
        with scraping_status_lock:
            status = scraping_status.get(job_id, {
                'status': 'unknown',
                'events': [],
                'progress': 0,
                'message': 'Job not found'
            })
        
        return jsonify({
            'success': True,
            'status': status.get('status', 'unknown'),
            'events': status.get('events', []),
            'progress': status.get('progress', 0),
            'message': status.get('message', ''),
            'timestamp': status.get('timestamp', datetime.now().isoformat())
        })
    except Exception as e:
        logger.exception(f"Error getting scrape status for {job_id}: {e}")
        return jsonify({'error': str(e)}), 500

def update_scrape_status(job_id, status, message=None, progress=None):
    """Update scraping status for real-time preview"""
    try:
        with scraping_status_lock:
            if job_id not in scraping_status:
                scraping_status[job_id] = {
                    'status': 'running',
                    'events': [],
                    'progress': 0,
                    'message': '',
                    'timestamp': datetime.now().isoformat()
                }
            
            scraping_status[job_id]['status'] = status
            scraping_status[job_id]['timestamp'] = datetime.now().isoformat()
            
            if message:
                scraping_status[job_id]['message'] = message
                # Add event (keep last 50 events)
                scraping_status[job_id]['events'].append({
                    'timestamp': datetime.now().isoformat(),
                    'message': message,
                    'status': status
                })
                # Keep only last 50 events
                if len(scraping_status[job_id]['events']) > 50:
                    scraping_status[job_id]['events'] = scraping_status[job_id]['events'][-50:]
            
            if progress is not None:
                scraping_status[job_id]['progress'] = progress
    except Exception as e:
        logger.warning(f"Error updating scrape status: {e}")

def cleanup_scrape_status(job_id):
    """Clean up scraping status after completion (keep for 5 minutes)"""
    def cleanup_after_delay():
        time.sleep(300)  # 5 minutes
        try:
            with scraping_status_lock:
                if job_id in scraping_status:
                    del scraping_status[job_id]
        except:
            pass
    
    Thread(target=cleanup_after_delay, daemon=True).start()

@app.route('/api/brands/download-excel', methods=['GET'])
def download_brand_excel():
    """Download brand data as Excel file"""
    brand = request.args.get('brand', '')
    tier = request.args.get('tier', 'mid_range')
    
    if not brand:
        return jsonify({'error': 'Brand name is required'}), 400
    
    try:
        import pandas as pd
        from io import BytesIO
        import json
        import re
        
        # Normalize tier name
        tier_map = {
            'budgetary': 'budgetary',
            'mid-range': 'mid_range',
            'mid_range': 'mid_range',
            'high-end': 'high_end',
            'high_end': 'high_end'
        }
        tier = tier_map.get(tier.lower(), tier.lower())
        
        # Load brand data
        safe_brand_name = re.sub(r'[^\w\-_]', '', brand.replace(' ', '_'))
        filename = f"{safe_brand_name}_{tier}.json"
        filepath = os.path.join('brands_data', filename)
        
        if not os.path.exists(filepath):
            # Try case-insensitive search
            brands_data_dir = 'brands_data'
            if os.path.exists(brands_data_dir):
                for f in os.listdir(brands_data_dir):
                    if f.lower() == filename.lower():
                        filepath = os.path.join(brands_data_dir, f)
                        break
        
        if not os.path.exists(filepath):
            return jsonify({'error': f'Brand data not found for {brand} ({tier})'}), 404
        
        with open(filepath, 'r', encoding='utf-8') as f:
            brand_data = json.load(f)
        
        # Prepare data for Excel - flatten structure
        rows = []
        categories_data = brand_data.get('categories', {})
        
        for category_name, subcategories in categories_data.items():
            for subcategory_name, models in subcategories.items():
                for model in models:
                    rows.append({
                        'Brand': brand_data.get('brand', brand),
                        'Category': category_name,
                        'Sub-Category': subcategory_name,
                        'Model': model.get('model', ''),
                        'Price': model.get('price'),
                        'Price Range': model.get('price_range', 'Contact for price'),
                        'Description': model.get('description', ''),
                        'Product ID': model.get('product_id', ''),
                        'URL': model.get('source_url', ''),
                        'Image URL': model.get('image_url', ''),
                        'Features': ', '.join(model.get('features', [])) if isinstance(model.get('features'), list) else str(model.get('features', ''))
                    })
        
        # If collections format exists, also include those
        if 'collections' in brand_data:
            for collection_name, collection_data in brand_data.get('collections', {}).items():
                clean_collection_name = collection_name.split('\n')[0].strip()
                for product in collection_data.get('products', []):
                    rows.append({
                        'Brand': brand_data.get('brand', brand),
                        'Category': clean_collection_name,
                        'Sub-Category': 'general',
                        'Model': product.get('name', product.get('model', '')),
                        'Price': None,
                        'Price Range': 'Contact for price',
                        'Description': '',
                        'Product ID': product.get('product_id', ''),
                        'URL': product.get('url', ''),
                        'Image URL': product.get('image_url', ''),
                        'Features': ''
                    })
        
        if not rows:
            return jsonify({'error': 'No products found in brand data'}), 404
        
        # Create DataFrame
        df = pd.DataFrame(rows)
        
        # Create Excel file in memory
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Products', index=False)
        
        output.seek(0)
        
        # Generate filename
        safe_filename = f"{safe_brand_name}_{tier}.xlsx"
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=safe_filename
        )
        
    except Exception as e:
        logger.exception('Error generating Excel download for brand')
        return jsonify({'error': str(e)}), 500

@app.route('/api/brands/upload-excel', methods=['POST'])
def upload_brand_excel():
    """Upload and convert Excel file to brand JSON format"""
    try:
        import pandas as pd
        import json
        import re
        
        if 'file' not in request.files:
            return jsonify({'error': 'No file provided'}), 400
        
        file = request.files['file']
        brand = request.form.get('brand', '')
        tier = request.form.get('tier', 'mid_range')
        
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        
        if not file.filename.endswith(('.xlsx', '.xls')):
            return jsonify({'error': 'Invalid file format. Please upload an Excel file (.xlsx or .xls)'}), 400
        
        # Normalize tier name
        tier_map = {
            'budgetary': 'budgetary',
            'mid-range': 'mid_range',
            'mid_range': 'mid_range',
            'high-end': 'high_end',
            'high_end': 'high_end'
        }
        tier = tier_map.get(tier.lower(), tier.lower())
        
        # Read Excel file
        try:
            df = pd.read_excel(file)
        except Exception as e:
            return jsonify({'error': f'Error reading Excel file: {str(e)}'}), 400
        
        # Validate required columns
        required_columns = ['Brand', 'Category', 'Model']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            return jsonify({'error': f'Missing required columns: {", ".join(missing_columns)}'}), 400
        
        # Get brand name from data if not provided in form
        if not brand and 'Brand' in df.columns:
            brand = df['Brand'].iloc[0] if len(df) > 0 else 'Unknown'
        
        if not brand:
            return jsonify({'error': 'Brand name is required'}), 400
        
        # Convert Excel to JSON structure
        organized_data = {
            'brand': brand,
            'website': request.form.get('website', ''),
            'country': request.form.get('country', 'Unknown'),
            'tier': tier,
            'categories': {}
        }
        
        # Group by category and subcategory
        for _, row in df.iterrows():
            category = str(row.get('Category', 'General')).strip()
            subcategory = str(row.get('Sub-Category', 'general')).strip() if pd.notna(row.get('Sub-Category')) else 'general'
            model = str(row.get('Model', '')).strip()
            
            if not category or not model:
                continue
            
            # Initialize category if needed
            if category not in organized_data['categories']:
                organized_data['categories'][category] = {}
            
            # Initialize subcategory if needed
            if subcategory not in organized_data['categories'][category]:
                organized_data['categories'][category][subcategory] = []
            
            # Create model entry
            model_entry = {
                'model': model,
                'price': row.get('Price') if pd.notna(row.get('Price')) else None,
                'price_range': str(row.get('Price Range', 'Contact for price')) if pd.notna(row.get('Price Range')) else 'Contact for price',
                'description': str(row.get('Description', '')) if pd.notna(row.get('Description')) else '',
                'product_id': str(row.get('Product ID', '')) if pd.notna(row.get('Product ID')) else '',
                'source_url': str(row.get('URL', '')) if pd.notna(row.get('URL')) else '',
                'image_url': str(row.get('Image URL', '')) if pd.notna(row.get('Image URL')) else '',
                'features': []
            }
            
            # Parse features if provided
            if 'Features' in df.columns and pd.notna(row.get('Features')):
                features_str = str(row.get('Features', ''))
                if features_str:
                    # Split by comma or semicolon
                    model_entry['features'] = [f.strip() for f in re.split(r'[,;]', features_str) if f.strip()]
            
            organized_data['categories'][category][subcategory].append(model_entry)
        
        # Save to brands_data folder
        from utils.brand_scraper import BrandScraper
        scraper = BrandScraper()
        filepath = scraper.save_brand_data(organized_data, tier, output_dir='brands_data')
        
        # Count total products
        total_products = 0
        for category_data in organized_data['categories'].values():
            for subcat_data in category_data.values():
                total_products += len(subcat_data)
        
        logger.info(f"Brand {brand} uploaded with {total_products} products from Excel. File: {filepath}")
        
        # Update brands_dynamic.json
        website = organized_data.get('website', '')
        country = organized_data.get('country', 'Unknown')
        update_brands_dynamic_json(
            brand_name=brand,
            website=website,
            country=country,
            tier=tier,
            categories=organized_data.get('categories', {}),
            source='excel_upload',
            scraped_at=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        )
        
        return jsonify({
            'success': True,
            'message': f'Successfully uploaded and saved {brand} with {total_products} products. Existing data has been replaced.',
            'brand': organized_data,
            'products_count': total_products,
            'categories': list(organized_data['categories'].keys()),
            'filepath': filepath
        })
        
    except Exception as e:
        logger.exception('Error uploading Excel file')
        return jsonify({'error': str(e)}), 500

def save_brand_database(database):
    """Save brand database to file"""
    try:
        import json
        db_file = os.path.join('utils', 'brand_database_custom.json')
        with open(db_file, 'w', encoding='utf-8') as f:
            json.dump(database, f, indent=2, ensure_ascii=False)
        logger.info(f"Brand database saved to {db_file}")
    except Exception as e:
        logger.error(f"Error saving brand database: {e}")

def save_individual_brand_file(brand_name: str, website: str, country: str, tier: str, scraped_data: dict):
    """
    Save brand data to individual brand file (e.g., OTTIMO_budgetary.json)
    
    Args:
        brand_name: Name of the brand
        website: Brand website URL
        country: Country of origin
        tier: Budget tier (budgetary, mid_range, high_end)
        scraped_data: Complete scraped data including categories/category_tree
    """
    try:
        # Create safe filename
        import re
        safe_brand_name = re.sub(r'[^\w\-_]', '', brand_name.replace(' ', '_'))
        filename = f"{safe_brand_name}_{tier}.json"
        filepath = os.path.join('brands_data', filename)
        
        # Build individual brand file structure
        brand_file_data = {
            "brand": brand_name,
            "website": website,
            "country": country,
            "tier": tier,
            "categories": scraped_data.get('collections', {}),
            "category_tree": scraped_data.get('category_tree', {}),
            "source": scraped_data.get('source', 'brand_website'),
            "scraped_at": scraped_data.get('scraped_at', datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
            "added_date": datetime.now().isoformat(),
            "updated_date": datetime.now().isoformat()
        }
        
        # Add metadata if available
        if 'total_products' in scraped_data:
            brand_file_data['total_products'] = scraped_data['total_products']
        if 'includes_descriptions' in scraped_data:
            brand_file_data['includes_descriptions'] = scraped_data['includes_descriptions']
        
        # Save to individual file
        os.makedirs('brands_data', exist_ok=True)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(brand_file_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"✅ Saved individual brand file: {filename}")
        
    except Exception as e:
        logger.error(f"Error saving individual brand file for {brand_name}: {e}")
        # Don't fail the entire scraping operation if individual file save fails
        logger.exception("Full traceback:")

def update_brands_dynamic_json(brand_name: str, website: str, country: str, tier: str, 
                                categories: dict = None, source: str = 'brand_website', 
                                scraped_at: str = None):
    """
    Update brands_dynamic.json with brand information
    
    Args:
        brand_name: Name of the brand
        website: Brand website URL
        country: Country of origin
        tier: Budget tier (budgetary, mid_range, high_end)
        categories: Dictionary of categories and products (optional)
        source: Source of data (default: 'brand_website')
        scraped_at: Timestamp when scraping was completed (optional)
    """
    try:
        brands_dynamic_path = os.path.join('brands_data', 'brands_dynamic.json')
        
        # Load existing brands_dynamic.json or create new structure
        if os.path.exists(brands_dynamic_path):
            with open(brands_dynamic_path, 'r', encoding='utf-8') as f:
                brands_dynamic = json.load(f)
        else:
            brands_dynamic = {
                "brands": [],
                "last_updated": None,
                "version": "2.0"
            }
        
        # Ensure brands list exists
        if 'brands' not in brands_dynamic:
            brands_dynamic['brands'] = []
        
        # Normalize tier name
        tier_map = {
            'budgetary': 'budgetary',
            'mid-range': 'mid_range',
            'mid_range': 'mid_range',
            'high-end': 'high_end',
            'high_end': 'high_end'
        }
        tier = tier_map.get(tier.lower(), tier.lower())
        
        # Check if brand already exists (by name and tier)
        brand_found = False
        current_time = datetime.now().isoformat()
        
        for i, brand in enumerate(brands_dynamic['brands']):
            if brand.get('name', '').lower() == brand_name.lower() and brand.get('tier') == tier:
                # Update existing brand
                brand['website'] = website
                brand['country'] = country
                brand['tier'] = tier
                brand['updated_date'] = current_time
                brand['source'] = source
                
                if categories is not None:
                    brand['categories'] = categories
                
                if scraped_at:
                    brand['scraped_at'] = scraped_at
                else:
                    brand['scraped_at'] = current_time.split('T')[0] + ' ' + current_time.split('T')[1].split('.')[0]
                
                brands_dynamic['brands'][i] = brand
                brand_found = True
                logger.info(f"Updated existing brand {brand_name} ({tier}) in brands_dynamic.json")
                break
        
        # Add new brand if not found
        if not brand_found:
            new_brand = {
                "name": brand_name,
                "website": website,
                "country": country,
                "tier": tier,
                "categories": categories if categories is not None else {},
                "added_date": current_time,
                "updated_date": current_time,
                "source": source,
                "scraped_at": scraped_at if scraped_at else (current_time.split('T')[0] + ' ' + current_time.split('T')[1].split('.')[0])
            }
            brands_dynamic['brands'].append(new_brand)
            logger.info(f"Added new brand {brand_name} ({tier}) to brands_dynamic.json")
        
        # Update metadata
        brands_dynamic['last_updated'] = current_time
        if 'version' not in brands_dynamic:
            brands_dynamic['version'] = "2.0"
        
        # Save updated file
        os.makedirs('brands_data', exist_ok=True)
        with open(brands_dynamic_path, 'w', encoding='utf-8') as f:
            json.dump(brands_dynamic, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Successfully updated brands_dynamic.json with {brand_name}")
        
    except Exception as e:
        logger.error(f"Error updating brands_dynamic.json: {e}")
        logger.exception("Full traceback:")

@app.route('/download/<file_type>/<file_id>', methods=['GET'])
def download(file_type, file_id):
    """Download generated files"""
    format_type = request.args.get('format', 'pdf')
    
    try:
        from utils.download_manager import DownloadManager
        manager = DownloadManager()
        file_path = manager.prepare_download(file_id, file_type, format_type, session)
        
        return send_file(file_path, as_attachment=True)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/download/extracted/<file_id>', methods=['GET'])
def download_extracted(file_id):
    """Download extracted table data as Excel"""
    format_type = request.args.get('format', 'excel')
    
    try:
        import pandas as pd
        from io import BytesIO
        
        # Get file info
        uploaded_files = session.get('uploaded_files', [])
        file_info = None
        
        for f in uploaded_files:
            if f['id'] == file_id:
                file_info = f
                break
        
        if not file_info or 'extraction_result' not in file_info:
            return jsonify({'error': 'Extraction result not found'}), 404
        
        extraction_result = file_info['extraction_result']
        
        # Create Excel file
        output = BytesIO()
        writer = pd.ExcelWriter(output, engine='openpyxl')
        
        for page_idx, layout_result in enumerate(extraction_result.get('layoutParsingResults', [])):
            markdown_text = layout_result.get('markdown', {}).get('text', '')
            
            # Try to parse HTML tables from markdown
            if '<table' in markdown_text:
                try:
                    # Use pandas to read HTML tables
                    tables = pd.read_html(markdown_text)
                    for table_idx, df in enumerate(tables):
                        sheet_name = f"Page{page_idx+1}_Table{table_idx+1}"[:31]  # Excel sheet name limit
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                except Exception as e:
                    logger.error(f"Error parsing HTML table: {e}")
                    # Fallback: create a sheet with raw text
                    df = pd.DataFrame({'Extracted Text': [markdown_text]})
                    sheet_name = f"Page{page_idx+1}"[:31]
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
            else:
                # No HTML tables, save as text
                df = pd.DataFrame({'Extracted Text': [markdown_text]})
                sheet_name = f"Page{page_idx+1}"[:31]
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        writer.close()
        output.seek(0)
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=f"extracted_{file_info['original_name'].rsplit('.', 1)[0]}.xlsx"
        )
    except Exception as e:
        logger.exception('Error generating Excel download')
        return jsonify({'error': str(e)}), 500

@app.route('/download/stitched/<file_id>', methods=['GET'])
def download_stitched(file_id):
    """Download stitched table as Excel"""
    format_type = request.args.get('format', 'excel')
    
    try:
        import pandas as pd
        from io import BytesIO
        
        # Get file info
        uploaded_files = session.get('uploaded_files', [])
        file_info = None
        
        for f in uploaded_files:
            if f['id'] == file_id:
                file_info = f
                break
        
        if not file_info or 'stitched_table' not in file_info:
            return jsonify({'error': 'Stitched table not found. Please stitch tables first.'}), 404
        
        stitched_html = file_info['stitched_table']['html']
        
        # Create Excel file
        output = BytesIO()
        writer = pd.ExcelWriter(output, engine='openpyxl')
        
        try:
            # Use pandas to read the stitched HTML table
            tables = pd.read_html(stitched_html)
            if tables:
                df = tables[0]
                df.to_excel(writer, sheet_name='Stitched_Table', index=False)
            else:
                return jsonify({'error': 'No table found in stitched data'}), 404
        except Exception as e:
            logger.error(f"Error parsing stitched HTML table: {e}")
            return jsonify({'error': f'Error parsing table: {str(e)}'}), 500
        
        writer.close()
        output.seek(0)
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=f"stitched_{file_info['original_name'].rsplit('.', 1)[0]}.xlsx"
        )
    except Exception as e:
        logger.exception('Error generating stitched Excel download')
        return jsonify({'error': str(e)}), 500

@app.route('/admin/cleanup', methods=['POST'])
def admin_cleanup():
    """Manual trigger for cleanup (admin endpoint)"""
    hours = request.json.get('hours', 24) if request.is_json else 24
    try:
        cleaned = cleanup_old_files(hours=hours)
        return jsonify({
            'success': True,
            'message': f'Cleanup completed',
            'cleaned': cleaned
        })
    except Exception as e:
        logger.exception('Error in manual cleanup')
        return jsonify({'error': str(e)}), 500

@app.route('/api/cleanup-session', methods=['POST'])
def cleanup_session_api():
    """API endpoint for cleaning up current session data"""
    try:
        cleanup_session_files()
        return jsonify({
            'success': True,
            'message': 'Session data cleaned'
        })
    except Exception as e:
        logger.exception('Error in session cleanup')
        return jsonify({'error': str(e)}), 500

@app.route('/api/cleanup-all', methods=['POST'])
def cleanup_all_api():
    """API endpoint for cleaning all sessions (triggered on page load)"""
    try:
        session_id = session.get('session_id')
        if session_id:
            cleaned = cleanup_other_sessions(session_id)
            return jsonify({
                'success': True,
                'message': 'Other sessions cleaned',
                'cleaned': cleaned
            })
        return jsonify({'success': False, 'message': 'No active session'}), 400
    except Exception as e:
        logger.exception('Error in cleanup all')
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    # Run cleanup on server start (only files older than 1 hour to avoid deleting active work)
    try:
        logger.info("Running startup cleanup (files older than 1 hour)...")
        cleaned = cleanup_old_files(hours=1)
        logger.info(f"Startup cleanup completed: {cleaned}")
    except Exception as e:
        logger.error(f"Error in startup cleanup: {e}")
    
    # Start background cleanup thread (runs every hour, cleans files older than 24h)
    cleanup_thread = Thread(target=periodic_cleanup, daemon=True)
    cleanup_thread.start()
    logger.info("Started periodic cleanup thread (runs every hour, cleans files older than 24h)")
    
    # Get port from environment variable (Railway provides this) or default to 5000
    port = int(os.environ.get('PORT', 5000))
    # Disable debug mode in production
    debug = os.environ.get('FLASK_DEBUG', 'False').lower() == 'true'
    
    app.run(debug=debug, host='0.0.0.0', port=port)
