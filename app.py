import os
import logging
import json
from flask import Flask, render_template, request, jsonify, session, Response
from utils.clickhouse_client import ClickHouseClient
from utils.flat_file_client import FlatFileClient
from utils.data_integrator import DataIntegrator

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Create Flask app
app = Flask(__name__)
app.secret_key = os.environ.get("SESSION_SECRET", "dev_secret_key")

# Create upload directory if it doesn't exist
os.makedirs('uploads', exist_ok=True)

@app.route('/')
def index():
    """Render the main application page"""
    return render_template('index.html')

@app.route('/test-clickhouse-connection', methods=['POST'])
def test_clickhouse_connection():
    """Test ClickHouse connection with provided credentials"""
    try:
        data = request.json
        client = ClickHouseClient(
            host=data.get('host'),
            port=data.get('port'),
            database=data.get('database'),
            user=data.get('user'),
            jwt_token=data.get('jwt_token'),
            secure=True
        )
        
        connection_result = client.test_connection()
        
        if connection_result['success']:
            # Store connection info in session for later use
            session['clickhouse_config'] = {
                'host': data.get('host'),
                'port': data.get('port'),
                'database': data.get('database'),
                'user': data.get('user'),
                'jwt_token': data.get('jwt_token')
            }
            
        return jsonify(connection_result)
    except Exception as e:
        logger.error(f"Error testing ClickHouse connection: {str(e)}")
        return jsonify({'success': False, 'message': str(e)})

@app.route('/get-clickhouse-tables', methods=['GET'])
def get_clickhouse_tables():
    """Get available tables from the ClickHouse database"""
    try:
        if 'clickhouse_config' not in session:
            return jsonify({'success': False, 'message': 'No ClickHouse connection configured'})
            
        config = session['clickhouse_config']
        client = ClickHouseClient(
            host=config.get('host'),
            port=config.get('port'),
            database=config.get('database'),
            user=config.get('user'),
            jwt_token=config.get('jwt_token'),
            secure=True
        )
        
        tables = client.get_tables()
        return jsonify({'success': True, 'tables': tables})
    except Exception as e:
        logger.error(f"Error getting ClickHouse tables: {str(e)}")
        return jsonify({'success': False, 'message': str(e)})

@app.route('/get-table-columns', methods=['POST'])
def get_table_columns():
    """Get columns for a specified ClickHouse table"""
    try:
        data = request.json
        table_name = data.get('table_name')
        
        if 'clickhouse_config' not in session:
            return jsonify({'success': False, 'message': 'No ClickHouse connection configured'})
            
        config = session['clickhouse_config']
        client = ClickHouseClient(
            host=config.get('host'),
            port=config.get('port'),
            database=config.get('database'),
            user=config.get('user'),
            jwt_token=config.get('jwt_token'),
            secure=True
        )
        
        columns = client.get_table_columns(table_name)
        return jsonify({'success': True, 'columns': columns})
    except Exception as e:
        logger.error(f"Error getting table columns: {str(e)}")
        return jsonify({'success': False, 'message': str(e)})

@app.route('/get-file-by-path', methods=['POST'])
def get_file_by_path():
    """Get columns from an existing file path"""
    try:
        data = request.json
        file_path = data.get('file_path')
        delimiter = data.get('delimiter', ',')
        
        logger.debug(f"Using existing file path: {file_path} with delimiter: '{delimiter}'")
        
        # Validate file path exists
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return jsonify({'success': False, 'message': f"File not found: {file_path}"})
        
        logger.debug(f"File exists. Size: {os.path.getsize(file_path)} bytes")
        
        # Store file info in session for later use
        session['flat_file_config'] = {
            'file_path': file_path,
            'delimiter': delimiter
        }
        
        client = FlatFileClient(file_path, delimiter)
        columns = client.get_columns()
        preview_data = client.get_preview(100)  # Get first 100 rows for preview
        
        return jsonify({
            'success': True, 
            'columns': columns,
            'previewData': preview_data,
            'totalRows': client.get_total_rows()
        })
    except Exception as e:
        logger.error(f"Error getting file by path: {str(e)}")
        return jsonify({'success': False, 'message': str(e)})

@app.route('/get-file-columns', methods=['POST'])
def get_file_columns():
    """Get columns from a flat file"""
    try:
        # Check if file was uploaded
        if 'file' not in request.files:
            return jsonify({'success': False, 'message': 'No file uploaded'})
            
        file = request.files['file']
        delimiter = request.form.get('delimiter', ',')
        
        # Store file in uploads directory
        file_path = os.path.join('uploads', file.filename)
        logger.debug(f"Saving uploaded file '{file.filename}' to path: {file_path}")
        
        # Ensure uploads directory exists (just in case)
        os.makedirs('uploads', exist_ok=True)
        
        file.save(file_path)
        logger.debug(f"File saved successfully. Size: {os.path.getsize(file_path)} bytes")
        
        # Store file info in session for later use
        session['flat_file_config'] = {
            'file_path': file_path,
            'delimiter': delimiter
        }
        logger.debug(f"File config stored in session with delimiter: '{delimiter}'")
        
        client = FlatFileClient(file_path, delimiter)
        columns = client.get_columns()
        preview_data = client.get_preview(100)  # Get first 100 rows for preview
        
        return jsonify({
            'success': True, 
            'columns': columns,
            'previewData': preview_data,
            'totalRows': client.get_total_rows()
        })
    except Exception as e:
        logger.error(f"Error getting file columns: {str(e)}")
        return jsonify({'success': False, 'message': str(e)})

@app.route('/preview-data', methods=['POST'])
def preview_data():
    """Preview data from the selected source with selected columns"""
    try:
        data = request.json
        source = data.get('source')
        
        logger.debug(f"Preview data request for source: {source}")
        logger.debug(f"Preview data request body: {data}")
        
        if source == 'clickhouse':
            table_name = data.get('table_name')
            selected_columns = data.get('selected_columns', [])
            join_config = data.get('join_config', None)
            
            logger.debug(f"ClickHouse preview for table: {table_name}")
            logger.debug(f"Selected columns for preview: {selected_columns}")
            logger.debug(f"Join config for preview: {join_config}")
            
            if 'clickhouse_config' not in session:
                return jsonify({'success': False, 'message': 'No ClickHouse connection configured'})
                
            config = session['clickhouse_config']
            client = ClickHouseClient(
                host=config.get('host'),
                port=config.get('port'),
                database=config.get('database'),
                user=config.get('user'),
                jwt_token=config.get('jwt_token'),
                secure=True
            )
            
            preview_data = client.get_preview_data(table_name, selected_columns, join_config)
            return jsonify({'success': True, 'previewData': preview_data})
            
        elif source == 'flatfile':
            selected_columns = data.get('selected_columns', [])
            
            logger.debug(f"Flat file preview request")
            logger.debug(f"Selected columns for flat file preview: {selected_columns}")
            
            if 'flat_file_config' not in session:
                logger.error("No flat file configuration found in session")
                return jsonify({'success': False, 'message': 'No flat file configured'})
                
            config = session['flat_file_config']
            client = FlatFileClient(config.get('file_path'), config.get('delimiter'))
            
            preview_data = client.get_preview(100, selected_columns)
            return jsonify({'success': True, 'previewData': preview_data})
            
        else:
            return jsonify({'success': False, 'message': 'Invalid source specified'})
    except Exception as e:
        logger.error(f"Error previewing data: {str(e)}")
        return jsonify({'success': False, 'message': str(e)})

@app.route('/start-ingestion', methods=['POST'])
def start_ingestion():
    """Start the data ingestion process"""
    try:
        # Validate content type to prevent bad request errors
        if not request.is_json:
            logger.error(f"Non-JSON request received. Content-Type: {request.content_type}")
            return jsonify({
                'success': False,
                'message': 'Invalid request format. Expected JSON.',
                'records_processed': 0
            })
        
        data = request.json
        if not data:
            logger.error("Empty JSON request received")
            return jsonify({
                'success': False,
                'message': 'Empty request received. Please provide source, target, and column data.',
                'records_processed': 0
            })
            
        source = data.get('source')
        target = data.get('target')
        selected_columns = data.get('selected_columns', [])
        
        logger.debug(f"Starting ingestion from {source} to {target}")
        logger.debug(f"Ingestion request data: {data}")
        logger.debug(f"Selected columns for ingestion: {selected_columns}")
        
        integrator = DataIntegrator()
        
        if source == 'clickhouse' and target == 'flatfile':
            table_name = data.get('table_name')
            join_config = data.get('join_config', None)
            target_file_name = data.get('target_file_path')
            target_delimiter = data.get('target_delimiter', ',')
            
            if not table_name:
                logger.error("Missing table_name in request")
                return jsonify({
                    'success': False,
                    'message': 'Missing table name in request',
                    'records_processed': 0
                })
                
            if not target_file_name:
                logger.error("Missing target_file_path in request")
                return jsonify({
                    'success': False, 
                    'message': 'Missing target file path in request',
                    'records_processed': 0
                })
            
            # Make sure target file is saved in uploads directory
            target_file_path = os.path.join('uploads', target_file_name)
            
            if 'clickhouse_config' not in session:
                logger.error("No ClickHouse connection configured in session")
                return jsonify({
                    'success': False, 
                    'message': 'No ClickHouse connection configured',
                    'records_processed': 0
                })
                
            config = session['clickhouse_config']
            logger.debug(f"Using ClickHouse config: {config.get('host')}:{config.get('port')}, db: {config.get('database')}")
            
            clickhouse_client = ClickHouseClient(
                host=config.get('host'),
                port=config.get('port'),
                database=config.get('database'),
                user=config.get('user'),
                jwt_token=config.get('jwt_token'),
                secure=True
            )
            
            flat_file_client = FlatFileClient(target_file_path, target_delimiter)
            
            result = integrator.clickhouse_to_flat_file(
                clickhouse_client, 
                flat_file_client, 
                table_name, 
                selected_columns,
                join_config
            )
            
            return jsonify(result)
            
        elif source == 'flatfile' and target == 'clickhouse':
            target_table = data.get('target_table')
            
            if 'clickhouse_config' not in session or 'flat_file_config' not in session:
                return jsonify({'success': False, 'message': 'Missing configuration'})
                
            ch_config = session['clickhouse_config']
            ff_config = session['flat_file_config']
            
            clickhouse_client = ClickHouseClient(
                host=ch_config.get('host'),
                port=ch_config.get('port'),
                database=ch_config.get('database'),
                user=ch_config.get('user'),
                jwt_token=ch_config.get('jwt_token'),
                secure=True
            )
            
            flat_file_client = FlatFileClient(ff_config.get('file_path'), ff_config.get('delimiter'))
            
            result = integrator.flat_file_to_clickhouse(
                flat_file_client,
                clickhouse_client,
                target_table,
                selected_columns
            )
            
            return jsonify(result)
            
        else:
            return jsonify({'success': False, 'message': 'Invalid source/target combination'})
    except Exception as e:
        logger.error(f"Error during ingestion: {str(e)}")
        logger.exception("Full exception details for ingestion:")
        # Return a more user-friendly message with technical details
        return jsonify({
            'success': False, 
            'message': f"Error during data ingestion: {str(e)}. Check logs for details.",
            'records_processed': 0
        })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
