import logging
import clickhouse_connect
from typing import Dict, List, Optional, Any, Tuple

logger = logging.getLogger(__name__)

class ClickHouseClient:
    """Client for interacting with ClickHouse database"""
    
    def __init__(self, host: str, port: str, database: str, user: str, jwt_token: str, secure: bool = True):
        """
        Initialize ClickHouse client
        
        Args:
            host: ClickHouse server host
            port: ClickHouse server port
            database: Database name
            user: Username
            jwt_token: JWT token for authentication
            secure: Whether to use HTTPS connection
        """
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.jwt_token = jwt_token
        self.secure = secure
        self.client = None
        
    def connect(self) -> None:
        """Establish connection to ClickHouse"""
        try:
            self.client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.jwt_token,  # JWT token used as password
                secure=self.secure
            )
        except Exception as e:
            logger.error(f"Error connecting to ClickHouse: {str(e)}")
            raise
            
    def test_connection(self) -> Dict[str, Any]:
        """
        Test connection to ClickHouse
        
        Returns:
            Dict with connection status and message
        """
        try:
            self.connect()
            result = self.client.query("SELECT 1")
            if result and result.result_set and result.result_set[0][0] == 1:
                return {'success': True, 'message': 'Connection successful'}
            else:
                return {'success': False, 'message': 'Connection failed: Unexpected response'}
        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            return {'success': False, 'message': f'Connection failed: {str(e)}'}
            
    def get_tables(self) -> List[str]:
        """
        Get list of tables in the connected database
        
        Returns:
            List of table names
        """
        try:
            if not self.client:
                self.connect()
                
            query = f"SHOW TABLES FROM {self.database}"
            result = self.client.query(query)
            
            tables = [row[0] for row in result.result_set]
            return tables
        except Exception as e:
            logger.error(f"Error getting tables: {str(e)}")
            raise
            
    def get_table_columns(self, table_name: str) -> List[Dict[str, str]]:
        """
        Get columns for a specified table
        
        Args:
            table_name: The name of the table
            
        Returns:
            List of column information (name and type)
        """
        try:
            if not self.client:
                self.connect()
                
            query = f"DESCRIBE TABLE {self.database}.{table_name}"
            result = self.client.query(query)
            
            columns = []
            for row in result.result_set:
                columns.append({
                    'name': row[0],
                    'type': row[1]
                })
            return columns
        except Exception as e:
            logger.error(f"Error getting table columns: {str(e)}")
            raise
            
    def get_preview_data(self, table_name: str, selected_columns: List[str], join_config: Optional[Dict] = None) -> List[Dict]:
        """
        Get preview data from a table with selected columns
        
        Args:
            table_name: The name of the table
            selected_columns: List of column names to include
            join_config: Configuration for table joins (optional)
            
        Returns:
            Preview data as list of dictionaries
        """
        try:
            if not self.client:
                self.connect()
                
            # Log the selected columns for debugging    
            logger.debug(f"Selected columns for preview: {selected_columns}")
            
            # Prepare columns for query
            if not selected_columns:
                columns_str = "*"
            else:
                # Ensure all column names are properly quoted to avoid SQL errors
                quoted_columns = []
                for col in selected_columns:
                    # If column contains special characters, quote it
                    if any(c in col for c in [' ', '-', '(', ')', '.', ',']):
                        quoted_columns.append(f"`{col}`")
                    else:
                        quoted_columns.append(col)
                columns_str = ", ".join(quoted_columns)
                
            logger.debug(f"Column string for query: {columns_str}")
                
            if join_config:
                # Handle multi-table join
                main_table = table_name
                join_tables = join_config.get('tables', [])
                join_conditions = join_config.get('conditions', [])
                
                query_parts = [f"SELECT {columns_str} FROM {self.database}.{main_table}"]
                
                for i, join_table in enumerate(join_tables):
                    condition = join_conditions[i] if i < len(join_conditions) else ""
                    query_parts.append(f"JOIN {self.database}.{join_table} ON {condition}")
                    
                query = " ".join(query_parts)
                query += " LIMIT 100"
            else:
                # Simple single table query
                query = f"SELECT {columns_str} FROM {self.database}.{table_name} LIMIT 100"
            
            logger.debug(f"Preview query: {query}")
            result = self.client.query(query)
            
            # Convert result to list of dictionaries
            preview_data = []
            column_names = [col[0] for col in result.column_names]
            
            logger.debug(f"Result column names: {column_names}")
            
            for row in result.result_set:
                row_dict = {}
                for i, value in enumerate(row):
                    if i < len(column_names):  # Ensure index is valid
                        row_dict[column_names[i]] = value
                preview_data.append(row_dict)
            
            # Make sure we're returning data with all selected columns    
            if preview_data and len(preview_data) > 0:
                logger.debug(f"Preview data sample: {preview_data[0]}")
                logger.debug(f"Preview data columns: {list(preview_data[0].keys())}")
                
            return preview_data
        except Exception as e:
            logger.error(f"Error getting preview data: {str(e)}")
            logger.exception("Full exception details:")
            raise
            
    def execute_query(self, query: str) -> Tuple[List, List]:
        """
        Execute a SQL query
        
        Args:
            query: SQL query to execute
            
        Returns:
            Tuple of (column_names, result_rows)
        """
        try:
            if not self.client:
                self.connect()
            
            logger.debug(f"Executing query: {query}")
            result = self.client.query(query)
            
            column_names = result.column_names
            logger.debug(f"Query result columns: {column_names}")
            logger.debug(f"Query result rows count: {len(result.result_set)}")
            
            return column_names, result.result_set
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            logger.exception("Full exception details for query execution:")
            raise
            
    def create_table_from_schema(self, table_name: str, columns: List[Dict[str, str]]) -> None:
        """
        Create a new table with the specified schema
        
        Args:
            table_name: Name of the table to create
            columns: List of column definitions (name and type)
        """
        try:
            if not self.client:
                self.connect()
                
            column_definitions = []
            for column in columns:
                column_definitions.append(f"{column['name']} {column['type']}")
                
            query = f"""
            CREATE TABLE IF NOT EXISTS {self.database}.{table_name} (
                {', '.join(column_definitions)}
            ) ENGINE = MergeTree() ORDER BY tuple()
            """
            
            self.client.command(query)
        except Exception as e:
            logger.error(f"Error creating table: {str(e)}")
            raise
            
    def insert_data(self, table_name: str, columns: List[str], data: List[List]) -> int:
        """
        Insert data into a table
        
        Args:
            table_name: Name of the target table
            columns: List of column names
            data: List of data rows
            
        Returns:
            Number of rows inserted
        """
        try:
            if not self.client:
                self.connect()
                
            if not data:
                logger.warning("No data provided for insertion")
                return 0
            
            # Log the insert operation details for debugging
            logger.debug(f"Inserting data into {self.database}.{table_name}")
            logger.debug(f"Columns: {columns}")
            logger.debug(f"Number of rows to insert: {len(data)}")
            logger.debug(f"Sample row data (first row): {data[0] if data else 'No data'}")
            
            # Ensure all columns exist in table schema
            try:
                table_columns = self.get_table_columns(table_name)
                table_column_names = [col['name'] for col in table_columns]
                missing_columns = [col for col in columns if col not in table_column_names]
                
                if missing_columns:
                    logger.warning(f"Missing columns in table schema: {missing_columns}")
                    # You could throw an error here or continue without those columns
            except Exception as schema_error:
                logger.warning(f"Could not verify schema: {str(schema_error)}")
                
            # Perform the insert operation
            result = self.client.insert(
                table=f"{self.database}.{table_name}",
                data=data,
                column_names=columns
            )
            
            # For clickhouse-connect, insert returns None on success
            logger.debug(f"Insert completed successfully, inserted {len(data)} rows")
            return len(data)
        except Exception as e:
            logger.error(f"Error inserting data: {str(e)}")
            logger.exception("Full exception details for insert operation:")
            raise Exception(f"Failed to insert data into ClickHouse: {str(e)}")
