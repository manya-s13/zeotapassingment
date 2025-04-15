import logging
from typing import Dict, List, Optional, Any
import pandas as pd
from utils.clickhouse_client import ClickHouseClient
from utils.flat_file_client import FlatFileClient

logger = logging.getLogger(__name__)

class DataIntegrator:
    """Class for handling data integration between ClickHouse and flat files"""
    
    def __init__(self, batch_size: int = 10000):
        """
        Initialize the data integrator
        
        Args:
            batch_size: Number of rows to process in a batch
        """
        self.batch_size = batch_size
        
    def clickhouse_to_flat_file(
        self, 
        clickhouse_client: ClickHouseClient, 
        flat_file_client: FlatFileClient,
        table_name: str,
        selected_columns: List[str],
        join_config: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Transfer data from ClickHouse to a flat file
        
        Args:
            clickhouse_client: ClickHouse client instance
            flat_file_client: Flat file client instance
            table_name: Source table name
            selected_columns: Columns to transfer
            join_config: Configuration for table joins (optional)
            
        Returns:
            Dictionary with status and record count
        """
        try:
            # Log the selected columns for debugging    
            logger.debug(f"Selected columns for data transfer: {selected_columns}")
            
            # Prepare query
            if not selected_columns:
                # Limit columns for memory safety
                logger.warning("No columns selected, using first 5 columns")
                columns_str = "*"
                limit_cols = True
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
                limit_cols = False
            
            logger.debug(f"Column string for query: {columns_str}")
                
            if join_config:
                # Handle multi-table join
                main_table = table_name
                join_tables = join_config.get('tables', [])
                join_conditions = join_config.get('conditions', [])
                
                query_parts = [f"SELECT {columns_str} FROM {clickhouse_client.database}.{main_table}"]
                
                for i, join_table in enumerate(join_tables):
                    condition = join_conditions[i] if i < len(join_conditions) else ""
                    query_parts.append(f"JOIN {clickhouse_client.database}.{join_table} ON {condition}")
                    
                query = " ".join(query_parts)
            else:
                # Simple single table query
                query = f"SELECT {columns_str} FROM {clickhouse_client.database}.{table_name}"
            
            # For very large tables, we need to use batches to avoid memory issues
            # Add a LIMIT clause to limit the amount of data we're working with
            max_rows = 10000  # Process maximum 10k rows at a time to prevent memory issues
            query_with_limit = f"{query} LIMIT {max_rows}"
            
            logger.debug(f"Data transfer query: {query_with_limit}")
                
            # Execute query and process results
            column_names, result_data = clickhouse_client.execute_query(query_with_limit)
            column_names = [col[0] for col in column_names]
            
            # If we got limited columns but have more columns selected, fix the mismatch
            if limit_cols and selected_columns and len(column_names) > len(selected_columns):
                # Take only the first columns from the result that match our selection count
                column_names = column_names[:len(selected_columns)]
                # For each row, take only the first fields that match our selection count
                result_data = [row[:len(selected_columns)] for row in result_data]
                
            logger.debug(f"Query returned {len(result_data)} rows with columns: {column_names}")
            
            # Write data to flat file
            rows_written = flat_file_client.write_data(column_names, result_data)
            
            logger.debug(f"Wrote {rows_written} rows to file: {flat_file_client.file_path}")
            
            return {
                'success': True,
                'message': 'Data transfer completed successfully',
                'records_processed': len(result_data)
            }
        except Exception as e:
            logger.error(f"Error transferring data from ClickHouse to flat file: {str(e)}")
            logger.exception("Full exception details for data transfer:")
            return {
                'success': False,
                'message': f'Error transferring data: {str(e)}',
                'records_processed': 0
            }
            
    def flat_file_to_clickhouse(
        self,
        flat_file_client: FlatFileClient,
        clickhouse_client: ClickHouseClient,
        target_table: str,
        selected_columns: List[str]
    ) -> Dict[str, Any]:
        """
        Transfer data from a flat file to ClickHouse
        
        Args:
            flat_file_client: Flat file client instance
            clickhouse_client: ClickHouse client instance
            target_table: Target table name
            selected_columns: Columns to transfer
            
        Returns:
            Dictionary with status and record count
        """
        try:
            # Log the selected columns for debugging
            logger.debug(f"Selected columns for flat file to ClickHouse transfer: {selected_columns}")
            logger.debug(f"Source file: {flat_file_client.file_path}")
            logger.debug(f"Target table: {target_table}")
            
            # Read data from flat file
            column_names, data_rows = flat_file_client.read_data(selected_columns)
            
            logger.debug(f"Read {len(data_rows)} rows from flat file")
            logger.debug(f"Column names from file: {column_names}")
            
            if not data_rows:
                logger.warning("No data rows found in file")
                return {
                    'success': True,
                    'message': 'No data to transfer',
                    'records_processed': 0
                }
                
            # Prepare schema for ClickHouse table
            columns_info = []
            for i, col in enumerate(column_names):
                # Try to infer type from first few values
                sample_values = [row[i] for row in data_rows[:100] if i < len(row)]
                
                # Default to String
                col_type = 'String'
                
                # Try to infer numeric types
                if all(self._is_integer(val) for val in sample_values if val):
                    col_type = 'Int64'
                elif all(self._is_float(val) for val in sample_values if val):
                    col_type = 'Float64'
                    
                columns_info.append({
                    'name': col,
                    'type': col_type
                })
            
            logger.debug(f"Inferred column types: {columns_info}")
                
            # Create table if it doesn't exist
            logger.debug(f"Creating or verifying table: {target_table}")
            clickhouse_client.create_table_from_schema(target_table, columns_info)
            
            # Insert data in batches
            total_inserted = 0
            batch_count = 0
            
            for i in range(0, len(data_rows), self.batch_size):
                batch = data_rows[i:i+self.batch_size]
                batch_count += 1
                logger.debug(f"Processing batch {batch_count}: {len(batch)} rows")
                
                inserted = clickhouse_client.insert_data(target_table, column_names, batch)
                total_inserted += inserted
                
                logger.debug(f"Inserted {inserted} rows, total so far: {total_inserted}")
                
            logger.debug(f"Transfer completed: {total_inserted} total rows inserted")
                
            return {
                'success': True,
                'message': 'Data transfer completed successfully',
                'records_processed': total_inserted
            }
        except Exception as e:
            logger.error(f"Error transferring data from flat file to ClickHouse: {str(e)}")
            logger.exception("Full exception details for flat file to ClickHouse transfer:")
            return {
                'success': False,
                'message': f'Error transferring data: {str(e)}',
                'records_processed': 0
            }
            
    def _is_integer(self, value: str) -> bool:
        """Check if a string value can be converted to an integer"""
        try:
            int(value)
            return True
        except (ValueError, TypeError):
            return False
            
    def _is_float(self, value: str) -> bool:
        """Check if a string value can be converted to a float"""
        try:
            float(value)
            return True
        except (ValueError, TypeError):
            return False
