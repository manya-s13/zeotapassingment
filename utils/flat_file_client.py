import csv
import logging
import os
from typing import List, Dict, Optional, Any, Tuple

logger = logging.getLogger(__name__)

class FlatFileClient:
    """Client for interacting with flat files (CSV, TSV, etc.)"""
    
    def __init__(self, file_path: str, delimiter: str = ','):
        """
        Initialize Flat File client
        
        Args:
            file_path: Path to the flat file
            delimiter: Field delimiter character
        """
        self.file_path = file_path
        self.delimiter = delimiter
        
        # Log initialization
        logger.debug(f"Initialized FlatFileClient with path: {self.file_path}, delimiter: '{self.delimiter}'")
        
        # Create directory if it doesn't exist
        directory = os.path.dirname(self.file_path)
        if directory and not os.path.exists(directory):
            logger.debug(f"Creating directory: {directory}")
            os.makedirs(directory, exist_ok=True)
        
    def get_columns(self) -> List[Dict[str, str]]:
        """
        Get column names from the flat file
        
        Returns:
            List of column information (name and inferred type)
        """
        try:
            with open(self.file_path, 'r', newline='') as file:
                reader = csv.reader(file, delimiter=self.delimiter)
                header = next(reader)
                
                # Try to infer types from first data row
                types = ['String'] * len(header)  # Default type
                try:
                    first_row = next(reader)
                    for i, value in enumerate(first_row):
                        try:
                            int(value)
                            types[i] = 'Int64'
                        except ValueError:
                            try:
                                float(value)
                                types[i] = 'Float64'
                            except ValueError:
                                types[i] = 'String'
                except StopIteration:
                    pass  # No data rows, just use default String type
                
                columns = []
                for i, col in enumerate(header):
                    columns.append({
                        'name': col,
                        'type': types[i]
                    })
                    
                return columns
        except Exception as e:
            logger.error(f"Error getting columns from file: {str(e)}")
            raise
            
    def get_preview(self, num_rows: int = 100, selected_columns: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Get preview data from the flat file
        
        Args:
            num_rows: Number of rows to preview
            selected_columns: Optional list of columns to include (if None, all columns are included)
            
        Returns:
            Preview data as a list of dictionaries
        """
        try:
            preview_data = []
            with open(self.file_path, 'r', newline='') as file:
                reader = csv.DictReader(file, delimiter=self.delimiter)
                
                for i, row in enumerate(reader):
                    if i >= num_rows:
                        break
                        
                    if selected_columns:
                        filtered_row = {k: v for k, v in row.items() if k in selected_columns}
                        preview_data.append(filtered_row)
                    else:
                        preview_data.append(row)
                        
            return preview_data
        except Exception as e:
            logger.error(f"Error getting preview from file: {str(e)}")
            raise
            
    def get_total_rows(self) -> int:
        """
        Get total number of rows in the file
        
        Returns:
            Total row count
        """
        try:
            with open(self.file_path, 'r', newline='') as file:
                reader = csv.reader(file, delimiter=self.delimiter)
                next(reader)  # Skip header
                count = sum(1 for _ in reader)
            return count
        except Exception as e:
            logger.error(f"Error counting rows in file: {str(e)}")
            raise
            
    def read_data(self, selected_columns: Optional[List[str]] = None) -> Tuple[List[str], List[List]]:
        """
        Read data from the flat file
        
        Args:
            selected_columns: Optional list of columns to read
            
        Returns:
            Tuple of (column_names, data_rows)
        """
        try:
            logger.debug(f"Reading data from file: {self.file_path}")
            logger.debug(f"Selected columns: {selected_columns}")
            
            with open(self.file_path, 'r', newline='') as file:
                reader = csv.reader(file, delimiter=self.delimiter)
                header = next(reader)
                
                logger.debug(f"File header: {header}")
                
                # If selected columns are specified, get their indices
                if selected_columns:
                    # Check for missing columns
                    missing_columns = [col for col in selected_columns if col not in header]
                    if missing_columns:
                        logger.warning(f"Some selected columns not found in file: {missing_columns}")
                    
                    # Get indices for columns that exist in the file
                    column_indices = []
                    filtered_header = []
                    for col in selected_columns:
                        if col in header:
                            idx = header.index(col)
                            column_indices.append(idx)
                            filtered_header.append(col)
                else:
                    column_indices = list(range(len(header)))
                    filtered_header = header.copy()
                
                logger.debug(f"Using columns: {filtered_header}")
                logger.debug(f"Column indices: {column_indices}")
                
                # Read data rows
                data_rows = []
                row_count = 0
                
                for row in reader:
                    row_count += 1
                    # Handle rows that might be shorter than expected
                    if len(row) < max(column_indices) + 1:
                        # Pad the row with empty strings
                        row = row + [''] * (max(column_indices) + 1 - len(row))
                        
                    filtered_row = [row[i] for i in column_indices]
                    data_rows.append(filtered_row)
                
                logger.debug(f"Read {row_count} rows from file")
                    
                return filtered_header, data_rows
        except FileNotFoundError as e:
            logger.error(f"File not found: {self.file_path}")
            raise
        except Exception as e:
            logger.error(f"Error reading data from file: {str(e)}")
            logger.exception("Full exception details for file reading:")
            raise
            
    def write_data(self, column_names: List[str], data: List[List]) -> int:
        """
        Write data to a flat file
        
        Args:
            column_names: List of column names
            data: List of data rows
            
        Returns:
            Number of rows written
        """
        try:
            # Make sure directory exists
            directory = os.path.dirname(self.file_path)
            if directory and not os.path.exists(directory):
                os.makedirs(directory, exist_ok=True)
            
            logger.debug(f"Writing {len(data)} rows with {len(column_names)} columns to file: {self.file_path}")
            logger.debug(f"Columns: {column_names}")
            
            if not data:
                logger.warning("No data to write to file")
                # Create an empty file with just the header
                with open(self.file_path, 'w', newline='') as file:
                    writer = csv.writer(file, delimiter=self.delimiter)
                    writer.writerow(column_names)
                return 0
                
            # Convert all values to strings to avoid write errors
            processed_data = []
            for row in data:
                processed_row = []
                for value in row:
                    if value is None:
                        processed_row.append('')
                    else:
                        processed_row.append(str(value))
                processed_data.append(processed_row)
                
            with open(self.file_path, 'w', newline='') as file:
                writer = csv.writer(file, delimiter=self.delimiter)
                writer.writerow(column_names)
                writer.writerows(processed_data)
            
            # Verify the file was written correctly
            if os.path.exists(self.file_path):
                file_size = os.path.getsize(self.file_path)
                logger.debug(f"Successfully wrote file. Size: {file_size} bytes")
                
            return len(data)
        except Exception as e:
            logger.error(f"Error writing data to file: {str(e)}")
            logger.exception("Full exception details for file writing:")
            raise
