document.addEventListener('DOMContentLoaded', function() {
    // Elements
    const sourceSelect = document.getElementById('source-select');
    const clickhouseForm = document.getElementById('clickhouse-form');
    const flatFileForm = document.getElementById('flat-file-form');
    const connectionForm = document.getElementById('connection-form');
    const tableSelector = document.getElementById('table-selector');
    const columnSelector = document.getElementById('column-selector');
    const previewSection = document.getElementById('preview-section');
    const targetSection = document.getElementById('target-section');
    const ingestionSection = document.getElementById('ingestion-section');
    const resultSection = document.getElementById('result-section');
    const progressBar = document.getElementById('progress-bar');
    const joinContainer = document.getElementById('join-container');
    const addJoinButton = document.getElementById('add-join-button');
    const stepItems = document.querySelectorAll('.step');

    // App state
    const state = {
        source: null,
        target: null,
        clickhouseConnected: false,
        tables: [],
        selectedTable: null,
        joinTables: [],
        joinConditions: [],
        columns: [],
        selectedColumns: [],
        flatFilePath: null,
        flatFileDelimiter: ',',
        targetTableName: null,
        targetFilePath: null,
        targetDelimiter: ',',
        previewData: null,
        totalRecords: 0,
        processingStatus: null,
        currentStep: 0
    };

    // Initialize UI
    initializeUI();

    // Initialize event listeners
    function initializeUI() {
        // Hide all sections initially except source selection
        clickhouseForm.style.display = 'none';
        flatFileForm.style.display = 'none';
        tableSelector.style.display = 'none';
        columnSelector.style.display = 'none';
        previewSection.style.display = 'none';
        targetSection.style.display = 'none';
        ingestionSection.style.display = 'none';
        resultSection.style.display = 'none';
        joinContainer.style.display = 'none';

        // Event listeners
        sourceSelect.addEventListener('change', handleSourceChange);
        
        // Connection buttons
        document.getElementById('test-clickhouse-connection').addEventListener('click', testClickHouseConnection);
        document.getElementById('upload-file-button').addEventListener('click', handleFileUpload);
        document.getElementById('use-existing-file-button').addEventListener('click', handleExistingFilePath);
        
        // Table selection
        document.getElementById('load-tables-button').addEventListener('click', loadTables);
        document.getElementById('select-table-button').addEventListener('click', selectTable);
        
        // Column selection
        document.getElementById('select-columns-button').addEventListener('click', selectColumns);
        document.getElementById('select-all-columns').addEventListener('click', toggleAllColumns);
        
        // Preview
        document.getElementById('preview-data-button').addEventListener('click', previewData);
        
        // Target configuration
        document.getElementById('configure-target-button').addEventListener('click', configureTarget);
        
        // Ingestion
        document.getElementById('start-ingestion-button').addEventListener('click', startIngestion);
        
        // Join functionality
        addJoinButton.addEventListener('click', addJoinCondition);
    }

    // Handle source selection change
    function handleSourceChange() {
        state.source = sourceSelect.value;
        
        // Reset UI
        clickhouseForm.style.display = 'none';
        flatFileForm.style.display = 'none';
        tableSelector.style.display = 'none';
        columnSelector.style.display = 'none';
        previewSection.style.display = 'none';
        targetSection.style.display = 'none';
        ingestionSection.style.display = 'none';
        resultSection.style.display = 'none';
        
        // Reset state
        state.clickhouseConnected = false;
        
        // Show appropriate form based on selection
        if (state.source === 'clickhouse') {
            clickhouseForm.style.display = 'block';
            state.target = 'flatfile';
        } else if (state.source === 'flatfile') {
            flatFileForm.style.display = 'block';
            state.target = 'clickhouse';
            
            // Show notice that ClickHouse connection will be needed for target
            showMessage('Please note: You will need to connect to ClickHouse for the target database after uploading your file.', 'info');
        }
        
        // Update step indicator
        state.currentStep = 1;
        updateStepIndicator();
        
        // Update target label
        updateTargetLabel();
    }
    
    // Update step indicator
    function updateStepIndicator() {
        // Remove active and completed classes from all steps
        stepItems.forEach((item, index) => {
            item.classList.remove('active', 'completed');
            if (index < state.currentStep) {
                item.classList.add('completed');
            } else if (index === state.currentStep) {
                item.classList.add('active');
            }
        });
    }

    // Test ClickHouse connection
    function testClickHouseConnection() {
        const host = document.getElementById('clickhouse-host').value;
        const port = document.getElementById('clickhouse-port').value;
        const database = document.getElementById('clickhouse-database').value;
        const user = document.getElementById('clickhouse-user').value;
        const jwtToken = document.getElementById('clickhouse-jwt').value;

        if (!host || !port || !database || !user || !jwtToken) {
            showMessage('Please fill in all ClickHouse connection fields', 'error');
            return;
        }

        // Show loading state
        const button = document.getElementById('test-clickhouse-connection');
        const originalText = button.innerHTML;
        button.innerHTML = '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> Testing...';
        button.disabled = true;

        // Send request to test connection
        fetch('/test-clickhouse-connection', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                host: host,
                port: port,
                database: database,
                user: user,
                jwt_token: jwtToken
            }),
        })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                showMessage('Connection successful!', 'success');
                state.clickhouseConnected = true;
                
                // If source is ClickHouse, show table selector
                if (state.source === 'clickhouse') {
                    tableSelector.style.display = 'block';
                } else {
                    // If target is ClickHouse, we can proceed to column selection
                    columnSelector.style.display = 'block';
                }
            } else {
                showMessage(`Connection failed: ${data.message}`, 'error');
            }
        })
        .catch(error => {
            showMessage(`Error: ${error.message}`, 'error');
        })
        .finally(() => {
            // Restore button
            button.innerHTML = originalText;
            button.disabled = false;
        });
    }

    // Handle flat file upload
    // Handle existing file path input
    function handleExistingFilePath() {
        const filePathInput = document.getElementById('existing-file-path');
        const delimiterInput = document.getElementById('flat-file-delimiter');
        
        if (!filePathInput.value) {
            showMessage('Please enter a file path', 'error');
            return;
        }
        
        const filePath = filePathInput.value;
        const delimiter = delimiterInput.value || ',';
        
        // Show loading state
        const button = document.getElementById('use-existing-file-button');
        const originalText = button.innerHTML;
        button.innerHTML = '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> Loading...';
        button.disabled = true;
        
        // Send file path to server
        fetch('/get-file-by-path', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                file_path: filePath,
                delimiter: delimiter
            }),
        })
        .then(response => {
            // Check if response is JSON
            const contentType = response.headers.get('content-type');
            if (contentType && contentType.includes('application/json')) {
                return response.json();
            } else {
                throw new Error('Server returned non-JSON response. Please check server logs.');
            }
        })
        .then(data => {
            if (data.success) {
                // Update state
                state.flatFilePath = filePath;
                state.flatFileDelimiter = delimiter;
                state.columns = data.columns;
                
                // Display columns
                displayColumns(data.columns);
                
                // Update UI
                columnSelector.style.display = 'block';
                
                // Update step indicator
                state.currentStep = 3;
                updateStepIndicator();
                
                showMessage('File analyzed successfully!', 'success');
            } else {
                showMessage(`Error: ${data.message}`, 'error');
            }
        })
        .catch(error => {
            showMessage(`Error: ${error.message}`, 'error');
        })
        .finally(() => {
            // Restore button
            button.innerHTML = originalText;
            button.disabled = false;
        });
    }

    function handleFileUpload() {
        const fileInput = document.getElementById('flat-file-input');
        const delimiterInput = document.getElementById('flat-file-delimiter');
        
        if (!fileInput.files || fileInput.files.length === 0) {
            showMessage('Please select a file', 'error');
            return;
        }
        
        const file = fileInput.files[0];
        const delimiter = delimiterInput.value || ',';
        
        // Create FormData
        const formData = new FormData();
        formData.append('file', file);
        formData.append('delimiter', delimiter);
        
        // Show loading state
        const button = document.getElementById('upload-file-button');
        const originalText = button.innerHTML;
        button.innerHTML = '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> Uploading...';
        button.disabled = true;
        
        // Send file to server
        fetch('/get-file-columns', {
            method: 'POST',
            body: formData
        })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                state.columns = data.columns;
                state.flatFilePath = file.name;
                state.flatFileDelimiter = delimiter;
                state.totalRecords = data.totalRows;
                state.previewData = data.previewData;
                
                // Display columns for selection
                displayColumns(data.columns);
                
                // Show column selector
                columnSelector.style.display = 'block';
                
                showMessage('File uploaded successfully!', 'success');
            } else {
                showMessage(`File upload failed: ${data.message}`, 'error');
            }
        })
        .catch(error => {
            showMessage(`Error: ${error.message}`, 'error');
        })
        .finally(() => {
            // Restore button
            button.innerHTML = originalText;
            button.disabled = false;
        });
    }

    // Load ClickHouse tables
    function loadTables() {
        // Show loading state
        const button = document.getElementById('load-tables-button');
        const originalText = button.innerHTML;
        button.innerHTML = '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> Loading...';
        button.disabled = true;
        
        // Fetch tables from ClickHouse
        fetch('/get-clickhouse-tables')
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    state.tables = data.tables;
                    
                    // Populate table dropdown
                    const tableSelect = document.getElementById('table-select');
                    tableSelect.innerHTML = '';
                    
                    data.tables.forEach(table => {
                        const option = document.createElement('option');
                        option.value = table;
                        option.textContent = table;
                        tableSelect.appendChild(option);
                    });
                    
                    // Enable join functionality if there are multiple tables
                    if (data.tables.length > 1) {
                        joinContainer.style.display = 'block';
                    }
                    
                    showMessage('Tables loaded successfully!', 'success');
                } else {
                    showMessage(`Failed to load tables: ${data.message}`, 'error');
                }
            })
            .catch(error => {
                showMessage(`Error: ${error.message}`, 'error');
            })
            .finally(() => {
                // Restore button
                button.innerHTML = originalText;
                button.disabled = false;
            });
    }

    // Select a ClickHouse table
    function selectTable() {
        const tableSelect = document.getElementById('table-select');
        state.selectedTable = tableSelect.value;
        
        if (!state.selectedTable) {
            showMessage('Please select a table', 'error');
            return;
        }
        
        // Show loading state
        const button = document.getElementById('select-table-button');
        const originalText = button.innerHTML;
        button.innerHTML = '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> Loading columns...';
        button.disabled = true;
        
        // Fetch columns for the selected table
        fetch('/get-table-columns', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                table_name: state.selectedTable
            }),
        })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                state.columns = data.columns;
                
                // Display columns for selection
                displayColumns(data.columns);
                
                // Show column selector
                columnSelector.style.display = 'block';
                
                // Update step indicator
                state.currentStep = 2;
                updateStepIndicator();
                
                showMessage('Columns loaded successfully!', 'success');
            } else {
                showMessage(`Failed to load columns: ${data.message}`, 'error');
            }
        })
        .catch(error => {
            showMessage(`Error: ${error.message}`, 'error');
        })
        .finally(() => {
            // Restore button
            button.innerHTML = originalText;
            button.disabled = false;
        });
    }

    // Display columns for selection
    function displayColumns(columns) {
        const columnList = document.getElementById('column-list');
        columnList.innerHTML = '';
        
        columns.forEach(column => {
            const columnItem = document.createElement('div');
            columnItem.className = 'form-check';
            
            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.className = 'form-check-input column-checkbox';
            checkbox.id = `column-${column.name}`;
            checkbox.value = column.name;
            
            const label = document.createElement('label');
            label.className = 'form-check-label';
            label.htmlFor = `column-${column.name}`;
            label.textContent = `${column.name} (${column.type})`;
            
            columnItem.appendChild(checkbox);
            columnItem.appendChild(label);
            columnList.appendChild(columnItem);
        });
    }

    // Toggle all columns selection
    function toggleAllColumns() {
        const selectAllCheckbox = document.getElementById('select-all-columns');
        const columnCheckboxes = document.querySelectorAll('.column-checkbox');
        
        columnCheckboxes.forEach(checkbox => {
            checkbox.checked = selectAllCheckbox.checked;
        });
    }

    // Select columns for transfer
    function selectColumns() {
        const columnCheckboxes = document.querySelectorAll('.column-checkbox:checked');
        state.selectedColumns = Array.from(columnCheckboxes).map(checkbox => checkbox.value);
        
        if (state.selectedColumns.length === 0) {
            showMessage('Please select at least one column', 'error');
            return;
        }
        
        // Show preview section
        previewSection.style.display = 'block';
        
        // Update step indicator
        state.currentStep = 3;
        updateStepIndicator();
        
        showMessage(`${state.selectedColumns.length} columns selected`, 'success');
    }

    // Preview data
    function previewData() {
        // Show loading state
        const button = document.getElementById('preview-data-button');
        const originalText = button.innerHTML;
        button.innerHTML = '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> Loading...';
        button.disabled = true;
        
        // Prepare join configuration if applicable
        let joinConfig = null;
        if (state.source === 'clickhouse' && state.joinTables.length > 0) {
            joinConfig = {
                tables: state.joinTables,
                conditions: state.joinConditions
            };
        }
        
        // Fetch preview data
        fetch('/preview-data', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                source: state.source,
                table_name: state.selectedTable,
                selected_columns: state.selectedColumns,
                join_config: joinConfig
            }),
        })
        .then(response => {
            // Check if response is JSON
            const contentType = response.headers.get('content-type');
            if (contentType && contentType.includes('application/json')) {
                return response.json();
            } else {
                throw new Error('Server returned non-JSON response. Please check server logs.');
            }
        })
        .then(data => {
            if (data.success) {
                state.previewData = data.previewData;
                displayPreviewData(data.previewData);
                
                // Show target configuration section
                targetSection.style.display = 'block';
                
                // Update step indicator
                state.currentStep = 4;
                updateStepIndicator();
                
                showMessage('Preview data loaded successfully!', 'success');
            } else {
                showMessage(`Failed to load preview data: ${data.message}`, 'error');
            }
        })
        .catch(error => {
            showMessage(`Error: ${error.message}`, 'error');
        })
        .finally(() => {
            // Restore button
            button.innerHTML = originalText;
            button.disabled = false;
        });
    }

    // Display preview data
    function displayPreviewData(data) {
        const previewContainer = document.getElementById('preview-data');
        previewContainer.innerHTML = '';
        
        if (!data || data.length === 0) {
            previewContainer.innerHTML = '<div class="alert alert-info">No data available for preview</div>';
            return;
        }
        
        // Create table for preview
        const table = document.createElement('table');
        table.className = 'table table-striped table-sm data-table';
        
        // Create header
        const thead = document.createElement('thead');
        const headerRow = document.createElement('tr');
        
        Object.keys(data[0]).forEach(key => {
            const th = document.createElement('th');
            th.textContent = key;
            headerRow.appendChild(th);
        });
        
        thead.appendChild(headerRow);
        table.appendChild(thead);
        
        // Create body
        const tbody = document.createElement('tbody');
        
        data.forEach(row => {
            const tr = document.createElement('tr');
            
            Object.values(row).forEach(value => {
                const td = document.createElement('td');
                td.textContent = value !== null ? value.toString() : 'null';
                tr.appendChild(td);
            });
            
            tbody.appendChild(tr);
        });
        
        table.appendChild(tbody);
        previewContainer.appendChild(table);
        
        // Update preview count
        document.getElementById('preview-count').textContent = data.length;
    }

    // Configure target
    function configureTarget() {
        if (state.target === 'flatfile') {
            state.targetFilePath = document.getElementById('target-file-path').value;
            state.targetDelimiter = document.getElementById('target-delimiter').value || ',';
            
            if (!state.targetFilePath) {
                showMessage('Please specify a target file path', 'error');
                return;
            }
        } else if (state.target === 'clickhouse') {
            state.targetTableName = document.getElementById('target-table-name').value;
            
            if (!state.targetTableName) {
                showMessage('Please specify a target table name', 'error');
                return;
            }
            
            if (!state.clickhouseConnected) {
                // If ClickHouse is not connected yet, show the connection form
                showMessage('Please connect to ClickHouse first using the form below', 'warning');
                
                // Show ClickHouse connection form if it's not already visible
                if (clickhouseForm.style.display !== 'block') {
                    clickhouseForm.style.display = 'block';
                }
                
                return;
            }
        }
        
        // Show ingestion section
        ingestionSection.style.display = 'block';
        updateIngestionSummary();
        
        // Update step indicator
        state.currentStep = 5;
        updateStepIndicator();
        
        showMessage('Target configured successfully!', 'success');
    }

    // Update ingestion summary
    function updateIngestionSummary() {
        const summarySource = document.getElementById('summary-source');
        const summaryColumns = document.getElementById('summary-columns');
        const summaryTarget = document.getElementById('summary-target');
        
        // Update source info
        if (state.source === 'clickhouse') {
            summarySource.textContent = `ClickHouse table: ${state.selectedTable}`;
            if (state.joinTables.length > 0) {
                summarySource.textContent += ` (with ${state.joinTables.length} joined tables)`;
            }
        } else {
            summarySource.textContent = `Flat file: ${state.flatFilePath}`;
        }
        
        // Update columns info
        summaryColumns.textContent = `${state.selectedColumns.length} columns selected`;
        
        // Update target info
        if (state.target === 'clickhouse') {
            summaryTarget.textContent = `ClickHouse table: ${state.targetTableName}`;
        } else {
            summaryTarget.textContent = `Flat file: ${state.targetFilePath}`;
        }
    }

    // Start ingestion process
    function startIngestion() {
        // Show loading state
        const button = document.getElementById('start-ingestion-button');
        const originalText = button.innerHTML;
        button.innerHTML = '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> Processing...';
        button.disabled = true;
        
        // Show progress bar
        progressBar.style.width = '0%';
        progressBar.classList.add('active');
        
        // Update step indicator
        state.currentStep = 5;
        updateStepIndicator();
        
        // Simulate progress updates
        let progress = 0;
        const progressInterval = setInterval(() => {
            progress += 5;
            if (progress <= 90) {
                progressBar.style.width = `${progress}%`;
            }
        }, 300);
        
        // Prepare join configuration if applicable
        let joinConfig = null;
        if (state.source === 'clickhouse' && state.joinTables.length > 0) {
            joinConfig = {
                tables: state.joinTables,
                conditions: state.joinConditions
            };
        }
        
        // Prepare request data
        const requestData = {
            source: state.source,
            target: state.target,
            selected_columns: state.selectedColumns
        };
        
        // Add source-specific info
        if (state.source === 'clickhouse') {
            requestData.table_name = state.selectedTable;
            requestData.join_config = joinConfig;
        }
        
        // Add target-specific info
        if (state.target === 'clickhouse') {
            requestData.target_table = state.targetTableName;
        } else {
            requestData.target_file_path = state.targetFilePath;
            requestData.target_delimiter = state.targetDelimiter;
        }
        
        // Start ingestion
        fetch('/start-ingestion', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(requestData),
        })
        .then(response => {
            // Check if response is JSON
            const contentType = response.headers.get('content-type');
            if (contentType && contentType.includes('application/json')) {
                return response.json();
            } else {
                throw new Error('Server returned non-JSON response. Please check server logs.');
            }
        })
        .then(data => {
            // Clear progress interval
            clearInterval(progressInterval);
            
            // Update progress to 100%
            progressBar.style.width = '100%';
            progressBar.classList.remove('active');
            
            // Display results
            resultSection.style.display = 'block';
            
            // Update step indicator to final step
            state.currentStep = 5;
            updateStepIndicator();
            
            if (data.success) {
                document.getElementById('result-status').textContent = 'Success';
                document.getElementById('result-status').className = 'text-success';
                document.getElementById('result-message').textContent = data.message;
                document.getElementById('records-processed').textContent = data.records_processed;
                
                showMessage(`Ingestion completed successfully! ${data.records_processed} records processed.`, 'success');
            } else {
                document.getElementById('result-status').textContent = 'Failed';
                document.getElementById('result-status').className = 'text-danger';
                document.getElementById('result-message').textContent = data.message;
                document.getElementById('records-processed').textContent = data.records_processed || 0;
                
                showMessage(`Ingestion failed: ${data.message}`, 'error');
            }
        })
        .catch(error => {
            // Clear progress interval
            clearInterval(progressInterval);
            
            // Update progress indicator
            progressBar.style.width = '100%';
            progressBar.classList.remove('active');
            progressBar.className = 'progress-bar bg-danger';
            
            // Display error
            resultSection.style.display = 'block';
            document.getElementById('result-status').textContent = 'Error';
            document.getElementById('result-status').className = 'text-danger';
            document.getElementById('result-message').textContent = error.message;
            document.getElementById('records-processed').textContent = 0;
            
            showMessage(`Error: ${error.message}`, 'error');
        })
        .finally(() => {
            // Restore button
            button.innerHTML = originalText;
            button.disabled = false;
        });
    }

    // Add join condition
    function addJoinCondition() {
        const joinTableSelect = document.getElementById('join-table-select');
        const joinTableName = joinTableSelect.value;
        
        if (!joinTableName) {
            showMessage('Please select a table to join', 'error');
            return;
        }
        
        const joinConditionInput = document.getElementById('join-condition-input');
        const joinCondition = joinConditionInput.value;
        
        if (!joinCondition) {
            showMessage('Please specify a join condition', 'error');
            return;
        }
        
        // Add to state
        state.joinTables.push(joinTableName);
        state.joinConditions.push(joinCondition);
        
        // Display added join
        displayJoinConditions();
        
        // Clear inputs
        joinTableSelect.selectedIndex = 0;
        joinConditionInput.value = '';
        
        showMessage('Join condition added!', 'success');
    }

    // Display join conditions
    function displayJoinConditions() {
        const joinList = document.getElementById('join-list');
        joinList.innerHTML = '';
        
        if (state.joinTables.length === 0) {
            joinList.innerHTML = '<div class="text-muted">No joins configured</div>';
            return;
        }
        
        state.joinTables.forEach((table, index) => {
            const joinItem = document.createElement('div');
            joinItem.className = 'join-condition mb-2';
            
            const joinText = document.createElement('div');
            joinText.innerHTML = `<strong>JOIN ${table}</strong> ON ${state.joinConditions[index]}`;
            
            const removeButton = document.createElement('button');
            removeButton.className = 'btn btn-sm btn-danger mt-2';
            removeButton.textContent = 'Remove';
            removeButton.addEventListener('click', () => removeJoinCondition(index));
            
            joinItem.appendChild(joinText);
            joinItem.appendChild(removeButton);
            joinList.appendChild(joinItem);
        });
    }

    // Remove join condition
    function removeJoinCondition(index) {
        state.joinTables.splice(index, 1);
        state.joinConditions.splice(index, 1);
        
        displayJoinConditions();
        showMessage('Join condition removed', 'success');
    }

    // Update target label based on source
    function updateTargetLabel() {
        const targetLabel = document.getElementById('target-label');
        
        if (state.target === 'clickhouse') {
            targetLabel.textContent = 'ClickHouse Target Configuration';
            document.getElementById('clickhouse-target-config').style.display = 'block';
            document.getElementById('flatfile-target-config').style.display = 'none';
        } else {
            targetLabel.textContent = 'Flat File Target Configuration';
            document.getElementById('clickhouse-target-config').style.display = 'none';
            document.getElementById('flatfile-target-config').style.display = 'block';
        }
    }

    // Show message to user
    function showMessage(message, type) {
        const alertDiv = document.createElement('div');
        let alertClass = 'success';
        
        if (type === 'error') alertClass = 'danger';
        else if (type === 'warning') alertClass = 'warning';
        else if (type === 'info') alertClass = 'info';
        
        alertDiv.className = `alert alert-${alertClass} alert-dismissible fade show`;
        alertDiv.role = 'alert';
        
        alertDiv.innerHTML = `
            ${message}
            <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
        `;
        
        const messageContainer = document.getElementById('message-container');
        messageContainer.appendChild(alertDiv);
        
        // Auto-dismiss after 5 seconds
        setTimeout(() => {
            alertDiv.classList.remove('show');
            setTimeout(() => {
                alertDiv.remove();
            }, 150);
        }, 5000);
    }
});
