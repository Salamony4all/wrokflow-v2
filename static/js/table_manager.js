// Table Stitching and Management Functions

async function stitchTables(fileId) {
    const stitchResult = document.getElementById(`stitch-result-${fileId}`);
    
    if (!stitchResult) {
        console.error('Stitch result element not found for file:', fileId);
        showAlert('Error: Could not find result container. Please refresh the page and try again.', 'error');
        return;
    }

    // Show loading
    stitchResult.style.display = 'block';
    stitchResult.innerHTML = '<div class="loading"></div> Stitching tables from all pages...';

    try {
        const response = await fetch(`/stitch-tables/${fileId}`, {
            method: 'POST'
        });

        if (!response.ok) {
            const errorData = await response.json().catch(() => ({ error: `HTTP ${response.status}` }));
            let errorMessage = errorData.error || errorData.details || `HTTP ${response.status}`;
            let recoverySteps = '';

            if (response.status === 404) {
                const errorData = await response.json().catch(() => ({}));
                if (errorData.available_files && errorData.available_files.length > 0) {
                    const filesList = errorData.available_files.map(f => `<li>${f.name || f.id}</li>`).join('');
                    errorMessage = `File not found in session. Available files: <ul>${filesList}</ul>`;
                    recoverySteps = `<p><strong>🔧 How to fix:</strong> Click on one of the available files above to extract it again, or re-upload your file.</p>`;
                } else {
                    errorMessage = `File not found. The file may not exist in the session. This can happen if:<br>
                    • The session expired (files are kept for 2 hours)<br>
                    • The page was refreshed before extraction completed<br>
                    • The browser was closed and reopened`;
                    recoverySteps = `<p><strong>🔧 How to fix:</strong><br>
                    1. Re-upload your file using the upload button above<br>
                    2. Wait for extraction to complete<br>
                    3. Then click "Stitch All Tables" again</p>`;
                }
            } else if (response.status === 400) {
                errorMessage = errorData.details || errorData.error || 'No tables found to stitch. The extraction may not have found any tables in the document.';
                if (errorData.available_keys) {
                    errorMessage += ` Available keys: ${errorData.available_keys.join(', ')}`;
                }
                recoverySteps = `<p><strong>🔧 How to fix:</strong> Make sure the file contains tables and try extracting it again.</p>`;
            }

            stitchResult.innerHTML = `<div style="background: #ffebee; padding: 20px; border-radius: 6px; border-left: 4px solid #f44336; color: #c62828;">
                <strong>❌ Error stitching tables:</strong><br>
                ${errorMessage}
                ${recoverySteps}
            </div>`;
            console.error('Stitch error:', errorData);
            return;
        }

        const result = await response.json();

        if (result.success) {
            showAlert('Tables stitched successfully! You can now edit cells, drag images, and manage rows.', 'success');

            // Clear any previous error messages
            stitchResult.innerHTML = '';

            // Display stitched table
            let stitchedHtml = `
                <div style="background: #e8f5e9; padding: 20px; border-radius: 8px; margin: 20px 0; border-left: 4px solid #4caf50;">
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px;">
                        <div>
                            <h4 style="color: #2e7d32; margin: 0 0 5px 0;">✅ Stitched Table (Fully Editable)</h4>
                            <p style="margin: 0; color: #555; font-size: 0.9em;">
                                <strong>Total Rows:</strong> ${result.row_count} | <strong>Pages:</strong> ${result.page_count}
                                | ✏️ Click cells to edit | 🖼️ Drag images | ➕ Add rows | 🗑️ Delete rows
                            </p>
                        </div>
                    </div>
                    <div id="editable-table-${fileId}" style="background: white; padding: 15px; border-radius: 4px; overflow-x: auto; box-shadow: 0 2px 8px rgba(0,0,0,0.1); position: relative;">
            `;

            // Parse and style the stitched table
            let tempDiv = document.createElement('div');
            tempDiv.innerHTML = result.stitched_html;

            // DEBUG: Check if images are in the HTML
            const allImagesBefore = tempDiv.querySelectorAll('img');
            console.log(`[DEBUG] Images found in stitched_html: ${allImagesBefore.length}`);
            if (allImagesBefore.length > 0) {
                allImagesBefore.forEach((img, idx) => {
                    console.log(`[DEBUG] Image ${idx + 1}: src="${img.src}", alt="${img.alt}"`);
                });
            } else {
                console.warn('[DEBUG] NO IMAGES FOUND in stitched_html!');
                // Check if HTML contains img tags as text
                if (result.stitched_html.includes('<img')) {
                    console.warn('[DEBUG] HTML contains <img tags but querySelectorAll found none - might be malformed HTML');
                }
            }

            // Style the stitched table and make it editable
            tempDiv.querySelectorAll('table').forEach(table => {
                table.style.width = '100%';
                table.style.borderCollapse = 'collapse';
                table.style.marginTop = '10px';
                table.style.position = 'relative';
                table.setAttribute('border', '1');
                table.setAttribute('id', `table-${fileId}`);

                table.querySelectorAll('td, th').forEach(cell => {
                    cell.style.border = '1px solid #ddd';
                    cell.style.padding = '8px';
                    cell.style.textAlign = 'left';
                    cell.style.verticalAlign = 'middle';
                    cell.style.position = 'relative';
                    cell.style.minHeight = '40px';
                    cell.style.cursor = 'text';
                });

                // Find and style header row from thead (if exists) or first row
                let headerRow = null;
                const thead = table.querySelector('thead');
                if (thead) {
                    headerRow = thead.querySelector('tr');
                } else {
                    // If no thead, check if first row in tbody is a header
                    const tbody = table.querySelector('tbody');
                    if (tbody) {
                        headerRow = tbody.querySelector('tr:first-child');
                    } else {
                        // Fallback to first row in table
                        headerRow = table.querySelector('tr');
                    }
                }

                if (headerRow) {
                    // Convert all td to th and style as header
                    const cells = Array.from(headerRow.querySelectorAll('td, th'));
                    cells.forEach(cell => {
                        // Convert td to th if it's a header row
                        if (cell.tagName === 'TD') {
                            const th = document.createElement('th');
                            th.innerHTML = cell.innerHTML;
                            th.style.cssText = cell.style.cssText;
                            Array.from(cell.attributes).forEach(attr => {
                                if (attr.name !== 'style') {
                                    th.setAttribute(attr.name, attr.value);
                                }
                            });
                            cell.parentNode.replaceChild(th, cell);
                        } else {
                            // It's already a th, just style it
                            const th = cell;
                            th.style.backgroundColor = '#4caf50';
                            th.style.color = 'white';
                            th.style.fontWeight = '600';
                            th.setAttribute('contenteditable', 'false');
                            th.setAttribute('onblur', 'this.style.outline="none"; this.style.backgroundColor="#4caf50";');
                        }
                    });

                    // Re-query to get converted cells and style them
                    headerRow.querySelectorAll('th').forEach(th => {
                        th.style.backgroundColor = '#4caf50';
                        th.style.color = 'white';
                        th.style.fontWeight = '600';
                        th.setAttribute('contenteditable', 'false');
                        th.setAttribute('onblur', 'this.style.outline="none"; this.style.backgroundColor="#4caf50";');
                    });

                    // Move header row to thead if not already there
                    if (!thead) {
                        const newThead = document.createElement('thead');
                        newThead.appendChild(headerRow);
                        table.insertBefore(newThead, table.firstChild);
                    }

                    // Add action column header if it doesn't exist
                    let actionHeader = headerRow.querySelector('.action-column-header');
                    if (!actionHeader) {
                        actionHeader = document.createElement('th');
                        actionHeader.className = 'action-column-header';
                        actionHeader.textContent = 'Actions';
                        actionHeader.contentEditable = 'false';
                        actionHeader.style.cssText = 'width:100px;border:1px solid #ddd;background:#4caf50;color:white;font-weight:600;text-align:center;padding:8px;';
                        headerRow.appendChild(actionHeader);
                    }
                }

                // Make cells editable and add action buttons to each data row (skip header)
                const tbody = table.querySelector('tbody') || table;
                const dataRows = tbody.querySelectorAll('tr');

                // Filter out empty rows before processing
                const rowsToRemove = [];
                dataRows.forEach((row, index) => {
                    // Skip header row
                    if (headerRow && (row === headerRow || row.closest('thead'))) {
                        return;
                    }

                    // Check if row is empty (no text content in any cell)
                    const cells = row.querySelectorAll('td, th');
                    let hasContent = false;

                    cells.forEach(cell => {
                        const cellText = cell.textContent.trim();
                        // Check if cell has actual content (not just whitespace) OR contains images
                        const hasImage = cell.querySelector('img') || cell.innerHTML.includes('<img');
                        if ((cellText && cellText.length > 0) || hasImage) {
                            hasContent = true;
                        }
                    });

                    // Mark empty rows for removal
                    if (!hasContent) {
                        rowsToRemove.push(row);
                    }
                });

                // Remove empty rows
                rowsToRemove.forEach(row => {
                    console.log('Removing empty row from table_manager.js:', row);
                    row.remove();
                });

                // Re-query data rows after filtering
                const filteredDataRows = tbody.querySelectorAll('tr');
                let dataRowIndex = 0;

                filteredDataRows.forEach((row, index) => {
                    // Skip header row
                    if (headerRow && (row === headerRow || row.closest('thead'))) {
                        return;
                    }

                    dataRowIndex++;

                    // Apply alternating row colors
                    if (dataRowIndex % 2 === 0) {
                        row.style.backgroundColor = '#f8f9fa';
                    } else {
                        row.style.backgroundColor = 'white';
                    }

                    // Remove any existing action buttons from ALL cells except Actions column
                    // This includes removing buttons from first column (Sl.No) or any other non-Actions cells
                    Array.from(row.cells).forEach((cell, cellIndex) => {
                        // Skip action column - that's where we WANT the buttons
                        if (cell.classList.contains('action-column-cell')) {
                            return;
                        }

                        // Remove ALL button elements and their containers from this cell
                        const allButtons = cell.querySelectorAll('button, .row-action-btn');
                        allButtons.forEach(btn => {
                            // Check if button is an action button (add/delete)
                            let isActionBtn = btn.classList.contains('row-action-btn') ||
                                btn.getAttribute('data-action') === 'add' ||
                                btn.getAttribute('data-action') === 'delete' ||
                                (btn.onclick && (btn.onclick.toString().includes('addRow') || btn.onclick.toString().includes('deleteRow')));

                            // Check text content for emojis if not already identified
                            if (!isActionBtn && btn.textContent) {
                                const text = btn.textContent.trim();
                                if (text.includes('➕') || text.includes('🗑️') || text.includes('+') || text.includes('×') || text.includes('x')) {
                                    isActionBtn = true;
                                }
                            }

                            if (isActionBtn) {
                                // Check if button is in a container div
                                const container = btn.closest('div');
                                if (container && container.parentNode === cell) {
                                    // Check if container only has buttons (no other content)
                                    const containerText = container.textContent.trim().replace(/[➕🗑️+×x\s]/g, '');
                                    const buttonsInContainer = container.querySelectorAll('button');
                                    if (buttonsInContainer.length > 0 && containerText.length === 0) {
                                        // Container only contains buttons, remove it
                                        container.remove();
                                    } else {
                                        // Container has other content, just remove the button
                                        btn.remove();
                                    }
                                } else {
                                    // Button is direct child or in nested container, remove it
                                    btn.remove();
                                }
                            }
                        });

                        // Clean up any empty divs or spans left behind
                        cell.querySelectorAll('div, span').forEach(el => {
                            if (el.innerHTML.trim() === '') {
                                el.remove();
                            }
                        });
                    });

                    // Remove existing action column cell if present
                    const existingActionCell = row.querySelector('.action-column-cell');
                    if (existingActionCell) {
                        existingActionCell.remove();
                    }

                    // Make all existing cells editable (except action column)
                    for (let j = 0; j < row.cells.length; j++) {
                        const cell = row.cells[j];
                        if (!cell.classList.contains('action-column-cell')) {
                            cell.setAttribute('contenteditable', 'true');
                            cell.setAttribute('ondrop', 'handleDrop(event)');
                            cell.setAttribute('ondragover', 'handleDragOver(event)');
                            const bgColor = (dataRowIndex % 2 === 0) ? '#f8f9fa' : 'white';
                            cell.setAttribute('onfocus', 'this.style.outline="2px solid #2196F3"; this.style.backgroundColor="#fff9e6";');
                            cell.setAttribute('onblur', `this.style.outline="none"; this.style.backgroundColor="${bgColor}";`);
                        }
                    }

                    // Add action buttons column (only once, at the end)
                    addActionButtonsToRow(row, fileId);
                });

                // Make images draggable
                const imagesInTable = table.querySelectorAll('img');
                console.log(`[DEBUG] Found ${imagesInTable.length} images in table after styling`);
                
                imagesInTable.forEach((img, idx) => {
                    console.log(`[DEBUG] Processing image ${idx + 1}: src="${img.src}"`);
                    
                    img.style.maxWidth = '100px';
                    img.style.maxHeight = '100px';
                    img.style.width = 'auto';
                    img.style.height = 'auto';
                    img.style.display = 'block';
                    img.style.margin = '3px auto';
                    img.style.borderRadius = '3px';
                    img.style.cursor = 'move';
                    img.style.objectFit = 'contain';
                    img.style.boxShadow = '0 1px 3px rgba(0,0,0,0.1)';
                    img.style.transition = 'transform 0.2s, box-shadow 0.2s';

                    // Make draggable
                    img.setAttribute('draggable', 'true');
                    img.setAttribute('ondragstart', 'handleDragStart(event)');
                    img.setAttribute('ondragend', 'handleDragEnd(event)');

                    // Click to enlarge
                    img.onclick = function (e) {
                        e.stopPropagation();
                        showImage(this.src);
                    };

                    // Hover effect
                    img.onmouseover = function () {
                        this.style.transform = 'scale(1.05)';
                        this.style.boxShadow = '0 4px 12px rgba(0,0,0,0.2)';
                    };
                    img.onmouseout = function () {
                        this.style.transform = 'scale(1)';
                        this.style.boxShadow = '0 1px 3px rgba(0,0,0,0.1)';
                    };
                });
                
                if (imagesInTable.length === 0) {
                    console.warn('[DEBUG] NO IMAGES FOUND in table after processing!');
                    // Check cells for image HTML
                    table.querySelectorAll('td').forEach((cell, cellIdx) => {
                        if (cell.innerHTML.includes('<img')) {
                            console.log(`[DEBUG] Cell ${cellIdx} contains <img tag but no img element found. HTML: ${cell.innerHTML.substring(0, 200)}`);
                        }
                    });
                }
            });

            stitchedHtml += tempDiv.innerHTML + `
                    </div>
                    <div style="margin-top: 20px; display: flex; justify-content: center;">
                        <button class="action-btn" onclick="openCosting('${fileId}')">💰 Apply Costing</button>
                    </div>
                    <div style="margin-top: 15px; display: flex; gap: 12px; flex-wrap: wrap; justify-content: center;">
                        <button class="action-btn" onclick="generateOfferPDF('${fileId}')">📄 Download Offer PDF</button>
                        <button class="action-btn" onclick="generateOfferExcel('${fileId}')">📊 Download Offer Excel</button>
                        <button class="action-btn" onclick="generatePresentationPPTX('${fileId}')">📽️ Generate Presentation</button>
                        <button class="action-btn" onclick="generatePresentationPDF('${fileId}')">📑 Generate Presentation PDF</button>
                        <button class="action-btn" onclick="generateMAS('${fileId}')">📋 Generate MAS</button>
                    </div>
                </div>
            `;

            stitchResult.innerHTML = stitchedHtml;

            // Store original HTML for reset functionality
            window[`originalTable_${fileId}`] = tempDiv.innerHTML;

            // Set current file ID for all workflows
            currentFileId = fileId;

            // Setup event delegation for action buttons on the table
            setTimeout(() => {
                const table = document.getElementById(`table-${fileId}`);
                if (table) {
                    setupTableActionButtons(table, fileId);
                    // Final cleanup pass to ensure no duplicate buttons
                    cleanupDuplicateButtons(table);
                }
            }, 100);

            // Show workflow-specific cards based on current workflow type
            const workflowType = window.currentWorkflowType || 'quote-pricelist';

            switch (workflowType) {
                case 'quote-pricelist':
                    // Set current file ID for costing (card will be shown when user clicks "Apply Costing" button)
                    currentFileIdForCosting = fileId;
                    break;
                case 'presentation':
                    // Show presentation card
                    const presentationCard = document.getElementById('presentationCard');
                    if (presentationCard) {
                        presentationCard.style.display = 'block';
                        presentationCard.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
                    }
                    break;
                case 'mas':
                    // Show MAS card
                    const masCard = document.getElementById('masCard');
                    if (masCard) {
                        masCard.style.display = 'block';
                        masCard.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
                    }
                    break;
                case 'multi-budget':
                    // Show multi-budget card
                    const multiBudgetCard = document.getElementById('multiBudgetCard');
                    if (multiBudgetCard) {
                        multiBudgetCard.style.display = 'block';
                        multiBudgetCard.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
                    }
                    break;
            }
        } else {
            stitchResult.innerHTML = `<div class="alert-error">Error: ${result.error}</div>`;
        }
    } catch (error) {
        stitchResult.innerHTML = `<div class="alert-error">Error: ${error.message}</div>`;
    }
}

// Costing Functions
let currentFileId = null;  // Set when table is extracted
let currentFileIdForCosting = null;  // Set when costing is applied

function openCosting(fileId) {
    currentFileIdForCosting = fileId;
    const costingCard = document.getElementById('costingCard');
    costingCard.style.display = 'block';

    // Scroll to costing card
    costingCard.scrollIntoView({ behavior: 'smooth', block: 'start' });

    showAlert('Costing panel opened! Adjust factors and click Apply Costing 💰', 'success');
}

async function applyCosting() {
    if (!currentFileIdForCosting) {
        showAlert('Please select a table first by clicking "Apply Costing" button', 'error');
        return;
    }

    // Extract table data from DOM
    const table = document.getElementById(`table-${currentFileIdForCosting}`);
    if (!table) {
        showAlert('Table not found in the page', 'error');
        return;
    }

    const tableData = extractTableData(table);

    const factors = {
        net_margin: parseFloat(document.getElementById('netMarginSlider').value),
        freight: parseFloat(document.getElementById('freightSlider').value),
        customs: parseFloat(document.getElementById('customsSlider').value),
        installation: parseFloat(document.getElementById('installationSlider').value),
        exchange_rate: parseFloat(document.getElementById('exchangeRateSlider').value),
        additional: parseFloat(document.getElementById('additionalSlider').value)
    };

    try {
        const response = await fetch('/costing', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                file_id: currentFileIdForCosting,
                factors: factors,
                table_data: tableData
            })
        });

        const result = await response.json();

        if (result.success) {
            displayCostedTable(result.result);
            showAlert('Costing applied successfully! 🎯', 'success');

            // Show offer actions card after successful costing
            const offerActionsCard = document.getElementById('offerActionsCard');
            if (offerActionsCard) {
                offerActionsCard.style.display = 'block';
                offerActionsCard.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
            }

            // Show presentation and MAS cards with costed data
            const presentationCardCosted = document.getElementById('presentationCardCosted');
            if (presentationCardCosted) {
                presentationCardCosted.style.display = 'block';
            }

            const masCardCosted = document.getElementById('masCardCosted');
            if (masCardCosted) {
                masCardCosted.style.display = 'block';
            }
        } else {
            showAlert('Error: ' + result.error, 'error');
        }
    } catch (error) {
        showAlert('Error: ' + error.message, 'error');
    }
}

function extractTableData(table) {
    const headers = [];
    const rows = [];

    // Get headers from first row
    const headerRow = table.rows[0];
    for (let i = 0; i < headerRow.cells.length; i++) {
        headers.push(headerRow.cells[i].textContent.trim());
    }

    // Get data rows (skip header row)
    for (let i = 1; i < table.rows.length; i++) {
        const row = table.rows[i];
        const rowData = {};

        for (let j = 0; j < row.cells.length; j++) {
            const cell = row.cells[j];
            // Check if cell contains an image
            const imgElement = cell.querySelector('img');
            if (imgElement) {
                rowData[headers[j]] = cell.innerHTML;
            } else {
                rowData[headers[j]] = cell.textContent.trim();
            }
        }

        rows.push(rowData);
    }

    return {
        headers: headers,
        rows: rows
    };
}

function displayCostedTable(tables) {
    const previewSection = document.getElementById('costingPreviewCard');
    const tableContent = document.getElementById('costedTableContent');

    if (!previewSection) {
        console.error('costingPreviewCard element not found');
        return;
    }

    previewSection.style.display = 'block';

    let html = '';
    tables.forEach((table, idx) => {
        html += `<h3>Table ${idx + 1}</h3>`;
        html += '<table style="width: 100%; border-collapse: collapse; margin: 20px 0;">';
        html += '<tr>';
        table.headers.forEach(header => {
            // Skip Action column in costed preview
            if (header.toLowerCase() === 'actions' || header.toLowerCase() === 'action') {
                return;
            }
            html += `<th style="border: 1px solid #ddd; padding: 12px; background: #667eea; color: white;">${header}</th>`;
        });
        html += '</tr>';

        table.rows.forEach(row => {
            html += '<tr>';
            table.headers.forEach(header => {
                // Skip Action column in costed preview
                if (header.toLowerCase() === 'actions' || header.toLowerCase() === 'action') {
                    return;
                }
                let cellValue = row[header] || '';
                html += `<td style="border: 1px solid #ddd; padding: 12px;">${cellValue}</td>`;
            });
            html += '</tr>';
        });

        html += '</table>';
    });

    tableContent.innerHTML = html;

    // Calculate summary
    calculateCostingSummary(tables);
}

function calculateCostingSummary(tables) {
    const summarySection = document.getElementById('costingSummary');

    let subtotal = 0;

    // Calculate totals from all tables
    tables.forEach(table => {
        table.rows.forEach(row => {
            for (let key in row) {
                const keyLower = key.toLowerCase();
                // Look for total or amount columns
                if ((keyLower.includes('total') || keyLower.includes('amount')) && !key.includes('_original')) {
                    const valueStr = String(row[key]).replace(/[^0-9.-]/g, '');
                    const value = parseFloat(valueStr);
                    if (!isNaN(value) && value > 0) {
                        subtotal += value;
                    }
                }
            }
        });
    });

    const vat = subtotal * 0.05; // 5% VAT
    const grandTotal = subtotal + vat;

    summarySection.innerHTML = `
        <div class="summary-row" style="display: flex; justify-content: space-between; padding: 10px 0; border-bottom: 1px solid #ddd;">
            <span>Subtotal:</span>
            <span>${subtotal.toFixed(2)}</span>
        </div>
        <div class="summary-row" style="display: flex; justify-content: space-between; padding: 10px 0; border-bottom: 1px solid #ddd;">
            <span>VAT (5%):</span>
            <span>${vat.toFixed(2)}</span>
        </div>
        <div class="summary-row grand-total" style="display: flex; justify-content: space-between; padding: 10px 0; font-weight: 600; font-size: 1.2em;">
            <span>Grand Total:</span>
            <span>${grandTotal.toFixed(2)}</span>
        </div>
    `;
}

// Generate documents
async function generateOffer() {
    if (!currentFileIdForCosting) {
        showAlert('Please select a table first', 'error');
        return;
    }

    try {
        const response = await fetch(`/generate-offer/${currentFileIdForCosting}`, {
            method: 'POST'
        });

        const result = await response.json();

        if (result.success) {
            showAlert('Offer generated successfully! 📄', 'success');
            window.open(`/download/offer/${currentFileIdForCosting}?format=pdf`, '_blank');
        } else {
            showAlert('Error: ' + result.error, 'error');
        }
    } catch (error) {
        showAlert('Error: ' + error.message, 'error');
    }
}

async function generatePresentation() {
    if (!currentFileIdForCosting) {
        showAlert('Please select a table first', 'error');
        return;
    }

    try {
        const response = await fetch(`/generate-presentation/${currentFileIdForCosting}`, {
            method: 'POST'
        });

        const result = await response.json();

        if (result.success) {
            showAlert('Presentation generated successfully! 🎨', 'success');
            window.open(`/download/presentation/${currentFileIdForCosting}?format=pdf`, '_blank');
        } else {
            showAlert('Error: ' + result.error, 'error');
        }
    } catch (error) {
        showAlert('Error: ' + error.message, 'error');
    }
}

async function generateMASFromCosting() {
    console.log('generateMASFromCosting called - currentFileIdForCosting:', currentFileIdForCosting);
    
    if (!currentFileIdForCosting) {
        showAlert('Please select a table first', 'error');
        return;
    }

    let popup = null;
    try {
        popup = showProgressPopup('Preparing MAS PDF...');
        updateProgressPopup(20, 'Preparing MAS PDF...');

        updateProgressPopup(50, 'Generating Material Approval Sheet...');
        const response = await fetch(`/generate-mas/${currentFileIdForCosting}`, {
            method: 'POST'
        });

        const result = await response.json();
        console.log('MAS generation result:', result);

        if (result.success) {
            updateProgressPopup(90, 'Finalizing...');
            window.open(`/download/mas/${currentFileIdForCosting}?format=pdf`, '_blank');
            
            updateProgressPopup(100, 'Completed!');
            setTimeout(() => {
                closeProgressPopup();
                showAlert('✅ MAS generated successfully!', 'success');
            }, 800);
        } else {
            closeProgressPopup();
            showAlert('Error: ' + result.error, 'error');
        }
    } catch (error) {
        console.error('MAS generation error:', error);
        closeProgressPopup();
        showAlert('Error: ' + error.message, 'error');
    }
}

async function valueEngineering() {
    if (!currentFileIdForCosting) {
        showAlert('Please select a table first', 'error');
        return;
    }

    const budgetOption = prompt('Select budget option:\n1. Budgetary\n2. Medium Range\n3. High End', '2');
    const options = { '1': 'budgetary', '2': 'medium', '3': 'high_end' };

    if (!budgetOption || !options[budgetOption]) {
        return;
    }

    try {
        const response = await fetch(`/value-engineering/${currentFileIdForCosting}`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                budget_option: options[budgetOption]
            })
        });

        const result = await response.json();

        if (result.success) {
            showAlert('Value engineering alternatives generated! 💡', 'success');
            // Display alternatives
        } else {
            showAlert('Error: ' + result.error, 'error');
        }
    } catch (error) {
        showAlert('Error: ' + error.message, 'error');
    }
}

// ==================== TABLE ROW MANAGEMENT FUNCTIONS ====================

// Setup event delegation for table action buttons
function cleanupDuplicateButtons(table) {
    if (!table) return;

    // Get all rows except header
    const tbody = table.querySelector('tbody') || table;
    const rows = tbody.querySelectorAll('tr');

    rows.forEach((row) => {
        // Skip header row
        if (row.closest('thead')) {
            return;
        }

        // Get all cells in this row
        Array.from(row.cells).forEach((cell) => {
            // Skip action column - that's where we WANT the buttons
            if (cell.classList.contains('action-column-cell')) {
                return;
            }

            // Remove ALL button elements and their containers from this cell
            const allButtons = cell.querySelectorAll('button, .row-action-btn');
            allButtons.forEach(btn => {
                // Check if button is an action button (add/delete)
                let isActionBtn = btn.classList.contains('row-action-btn') ||
                    btn.getAttribute('data-action') === 'add' ||
                    btn.getAttribute('data-action') === 'delete' ||
                    (btn.onclick && (btn.onclick.toString().includes('addRow') || btn.onclick.toString().includes('deleteRow')));

                // Check text content for emojis if not already identified
                if (!isActionBtn && btn.textContent) {
                    const text = btn.textContent.trim();
                    if (text.includes('➕') || text.includes('🗑️') || text.includes('+') || text.includes('×') || text.includes('x')) {
                        isActionBtn = true;
                    }
                }

                if (isActionBtn) {
                    // Check if button is in a container div
                    const container = btn.closest('div');
                    if (container && container.parentNode === cell) {
                        // Check if container only has buttons (no other content)
                        const containerText = container.textContent.trim().replace(/[➕🗑️+×x\s]/g, '');
                        const buttonsInContainer = container.querySelectorAll('button');
                        if (buttonsInContainer.length > 0 && containerText.length === 0) {
                            // Container only contains buttons, remove it
                            container.remove();
                        } else {
                            // Container has other content, just remove the button
                            btn.remove();
                        }
                    } else {
                        // Button is direct child or in nested container, remove it
                        btn.remove();
                    }
                }
            });

            // Clean up any empty divs or spans left behind
            cell.querySelectorAll('div, span').forEach(el => {
                if (el.innerHTML.trim() === '') {
                    el.remove();
                }
            });
        });

        // Specific cleanup for first column (Sl.No) which often gets polluted
        if (row.cells.length > 0) {
            const firstCell = row.cells[0];
            // Remove any buttons in first cell
            firstCell.querySelectorAll('button, .row-action-btn').forEach(btn => btn.remove());
            // Remove divs containing buttons or button-like text
            firstCell.querySelectorAll('div').forEach(div => {
                if (div.querySelector('button, .row-action-btn') ||
                    (div.textContent && (div.textContent.includes('➕') || div.textContent.includes('🗑️')))) {
                    div.remove();
                }
            });
        }
    });
}

function setupTableActionButtons(table, fileId) {
    if (!table) return;

    // Remove existing listeners if any
    table.removeEventListener('click', handleTableClick);
    table.removeEventListener('mousedown', handleTableMousedown);

    // Add event delegation for all button clicks
    table.addEventListener('click', handleTableClick, true);

    // Prevent contenteditable on action cells
    table.addEventListener('mousedown', handleTableMousedown, true);
}

function handleTableClick(e) {
    const button = e.target.closest('.row-action-btn');
    if (!button) return;

    e.stopPropagation();
    e.preventDefault();

    const action = button.getAttribute('data-action');
    const fileId = button.getAttribute('data-file-id');
    const row = button.closest('tr');

    if (!row) return;

    if (action === 'add') {
        handleAddRow(row, fileId);
    } else if (action === 'delete') {
        handleDeleteRow(row);
    }
}

function handleTableMousedown(e) {
    const actionCell = e.target.closest('.action-column-cell');
    if (actionCell) {
        const button = e.target.closest('.row-action-btn');
        if (!button) {
            e.preventDefault();
            return false;
        }
    }
}

// Add action buttons to a table row
function addActionButtonsToRow(row, fileId) {
    // Check if action column already exists
    const existingActionCell = row.querySelector('.action-column-cell');
    if (existingActionCell) {
        // Update existing cell
        existingActionCell.innerHTML = `
            <div style="display:flex;gap:5px;justify-content:center;align-items:center;">
                <button type="button" class="row-action-btn" data-action="add" data-file-id="${fileId}" title="Add row below"
                    style="background:linear-gradient(135deg,#4caf50,#45a049);color:white;padding:8px 12px;border:none;cursor:pointer;border-radius:6px;font-size:16px;box-shadow:0 2px 5px rgba(0,0,0,0.2);transition:all 0.2s;">
                    ➕
                </button>
                <button type="button" class="row-action-btn" data-action="delete" title="Delete row"
                    style="background:linear-gradient(135deg,#f44336,#e53935);color:white;padding:8px 12px;border:none;cursor:pointer;border-radius:6px;font-size:16px;box-shadow:0 2px 5px rgba(0,0,0,0.2);transition:all 0.2s;">
                    🗑️
                </button>
            </div>
        `;
        return;
    }

    // Set row index for tracking
    row.setAttribute('data-row-index', row.rowIndex);
    row.setAttribute('data-file-id', fileId);
    row.style.position = 'relative';

    // Create action column cell
    const actionCell = document.createElement('td');
    actionCell.className = 'action-column-cell';
    actionCell.style.cssText = 'width:120px;border:1px solid #ddd;background:#f8f9fa;padding:4px;text-align:center;vertical-align:middle;';
    actionCell.contentEditable = 'false';
    actionCell.setAttribute('contenteditable', 'false');

    // Create buttons container as plain HTML
    actionCell.innerHTML = `
        <div style="display:flex;gap:5px;justify-content:center;align-items:center;">
            <button type="button" class="row-action-btn" data-action="add" data-file-id="${fileId}" title="Add row below"
                style="background:linear-gradient(135deg,#4caf50,#45a049);color:white;padding:8px 12px;border:none;cursor:pointer;border-radius:6px;font-size:16px;box-shadow:0 2px 5px rgba(0,0,0,0.2);transition:all 0.2s;">
                ➕
            </button>
            <button type="button" class="row-action-btn" data-action="delete" title="Delete row"
                style="background:linear-gradient(135deg,#f44336,#e53935);color:white;padding:8px 12px;border:none;cursor:pointer;border-radius:6px;font-size:16px;box-shadow:0 2px 5px rgba(0,0,0,0.2);transition:all 0.2s;">
                🗑️
            </button>
        </div>
    `;

    row.appendChild(actionCell);
}

// Handle adding a new row below current row
function handleAddRow(currentRow, fileId) {
    const table = currentRow.closest('table');
    if (!table) return;

    const newRowIndex = currentRow.rowIndex + 1;
    const columnCount = currentRow.cells.length - 1; // Exclude action column

    // Insert new row
    const newRow = table.insertRow(newRowIndex);
    newRow.style.position = 'relative';

    // Determine background color for new row
    const bgColor = (newRowIndex % 2 === 0) ? '#f8f9fa' : 'white';
    newRow.style.backgroundColor = bgColor;

    // Create data cells
    for (let i = 0; i < columnCount; i++) {
        const cell = newRow.insertCell(i);
        cell.style.cssText = 'border:1px solid #ddd;padding:8px;text-align:left;vertical-align:middle;cursor:text;min-height:40px;';
        cell.setAttribute('contenteditable', 'true');
        cell.setAttribute('ondrop', 'handleDrop(event)');
        cell.setAttribute('ondragover', 'handleDragOver(event)');
        cell.setAttribute('onfocus', 'this.style.outline="2px solid #2196F3";this.style.backgroundColor="#fff9e6";');
        cell.setAttribute('onblur', `this.style.outline="none";this.style.backgroundColor="${bgColor}";`);
        cell.textContent = '';

        // Make cell actually editable with proper event handling
        cell.addEventListener('focus', function () {
            this.style.outline = '2px solid #2196F3';
            this.style.backgroundColor = '#fff9e6';
        });
        cell.addEventListener('blur', function () {
            this.style.outline = 'none';
            this.style.backgroundColor = bgColor;
        });
    }

    // Add action buttons to new row
    addActionButtonsToRow(newRow, fileId);

    // Reapply row colors
    updateTableRowColors(table);

    showAlert('Row added successfully! ➕', 'success');

    // Focus first cell of new row
    if (newRow.cells[0]) {
        setTimeout(() => newRow.cells[0].focus(), 100);
    }
}

// Handle deleting a row
function handleDeleteRow(row) {
    const table = row.closest('table');
    if (!table) return;

    // Prevent deleting if only header and one row remain
    if (table.rows.length <= 2) {
        showAlert('Cannot delete the last row!', 'error');
        return;
    }

    if (confirm('Delete this row?')) {
        const rowIndex = row.rowIndex;
        row.remove();
        updateTableRowColors(table);
        showAlert('Row deleted! 🗑️', 'success');
    }
}

// Update row colors and indices after row changes
function updateTableRowColors(table) {
    Array.from(table.rows).forEach((row, index) => {
        if (index === 0) return; // Skip header

        row.setAttribute('data-row-index', index);
        row.style.backgroundColor = (index % 2 === 0) ? '#f8f9fa' : 'white';

        // Update cell blur handlers
        row.querySelectorAll('td:not(.action-column-cell)').forEach(cell => {
            const bgColor = (index % 2 === 0) ? '#f8f9fa' : '';
            cell.setAttribute('onblur', `this.style.outline="none"; this.style.backgroundColor="${bgColor}";`);
        });
    });
}

// Add new row at bottom of table
function addTableRow(fileId) {
    const table = document.getElementById(`table-${fileId}`);
    if (!table) {
        showAlert('Table not found', 'error');
        return;
    }

    const firstRow = table.querySelector('tr');
    if (!firstRow) return;

    const columnCount = firstRow.querySelectorAll('td, th').length - 1; // Exclude action column
    const newRow = table.insertRow(-1);
    const newRowIndex = newRow.rowIndex;
    const bgColor = (newRowIndex % 2 === 0) ? '#f8f9fa' : 'white';

    newRow.style.position = 'relative';
    newRow.style.backgroundColor = bgColor;

    // Create data cells
    for (let i = 0; i < columnCount; i++) {
        const cell = newRow.insertCell(i);
        cell.style.cssText = 'border:1px solid #ddd;padding:8px;text-align:left;vertical-align:middle;cursor:text;min-height:40px;';
        cell.setAttribute('contenteditable', 'true');
        cell.setAttribute('ondrop', 'handleDrop(event)');
        cell.setAttribute('ondragover', 'handleDragOver(event)');
        cell.setAttribute('onfocus', 'this.style.outline="2px solid #2196F3";this.style.backgroundColor="#fff9e6";');
        cell.setAttribute('onblur', `this.style.outline="none";this.style.backgroundColor="${bgColor}";`);
        cell.textContent = i === 0 ? (table.rows.length - 1) : '';

        // Add event listeners
        cell.addEventListener('focus', function () {
            this.style.outline = '2px solid #2196F3';
            this.style.backgroundColor = '#fff9e6';
        });
        cell.addEventListener('blur', function () {
            this.style.outline = 'none';
            this.style.backgroundColor = bgColor;
        });
    }

    // Add action buttons
    addActionButtonsToRow(newRow, fileId);
    updateTableRowColors(table);

    showAlert('Row added at bottom! ➕', 'success');

    // Focus first cell of new row
    if (newRow.cells[0]) {
        setTimeout(() => {
            newRow.cells[0].focus();
            newRow.scrollIntoView({ behavior: 'smooth', block: 'center' });
        }, 100);
    }
}

// Reset table to original
function resetTable(fileId) {
    if (!confirm('Are you sure you want to reset all changes? This cannot be undone.')) {
        return;
    }

    const tableContainer = document.getElementById(`editable-table-${fileId}`);
    if (!tableContainer) {
        showAlert('Table not found', 'error');
        return;
    }

    const originalHtml = window[`originalTable_${fileId}`];
    if (originalHtml) {
        tableContainer.innerHTML = originalHtml;

        // Reapply all styles and functionality
        const table = tableContainer.querySelector('table');
        if (table) {
            table.setAttribute('id', `table-${fileId}`);

            // Make cells editable
            table.querySelectorAll('td, th').forEach(cell => {
                // Skip action column cells
                if (!cell.classList.contains('action-column-header') && !cell.classList.contains('action-column-cell')) {
                    cell.setAttribute('contenteditable', 'true');
                    cell.setAttribute('ondrop', 'handleDrop(event)');
                    cell.setAttribute('ondragover', 'handleDragOver(event)');

                    const row = cell.parentElement;
                    const bgColor = (row.rowIndex % 2 === 0) ? '#f8f9fa' : '';
                    cell.setAttribute('onfocus', 'this.style.outline="2px solid #2196F3";this.style.backgroundColor="#fff9e6";');
                    cell.setAttribute('onblur', `this.style.outline="none";this.style.backgroundColor="${bgColor}";`);
                }
            });

            // Add action buttons to all data rows (skip header)
            for (let i = 1; i < table.rows.length; i++) {
                const row = table.rows[i];

                // Check if action column already exists
                const hasActionColumn = row.querySelector('.action-column-cell');
                if (!hasActionColumn) {
                    addActionButtonsToRow(row, fileId);
                }
            }

            // Reapply images draggable
            table.querySelectorAll('img').forEach(img => {
                img.setAttribute('draggable', 'true');
                img.setAttribute('ondragstart', 'handleDragStart(event)');
                img.setAttribute('ondragend', 'handleDragEnd(event)');
                img.style.cursor = 'move';
            });

            // Setup event delegation for action buttons
            setupTableActionButtons(table, fileId);
        }

        showAlert('Table reset to original! 🔄', 'success');
    }
}

// Download edited table
async function downloadEditedTable(fileId) {
    const table = document.getElementById(`table-${fileId}`);
    if (!table) {
        showAlert('Table not found', 'error');
        return;
    }

    // Clone table and clean up for export
    const clonedTable = table.cloneNode(true);

    // Remove action column header
    let headerRow = clonedTable.rows[0];
    if (headerRow) {
        let actionHeader = headerRow.querySelector('.action-column-header');
        if (actionHeader) actionHeader.remove();
    }

    // Remove action column cells from all data rows
    Array.from(clonedTable.rows).forEach((row, i) => {
        if (i > 0) { // Skip header
            let actionCell = row.querySelector('.action-column-cell');
            if (actionCell) actionCell.remove();
        }
    });

    // Remove contenteditable and event handlers
    clonedTable.querySelectorAll('[contenteditable]').forEach(el => {
        el.removeAttribute('contenteditable');
        el.removeAttribute('onfocus');
        el.removeAttribute('onblur');
        el.removeAttribute('ondrop');
        el.removeAttribute('ondragover');
    });

    clonedTable.querySelectorAll('img').forEach(img => {
        img.removeAttribute('draggable');
        img.removeAttribute('ondragstart');
        img.removeAttribute('ondragend');
    });

    // Create HTML file content
    const htmlContent = `
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Edited BOQ Table</title>
    <style>
        body { font-family: Arial, sans-serif; padding: 20px; }
        table { border-collapse: collapse; width: 100%; margin: 20px 0; }
        td, th { border: 1px solid #ddd; padding: 12px; text-align: left; vertical-align: middle; }
        tr:first-child td { background-color: #4caf50; color: white; font-weight: 600; }
        tr:nth-child(even) { background-color: #f8f9fa; }
        img { max-width: 150px; max-height: 150px; display: block; margin: 5px auto; border-radius: 4px; }
    </style>
</head>
<body>
    <h1>Bill of Quantities (BOQ)</h1>
    <p>Edited and exported from Questemate</p>
    ${clonedTable.outerHTML}
</body>
</html>`;

    // Create download
    const blob = new Blob([htmlContent], { type: 'text/html' });
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `edited_boq_${fileId}.html`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    window.URL.revokeObjectURL(url);

    showAlert('Edited table downloaded! 📥', 'success');
}

// ==================== DRAG AND DROP FUNCTIONALITY ====================

let draggedImage = null;

function handleDragStart(event) {
    draggedImage = event.target;
    event.target.style.opacity = '0.5';
    event.dataTransfer.effectAllowed = 'move';
    event.dataTransfer.setData('text/html', event.target.outerHTML);
}

function handleDragEnd(event) {
    event.target.style.opacity = '1';
}

function handleDragOver(event) {
    if (event.preventDefault) {
        event.preventDefault();
    }
    event.dataTransfer.dropEffect = 'move';

    // Highlight drop target
    if (event.currentTarget.tagName === 'TD') {
        event.currentTarget.style.backgroundColor = '#bbdefb';
    }

    return false;
}

function handleDrop(event) {
    if (event.stopPropagation) {
        event.stopPropagation();
    }
    event.preventDefault();

    const targetCell = event.currentTarget;

    // Reset background
    targetCell.style.backgroundColor = '';

    if (draggedImage && draggedImage.parentNode) {
        // Remove image from source cell
        const sourceCell = draggedImage.parentNode;
        draggedImage.remove();

        // Add image to target cell
        const newImg = document.createElement('img');
        newImg.src = draggedImage.src;
        newImg.style.cssText = draggedImage.style.cssText;
        newImg.setAttribute('draggable', 'true');
        newImg.setAttribute('ondragstart', 'handleDragStart(event)');
        newImg.setAttribute('ondragend', 'handleDragEnd(event)');
        newImg.onclick = function (e) {
            e.stopPropagation();
            showImage(this.src);
        };
        newImg.onmouseover = function () {
            this.style.transform = 'scale(1.05)';
            this.style.boxShadow = '0 4px 12px rgba(0,0,0,0.2)';
        };
        newImg.onmouseout = function () {
            this.style.transform = 'scale(1)';
            this.style.boxShadow = '0 1px 3px rgba(0,0,0,0.1)';
        };

        targetCell.appendChild(newImg);

        showAlert('Image moved successfully! 🖼️', 'success');
    }

    return false;
}

// ============================================
// New Zero-Costing Document Generation Functions
// ============================================

/**
 * Helper function to apply zero costing and then execute a callback
 */
async function applyZeroCostingAndExecute(fileId, actionName, callback) {
    try {
        // Show progress indicator
        showAlert(`⏳ Preparing ${actionName}...`, 'info');
        
        // Get table data from DOM
        const table = document.getElementById(`table-${fileId}`);
        if (!table) {
            throw new Error('Table not found');
        }
        
        const tableData = extractTableData(table);
        
        // Apply zero costing factors
        const response = await fetch(`/apply-zero-costing/${fileId}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ table_data: tableData })
        });
        
        if (!response.ok) {
            const error = await response.json();
            throw new Error(error.error || 'Failed to apply zero costing');
        }
        
        const result = await response.json();
        if (!result.success) {
            throw new Error(result.error || 'Failed to apply zero costing');
        }
        
        // Execute the callback function
        showAlert(`✅ Generating ${actionName}...`, 'info');
        await callback(fileId);
        
    } catch (error) {
        console.error(`Error in ${actionName}:`, error);
        showAlert(`❌ Failed to generate ${actionName}: ${error.message}`, 'error');
    }
}

/**
 * Extract table data from DOM for costing
 */
function extractTableData(table) {
    const headers = [];
    const rows = [];
    
    // Extract headers
    const headerRow = table.rows[0];
    for (let i = 0; i < headerRow.cells.length; i++) {
        const cell = headerRow.cells[i];
        if (!cell.classList.contains('action-column-header')) {
            headers.push(cell.textContent.trim());
        }
    }
    
    // Extract rows
    for (let i = 1; i < table.rows.length; i++) {
        const row = table.rows[i];
        const rowData = {};
        let colIndex = 0;
        
        for (let j = 0; j < row.cells.length; j++) {
            const cell = row.cells[j];
            if (!cell.classList.contains('action-column-cell')) {
                // Check if cell contains an image
                const img = cell.querySelector('img');
                if (img && img.src) {
                    // Store cell HTML with image tag so presentation generator can extract it
                    rowData[headers[colIndex]] = cell.innerHTML;
                } else {
                    // Just store text for non-image cells
                    rowData[headers[colIndex]] = cell.textContent.trim();
                }
                
                colIndex++;
            }
        }
        rows.push(rowData);
    }
    
    return { headers, rows };
}

/**
 * Generate and download Offer PDF
 */
async function generateOfferPDF(fileId) {
    await applyZeroCostingAndExecute(fileId, 'Offer PDF', async (fId) => {
        const response = await fetch(`/generate-offer/${fId}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' }
        });
        
        if (!response.ok) {
            const error = await response.json();
            throw new Error(error.error || 'Failed to generate offer PDF');
        }
        
        const result = await response.json();
        if (result.success && result.file_path) {
            showAlert('✅ Offer PDF generated successfully!', 'success');
            // Trigger download instead of opening in browser
            const link = document.createElement('a');
            link.href = result.file_path;
            link.download = result.file_path.split('/').pop();
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
        } else {
            throw new Error(result.error || 'Failed to generate offer PDF');
        }
    });
}

/**
 * Generate and download Offer Excel
 */
async function generateOfferExcel(fileId) {
    await applyZeroCostingAndExecute(fileId, 'Offer Excel', async (fId) => {
        // Download costed table as Excel
        window.open(`/download/costed/${fId}`, '_blank');
        showAlert('✅ Offer Excel downloaded successfully!', 'success');
    });
}

/**
 * Generate Presentation (PPTX)
 */
async function generatePresentationPPTX(fileId) {
    await applyZeroCostingAndExecute(fileId, 'Presentation', async (fId) => {
        const response = await fetch(`/generate-presentation/${fId}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ format: 'pptx' })
        });
        
        if (!response.ok) {
            const error = await response.json();
            throw new Error(error.error || 'Failed to generate presentation');
        }
        
        const result = await response.json();
        if (result.success && result.file_path) {
            showAlert('✅ Presentation generated successfully!', 'success');
            // Trigger download
            const link = document.createElement('a');
            link.href = result.file_path;
            link.download = result.file_path.split('/').pop();
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
        } else {
            throw new Error(result.error || 'Failed to generate presentation');
        }
    });
}

/**
 * Generate Presentation PDF
 */
async function generatePresentationPDF(fileId) {
    await applyZeroCostingAndExecute(fileId, 'Presentation PDF', async (fId) => {
        const response = await fetch(`/generate-presentation/${fId}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ format: 'pdf' })
        });
        
        if (!response.ok) {
            const error = await response.json();
            throw new Error(error.error || 'Failed to generate presentation PDF');
        }
        
        const result = await response.json();
        if (result.success && result.file_path) {
            showAlert('✅ Presentation PDF generated successfully!', 'success');
            // Trigger download
            const link = document.createElement('a');
            link.href = result.file_path;
            link.download = result.file_path.split('/').pop();
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
        } else {
            throw new Error(result.error || 'Failed to generate presentation PDF');
        }
    });
}

/**
 * Generate MAS document
 */
async function generateMAS(fileId) {
    await applyZeroCostingAndExecute(fileId, 'MAS', async (fId) => {
        const response = await fetch(`/generate-mas/${fId}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' }
        });
        
        if (!response.ok) {
            const error = await response.json();
            throw new Error(error.error || 'Failed to generate MAS');
        }
        
        const result = await response.json();
        if (result.success && result.file_path) {
            showAlert('✅ MAS generated successfully!', 'success');
            // Trigger download
            const link = document.createElement('a');
            link.href = result.file_path;
            link.download = result.file_path.split('/').pop();
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
        } else {
            throw new Error(result.error || 'Failed to generate MAS');
        }
    });
}
