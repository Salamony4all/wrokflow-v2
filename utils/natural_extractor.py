"""
NATURAL FLOW TABLE EXTRACTOR
Follows the document structure exactly as it appears, preserving all rows and images in sequence
"""
import logging
from typing import List, Dict, Tuple, Optional
import re

logger = logging.getLogger(__name__)


class NaturalTableExtractor:
    """Extract tables following natural document flow - preserve everything in order"""
    
    def __init__(self):
        self.header_keywords = [
            'sn', 's.n', 'serial', 'no', 'item', '#',
            'location', 'room', 'area', 'zone',
            'image', 'photo', 'picture', 'img',
            'description', 'desc', 'details', 'product',
            'quantity', 'qty', 'units', 'nos',
            'rate', 'price', 'cost',
            'total', 'amount', 'value',
            'supplier', 'brand', 'manufacturer'
        ]
    
    def extract_natural_flow(self, pdfplumber_table: List[List], page_num: int) -> Dict:
        """
        NATURAL EXTRACTION FLOW:
        1. Recognize table header dynamically
        2. Identify section headers (merged cells, all caps, no numbers)
        3. Extract ALL body rows sequentially by serial number
        4. Preserve everything in top-to-bottom order
        """
        if not pdfplumber_table or len(pdfplumber_table) == 0:
            return None
        
        logger.info(f'=== NATURAL FLOW EXTRACTION: Page {page_num}, {len(pdfplumber_table)} rows ===')
        
        # STEP 1: Find table header row
        header_idx, headers = self._find_header_row(pdfplumber_table)
        logger.info(f'STEP 1: Header at row {header_idx}: {headers}')
        
        # STEP 2 & 3: Process all rows after header, preserving order
        data_rows = []
        section_headers = []
        start_row = header_idx + 1 if header_idx >= 0 else 0
        
        for row_idx in range(start_row, len(pdfplumber_table)):
            row = pdfplumber_table[row_idx]
            if not row:
                continue
            
            # STEP 2: Check if this is a SECTION HEADER
            if self._is_section_header(row):
                section_text = self._get_section_text(row)
                section_headers.append({
                    'text': section_text,
                    'position': len(data_rows),
                    'row_index': row_idx
                })
                logger.info(f'STEP 2: Found SECTION HEADER at row {row_idx}: "{section_text}"')
                continue
            
            # STEP 3: Extract body row with serial number
            serial_num = self._extract_serial_number(row)
            row_data = {
                'row_index': row_idx,
                'serial_number': serial_num,
                'cells': [str(cell).strip() if cell else '' for cell in row],
                'raw_row': row
            }
            data_rows.append(row_data)
            
            if serial_num:
                logger.debug(f'STEP 3: Body row {row_idx}, SN: {serial_num}')
        
        # STEP 4: Sort by serial number while preserving non-numbered rows
        sorted_rows = self._sort_by_serial_preserve_order(data_rows)
        
        logger.info(f'STEP 4: Extracted {len(sorted_rows)} data rows in natural order')
        
        return {
            'headers': headers,
            'header_index': header_idx,
            'rows': sorted_rows,
            'section_headers': section_headers,
            'total_rows': len(pdfplumber_table),
            'data_rows': len(sorted_rows)
        }
    
    def _find_header_row(self, table: List[List]) -> Tuple[int, List[str]]:
        """STEP 1: Dynamically recognize table header"""
        if not table:
            return -1, []
        
        # Check first 3 rows
        for row_idx in range(min(3, len(table))):
            row = table[row_idx]
            if not row:
                continue
            
            # Skip section headers (merged cells with single value)
            if self._is_section_header(row):
                logger.info(f'Row {row_idx} is section header, skipping')
                continue
            
            # Check for header keywords
            row_text = ' '.join([str(cell).lower() if cell else '' for cell in row])
            matches = sum(1 for keyword in self.header_keywords if keyword in row_text)
            
            # Must have at least 3 header keywords
            if matches >= 3:
                headers = [str(cell).strip() if cell else f'Column {i+1}' 
                          for i, cell in enumerate(row)]
                return row_idx, headers
        
        # No clear header found - use first non-section-header row
        for row_idx in range(len(table)):
            if not self._is_section_header(table[row_idx]):
                headers = [f'Column {i+1}' for i in range(len(table[row_idx]))]
                logger.info(f'No clear header, using row {row_idx} with generic headers')
                return -1, headers  # -1 means no header, treat all rows as data
        
        return -1, []
    
    def _is_section_header(self, row: List) -> bool:
        """STEP 2: Identify section headers (merged cells, all caps, no numbers)"""
        if not row:
            return False
        
        # Get non-empty cells
        non_empty = [str(cell).strip() for cell in row if cell and str(cell).strip()]
        
        # Section header: exactly 1 non-empty cell
        if len(non_empty) != 1:
            return False
        
        text = non_empty[0]
        
        # Must be all uppercase or title case
        if not (text.isupper() or text.istitle()):
            return False
        
        # Should NOT contain numbers (serial numbers, prices, etc.)
        if any(char.isdigit() for char in text):
            return False
        
        # Should be short (< 50 chars)
        if len(text) > 50:
            return False
        
        return True
    
    def _get_section_text(self, row: List) -> str:
        """Extract text from section header"""
        non_empty = [str(cell).strip() for cell in row if cell and str(cell).strip()]
        return non_empty[0] if non_empty else ''
    
    def _extract_serial_number(self, row: List) -> Optional[int]:
        """STEP 3: Extract serial number from first column"""
        if not row or len(row) == 0:
            return None
        
        first_cell = str(row[0]).strip()
        
        # Try to extract number from first cell
        # Handle formats: "1", "1.", "S.1", "No. 1", etc.
        match = re.search(r'(\d+)', first_cell)
        if match:
            num = int(match.group(1))
            if 1 <= num <= 10000:  # Reasonable range for serial numbers
                return num
        
        return None
    
    def _sort_by_serial_preserve_order(self, rows: List[Dict]) -> List[Dict]:
        """STEP 4: Sort by serial number, preserve order for non-numbered rows"""
        # Separate numbered and non-numbered rows
        numbered = [r for r in rows if r['serial_number'] is not None]
        non_numbered = [r for r in rows if r['serial_number'] is None]
        
        # Sort numbered rows by serial number
        numbered_sorted = sorted(numbered, key=lambda r: r['serial_number'])
        
        # Preserve original order for non-numbered rows
        # Insert them back at their relative positions
        result = []
        numbered_idx = 0
        non_numbered_idx = 0
        
        for i in range(len(rows)):
            original_row = rows[i]
            if original_row['serial_number'] is not None:
                if numbered_idx < len(numbered_sorted):
                    result.append(numbered_sorted[numbered_idx])
                    numbered_idx += 1
            else:
                if non_numbered_idx < len(non_numbered):
                    result.append(non_numbered[non_numbered_idx])
                    non_numbered_idx += 1
        
        return result
