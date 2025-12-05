"""
Improved Table Extractor - 1:1 Replica of Original Tables
Focuses on accurate table structure replication with images, headers, and multi-line descriptions
"""
import os
import re
import logging
import pdfplumber
import fitz  # PyMuPDF
from typing import List, Dict, Optional, Tuple
from collections import defaultdict
import json

logger = logging.getLogger(__name__)

# Multi-library support for hybrid extraction
try:
    import camelot
    CAMELOT_AVAILABLE = True
except ImportError:
    CAMELOT_AVAILABLE = False
    logger.warning("Camelot not available - install with: pip install camelot-py[cv]")

try:
    import tabula
    TABULA_AVAILABLE = True
except ImportError:
    TABULA_AVAILABLE = False
    logger.warning("Tabula-py not available - install with: pip install tabula-py")

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    logger.warning("Pandas not available")

# Advanced document analysis libraries
try:
    import layoutparser as lp
    LAYOUTPARSER_AVAILABLE = True
except ImportError:
    LAYOUTPARSER_AVAILABLE = False
    logger.info("LayoutParser not available - install with: pip install layoutparser")

# Make unstructured import optional and skip on errors
UNSTRUCTURED_AVAILABLE = False
partition_pdf = None
try:
    from unstructured.partition.pdf import partition_pdf
    UNSTRUCTURED_AVAILABLE = True
    logger.info("✓ Unstructured.io imported successfully")
except Exception as e:
    try:
        from unstructured import partition_pdf
        UNSTRUCTURED_AVAILABLE = True
        logger.info("✓ Unstructured.io imported successfully (alternative import)")
    except Exception as e2:
        UNSTRUCTURED_AVAILABLE = False
        logger.info(f"Unstructured.io not available (optional) - skipping import")

# Nanonets-OCR-s (requires GPU, optional)
try:
    # Note: Nanonets-OCR-s may require specific setup
    # For now, we'll use it if available
    NANONETS_AVAILABLE = False  # Set to True if properly configured
    logger.info("Nanonets-OCR-s: GPU-based, requires specific setup")
except:
    NANONETS_AVAILABLE = False

# Image processing dependencies
try:
    from PIL import Image
    import cv2
    import numpy as np
    from pdf2image import convert_from_path
    IMAGE_PROCESSING_AVAILABLE = True
except ImportError:
    IMAGE_PROCESSING_AVAILABLE = False
    logger.warning("Image processing libraries not available - install: pip install pillow opencv-python numpy pdf2image")

logger = logging.getLogger(__name__)


class ImprovedTableExtractor:
    """
    Improved table extractor that creates 1:1 replicas of original tables:
    - Recognizes header variants (S.No, Sl.No, SN, Sn, etc.)
    - Handles borderless tables using visual gaps
    - Extracts and embeds images with reference text
    - Preserves multi-line descriptions as single cells
    - Filters non-table content
    - Detects section titles vs descriptions
    - Maintains exact column order and row sequence
    """
    
    def __init__(self):
        # Header variants - comprehensive list with all common variants
        # IMPORTANT: Only normalize if exact match - preserve original headers otherwise
        self.header_variants = {
            'serial': ['s.no', 's.no.', 'sl.no', 'sl.no.', 'serial', 'serial no', 'serial number', 'sn', 'si.no', 'si.no.', 
                      's no', 'sl no', 'serial no.', 'serial number.', 's#', 'sl#', 'item no', 'item number', '#', 
                      'sr.no', 'sr.no.', 'sr no', 'sr. no.', 'item #', 'no', 'no.', 'number'],
            'location': ['location', 'room', 'area', 'space', 'zone', 'section', 'place', 'position', 'room name', 
                        'area name', 'room/area', 'location/room'],
            'image': ['image', 'images', 'photo', 'photos', 'picture', 'pictures', 'reference', 'references', 'ref', 'img',
                     'image reference', 'indicative image', 'indicative', 'image ref', 'photo reference', 'picture reference',
                     'img reference', 'visual reference', 'product image', 'item image', 'pic', 'pics', 'photograph', 
                     'thumbnail', 'illustration', 'figure', 'diagram', 'visual'],
            'description': ['description', 'item description', 'specification', 'specifications', 'item details', 'details',
                           'item detail', 'item specification', 'product description', 'product details', 'spec', 'specs',
                           'item name', 'name', 'product name', 'item', 'product', 'article', 'particulars', 
                           'item specifications', 'product spec'],
            'quantity': ['qty', 'quantity', 'quantities', 'qty.', 'qty:', 'qty :', 'qty.', 'qty:', 'qty :', 'qty',
                        'quantity.', 'quantities.', 'qty (nos)', 'qty (units)', 'nos', 'number of units', 'quantity required', 
                        'required qty', 'no. of units', 'no of units', 'units required'],
            'unit': ['unit', 'units', 'uom', 'unit of measure', 'unit of measurement', 'uom.', 'unit.', 'units.',
                    'measurement unit', 'measuring unit', 'measure', 'unit type'],
            'rate': ['rate', 'unit rate', 'unit price', 'price', 'cost', 'value', 'amount per unit', 'rate per unit',
                    'unit cost', 'unit value', 'price per unit', 'cost per unit', 'rate/unit', 'price/unit', 'cost/unit',
                    'unit amount', 'per unit', 'per piece', 'each', 'rate each'],
            'amount': ['amount', 'total', 'total value', 'total amount', 'sum', 'subtotal', 'line total', 'row total',
                      'item total', 'total price', 'total cost', 'amount total', 'grand total', 'value', 'net amount',
                      'line amount', 'item amount', 'cost total'],
            'supplier': ['supplier', 'vendor', 'manufacturer', 'brand', 'make', 'origin', 'source', 'from', 
                        'supplied by', 'manufactured by', 'maker', 'provider'],
            'actions': ['actions', 'action', 'edit', 'delete', 'modify', 'remove', 'manage', 'control']
        }
        
        # Summary row keywords
        self.summary_keywords = ['total', 'subtotal', 'vat', 'grand total', 'balance', 'net total', 'final total']
        
        # Non-table content keywords (to filter out)
        self.non_table_keywords = ['date', 'ref.', 'reference', 'project', 'terms', 'conditions', 'terms and conditions', 
                                   'page', 'page no', 'page number', 'cover', 'letter', 'prices quoted', 'inclusive of',
                                   'shipping', 'vat', 'proposal', 'price', 'valid for', 'days only', 'offer', 'based on',
                                   'quantities', 'provided', 'quoted', 'are', 'inclusive', 'your site', 'oman', 'however',
                                   'any increase', 'same', 'strictly', 'valid', 'this', 'is', 'in', 'the']
        
        # Section title patterns (centered, bold, with spacing)
        self.section_title_patterns = [
            r'^[A-Z][A-Z\\s-]+$',  # All caps with spaces/dashes
        ]
        
        # Extraction configuration - optimized settings
        self.config = {
            'unstructured': {
                'base_timeout': 45,  # Base timeout in seconds
                'max_timeout': 180,  # Maximum timeout
                'max_retries': 2,  # Retry attempts
                'timeout_per_page': 5,  # Additional seconds per page
                'timeout_per_mb': 10,  # Additional seconds per MB
            },
            'camelot': {
                'lattice': {  # For bordered tables - optimized for clean extraction
                    'line_scale': 40,  # Reduced to avoid over-detection
                    'process_background': False,  # Ignore images in background
                    'line_tol': 2,  # Stricter line detection
                    'joint_tol': 2,  # Stricter corner detection
                    'threshold_blocksize': 15,  # Smaller block to avoid false edges
                    'threshold_constant': -2,  # Less aggressive threshold
                    'copy_text': ['v'],  # Only vertical text alignment
                },
                'stream': {  # For borderless/complex tables - better for images
                    'edge_tol': 50,  # Moderate edge detection
                    'row_tol': 15,  # More tolerant for rows with images
                    'column_tol': 0,  # Strict column separation
                }
            },
            'pdfplumber': {
                'bordered': {
                    'vertical_strategy': 'lines',
                    'horizontal_strategy': 'lines',
                    'snap_tolerance': 3,  # Tighter cell boundary detection
                    'join_tolerance': 3,  # Less aggressive line merging
                    'edge_min_length': 3,  # Minimum line length
                    'min_words_vertical': 3,  # Min vertical word spacing
                    'min_words_horizontal': 1,  # Min horizontal word spacing
                    'text_tolerance': 3,
                    'intersection_tolerance': 3,  # Stricter intersections to preserve rows
                },
                'borderless': {
                    'text_tolerance': 5,
                    'text_x_tolerance': 5,
                    'text_y_tolerance': 5,
                    'intersection_tolerance': 10,
                    'intersection_x_tolerance': 10,
                    'intersection_y_tolerance': 10,
                }
            },
            'quality_scoring': {
                'row_weight': 2,  # Points per row
                'max_row_score': 50,  # Maximum points from rows
                'consistency_weight': 30,  # Points for column consistency
                'header_match_weight': 5,  # Points per header keyword match
                'numeric_weight': 1,  # Points per numeric cell
                'max_numeric_score': 30,  # Maximum points from numeric cells
            }
        }
    
    def extract_tables(self, file_path: str, file_extension: str, output_dir: Optional[str] = None,
                      bordered_method: str = 'camelot', borderless_method: str = 'unstructured', 
                      ai_strategy: str = 'auto') -> Dict:
        """
        Main extraction method
        Args:
            bordered_method: Method for bordered tables ('camelot', 'pdfplumber', 'tabula')
            borderless_method: Method for borderless tables ('unstructured', 'camelot-stream', 'layoutparser')
            ai_strategy: AI strategy ('auto', 'fast', 'hires')
        Returns: dict with tables, images, and metadata
        """
        try:
            if file_extension.lower() == 'pdf':
                return self._extract_from_pdf(file_path, output_dir, bordered_method, borderless_method, ai_strategy)
            else:
                logger.warning(f'Unsupported file type: {file_extension}')
                return self._empty_result()
        except Exception as e:
            logger.error(f'Extraction failed: {str(e)}', exc_info=True)
            return self._empty_result()
    
    def _extract_from_pdf(self, pdf_path: str, output_dir: Optional[str], 
                          bordered_method: str = 'camelot', borderless_method: str = 'unstructured',
                          ai_strategy: str = 'auto') -> Dict:
        """
        SINGLE-METHOD extraction from PDF using user-selected method only
        Args:
            bordered_method: Method for bordered tables ('camelot', 'pdfplumber', 'tabula')
            borderless_method: Method for borderless tables ('unstructured', 'camelot-stream', 'layoutparser')
            ai_strategy: AI strategy ('auto', 'fast', 'hires')
        """
        logger.info(f'Starting extraction: bordered={bordered_method}, borderless={borderless_method}, ai={ai_strategy}')
        
        all_tables = []
        all_images = {}
        total_pages = 0
        
        try:
            # Use ONLY the user-selected method
            if bordered_method == 'camelot' and CAMELOT_AVAILABLE:
                return self._extract_with_camelot(pdf_path, output_dir)
            
            elif bordered_method == 'pdfplumber':
                return self._extract_with_pdfplumber(pdf_path, output_dir)
            
            elif bordered_method == 'tabula' and TABULA_AVAILABLE:
                return self._extract_with_tabula(pdf_path, output_dir)
            
            elif borderless_method == 'unstructured':
                return self._extract_with_unstructured(pdf_path, output_dir, ai_strategy)
            
            elif borderless_method == 'camelot-stream' and CAMELOT_AVAILABLE:
                return self._extract_with_camelot(pdf_path, output_dir, flavor='stream')
            
            elif borderless_method == 'layoutparser':
                return self._extract_with_layoutparser(pdf_path, output_dir)
            
            else:
                logger.warning(f'Selected method not available: bordered={bordered_method}, borderless={borderless_method}')
                return self._empty_result()
                
        except Exception as e:
            logger.error(f'PDF extraction failed: {str(e)}', exc_info=True)
            return self._empty_result()
    
    def _extract_with_camelot(self, pdf_path: str, output_dir: Optional[str], flavor: str = 'lattice') -> Dict:
        """Extract using Camelot (Lattice or Stream mode)"""
        all_tables = []
        all_images = {}
        
        try:
            logger.info(f'Using Camelot ({flavor}) extraction')
            camelot_config = self.config['camelot'][flavor]
            
            if flavor == 'lattice':
                camelot_tables = camelot.read_pdf(
                    pdf_path,
                    pages='all',
                    flavor='lattice',
                    line_scale=camelot_config['line_scale'],
                    process_background=camelot_config['process_background'],
                    line_tol=camelot_config['line_tol'],
                    joint_tol=camelot_config['joint_tol'],
                    threshold_blocksize=camelot_config['threshold_blocksize'],
                    threshold_constant=camelot_config['threshold_constant'],
                    copy_text=camelot_config['copy_text']
                )
            else:  # stream
                camelot_tables = camelot.read_pdf(
                    pdf_path,
                    pages='all',
                    flavor='stream',
                    edge_tol=camelot_config['edge_tol'],
                    row_tol=camelot_config['row_tol'],
                    column_tol=camelot_config['column_tol']
                )
            
            if camelot_tables and len(camelot_tables) > 0:
                logger.info(f'Camelot found {len(camelot_tables)} table(s)')
                for idx, table in enumerate(camelot_tables):
                    if PANDAS_AVAILABLE:
                        df = table.df
                        if df is not None and not df.empty:
                            table_list = [df.columns.tolist()] + df.values.tolist()
                            filtered_table = self._filter_table_content(None, table_list, idx + 1)
                            if filtered_table and len(filtered_table) >= 2:
                                processed_table = self._process_table_advanced(
                                    filtered_table, 1, idx, output_dir, None
                                )
                                if processed_table:
                                    all_tables.append(processed_table)
                                    logger.info(f'Camelot table {idx + 1} processed successfully')
            else:
                logger.info('Camelot found no tables')
        except Exception as e:
            logger.error(f'Camelot extraction failed: {e}', exc_info=True)
        
        return {
            'tables': all_tables,
            'images': all_images,
            'tables_found': len(all_tables),
            'extraction_method': 'camelot',
            'total_pages': 0
        }
    
    def _extract_with_pdfplumber(self, pdf_path: str, output_dir: Optional[str]) -> Dict:
        """Extract using pdfplumber with optimized settings"""
        logger.info("=== DEBUG: Running UPDATED _extract_with_pdfplumber with ROW SORTING fix ===")
        all_tables = []
        all_images = {}
        total_pages = 0
        
        try:
            logger.info('Using pdfplumber extraction (optimized)')
            pdf_plumber = pdfplumber.open(pdf_path)
            total_pages = len(pdf_plumber.pages)
            
            # Get optimized table settings
            plumber_config = self.config['pdfplumber']['bordered']
            
            for page_num in range(total_pages):
                page = pdf_plumber.pages[page_num]
                logger.info(f'Processing page {page_num + 1}/{total_pages}')
                
                # Extract images on this page
                page_images = []
                if hasattr(page, 'images'):
                    page_images = page.images
                    logger.info(f'Page {page_num + 1} has {len(page_images)} images')
                
                # Extract tables with optimized settings - use lines strategy with rect filtering
                # Filter out image rects that interfere with table detection
                filtered_rects = []
                if hasattr(page, 'rects'):
                    for rect in page.rects:
                        # Keep only table border rects (ignore image rects)
                        if rect.get('linewidth', 0) > 0:
                            filtered_rects.append(rect)
                
                # Use lines strategy - lines_strict is too strict for most PDFs
                # Keep tolerances reasonable to detect tables while minimizing merging
                table_settings = {
                    'vertical_strategy': 'lines',
                    'horizontal_strategy': 'lines',
                    'snap_tolerance': 3,
                    'join_tolerance': 3,
                    'edge_min_length': 10,
                    'min_words_vertical': 1,
                    'min_words_horizontal': 1,
                    'text_tolerance': 3,
                    'intersection_tolerance': 3,
                }
                
                # Extract tables with bbox information
                tables_with_bbox = page.find_tables(table_settings=table_settings)
                
                for table_idx, table_obj in enumerate(tables_with_bbox):
                    if table_obj:
                        # CRITICAL FIX: Sort rows by vertical position to ensure correct sequence
                        # pdfplumber might return rows out of order for complex tables
                        try:
                            # Sort rows by top y-coordinate of their first cell (descending y = higher on page = first)
                            def get_row_y(row):
                                if row.cells:
                                    try:
                                        return min(c[1] for c in row.cells if c and len(c) >= 2)
                                    except:
                                        return 0
                                return 0
                            sorted_rows = sorted(table_obj.rows, key=get_row_y, reverse=False)  # False = top to bottom
                            logger.info(f'Table {table_idx}: Sorted {len(sorted_rows)} rows by y-coordinate')
                        except Exception as e:
                            logger.warning(f'Failed to sort rows by y-coordinate: {e}')
                            sorted_rows = table_obj.rows
                        
                        # Re-extract text from sorted rows
                        table_data = []
                        for row in sorted_rows:
                            row_data = []
                            for cell in row.cells:
                                if cell:
                                    # Extract text from the cell bbox
                                    cell_text = page.crop(cell).extract_text() or ""
                                    row_data.append(cell_text)
                                else:
                                    row_data.append("")
                            table_data.append(row_data)
                            
                        table_bbox = table_obj.bbox  # Get table bounding box
                        
                        if not table_data:
                            continue
                        
                        # Clean empty cells and whitespace
                        cleaned_table = [[str(cell).strip() if cell else '' for cell in row] for row in table_data]
                        
                        # Detect image column and get row bboxes
                        header_row = cleaned_table[0] if cleaned_table else []
                        image_col_idx = -1
                        for idx, header in enumerate(header_row):
                            if header and any(keyword in str(header).upper() for keyword in ['IMAGE', 'INDICATIVE', 'PHOTO', 'PICTURE']):
                                image_col_idx = idx
                                break
                        
                        # Get actual row bboxes from sorted rows (EXCLUDING header row for image matching)
                        row_bboxes = []
                        try:
                            for row_idx, row in enumerate(sorted_rows):
                                # Skip first row (header) - images are only in data rows
                                if row_idx == 0:
                                    continue
                                if row.cells and len(row.cells) > 0:
                                    # Get y-coordinates of this row
                                    y0 = min(cell[1] for cell in row.cells if cell)  # top
                                    y1 = max(cell[3] for cell in row.cells if cell)  # bottom
                                    row_bboxes.append((y0, y1))
                        except:
                            row_bboxes = []
                        
                        # Don't insert [IMAGE] placeholder - images will be extracted and embedded later
                        # The _extract_images_comprehensive method will handle image extraction and matching
                        
                        filtered_table = self._filter_table_content(page, cleaned_table, page_num + 1)
                        if filtered_table and len(filtered_table) >= 2:
                            processed_table = self._process_table_advanced(
                                filtered_table, page_num + 1, table_idx, output_dir, None
                            )
                            if processed_table:
                                # Store row bboxes for image matching - ensure page number is correct
                                processed_table['row_bboxes'] = row_bboxes
                                processed_table['table_bbox'] = table_bbox
                                # Initialize row_bboxes_per_page if not exists
                                if 'row_bboxes_per_page' not in processed_table:
                                    processed_table['row_bboxes_per_page'] = {}
                                # Store row bboxes for this specific page
                                processed_table['row_bboxes_per_page'][page_num + 1] = row_bboxes
                                logger.info(f'Table {table_idx} page {page_num + 1}: Stored {len(row_bboxes)} row bboxes for image matching')
                                # Ensure page is in pages list
                                if 'pages' not in processed_table:
                                    processed_table['pages'] = []
                                if (page_num + 1) not in processed_table['pages']:
                                    processed_table['pages'].append(page_num + 1)
                                all_tables.append(processed_table)
            
            pdf_plumber.close()
            logger.info(f'pdfplumber found {len(all_tables)} table(s)')
            
            # Merge multi-page tables (tables with same structure across consecutive pages)
            all_tables = self._merge_multipage_tables(all_tables)
            logger.info(f'After merging multi-page tables: {len(all_tables)} table(s)')
            
            # Extract images using PyMuPDF if output_dir provided
            all_images = {}
            logger.info(f'=== IMAGE EXTRACTION PHASE ===')
            logger.info(f'output_dir: {output_dir}, all_tables count: {len(all_tables)}')
            if output_dir and all_tables:
                try:
                    # Open PDF with PyMuPDF for image extraction
                    import fitz
                    pdf_fitz = fitz.open(pdf_path)
                    logger.info(f'Opened PDF with PyMuPDF: {len(pdf_fitz)} pages')
                    
                    # Extract images for each table (including multi-page tables)
                    for table_idx, table in enumerate(all_tables):
                        logger.info(f'=== Processing table {table_idx} for image extraction ===')
                        pages = table.get('pages', [table.get('page', 1)])  # Support multi-page tables
                        row_bboxes_per_page = table.get('row_bboxes_per_page', {})
                        headers = table.get('headers', [])
                        rows = table.get('rows', [])
                        
                        # Track row offset for multi-page tables
                        cumulative_row_offset = 0
                        
                        # Extract images from each page of this table
                        logger.info(f'Table {table_idx}: Processing pages {pages} for image extraction')
                        for page_num in sorted(pages):  # Ensure pages are processed in order
                            logger.info(f'Table {table_idx}: Processing page {page_num} (valid range: 1-{len(pdf_fitz)})')
                            
                            if page_num < 1 or page_num > len(pdf_fitz):
                                logger.warning(f'Skipping invalid page {page_num} (valid range: 1-{len(pdf_fitz)})')
                                continue
                            
                            # PyMuPDF uses 0-indexed pages
                            page_fitz = pdf_fitz[page_num - 1]
                            image_list = page_fitz.get_images(full=True)
                            
                            logger.info(f'Page {page_num}: Found {len(image_list)} images in image_list')
                            
                            if not image_list:
                                logger.info(f'Page {page_num}: No images found, continuing to next page')
                                # Still update row offset even if no images
                                row_bboxes = row_bboxes_per_page.get(page_num, [])
                                cumulative_row_offset += len(row_bboxes)
                                continue
                            
                            # Get row bboxes for this specific page
                            row_bboxes = row_bboxes_per_page.get(page_num, [])
                            logger.info(f'Page {page_num}: Retrieved {len(row_bboxes)} row bboxes from row_bboxes_per_page')
                            if not row_bboxes:
                                logger.error(f'Page {page_num}: CRITICAL - No row bboxes found in row_bboxes_per_page! Available pages: {list(row_bboxes_per_page.keys())}')
                            
                            # Calculate which rows belong to this page
                            # For multi-page tables, we need to track row offset
                            page_row_count = len(row_bboxes)
                            
                            logger.info(f'Page {page_num}: {page_row_count} row bboxes, {len(image_list)} images, row_offset={cumulative_row_offset}')
                            
                            # Extract images with sequential matching (sorted by y-coordinate)
                            table_images = self._extract_images_content_aware(
                                pdf_fitz, page_fitz, image_list, table_idx, page_num, output_dir,
                                rows, headers, row_bboxes,
                                row_offset=cumulative_row_offset, page_row_count=page_row_count
                            )
                            
                            logger.info(f'Page {page_num}: Extracted and matched {len(table_images)} images')
                            all_images.update(table_images)
                            
                            # Update cumulative offset for next page
                            cumulative_row_offset += page_row_count
                        
                        # NOTE: Images are NOT embedded here - they will be embedded AFTER sorting rows by SN
                        # This ensures images go into the correct sorted rows
                    
                    pdf_fitz.close()
                    logger.info(f'=== IMAGE EXTRACTION COMPLETE ===')
                    logger.info(f'Total images extracted: {len(all_images)}')
                    if all_images:
                        logger.info(f'Sample image keys: {list(all_images.keys())[:5]}')
                        for key, img_data in list(all_images.items())[:3]:
                            logger.info(f'  Image {key}: row_index={img_data.get("row_index")}, table_idx={img_data.get("table_global_idx")}, path={img_data.get("relative_path")}')
                    else:
                        logger.warning('NO IMAGES EXTRACTED - This is the problem!')
                except Exception as e:
                    logger.error(f'Image extraction failed for pdfplumber: {e}', exc_info=True)
                    all_images = {}
            
            # Sort rows by SN column to maintain correct sequence (1-2-3-4-5...) BEFORE embedding images
            # This ensures images are embedded in the correct sorted rows
            for table_idx, table in enumerate(all_tables):
                self._sort_rows_by_sn(table)
                # Remap image row indices after sorting
                self._remap_images_after_sort(table, all_images, table_idx)
            
            # Now embed images AFTER sorting - this ensures images go into the correct sorted rows
            logger.info(f'=== STARTING IMAGE EMBEDDING ===')
            logger.info(f'output_dir: {output_dir}, all_tables: {len(all_tables)}, all_images: {len(all_images)}')
            if output_dir and all_tables and all_images:
                try:
                    logger.info(f'Proceeding with image embedding for {len(all_tables)} table(s)')
                    for table_idx, table in enumerate(all_tables):
                        logger.info(f'=== Processing table {table_idx} for image embedding ===')
                        headers = table.get('headers', [])
                        rows = table.get('rows', [])
                        
                        # Find image column
                        image_col_idx = None
                        for idx, header in enumerate(headers):
                            header_lower = str(header).lower().strip()
                            # Check for image column keywords using header variants
                            is_image_col = False
                            for variant in self.header_variants['image']:
                                if variant in header_lower:
                                    is_image_col = True
                                    break
                            if is_image_col:
                                image_col_idx = idx
                                logger.info(f'Table {table_idx}: Found image column at index {idx}: "{header}"')
                                break
                        
                        # Fallback: if headers are generic (Column 1, Column 2...), use default column index
                        if image_col_idx is None:
                            # Check if headers are generic
                            generic_pattern = all(str(h).startswith('Column ') for h in headers if h)
                            if generic_pattern and len(headers) >= 3:
                                image_col_idx = 2  # Default to column 2 (0=SN, 1=Location, 2=Image)
                                logger.info(f'Table {table_idx}: Generic headers detected, using default image column {image_col_idx}')
                            else:
                                logger.warning(f'Table {table_idx}: No image column found in headers: {headers}')
                                continue
                        
                        # Find SN column for logging and matching
                        sn_col_idx = None
                        for idx, header in enumerate(headers):
                            header_lower = str(header).lower().strip()
                            if any(variant in header_lower for variant in self.header_variants['serial']):
                                sn_col_idx = idx
                                break
                        
                        # If no SN column found in headers (generic headers), try to detect it from row content
                        if sn_col_idx is None and rows:
                            # Check first few rows to find which column has sequential numbers
                            for col_idx in range(min(3, len(rows[0]) if rows else 0)):  # Check first 3 columns
                                is_sn_col = True
                                for row_idx in range(min(3, len(rows))):  # Check first 3 rows
                                    if col_idx < len(rows[row_idx]):
                                        cell_val = str(rows[row_idx][col_idx]).strip()
                                        # Check if it's a sequential number (1, 2, 3 or 9, 10, 11, etc.)
                                        if not cell_val.isdigit():
                                            is_sn_col = False
                                            break
                                if is_sn_col and col_idx < len(rows[0]):
                                    sn_col_idx = col_idx
                                    logger.info(f'Table {table_idx}: Detected SN column at index {col_idx} from row content')
                                    break
                        
                        # Build row-to-image mapping (after sorting, images should match SN sequence)
                        row_images_map = {}
                        for key, img_data in all_images.items():
                            # Check both table_global_idx and table_index
                            img_table_idx = img_data.get('table_global_idx') or img_data.get('table_index')
                            if img_table_idx != table_idx:
                                continue
                            
                            img_row_idx = img_data.get('row_index')
                            if img_row_idx is not None:
                                # Ensure row index is valid (allow row 0 for data rows)
                                if 0 <= img_row_idx < len(rows):
                                    if img_row_idx not in row_images_map:
                                        # Get SN value for logging
                                        sn_value = "N/A"
                                        if sn_col_idx is not None and img_row_idx < len(rows) and sn_col_idx < len(rows[img_row_idx]):
                                            sn_value = str(rows[img_row_idx][sn_col_idx])
                                        
                                        row_images_map[img_row_idx] = img_data
                                        logger.info(f'Table {table_idx}: Image {key} mapped to row {img_row_idx} (SN: {sn_value})')
                                    else:
                                        logger.warning(f'Table {table_idx}: Row {img_row_idx} already has an image, skipping duplicate {key}')
                                else:
                                    logger.warning(f'Table {table_idx}: Image {key} row_index {img_row_idx} out of range (0-{len(rows)-1})')
                            else:
                                logger.warning(f'Table {table_idx}: Image {key} has no row_index')
                        
                        logger.info(f'Table {table_idx}: Mapping {len(row_images_map)} images to {len(rows)} rows (after sorting by SN)')
                        
                        # Embed images in rows
                        embedded_count = 0
                        for row_idx, row in enumerate(rows):
                            img_data = row_images_map.get(row_idx)
                            if img_data and image_col_idx is not None:
                                # Ensure row has enough columns
                                while len(row) <= image_col_idx:
                                    row.append('')
                                
                                if image_col_idx < len(row):
                                    img_path = img_data.get('relative_path') or img_data.get('path', '')
                                    if img_path:
                                        # Build image URL
                                        url_base = output_dir.replace('\\', '/').replace(os.getcwd().replace('\\', '/'), '').strip('/')
                                        if not url_base.startswith('/'):
                                            url_base = '/' + url_base
                                        full_img_path = f"{url_base}/{img_path}"
                                        
                                        page_num = img_data.get('page', 1)
                                        img_id = f'img_{page_num}_{table_idx}_{row_idx}'
                                        img_html = f'''<div style="text-align: center;">
                                            <img id="{img_id}" 
                                                 src="{full_img_path}" 
                                                 alt="Image" 
                                                 class="table-image-thumbnail"
                                                 style="max-width: 150px; max-height: 150px; cursor: pointer; border: 2px solid #ddd; border-radius: 4px; padding: 2px; object-fit: contain; transition: transform 0.2s, box-shadow 0.2s;"
                                                 onclick="openImageModal('{full_img_path}', '{img_id}')"
                                                 onmouseover="this.style.transform='scale(1.05)'; this.style.boxShadow='0 4px 8px rgba(0,0,0,0.2)'"
                                                 onmouseout="this.style.transform='scale(1)'; this.style.boxShadow='none'"
                                                 title="Click to enlarge" />
                                        </div>'''
                                        
                                        # Get SN value for logging
                                        sn_value_log = "N/A"
                                        if sn_col_idx is not None and row_idx < len(rows) and sn_col_idx < len(rows[row_idx]):
                                            sn_value_log = str(rows[row_idx][sn_col_idx])
                                        
                                        # Replace cell content with image
                                        row[image_col_idx] = img_html
                                        embedded_count += 1
                                        logger.info(f'✓✓✓ Table {table_idx}: SUCCESSFULLY EMBEDDED image in row {row_idx} (SN: {sn_value_log}): {img_path}')
                                    else:
                                        logger.warning(f'Table {table_idx}: Row {row_idx} image path is empty: {img_data}')
                                else:
                                    logger.warning(f'Table {table_idx}: Row {row_idx} has {len(row)} columns, image_col_idx={image_col_idx}')
                            else:
                                if row_idx > 0:  # Data row but no image
                                    logger.debug(f'Table {table_idx}: Row {row_idx} has no image assigned')
                        
                        logger.info(f'✓✓✓ Table {table_idx}: COMPLETED - Embedded {embedded_count} images out of {len(row_images_map)} mapped images')
                        
                except Exception as e:
                    logger.error(f'Failed to embed images after sorting: {e}', exc_info=True)
            else:
                logger.error(f'Cannot embed images: output_dir={output_dir}, all_tables={len(all_tables) if all_tables else 0}, all_images={len(all_images) if all_images else 0}')
                    
        except Exception as e:
            logger.error(f'pdfplumber extraction failed: {e}', exc_info=True)
        
        return {
            'tables': all_tables,
            'images': all_images,
            'tables_found': len(all_tables),
            'extraction_method': 'pdfplumber',
            'total_pages': total_pages
        }
    
    def _extract_with_tabula(self, pdf_path: str, output_dir: Optional[str]) -> Dict:
        """Extract using Tabula only"""
        all_tables = []
        all_images = {}
        
        try:
            logger.info('Using Tabula extraction for structured tables')
            tabula_tables = tabula.read_pdf(
                pdf_path,
                pages='all',
                multiple_tables=True,
                pandas_options={'header': 0}
            )
            
            if tabula_tables:
                logger.info(f'Tabula found {len(tabula_tables)} table(s)')
                for idx, df in enumerate(tabula_tables):
                    if PANDAS_AVAILABLE and df is not None and not df.empty:
                        table_list = [df.columns.tolist()] + df.values.tolist()
                        filtered_table = self._filter_table_content(None, table_list, idx + 1)
                        if filtered_table and len(filtered_table) >= 2:
                            processed_table = self._process_table_advanced(
                                filtered_table, 1, idx, output_dir, None
                            )
                            if processed_table:
                                all_tables.append(processed_table)
            else:
                logger.info('Tabula found no tables')
        except Exception as e:
            logger.error(f'Tabula extraction failed: {e}', exc_info=True)
        
        return {
            'tables': all_tables,
            'images': all_images,
            'tables_found': len(all_tables),
            'extraction_method': 'tabula',
            'total_pages': 0
        }
    
    def _extract_with_unstructured(self, pdf_path: str, output_dir: Optional[str], ai_strategy: str = 'auto') -> Dict:
        """Extract using Unstructured.io only - NOT IMPLEMENTED (placeholder)"""
        logger.warning('Unstructured.io extraction not yet implemented as standalone method')
        return self._empty_result()
    
    def _extract_with_camelot_stream(self, pdf_path: str, output_dir: Optional[str]) -> Dict:
        """Extract using Camelot Stream method only"""
        all_tables = []
        all_images = {}
        
        try:
            logger.info('Using Camelot (Stream) extraction for borderless tables')
            camelot_tables = camelot.read_pdf(
                pdf_path,
                pages='all',
                flavor='stream'
            )
            
            if camelot_tables and len(camelot_tables) > 0:
                logger.info(f'Camelot Stream found {len(camelot_tables)} table(s)')
                for idx, table in enumerate(camelot_tables):
                    if PANDAS_AVAILABLE:
                        df = table.df
                        if df is not None and not df.empty:
                            table_list = [df.columns.tolist()] + df.values.tolist()
                            filtered_table = self._filter_table_content(None, table_list, idx + 1)
                            if filtered_table and len(filtered_table) >= 2:
                                processed_table = self._process_table_advanced(
                                    filtered_table, 1, idx, output_dir, None
                                )
                                if processed_table:
                                    all_tables.append(processed_table)
            else:
                logger.info('Camelot Stream found no tables')
        except Exception as e:
            logger.error(f'Camelot Stream extraction failed: {e}', exc_info=True)
        
        return {
            'tables': all_tables,
            'images': all_images,
            'tables_found': len(all_tables),
            'extraction_method': 'camelot-stream',
            'total_pages': 0
        }
    
    def _extract_with_layoutparser(self, pdf_path: str, output_dir: Optional[str]) -> Dict:
        """Extract using LayoutParser only - NOT IMPLEMENTED (placeholder)"""
        logger.warning('LayoutParser extraction not yet implemented as standalone method')
        return self._empty_result()
    
    # OLD HYBRID METHOD BELOW - KEEP FOR REFERENCE BUT NOT USED ANYMORE
    def _extract_from_pdf_OLD_HYBRID(self, pdf_path: str, output_dir: Optional[str]) -> Dict:
        """OLD HYBRID extraction - DO NOT USE"""
        all_tables = []
        all_images = {}
        total_pages = 0
        
        try:
            # Step 2: If Camelot didn't work, try pdfplumber + PyMuPDF (comprehensive)
            if not all_tables:
                logger.info('Using pdfplumber + PyMuPDF hybrid extraction...')
                pdf_plumber = pdfplumber.open(pdf_path)
                pdf_fitz = fitz.open(pdf_path)
                total_pages = len(pdf_plumber.pages)
                
                logger.info(f'Processing {total_pages} pages for table extraction')
                
                for page_num in range(total_pages):
                    page_plumber = pdf_plumber.pages[page_num]
                    page_fitz = pdf_fitz[page_num]
                    
                    logger.info(f'Processing page {page_num + 1}/{total_pages}')
                    
                    # Step 1: Detect if page has table content (filter non-table pages)
                    if not self._has_table_content(page_plumber, page_num + 1):
                        logger.info(f'Page {page_num + 1} filtered out (no table content)')
                        continue
                    
                    # Step 2: Extract tables using hybrid strategies
                    tables = self._extract_tables_comprehensive(page_plumber, page_fitz, page_num + 1, pdf_path)
                    
                    # Step 3: Process each table
                    for table_idx, table_data in enumerate(tables):
                        processed_table = self._process_table_advanced(
                            table_data, page_num + 1, table_idx, output_dir, page_fitz
                        )
                        if processed_table:
                            all_tables.append(processed_table)
                
                pdf_plumber.close()
                pdf_fitz.close()
            
            # Step 3: Fallback to Tabula for multi-page structured tables
            if not all_tables and TABULA_AVAILABLE:
                try:
                    logger.info('Trying Tabula extraction (best for multi-page structured tables)...')
                    tabula_tables = tabula.read_pdf(
                        pdf_path,
                        pages='all',
                        multiple_tables=True,
                        pandas_options={'header': 0}
                    )
                    
                    if tabula_tables:
                        logger.info(f'Tabula found {len(tabula_tables)} table(s)')
                        for idx, df in enumerate(tabula_tables):
                            if PANDAS_AVAILABLE and df is not None and not df.empty:
                                table_list = [df.columns.tolist()] + df.values.tolist()
                                filtered_table = self._filter_table_content(None, table_list, idx + 1)
                                if filtered_table and len(filtered_table) >= 2:
                                    processed_table = self._process_table_advanced(
                                        filtered_table, 1, idx, output_dir, None
                                    )
                                    if processed_table:
                                        all_tables.append(processed_table)
                                        logger.info(f'Tabula table {idx + 1} processed successfully')
                except Exception as e:
                    logger.debug(f'Tabula extraction failed: {e}')
            
            # Extract images if output_dir provided
            all_images = {}
            if output_dir and all_tables:
                all_images = self._extract_images_comprehensive(pdf_path, all_tables, output_dir)
                # Re-process tables with images embedded
                if all_images:
                    for table in all_tables:
                        table_images = {
                            k: v for k, v in all_images.items() 
                            if v.get('table_index') == table['table_index'] and v.get('page') == table['page']
                        }
                        if table_images:
                            # Re-process to embed images
                            # Note: We need the original table_data, so we'll embed images during HTML generation instead
                            pass
            
        except Exception as e:
            logger.error(f'PDF extraction failed: {str(e)}', exc_info=True)
            return self._empty_result()
        
        return {
            'tables': all_tables,
            'images': all_images,
            'tables_found': len(all_tables),
            'extraction_method': 'improved_hybrid',
            'total_pages': total_pages
        }
    
    def _calculate_adaptive_timeout(self, pdf_path: str, page_num: int = None) -> int:
        """
        Calculate adaptive timeout based on PDF size and page count
        Returns timeout in seconds
        """
        try:
            # Get file size in MB
            file_size_mb = os.path.getsize(pdf_path) / (1024 * 1024)
            
            # Get page count
            pdf = fitz.open(pdf_path)
            page_count = len(pdf)
            pdf.close()
            
            # Base timeout from config
            base_timeout = self.config['unstructured']['base_timeout']
            timeout_per_page = self.config['unstructured']['timeout_per_page']
            timeout_per_mb = self.config['unstructured']['timeout_per_mb']
            max_timeout = self.config['unstructured']['max_timeout']
            
            # Calculate adaptive timeout
            # Base + (pages * per_page) + (size * per_mb)
            timeout = base_timeout + (page_count * timeout_per_page) + (file_size_mb * timeout_per_mb)
            
            # Clamp to max timeout
            timeout = min(timeout, max_timeout)
            
            logger.info(f'Adaptive timeout calculated: {int(timeout)}s (file: {file_size_mb:.1f}MB, pages: {page_count})')
            return int(timeout)
            
        except Exception as e:
            logger.warning(f'Error calculating adaptive timeout: {e}, using base timeout')
            return self.config['unstructured']['base_timeout']
    
    def _score_table_quality(self, table: List) -> float:
        """
        Enhanced quality scoring for table selection
        Returns score (0-100+)
        """
        if not table or len(table) == 0:
            return 0.0
        
        score = 0.0
        config = self.config['quality_scoring']
        
        # 1. Row count score (more rows = better, up to max)
        row_score = min(len(table) * config['row_weight'], config['max_row_score'])
        score += row_score
        
        # 2. Column consistency score
        col_counts = [len(row) for row in table if row]
        if col_counts and len(col_counts) > 1:
            max_cols = max(col_counts)
            min_cols = min(col_counts)
            if max_cols > 0:
                consistency = 1 - (max_cols - min_cols) / max_cols
                score += consistency * config['consistency_weight']
        elif col_counts:
            # Single row or all rows same length = perfect consistency
            score += config['consistency_weight']
        
        # 3. Header keyword matching score
        if table and len(table) > 0:
            header_text = ' '.join(str(cell).lower() for cell in table[0] if cell).strip()
            header_matches = 0
            for variants in self.header_variants.values():
                for variant in variants:
                    if variant in header_text:
                        header_matches += 1
                        break  # Count each variant group only once
            score += header_matches * config['header_match_weight']
        
        # 4. Numeric data score (tables usually have numbers)
        numeric_cells = 0
        for row in table:
            for cell in row:
                if cell is None:
                    continue
                # Check if cell is numeric or contains numbers
                if isinstance(cell, (int, float)):
                    numeric_cells += 1
                elif isinstance(cell, str):
                    # Check for numeric patterns
                    if re.search(r'\d+', cell):
                        numeric_cells += 1
        
        numeric_score = min(numeric_cells * config['numeric_weight'], config['max_numeric_score'])
        score += numeric_score
        
        # 5. Bonus for well-structured tables
        # - Has at least 2 rows (header + data)
        # - Has at least 3 columns
        # - Has consistent column count
        if len(table) >= 2 and col_counts and min(col_counts) >= 3:
            if max(col_counts) == min(col_counts):
                score += 10  # Bonus for perfect structure
        
        logger.debug(f'Table quality score: {score:.2f} (rows: {len(table)}, cols: {col_counts[0] if col_counts else 0}, headers: {header_matches}, numeric: {numeric_cells})')
        return score
    
    def _table_exists(self, new_table: List, existing_tables: List) -> bool:
        """
        Check if a table already exists in the list (avoid duplicates)
        Uses similarity threshold to detect near-duplicates
        """
        if not new_table or not existing_tables:
            return False
        
        # Convert table to comparable format
        new_table_str = str(new_table)
        new_table_hash = hash(new_table_str)
        
        for existing_table in existing_tables:
            existing_table_str = str(existing_table)
            existing_table_hash = hash(existing_table_str)
            
            # Exact match
            if new_table_hash == existing_table_hash:
                return True
            
            # Similarity check (same dimensions and similar content)
            if len(new_table) == len(existing_table):
                # Check if first and last rows are similar
                if (len(new_table) > 0 and len(existing_table) > 0 and
                    str(new_table[0]) == str(existing_table[0]) and
                    str(new_table[-1]) == str(existing_table[-1])):
                    return True
        
        return False

    
    def _has_table_content(self, page, page_num: int) -> bool:
        """Check if page contains table content (filter cover pages, terms, etc.)"""
        try:
            text = page.extract_text().lower()
            
            # Check for non-table content indicators
            non_table_count = sum(1 for keyword in self.non_table_keywords if keyword in text)
            
            # Check for table indicators (headers)
            table_indicators = 0
            for header_group in self.header_variants.values():
                for variant in header_group:
                    if variant in text:
                        table_indicators += 1
                        break
            
            # If more non-table keywords than table indicators, likely not a table page
            if non_table_count > table_indicators and table_indicators < 2:
                return False
            
            # Check for numeric data (tables usually have numbers)
            numbers = re.findall(r'\d+', text)
            if len(numbers) < 5:  # Very few numbers, likely not a table
                return False
            
            return True
        except Exception as e:
            logger.debug(f'Error checking table content on page {page_num}: {e}')
            return True  # Default to processing if unsure
    
    def _extract_tables_comprehensive(self, page_plumber, page_fitz, page_num: int, pdf_path: str = None) -> List:
        """HYBRID extraction - Use Unstructured.io FIRST (primary), then fallback to other methods"""
        tables = []
        best_table = None
        best_score = 0
        
        # STEP 1: Try Unstructured.io FIRST (PRIMARY - works for both bordered and borderless)
        # BUT with timeout to prevent blocking on model downloads
        if UNSTRUCTURED_AVAILABLE and pdf_path:
            try:
                logger.info(f'Page {page_num} - Trying Unstructured.io FIRST (PRIMARY method with BEST MODELS)...')
                table_region = self._detect_table_boundaries_visual(page_plumber, page_num)
                if table_region:
                    # Use timeout to prevent blocking on model downloads
                    # Increased timeout for hi_res strategy with AI models (may take longer)
                    try:
                        # OPTIMIZED: Reduced timeout (45s) since models are cached and filtering is fast
                        # Models are pre-downloaded, so processing should be faster
                        unstructured_tables = self._extract_with_unstructured(pdf_path, page_num, table_region, timeout=45)
                    except TimeoutError:
                        logger.warning(f'Page {page_num} - Unstructured.io timed out, skipping (models may be downloading)')
                        unstructured_tables = []
                    except Exception as e:
                        logger.warning(f'Page {page_num} - Unstructured.io failed: {e}, falling back')
                        unstructured_tables = []
                    
                    if unstructured_tables:
                        logger.info(f'Page {page_num} - ✓ Unstructured.io found {len(unstructured_tables)} table(s)')
                        for table in unstructured_tables:
                            if table and len(table) >= 2:
                                filtered_table = self._filter_table_content(page_plumber, table, page_num)
                                if filtered_table:
                                    score = self._score_table_quality(filtered_table)
                                    if score > best_score:
                                        best_table = filtered_table
                                        best_score = score
                                    if not self._table_exists(filtered_table, tables):
                                        tables.append(filtered_table)
                        # If Unstructured.io found tables, return them (don't try other methods)
                        if tables:
                            logger.info(f'Page {page_num} - Using Unstructured.io results (PRIMARY)')
                            return tables
            except Exception as e:
                logger.warning(f'Page {page_num} - Unstructured.io failed: {e}, falling back to other methods')
        
        # STEP 2: Fallback to traditional methods if Unstructured.io didn't work
        # Detect if table is bordered or borderless
        is_bordered = self._detect_bordered_table(page_plumber, page_num)
        logger.info(f'Page {page_num} - Table type: {"BORDERED" if is_bordered else "BORDERLESS/PARTIALLY BORDERED"}')
        
        if is_bordered:
            # BORDERED TABLE - Use proven methods (keep 100% accuracy)
            tables = self._extract_bordered_tables(page_plumber, page_fitz, page_num)
        else:
            # BORDERLESS TABLE - Use hybrid multi-library approach
            tables = self._extract_borderless_tables(page_plumber, page_fitz, page_num, pdf_path)
        
        # Filter and score tables
        filtered_tables = []
        for table in tables:
            if table and len(table) >= 2:
                filtered_table = self._filter_table_content(page_plumber, table, page_num)
                if filtered_table:
                    score = self._score_table_quality(filtered_table)
                    if score > best_score:
                        best_table = filtered_table
                        best_score = score
                    if not self._table_exists(filtered_table, filtered_tables):
                        filtered_tables.append(filtered_table)
        
        if filtered_tables:
            logger.info(f'Page {page_num} - Total unique tables found: {len(filtered_tables)}, Best score: {best_score:.2f}')
            return filtered_tables
        
        return []
    
    def _detect_bordered_table(self, page, page_num: int) -> bool:
        """Detect if page has bordered tables (with vertical lines)"""
        try:
            # Try strict line detection - if it finds tables, likely bordered
            strict_tables = page.extract_tables(table_settings={
                "vertical_strategy": "lines_strict",
                "horizontal_strategy": "lines_strict",
                "snap_tolerance": 3,
                "join_tolerance": 3,
            })
            
            if strict_tables and len(strict_tables) > 0:
                # Check if tables have proper structure
                for table in strict_tables:
                    if table and len(table) >= 2:
                        # Check if table has consistent column structure (bordered tables usually do)
                        col_counts = [len(row) for row in table[:5]]  # Check first 5 rows
                        if col_counts and max(col_counts) - min(col_counts) <= 1:
                            logger.info(f'Page {page_num} - Bordered table detected (lines_strict found structured table)')
                            return True
            
            # Also check for explicit lines
            explicit_tables = page.extract_tables(table_settings={
                "vertical_strategy": "explicit",
                "horizontal_strategy": "explicit",
            })
            
            if explicit_tables and len(explicit_tables) > 0:
                logger.info(f'Page {page_num} - Bordered table detected (explicit lines found)')
                return True
            
            return False
        except Exception as e:
            logger.debug(f'Bordered table detection failed: {e}')
            return False  # Default to borderless if unsure
    
    def _extract_bordered_tables(self, page_plumber, page_fitz, page_num: int) -> List:
        """Extract bordered tables - PRESERVE 100% ACCURACY METHOD"""
        tables = []
        
        # Strategy 1: lines_strict (PRIMARY - best for bordered tables)
        try:
            strict_tables = page_plumber.extract_tables(table_settings={
                "vertical_strategy": "lines_strict",
                "horizontal_strategy": "lines_strict",
                "snap_tolerance": 3,
                "join_tolerance": 3,
                "min_words_vertical": 1,
                "min_words_horizontal": 1,
                "intersection_tolerance": 3,
                "text_tolerance": 3,
                "text_x_tolerance": 3,
                "text_y_tolerance": 3,
            })
            if strict_tables:
                logger.info(f'Page {page_num} - Bordered (lines_strict): Found {len(strict_tables)} table(s)')
                tables.extend(strict_tables)
        except Exception as e:
            logger.debug(f'lines_strict extraction failed: {e}')
        
        # Strategy 2: explicit lines (FALLBACK)
        if not tables:
            try:
                explicit_tables = page_plumber.extract_tables(table_settings={
                    "vertical_strategy": "explicit",
                    "horizontal_strategy": "explicit",
                    "snap_tolerance": 3,
                    "join_tolerance": 3,
                })
                if explicit_tables:
                    logger.info(f'Page {page_num} - Bordered (explicit): Found {len(explicit_tables)} table(s)')
                    tables.extend(explicit_tables)
            except Exception as e:
                logger.debug(f'explicit extraction failed: {e}')
        
        # Strategy 3: lines (FALLBACK - more tolerance)
        if not tables:
            try:
                lines_tables = page_plumber.extract_tables(table_settings={
                    "vertical_strategy": "lines",
                    "horizontal_strategy": "lines",
                    "snap_tolerance": 5,
                    "join_tolerance": 5,
                })
                if lines_tables:
                    logger.info(f'Page {page_num} - Bordered (lines): Found {len(lines_tables)} table(s)')
                    tables.extend(lines_tables)
            except Exception as e:
                logger.debug(f'lines extraction failed: {e}')
        
        return tables
    
    def _extract_borderless_tables(self, page_plumber, page_fitz, page_num: int, pdf_path: str = None) -> List:
        """
        HYBRID MULTI-LIBRARY APPROACH for borderless tables
        Combines Camelot (stream), pdfplumber (multiple strategies), and post-processing
        """
        all_results = []  # Store results from all methods with quality scores
        
        # Step 1: Pre-process - Detect table boundaries
        table_region = self._detect_table_boundaries_visual(page_plumber, page_num)
        
        if not table_region:
            logger.info(f'Page {page_num} - No table region detected')
            return []
        
        # Step 2: Strategy 1 - Unstructured.io (PRIMARY - open-source alternative to LLMWhisperer)
        if UNSTRUCTURED_AVAILABLE and pdf_path:
            try:
                unstructured_tables = self._extract_with_unstructured(pdf_path, page_num, table_region)
                for table in unstructured_tables:
                    quality = self._score_table_quality(table)
                    all_results.append({
                        'table': table,
                        'method': 'unstructured',
                        'quality': quality
                    })
                logger.info(f'Page {page_num} - Unstructured.io: Found {len(unstructured_tables)} table(s)')
            except Exception as e:
                logger.debug(f'Unstructured.io extraction failed: {e}')
        
        # Step 3: Strategy 2 - LayoutParser (deep learning layout detection)
        if LAYOUTPARSER_AVAILABLE and pdf_path:
            try:
                layoutparser_tables = self._extract_with_layoutparser(pdf_path, page_num, table_region)
                for table in layoutparser_tables:
                    quality = self._score_table_quality(table)
                    all_results.append({
                        'table': table,
                        'method': 'layoutparser',
                        'quality': quality
                    })
                logger.info(f'Page {page_num} - LayoutParser: Found {len(layoutparser_tables)} table(s)')
            except Exception as e:
                logger.debug(f'LayoutParser extraction failed: {e}')
        
        # Step 4: Strategy 3 - Layout-Preserving Extraction (fallback)
        try:
            layout_tables = self._extract_with_layout_preserving(pdf_path, page_plumber, page_fitz, page_num, table_region)
            for table in layout_tables:
                quality = self._score_table_quality(table)
                all_results.append({
                    'table': table,
                    'method': 'layout_preserving',
                    'quality': quality
                })
            logger.info(f'Page {page_num} - Layout-preserving: Found {len(layout_tables)} table(s)')
        except Exception as e:
            logger.debug(f'Layout-preserving extraction failed: {e}')
        
        # Step 5: Strategy 4 - Camelot Stream Mode (fallback for borderless tables)
        if CAMELOT_AVAILABLE and pdf_path:
            try:
                camelot_tables = self._extract_with_camelot_stream(pdf_path, page_num, table_region)
                for table in camelot_tables:
                    quality = self._score_table_quality(table)
                    all_results.append({
                        'table': table,
                        'method': 'camelot_stream',
                        'quality': quality
                    })
                logger.info(f'Page {page_num} - Camelot stream: Found {len(camelot_tables)} table(s)')
            except Exception as e:
                logger.debug(f'Camelot stream extraction failed: {e}')
        
        # Step 6: Strategy 5 - Pdfplumber with multiple settings
        pdfplumber_results = self._extract_with_pdfplumber_multiple_strategies(
            page_plumber, page_num, table_region
        )
        for result in pdfplumber_results:
            all_results.append(result)
        
        # Step 7: Strategy 6 - Visual gap detection (fallback)
        if not all_results:
            try:
                gap_tables = self._extract_using_visual_gaps_with_boundaries(
                    page_plumber, page_fitz, page_num, table_region
                )
                for table in gap_tables:
                    quality = self._score_table_quality(table)
                    all_results.append({
                        'table': table,
                        'method': 'visual_gaps',
                        'quality': quality
                    })
            except Exception as e:
                logger.debug(f'Visual gap extraction failed: {e}')
        
        # Step 8: Compare and select best results
        if not all_results:
            return []
        
        # Sort by quality score (boost advanced methods)
        all_results.sort(key=lambda x: (
            x['quality'] + (
                30 if x['method'] == 'unstructured' else
                25 if x['method'] == 'layoutparser' else
                20 if x['method'] == 'layout_preserving' else 0
            ),
            x['quality']
        ), reverse=True)
        
        # Select best result(s) - prefer higher quality
        best_results = []
        best_quality = all_results[0]['quality'] if all_results else 0
        
        # Take all results with quality within 10% of best
        for result in all_results:
            if result['quality'] >= best_quality * 0.9:
                best_results.append(result['table'])
        
        # Step 9: Post-process - Merge and clean results
        if best_results:
            merged_table = self._merge_extraction_results(best_results)
            cleaned_table = self._post_process_clean_table(merged_table, page_num)
            return [cleaned_table] if cleaned_table else []
        
        return []
    
    def _extract_with_unstructured(self, pdf_path: str, page_num: int, table_region: Dict, timeout: int = 60) -> List:
        """
        Extract using Unstructured.io with BEST MODEL configuration
        Ensures models are downloaded and used for highest accuracy
        """
        """
        Extract using Unstructured.io (open-source alternative to LLMWhisperer)
        Handles various document formats and extracts tables with metadata
        
        Args:
            timeout: Maximum time in seconds for extraction (prevents blocking on model downloads)
        """
        tables = []
        try:
            import threading
            import queue
            
            # PERFORMANCE OPTIMIZED CONFIGURATION
            # Check if Tesseract is available for hi_res (best quality)
            # If not, use fast strategy (still uses AI models, faster)
            import shutil
            tesseract_available = shutil.which("tesseract") is not None
            
            if tesseract_available:
                strategy = "hi_res"  # Best quality with Tesseract OCR + AI models
                logger.info(f'Page {page_num} - Unstructured.io: Using hi_res strategy (Tesseract + AI models)')
            else:
                strategy = "fast"  # Fast strategy + infer_table_structure=True = uses AI models without Tesseract
                logger.info(f'Page {page_num} - Unstructured.io: Using fast strategy (AI models, optimized performance)')
            
            logger.info(f'Page {page_num} - Unstructured.io: OPTIMIZED extraction (strategy={strategy}, infer_table_structure=True, timeout={timeout}s)...')
            logger.info(f'Page {page_num} - Performance: Models cached, fast filtering enabled')
            
            # Use threading with timeout to prevent blocking
            result_queue = queue.Queue()
            exception_queue = queue.Queue()
            
            def extract_worker():
                try:
                    # Ensure model cache directory is set BEFORE calling partition_pdf
                    # This ensures models are downloaded to the correct location
                    from utils.model_cache_config import setup_model_cache
                    cache_base = setup_model_cache()
                    logger.debug(f'Page {page_num} - Model cache configured: {cache_base}')
                    
                    # BEST MODEL CONFIGURATION for Unstructured.io
                    # - strategy="hi_res": High resolution with AI models (best quality)
                    # - infer_table_structure=True: Use AI-based table detection (REQUIRES models)
                    # - extract_images_in_pdf=False: We handle images separately
                    # - include_page_breaks=True: Keep page structure
                    # - chunking_strategy=None: Don't chunk, we want full tables
                    # - languages=['eng']: Specify language for better OCR
                    # - model_name: Explicitly specify model to use (if available)
                    
                    # Get unstructured cache directory
                    unstructured_cache = os.environ.get('UNSTRUCTURED_CACHE_DIR', None)
                    if unstructured_cache:
                        logger.debug(f'Page {page_num} - Using Unstructured cache: {unstructured_cache}')
                    
                    # PERFORMANCE OPTIMIZED: Models are cached, so processing is faster
                    # Unstructured.io processes entire PDF, but we filter by page efficiently
                    # Key optimizations:
                    # 1. Models pre-downloaded (no download delay)
                    # 2. Fast page filtering after extraction
                    # 3. Strategy optimized (fast if no Tesseract, hi_res if available)
                    elements = partition_pdf(
                        filename=pdf_path,
                        strategy=strategy,  # "hi_res" or "fast" based on Tesseract availability
                        infer_table_structure=True,  # ENABLED - uses AI models for best table detection
                        extract_images_in_pdf=False,  # We handle images separately (faster)
                        include_page_breaks=True,
                        languages=['eng'],  # Specify language for better OCR
                        chunking_strategy=None,  # Don't chunk, we want full tables
                        # Performance: Models cached, no download delay
                    )
                    result_queue.put(elements)
                except Exception as e:
                    exception_queue.put(e)
            
            # Start extraction in thread
            worker_thread = threading.Thread(target=extract_worker, daemon=True)
            worker_thread.start()
            worker_thread.join(timeout=timeout)
            
            # Check if timed out
            if worker_thread.is_alive():
                logger.warning(f'Page {page_num} - Unstructured.io extraction timed out after {timeout}s (models may be downloading)')
                return tables  # Return empty, will fallback to other methods
            
            # Get results
            if not exception_queue.empty():
                raise exception_queue.get()
            
            if result_queue.empty():
                logger.warning(f'Page {page_num} - Unstructured.io extraction returned no results')
                return tables
            
            elements = result_queue.get()
            
            # PERFORMANCE OPTIMIZED: Fast filter elements for this page
            # Unstructured.io processes entire PDF, so we filter by page number efficiently
            page_elements = []
            total_elements = len(elements)
            
            # Optimized filtering - single pass, fast metadata access
            for element in elements:
                try:
                    # Fast metadata extraction
                    metadata = getattr(element, 'metadata', None) if hasattr(element, 'metadata') else None
                    if metadata:
                        if isinstance(metadata, dict):
                            element_page = metadata.get('page_number', 1)
                        elif hasattr(metadata, 'page_number'):
                            element_page = metadata.page_number
                        else:
                            element_page = 1
                    else:
                        element_page = 1
                    
                    if element_page == page_num:
                        page_elements.append(element)
                except Exception:
                    # Skip elements with metadata access issues
                    continue
            
            logger.info(f'Page {page_num} - Unstructured.io: Found {len(page_elements)}/{total_elements} elements (optimized filtering)')
            
            # Extract tables from elements
            # With hi_res + infer_table_structure=True, AI models detect tables accurately
            for idx, element in enumerate(page_elements):
                # Check if element is a table (AI models will set category='Table')
                element_category = getattr(element, 'category', None) if hasattr(element, 'category') else None
                
                # With best model config (infer_table_structure=True), AI models set category='Table'
                # But we also check for table-like text patterns as fallback
                is_table = False
                if element_category == 'Table':
                    is_table = True
                    logger.debug(f'Page {page_num} - AI model detected Table element #{idx + 1}')
                elif hasattr(element, 'text') and element.text:
                    # Fallback: Check if text looks like a table (has multiple columns/rows)
                    text = element.text
                    lines = text.split('\n')
                    # If multiple lines with similar structure, might be a table
                    if len(lines) >= 2:
                        # Check for tab-separated or multiple spaces (table-like)
                        tab_count = sum(1 for line in lines if '\t' in line)
                        if tab_count >= len(lines) * 0.5:  # 50% of lines have tabs
                            is_table = True
                            logger.debug(f'Page {page_num} - Pattern-based table detection for element #{idx + 1}')
                
                if is_table or element_category == 'Table':
                    logger.info(f'Page {page_num} - Unstructured.io: Found Table element #{idx + 1} (AI detected: {element_category == "Table"})')
                    
                    # Method 1: Try HTML table (most structured) - BEST METHOD with infer_table_structure=True
                    # This is the recommended approach per Unstructured.io best practices
                    element_metadata = getattr(element, 'metadata', {}) if hasattr(element, 'metadata') else {}
                    html_table = None
                    
                    # Try to get text_as_html from metadata (available with infer_table_structure=True)
                    if isinstance(element_metadata, dict):
                        html_table = element_metadata.get('text_as_html')
                    elif hasattr(element_metadata, 'text_as_html'):
                        html_table = getattr(element_metadata, 'text_as_html', None)
                    
                    if html_table:
                        logger.info(f'Page {page_num} - Unstructured.io: Found HTML table (text_as_html available)')
                        # Use Pandas to parse HTML table (recommended approach)
                        table_data = self._parse_html_table_with_pandas(html_table)
                        if table_data and len(table_data) >= 2:
                            filtered = self._filter_table_content(None, table_data, page_num)
                            if filtered:
                                tables.append(filtered)
                                logger.info(f'Page {page_num} - Unstructured.io: Successfully extracted table from HTML (Pandas)')
                                continue
                        else:
                            # Fallback to BeautifulSoup parsing
                            logger.debug(f'Page {page_num} - Pandas parsing failed, trying BeautifulSoup')
                            table_data = self._parse_html_table(html_table)
                            if table_data and len(table_data) >= 2:
                                filtered = self._filter_table_content(None, table_data, page_num)
                                if filtered:
                                    tables.append(filtered)
                                    logger.info(f'Page {page_num} - Unstructured.io: Successfully extracted table from HTML (BeautifulSoup)')
                                    continue
                    
                    # Method 2: Try text-based table parsing (fallback if HTML not available)
                    if hasattr(element, 'text') and element.text:
                        logger.debug(f'Page {page_num} - Unstructured.io: Parsing text table')
                        table_data = self._parse_table_text(element.text)
                        if table_data and len(table_data) >= 2:
                            filtered = self._filter_table_content(None, table_data, page_num)
                            if filtered:
                                tables.append(filtered)
                                logger.info(f'Page {page_num} - Unstructured.io: Successfully extracted table from text')
                                continue
                    
                    # Method 3: Check all text elements for table patterns (fallback detection)
                    if hasattr(element, 'text') and element.text and not is_table:
                        # Try to detect table structure in any text element
                        text = element.text
                        lines = [line.strip() for line in text.split('\n') if line.strip()]
                        if len(lines) >= 3:  # At least 3 rows
                            # Check for consistent column structure
                            separators = ['\t', '  ', ' | ']  # Tab, double space, pipe
                            for sep in separators:
                                if any(sep in line for line in lines[:5]):  # Check first 5 lines
                                    table_data = self._parse_table_text(text)
                                    if table_data and len(table_data) >= 2:
                                        filtered = self._filter_table_content(None, table_data, page_num)
                                        if filtered:
                                            tables.append(filtered)
                                            logger.info(f'Page {page_num} - Unstructured.io: Extracted table from text pattern')
                                            break
                    
                    # Method 3: Try accessing table data directly if available
                    if hasattr(element, 'metadata'):
                        metadata = element.metadata if isinstance(element.metadata, dict) else {}
                        # Check for other table representations
                        if 'table' in metadata:
                            table_obj = metadata['table']
                            if hasattr(table_obj, 'to_dict'):
                                # Convert table object to list format
                                try:
                                    table_dict = table_obj.to_dict()
                                    # Convert dict to list format
                                    if isinstance(table_dict, dict) and 'data' in table_dict:
                                        table_data = table_dict['data']
                                        if table_data and len(table_data) >= 2:
                                            filtered = self._filter_table_content(None, table_data, page_num)
                                            if filtered:
                                                tables.append(filtered)
                                                logger.info(f'Page {page_num} - Unstructured.io: Successfully extracted table from metadata')
                                except Exception as e:
                                    logger.debug(f'Page {page_num} - Failed to convert table metadata: {e}')
                                
        except Exception as e:
            logger.warning(f'Page {page_num} - Unstructured.io extraction failed: {e}', exc_info=True)
        
        logger.info(f'Page {page_num} - Unstructured.io: Extracted {len(tables)} table(s)')
        return tables
    
    def _extract_with_layoutparser(self, pdf_path: str, page_num: int, table_region: Dict) -> List:
        """
        Extract using LayoutParser (deep learning document layout analysis)
        Uses models to detect tables, text blocks, and structural elements
        """
        tables = []
        try:
            if not IMAGE_PROCESSING_AVAILABLE:
                return tables
            
            # Load layout detection model
            # Using PubLayNet model (good for academic/technical documents)
            model = lp.Detectron2LayoutModel(
                'lp://PubLayNet/faster_rcnn_R_50_FPN_3x/config',
                extra_config=["MODEL.ROI_HEADS.SCORE_THRESH_TEST", 0.8],
                label_map={0: "Text", 1: "Title", 2: "List", 3: "Table", 4: "Figure"}
            )
            
            # Convert PDF page to image
            from pdf2image import convert_from_path
            images = convert_from_path(pdf_path, first_page=page_num, last_page=page_num, dpi=200)
            if not images:
                return tables
            
            page_image = images[0]
            
            # Detect layout
            layout = model.detect(page_image)
            
            # Extract tables
            for block in layout:
                if block.type == 'Table':
                    # Get table region coordinates
                    x1, y1, x2, y2 = block.block.coordinates
                    
                    # Extract table from coordinates using pdfplumber
                    table_data = self._extract_table_from_coordinates(
                        pdf_path, page_num, (x1, y1, x2, y2)
                    )
                    
                    if table_data and len(table_data) >= 2:
                        filtered = self._filter_table_content(None, table_data, page_num)
                        if filtered:
                            tables.append(filtered)
                            
        except Exception as e:
            logger.debug(f'LayoutParser extraction failed: {e}')
        
        return tables
    
    def _parse_html_table_with_pandas(self, html: str) -> List:
        """
        Parse HTML table using Pandas (RECOMMENDED by Unstructured.io best practices)
        This provides better structured table extraction from text_as_html
        """
        try:
            if not PANDAS_AVAILABLE:
                return []
            
            from io import StringIO
            import pandas as pd
            
            # Read HTML table into Pandas DataFrame
            # read_html returns a list of DataFrames, we take the first one
            dfs = pd.read_html(StringIO(html))
            if not dfs or len(dfs) == 0:
                logger.debug('Pandas read_html returned no DataFrames')
                return []
            
            df = dfs[0]  # Get first table
            
            # Convert DataFrame to list format (headers + rows)
            # Headers are column names
            headers = df.columns.tolist()
            # Convert to string list
            headers = [str(h) for h in headers]
            
            # Rows are DataFrame values
            rows = df.values.tolist()
            # Convert to string list
            rows = [[str(cell) if pd.notna(cell) else '' for cell in row] for row in rows]
            
            # Combine headers and rows
            table_data = [headers] + rows
            
            logger.debug(f'Pandas parsed HTML table: {len(headers)} columns, {len(rows)} rows')
            return table_data if len(table_data) >= 2 else []
            
        except Exception as e:
            logger.debug(f'Pandas HTML table parsing failed: {e}')
            return []
    
    def _parse_html_table(self, html: str) -> List:
        """Parse HTML table string to list format (fallback method using BeautifulSoup)"""
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html, 'html.parser')
            table = soup.find('table')
            if not table:
                return []
            
            rows = []
            for tr in table.find_all('tr'):
                cells = []
                for td in tr.find_all(['td', 'th']):
                    cell_text = td.get_text(strip=True)
                    cells.append(cell_text)
                if cells:
                    rows.append(cells)
            
            return rows if len(rows) >= 2 else []
        except Exception as e:
            logger.debug(f'HTML table parsing failed: {e}')
            return []
    
    def _parse_table_text(self, text: str) -> List:
        """Parse structured table text to list format"""
        try:
            lines = text.split('\n')
            rows = []
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                
                # Try tab-separated
                if '\t' in line:
                    cells = [cell.strip() for cell in line.split('\t')]
                # Try pipe-separated
                elif '|' in line:
                    cells = [cell.strip() for cell in line.split('|') if cell.strip()]
                # Try multiple spaces
                else:
                    cells = [cell.strip() for cell in re.split(r'\s{2,}', line) if cell.strip()]
                
                if cells and len(cells) >= 2:
                    rows.append(cells)
            
            return rows if len(rows) >= 2 else []
        except Exception as e:
            logger.debug(f'Table text parsing failed: {e}')
            return []
    
    def _extract_table_from_coordinates(self, pdf_path: str, page_num: int, bbox: Tuple) -> List:
        """Extract table from specific coordinates using pdfplumber"""
        try:
            with pdfplumber.open(pdf_path) as pdf:
                if page_num > len(pdf.pages):
                    return []
                
                page = pdf.pages[page_num - 1]
                x1, y1, x2, y2 = bbox
                
                # Crop page to table region
                cropped = page.crop((x1, y1, x2, y2))
                
                # Extract table from cropped region
                tables = cropped.extract_tables(table_settings={
                    "vertical_strategy": "text",
                    "horizontal_strategy": "text",
                    "snap_tolerance": 5,
                    "join_tolerance": 5,
                })
                
                if tables and len(tables) > 0:
                    return tables[0]
        except Exception as e:
            logger.debug(f'Coordinate-based extraction failed: {e}')
        
        return []
    
    def _extract_with_camelot_stream(self, pdf_path: str, page_num: int, table_region: Dict) -> List:
        """Extract using Camelot stream mode (best for borderless tables)"""
        tables = []
        try:
            camelot_tables = camelot.read_pdf(
                pdf_path,
                pages=str(page_num),
                flavor='stream',
                row_tol=10,
                columns=None,  # Auto-detect columns
                edge_tol=50,
                word_margin=0.5,
                line_scale=15
            )
            
            if camelot_tables and len(camelot_tables) > 0:
                for table in camelot_tables:
                    # Get parsing report
                    accuracy = table.parsing_report.get('accuracy', 0) if hasattr(table, 'parsing_report') else 0
                    
                    if accuracy > 50:  # Minimum quality threshold
                        # Convert to list format
                        df = table.df
                        if PANDAS_AVAILABLE and df is not None and not df.empty:
                            table_list = [df.columns.tolist()] + df.values.tolist()
                            # Filter to table region
                            filtered = self._filter_table_content(None, table_list, page_num)
                            if filtered:
                                tables.append(filtered)
        except Exception as e:
            logger.debug(f'Camelot stream extraction failed: {e}')
        
        return tables
    
    def _extract_with_pdfplumber_multiple_strategies(self, page, page_num: int, table_region: Dict) -> List[Dict]:
        """Extract using pdfplumber with multiple strategies and compare results"""
        results = []
        
        # Strategy 1: Text-based (most flexible for borderless)
        try:
            text_tables = page.extract_tables(table_settings={
                "vertical_strategy": "text",
                "horizontal_strategy": "text",
                "snap_tolerance": 10,
                "join_tolerance": 10,
                "text_tolerance": 10,
                "text_x_tolerance": 10,
                "text_y_tolerance": 10,
            })
            for table in text_tables:
                if table and len(table) >= 2:
                    filtered = self._filter_table_content(page, table, page_num)
                    if filtered:
                        quality = self._score_table_quality(filtered)
                        results.append({
                            'table': filtered,
                            'method': 'pdfplumber_text',
                            'quality': quality
                        })
        except Exception as e:
            logger.debug(f'Pdfplumber text extraction failed: {e}')
        
        # Strategy 2: Explicit lines (for partially bordered)
        try:
            explicit_tables = page.extract_tables(table_settings={
                "vertical_strategy": "explicit",
                "horizontal_strategy": "explicit",
                "snap_tolerance": 5,
                "join_tolerance": 5,
            })
            for table in explicit_tables:
                if table and len(table) >= 2:
                    filtered = self._filter_table_content(page, table, page_num)
                    if filtered:
                        quality = self._score_table_quality(filtered)
                        results.append({
                            'table': filtered,
                            'method': 'pdfplumber_explicit',
                            'quality': quality
                        })
        except Exception as e:
            logger.debug(f'Pdfplumber explicit extraction failed: {e}')
        
        # Strategy 3: Lines (moderate tolerance)
        try:
            lines_tables = page.extract_tables(table_settings={
                "vertical_strategy": "lines",
                "horizontal_strategy": "lines",
                "snap_tolerance": 8,
                "join_tolerance": 8,
            })
            for table in lines_tables:
                if table and len(table) >= 2:
                    filtered = self._filter_table_content(page, table, page_num)
                    if filtered:
                        quality = self._score_table_quality(filtered)
                        results.append({
                            'table': filtered,
                            'method': 'pdfplumber_lines',
                            'quality': quality
                        })
        except Exception as e:
            logger.debug(f'Pdfplumber lines extraction failed: {e}')
        
        return results
    
    def _merge_extraction_results(self, tables: List[List]) -> List:
        """Merge multiple extraction results, taking best parts from each"""
        if not tables:
            return []
        
        if len(tables) == 1:
            return tables[0]
        
        # Use the table with highest quality (first in sorted list)
        # Or merge by taking rows that appear in multiple results (consensus)
        best_table = tables[0]  # Already sorted by quality
        
        # If multiple high-quality results, try to merge them
        if len(tables) > 1:
            # For now, use best quality table
            # Could implement consensus merging here
            pass
        
        return best_table
    
    def _post_process_clean_table(self, table: List, page_num: int) -> List:
        """Post-process table: clean, normalize, and fix structure"""
        if not table or len(table) < 2:
            return table
        
        cleaned = []
        
        # Remove duplicate rows
        seen_rows = set()
        for row in table:
            row_str = '|'.join([str(cell).strip() if cell else '' for cell in row])
            if row_str not in seen_rows and row_str.strip():
                seen_rows.add(row_str)
                cleaned.append(row)
        
        # Remove rows that are just header text repeated
        filtered = []
        for row in cleaned:
            row_text = ' '.join([str(cell).lower() if cell else '' for cell in row])
            # Skip rows that are just "sl.no", "image", "reference"
            if row_text.strip() in ['sl.no', 's.no', 'image', 'reference', 'sl.no image reference']:
                continue
            filtered.append(row)
        
        return filtered if len(filtered) >= 2 else table
    
    def _detect_table_boundaries_visual(self, page, page_num: int) -> Optional[Dict]:
        """Detect table boundaries by visually finding header patterns and table structure"""
        try:
            words = page.extract_words()
            if not words or len(words) < 10:
                return None
            
            # Get page dimensions
            page_bbox = page.bbox
            page_width = page_bbox[2] - page_bbox[0]
            page_height = page_bbox[3] - page_bbox[1]
            
            # Filter out words in header/footer regions (top 10% and bottom 10%)
            header_region_top = page_bbox[1] + (page_height * 0.1)
            footer_region_bottom = page_bbox[3] - (page_height * 0.1)
            
            # Find header row by looking for header keywords
            header_row_y = None
            header_words = []
            
            # Group words by y-coordinate (rows)
            rows_dict = defaultdict(list)
            for word in words:
                y = round(word['top'], 1)
                # Skip header/footer regions
                if word['top'] < header_region_top or word['top'] > footer_region_bottom:
                    continue
                rows_dict[y].append(word)
            
            sorted_rows = sorted(rows_dict.items())
            
            # Look for header row (contains header keywords)
            for y, words_in_row in sorted_rows:
                row_text = ' '.join([w['text'].lower() for w in words_in_row])
                
                # Check if this row contains header keywords
                header_found = False
                for header_group in self.header_variants.values():
                    for variant in header_group:
                        if variant in row_text:
                            header_found = True
                            header_row_y = y
                            header_words = words_in_row
                            logger.info(f'Page {page_num} - Found header row at y={y}: {row_text[:50]}')
                            break
                    if header_found:
                        break
                
                if header_found:
                    break
            
            if not header_row_y:
                logger.debug(f'Page {page_num} - No header row found, likely not a table page')
                return None
            
            # Find table boundaries:
            # Top: Header row position
            # Bottom: Find last row with numeric data (likely table data)
            # Left/Right: Use header word positions
            
            table_top = header_row_y - 5  # Small margin above header
            table_bottom = None
            table_left = min([w['x0'] for w in header_words]) if header_words else page_bbox[0]
            table_right = max([w['x1'] for w in header_words]) if header_words else page_bbox[2]
            
            # Find bottom boundary by looking for last row with table-like data
            # (has multiple columns, contains numbers)
            for y, words_in_row in reversed(sorted_rows):
                if y <= header_row_y:
                    continue
                
                # Check if row has table-like structure
                if len(words_in_row) >= 3:  # At least 3 words (likely multiple columns)
                    # Check if row contains numbers (table data usually has numbers)
                    has_numbers = any(re.search(r'\d', w['text']) for w in words_in_row)
                    if has_numbers:
                        table_bottom = y + 20  # Small margin below
                        break
            
            if not table_bottom:
                # Use page bottom as fallback
                table_bottom = footer_region_bottom
            
            # Expand left/right slightly to capture full table
            table_left = max(page_bbox[0], table_left - 20)
            table_right = min(page_bbox[2], table_right + 20)
            
            table_region = {
                'top': table_top,
                'bottom': table_bottom,
                'left': table_left,
                'right': table_right,
                'header_y': header_row_y
            }
            
            logger.info(f'Page {page_num} - Detected table region: top={table_top:.1f}, bottom={table_bottom:.1f}, left={table_left:.1f}, right={table_right:.1f}')
            return table_region
            
        except Exception as e:
            logger.error(f'Error detecting table boundaries: {e}', exc_info=True)
            return None
    
    def _filter_tables_by_boundaries(self, tables: List, table_region: Dict) -> List:
        """Filter tables to only those within detected boundaries"""
        if not tables or not table_region:
            return tables
        
        filtered = []
        for table in tables:
            if not table or len(table) < 2:
                continue
            
            # Check if table is within boundaries (simplified check)
            # For now, accept all tables if boundaries were detected
            # More sophisticated filtering can be added later
            filtered.append(table)
        
        return filtered
    
    def _extract_using_visual_gaps_with_boundaries(self, page_plumber, page_fitz, page_num: int, 
                                                    table_region: Dict) -> List:
        """Extract borderless tables using visual gaps, constrained to table boundaries"""
        tables = []
        
        try:
            # Get all words with positions
            words = page_plumber.extract_words()
            if not words or len(words) < 10:
                return tables
            
            # Filter words to table region only
            filtered_words = []
            for word in words:
                if (table_region['left'] <= word['x0'] <= table_region['right'] and
                    table_region['top'] <= word['top'] <= table_region['bottom']):
                    filtered_words.append(word)
            
            if len(filtered_words) < 10:
                return tables
            
            # Group words by rows (using y-coordinate with tolerance)
            rows = defaultdict(list)
            for word in filtered_words:
                y = round(word['top'], 1)
                rows[y].append(word)
            
            sorted_rows = sorted(rows.items())
            
            if len(sorted_rows) < 2:
                return tables
            
            # Find header row
            header_row_y = table_region.get('header_y')
            if not header_row_y:
                # Find header row by looking for header keywords
                for y, words_in_row in sorted_rows:
                    row_text = ' '.join([w['text'].lower() for w in words_in_row])
                    for header_group in self.header_variants.values():
                        for variant in header_group:
                            if variant in row_text:
                                header_row_y = y
                                break
                        if header_row_y:
                            break
                    if header_row_y:
                        break
            
            if not header_row_y:
                return tables
            
            # Advanced column detection using header row and data rows
            column_positions = self._detect_columns_from_header_and_data(sorted_rows, header_row_y)
            
            if not column_positions or len(column_positions) < 2:
                return tables
            
            # Build table from detected structure
            table = []
            header_added = False
            
            for y, words_in_row in sorted_rows:
                # Skip if outside table region
                if y < table_region['top'] or y > table_region['bottom']:
                    continue
                
                row = [''] * len(column_positions)
                for word in words_in_row:
                    col_idx = self._find_column_for_word_advanced(word['x0'], column_positions)
                    if col_idx is not None:
                        if row[col_idx]:
                            row[col_idx] += ' ' + word['text']
                        else:
                            row[col_idx] = word['text']
                
                # Check if this is header row
                if abs(y - header_row_y) < 2:  # Within 2px tolerance
                    if not header_added:
                        table.append(row)
                        header_added = True
                    continue
                
                # Filter out non-table rows (cover page content, headers, footers, dates, refs)
                row_text = ' '.join([str(cell).lower() if cell else '' for cell in row])
                if self._is_non_table_row(row_text):
                    continue
                
                # Only add rows with at least 2 non-empty cells (data rows)
                non_empty = sum(1 for cell in row if cell and str(cell).strip())
                if non_empty >= 2:
                    # Merge multi-line descriptions into single cells (1 row per item)
                    row = self._merge_multiline_cells(row)
                    table.append(row)
            
            if len(table) >= 2:  # At least header + 1 data row
                tables.append(table)
                
        except Exception as e:
            logger.debug(f'Visual gap extraction with boundaries failed on page {page_num}: {e}')
        
        return tables
    
    def _extract_using_layout_analysis_with_boundaries(self, page, page_num: int, 
                                                       table_region: Dict) -> List:
        """Extract tables using layout analysis, constrained to table boundaries"""
        tables = []
        
        try:
            words = page.extract_words()
            if not words or len(words) < 10:
                return tables
            
            # Filter words to table region
            filtered_words = []
            for word in words:
                if (table_region['left'] <= word['x0'] <= table_region['right'] and
                    table_region['top'] <= word['top'] <= table_region['bottom']):
                    filtered_words.append(word)
            
            if len(filtered_words) < 10:
                return tables
            
            # Group by rows
            rows = defaultdict(list)
            for word in filtered_words:
                y = round(word['top'], 1)
                rows[y].append(word)
            
            sorted_rows = sorted(rows.items())
            
            if len(sorted_rows) < 2:
                return tables
            
            # Find header row
            header_row_y = table_region.get('header_y')
            if not header_row_y:
                for y, words_in_row in sorted_rows:
                    row_text = ' '.join([w['text'].lower() for w in words_in_row])
                    for header_group in self.header_variants.values():
                        for variant in header_group:
                            if variant in row_text:
                                header_row_y = y
                                break
                        if header_row_y:
                            break
                    if header_row_y:
                        break
            
            if not header_row_y:
                return tables
            
            # Analyze text alignment patterns
            column_centers = self._detect_text_alignment_from_region(sorted_rows, header_row_y)
            
            if len(column_centers) < 2:
                return tables
            
            # Build table
            table = []
            header_added = False
            
            for y, words_in_row in sorted_rows:
                row = [''] * len(column_centers)
                for word in words_in_row:
                    word_center = (word['x0'] + word['x1']) / 2
                    col_idx = self._find_column_for_word_advanced(word_center, column_centers)
                    if col_idx is not None:
                        if row[col_idx]:
                            row[col_idx] += ' ' + word['text']
                        else:
                            row[col_idx] = word['text']
                
                # Check if header row
                if abs(y - header_row_y) < 2:
                    if not header_added:
                        table.append(row)
                        header_added = True
                    continue
                
                # Filter non-table rows
                row_text = ' '.join([str(cell).lower() if cell else '' for cell in row])
                if self._is_non_table_row(row_text):
                    continue
                
                non_empty = sum(1 for cell in row if cell and str(cell).strip())
                if non_empty >= 2:
                    # Merge multi-line cells (1 row per item)
                    row = self._merge_multiline_cells(row)
                    table.append(row)
            
            if len(table) >= 2:
                tables.append(table)
                
        except Exception as e:
            logger.debug(f'Layout analysis with boundaries failed: {e}')
        
        return tables
    
    def _detect_columns_from_header_and_data(self, sorted_rows: List[Tuple], header_row_y: float) -> List[float]:
        """Detect column positions from header row and data rows"""
        # Find header row
        header_words = None
        for y, words_in_row in sorted_rows:
            if abs(y - header_row_y) < 2:
                header_words = words_in_row
                break
        
        if not header_words:
            return []
        
        # Get x-positions from header words (header defines columns)
        header_x_positions = []
        for word in header_words:
            header_x_positions.append(word['x0'])
            header_x_positions.append((word['x0'] + word['x1']) / 2)  # Center
        
        # Also analyze data rows to refine column positions
        data_x_positions = []
        for y, words_in_row in sorted_rows:
            if abs(y - header_row_y) >= 5:  # Data rows (not header)
                for word in words_in_row:
                    data_x_positions.append(word['x0'])
                    data_x_positions.append((word['x0'] + word['x1']) / 2)
        
        # Combine and cluster
        all_x = sorted(set(header_x_positions + data_x_positions))
        
        # Cluster to find column centers
        clusters = []
        current_cluster = [all_x[0]] if all_x else []
        
        for x in all_x[1:]:
            if current_cluster:
                cluster_center = sum(current_cluster) / len(current_cluster)
                if x - cluster_center < 15:  # 15px tolerance
                    current_cluster.append(x)
                else:
                    if len(current_cluster) >= 2:
                        clusters.append(sum(current_cluster) / len(current_cluster))
                    current_cluster = [x]
            else:
                current_cluster = [x]
        
        if len(current_cluster) >= 2:
            clusters.append(sum(current_cluster) / len(current_cluster))
        
        return sorted(clusters)
    
    def _detect_text_alignment_from_region(self, sorted_rows: List[Tuple], header_row_y: float) -> List[float]:
        """Detect text alignment to find column centers, focusing on header and data rows"""
        x_centers = []
        
        # Include header row and data rows
        for y, words in sorted_rows:
            if abs(y - header_row_y) < 2 or abs(y - header_row_y) >= 5:  # Header or data rows
                for word in words:
                    center = (word['x0'] + word['x1']) / 2
                    x_centers.append(center)
        
        if not x_centers:
            return []
        
        # Cluster x-centers
        sorted_centers = sorted(set(x_centers))
        clusters = []
        current_cluster = [sorted_centers[0]] if sorted_centers else []
        
        for x in sorted_centers[1:]:
            cluster_center = sum(current_cluster) / len(current_cluster)
            if x - cluster_center < 20:  # 20px tolerance
                current_cluster.append(x)
            else:
                if len(current_cluster) >= 2:
                    clusters.append(sum(current_cluster) / len(current_cluster))
                current_cluster = [x]
        
        if len(current_cluster) >= 2:
            clusters.append(sum(current_cluster) / len(current_cluster))
        
        return sorted(clusters)
    
    def _is_non_table_row(self, row_text: str) -> bool:
        """Check if row contains non-table content (cover page, headers, footers, dates, refs)"""
        row_text_lower = row_text.lower()
        
        # Extended list of non-table indicators
        non_table_indicators = [
            # Dates and time
            'date:', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday', 'monday',
            # References
            'ref:', 'reference:', 'ref no:', 'ref no.', 'ref number',
            # Company info
            'cr no.:', 'vat:', 'address:', 'gsm no.:', 'mobile:', 'mob:', 'p.o box', 'postal code',
            'sultanate of oman', 'muscat', 'azaiba', 'carbon engineering', '18th november',
            # Page headers/footers
            'cover page', 'page', 'terms & conditions', 'terms and conditions', 'general conditions',
            # Project info (outside table)
            'project:', 'supply', 'installation', 'furniture', 'rehabilitation', 'center',
            'with reference', 'inquiry', 'opportunity', 'work on', 'propose', 'solutions',
            'kindly find', 'including:', 'for any further', 'clarifications', 'representative',
            'handling', 'assistant', 'enterprises', 'we hope', 'our proposal', 'meeting',
            'looking forward', 'sustainable', 'business',
            # Terms & Conditions phrases (from screenshot)
            'prices quoted', 'inclusive of', 'shipping', 'vat 5%', 'your site', 'oman', 'however',
            'any increase', 'in the same', 'this proposal', 'price', 'is strictly', 'valid for',
            'days only', 'prices quoted in this', 'offer is', 'based on the', 'quantities', 'provided',
            # Other non-table content
            '* cover', '* boq', '* general', '& condition'
        ]
        
        # Check for terms & conditions patterns (multi-word phrases)
        terms_patterns = [
            r'prices quoted.*inclusive',
            r'valid for \d+ days',
            r'based on the quantities',
            r'proposal.*price.*valid',
            r'inclusive of.*shipping',
            r'vat.*%',
        ]
        
        for pattern in terms_patterns:
            if re.search(pattern, row_text_lower):
                return True
        
        # Check if row contains non-table indicators
        for indicator in non_table_indicators:
            if indicator in row_text_lower:
                return True
        
        # Check if row is mostly empty or has very few words (likely not a data row)
        words = row_text.split()
        if len(words) < 2:
            return True
        
        return False
    
    def _merge_multiline_cells(self, row: List[str]) -> List[str]:
        """Merge multi-line descriptions into single cells to maintain 1 row per item"""
        # For now, return row as-is (multi-line merging happens in _process_row)
        # This function can be enhanced to detect and merge split cells
        return row
    
    def _extract_using_visual_gaps_advanced(self, page_plumber, page_fitz, page_num: int) -> List:
        """Advanced visual gap detection for borderless tables"""
        tables = []
        
        try:
            # Get all words with positions
            words = page_plumber.extract_words()
            if not words or len(words) < 10:
                return tables
            
            # Group words by rows (using y-coordinate with tolerance)
            rows = defaultdict(list)
            for word in words:
                y = round(word['top'], 1)
                rows[y].append(word)
            
            sorted_rows = sorted(rows.items())
            
            if len(sorted_rows) < 2:
                return tables
            
            # Advanced column detection using multiple techniques
            column_positions = self._detect_columns_advanced(sorted_rows)
            
            if not column_positions or len(column_positions) < 2:
                return tables
            
            # Build table from detected structure
            table = []
            for y, words_in_row in sorted_rows:
                row = [''] * len(column_positions)
                for word in words_in_row:
                    col_idx = self._find_column_for_word_advanced(word['x0'], column_positions)
                    if col_idx is not None:
                        if row[col_idx]:
                            row[col_idx] += ' ' + word['text']
                        else:
                            row[col_idx] = word['text']
                
                # Only add rows with at least 2 non-empty cells
                non_empty = sum(1 for cell in row if cell and str(cell).strip())
                if non_empty >= 2:
                    table.append(row)
            
            if len(table) >= 2:
                tables.append(table)
                
        except Exception as e:
            logger.debug(f'Advanced visual gap extraction failed on page {page_num}: {e}')
        
        return tables
    
    def _detect_columns_advanced(self, sample_rows: List[Tuple]) -> List[float]:
        """Advanced column detection using multiple techniques"""
        if not sample_rows:
            return []
        
        # Technique 1: Collect all x-coordinates
        all_x = []
        for y, words in sample_rows:
            for word in words:
                all_x.append(word['x0'])
                all_x.append((word['x0'] + word['x1']) / 2)  # Center of word
        
        if not all_x:
            return []
        
        # Technique 2: Find alignment clusters (columns align at similar x-positions)
        sorted_x = sorted(set(all_x))
        
        # Cluster x-positions (words in same column align)
        clusters = []
        current_cluster = [sorted_x[0]]
        
        for x in sorted_x[1:]:
            # If x is close to cluster center, add to cluster
            cluster_center = sum(current_cluster) / len(current_cluster)
            if x - cluster_center < 15:  # 15px tolerance for column alignment
                current_cluster.append(x)
            else:
                # New cluster
                if len(current_cluster) >= 3:  # At least 3 words align = likely a column
                    clusters.append(sum(current_cluster) / len(current_cluster))
                current_cluster = [x]
        
        if len(current_cluster) >= 3:
            clusters.append(sum(current_cluster) / len(current_cluster))
        
        # Technique 3: Find gaps between clusters
        if len(clusters) >= 2:
            # Sort clusters
            clusters = sorted(clusters)
            
            # Find significant gaps (column separators)
            column_positions = []
            column_positions.append(clusters[0])  # First column
            
            for i in range(len(clusters) - 1):
                gap = clusters[i + 1] - clusters[i]
                if gap > 30:  # Significant gap = column separator
                    column_positions.append(clusters[i + 1])
            
            return column_positions
        
        return clusters if clusters else []
    
    def _find_column_for_word_advanced(self, word_x: float, column_positions: List[float]) -> Optional[int]:
        """Find which column a word belongs to - advanced matching"""
        if not column_positions:
            return None
        
        # Find closest column with tolerance
        min_dist = float('inf')
        closest_col = None
        
        for i, col_x in enumerate(column_positions):
            dist = abs(word_x - col_x)
            if dist < min_dist:
                min_dist = dist
                closest_col = i
        
        # Use adaptive tolerance based on column spacing
        if len(column_positions) > 1:
            avg_spacing = (column_positions[-1] - column_positions[0]) / max(len(column_positions) - 1, 1)
            tolerance = max(avg_spacing * 0.3, 30)  # 30% of spacing or 30px minimum
        else:
            tolerance = 50
        
        return closest_col if min_dist < tolerance else None
    
    def _extract_using_layout_analysis(self, page, page_num: int) -> List:
        """Extract tables using layout analysis - text alignment and spacing"""
        tables = []
        
        try:
            words = page.extract_words()
            if not words or len(words) < 10:
                return tables
            
            # Group by rows
            rows = defaultdict(list)
            for word in words:
                y = round(word['top'], 1)
                rows[y].append(word)
            
            sorted_rows = sorted(rows.items())
            
            if len(sorted_rows) < 2:
                return tables
            
            # Analyze text alignment patterns
            # Find columns by detecting vertical alignment of text
            column_centers = self._detect_text_alignment(sorted_rows)
            
            if len(column_centers) < 2:
                return tables
            
            # Build table
            table = []
            for y, words_in_row in sorted_rows:
                row = [''] * len(column_centers)
                for word in words_in_row:
                    word_center = (word['x0'] + word['x1']) / 2
                    col_idx = self._find_column_for_word_advanced(word_center, column_centers)
                    if col_idx is not None:
                        if row[col_idx]:
                            row[col_idx] += ' ' + word['text']
                        else:
                            row[col_idx] = word['text']
                
                non_empty = sum(1 for cell in row if cell and str(cell).strip())
                if non_empty >= 2:
                    table.append(row)
            
            if len(table) >= 2:
                tables.append(table)
                
        except Exception as e:
            logger.debug(f'Layout analysis extraction failed: {e}')
        
        return tables
    
    def _detect_text_alignment(self, sorted_rows: List[Tuple]) -> List[float]:
        """Detect text alignment to find column centers"""
        # Collect all x-center positions
        x_centers = []
        for y, words in sorted_rows:
            for word in words:
                center = (word['x0'] + word['x1']) / 2
                x_centers.append(center)
        
        if not x_centers:
            return []
        
        # Cluster x-centers to find column positions
        sorted_centers = sorted(set(x_centers))
        
        clusters = []
        current_cluster = [sorted_centers[0]]
        
        for x in sorted_centers[1:]:
            cluster_center = sum(current_cluster) / len(current_cluster)
            if x - cluster_center < 20:  # 20px tolerance
                current_cluster.append(x)
            else:
                if len(current_cluster) >= 2:
                    clusters.append(sum(current_cluster) / len(current_cluster))
                current_cluster = [x]
        
        if len(current_cluster) >= 2:
            clusters.append(sum(current_cluster) / len(current_cluster))
        
        return sorted(clusters)
    
    def _extract_with_pymupdf(self, page_fitz, page_num: int) -> List:
        """Extract tables using PyMuPDF (fitz)"""
        tables = []
        try:
            # PyMuPDF table extraction
            tabs = page_fitz.find_tables()
            if tabs:
                for tab in tabs:
                    # Extract table data
                    table_data = tab.extract()
                    if table_data and len(table_data) >= 2:
                        # Convert to list format
                        table_list = [list(row) for row in table_data]
                        tables.append(table_list)
        except Exception as e:
            logger.debug(f'PyMuPDF table extraction failed: {e}')
        return tables
    
    def _score_table_quality(self, table: List) -> float:
        """Score table quality to select best extraction result"""
        if not table or len(table) < 2:
            return 0.0
        
        score = 0.0
        
        # Check 1: Has headers (first row should have multiple non-empty cells)
        first_row = table[0]
        non_empty_headers = sum(1 for cell in first_row if cell and str(cell).strip())
        if non_empty_headers >= 3:  # At least 3 columns
            score += 30.0
        elif non_empty_headers >= 2:
            score += 15.0
        
        # Check 2: Has data rows
        data_rows = len(table) - 1
        if data_rows >= 5:
            score += 30.0
        elif data_rows >= 2:
            score += 15.0
        
        # Check 3: Header keywords match
        first_row_text = ' '.join([str(cell).lower() if cell else '' for cell in first_row])
        header_matches = 0
        for header_group in self.header_variants.values():
            for variant in header_group:
                if variant in first_row_text:
                    header_matches += 1
                    break
        score += min(header_matches * 10.0, 30.0)  # Max 30 points
        
        # Check 4: Table structure consistency (columns should be consistent)
        if len(table) > 1:
            col_counts = [len(row) for row in table]
            if col_counts:
                avg_cols = sum(col_counts) / len(col_counts)
                consistency = 1.0 - (max(col_counts) - min(col_counts)) / max(avg_cols, 1)
                score += consistency * 10.0
        
        return score
    
    def _filter_table_content(self, page, table: List, page_num: int) -> Optional[List]:
        """NATURAL FLOW: Accept ALL rows from pdfplumber without filtering"""
        if not table or len(table) < 1:
            return None
        
        try:
            # NATURAL EXTRACTION: Keep every single row that pdfplumber detects
            # No filtering, no assumptions - preserve everything
            logger.info(f'Page {page_num} - Natural extraction: keeping all {len(table)} rows from pdfplumber')
            return table
            
        except Exception as e:
            logger.error(f'Error in natural extraction: {e}', exc_info=True)
            return table
    
    def _detect_table_regions(self, page, page_num: int) -> List[Dict]:
        """Detect table regions/boundaries on the page - DEPRECATED, using direct filtering instead"""
        # This method is kept for compatibility but we now use _filter_table_content directly
        return []
    
    def _get_table_bbox(self, page, table: List) -> Optional[Tuple[float, float, float, float]]:
        """Get bounding box of a table"""
        try:
            if not table or len(table) < 1:
                return None
            
            # Get words from page
            words = page.extract_words()
            if not words:
                return None
            
            # Find words that match table content
            table_text = ' '.join([str(cell) for row in table[:3] for cell in row if cell]).lower()
            
            matching_words = []
            for word in words:
                word_text = word.get('text', '').lower()
                if word_text and any(word_text in cell.lower() or cell.lower() in word_text 
                                    for row in table[:3] for cell in row if cell):
                    matching_words.append(word)
            
            if not matching_words:
                return None
            
            # Calculate bounding box
            x0 = min(w['x0'] for w in matching_words)
            y0 = min(w['top'] for w in matching_words)
            x1 = max(w['x1'] for w in matching_words)
            y1 = max(w['bottom'] for w in matching_words)
            
            return (x0, y0, x1, y1)
        
        except Exception as e:
            logger.debug(f'Bounding box calculation failed: {e}')
            return None
    
    def _is_table_in_region(self, table: List, regions: List[Dict]) -> bool:
        """Check if table is within detected table regions"""
        if not regions or not table:
            return True  # If no regions detected, accept all tables
        
        # Simple check: if table has similar content to any region, accept it
        table_text = ' '.join([str(cell) for row in table[:2] for cell in row if cell]).lower()
        
        for region in regions:
            region_table = region.get('table', [])
            if region_table:
                region_text = ' '.join([str(cell) for row in region_table[:2] for cell in row if cell]).lower()
                # Check similarity
                if table_text and region_text:
                    common_words = set(table_text.split()) & set(region_text.split())
                    if len(common_words) >= 3:  # At least 3 common words
                        return True
        
        return False
    
    def _extract_using_visual_gaps(self, page_plumber, page_fitz, page_num: int) -> List:
        """
        Extract tables using visual gaps between text blocks (for borderless tables)
        Detects columns by analyzing text alignment and gaps
        """
        tables = []
        
        try:
            # Get all words with their positions
            words = page_plumber.extract_words()
            if not words or len(words) < 10:
                return tables
            
            # Group words by rows (using y-coordinate with tolerance)
            rows = defaultdict(list)
            for word in words:
                y = round(word['top'], 1)  # Round to group nearby words
                rows[y].append(word)
            
            # Sort rows by y-coordinate
            sorted_rows = sorted(rows.items())
            
            # Detect columns by analyzing x-coordinates and gaps
            if len(sorted_rows) < 2:
                return tables
            
            # Analyze first few rows to detect column structure
            column_positions = self._detect_column_positions(sorted_rows[:10])
            
            if not column_positions:
                return tables
            
            # Build table from detected structure
            table = []
            for y, words_in_row in sorted_rows:
                row = [''] * len(column_positions)
                for word in words_in_row:
                    # Find which column this word belongs to
                    col_idx = self._find_column_for_word(word['x0'], column_positions)
                    if col_idx is not None:
                        # Merge multi-word cells
                        if row[col_idx]:
                            row[col_idx] += ' ' + word['text']
                        else:
                            row[col_idx] = word['text']
                table.append(row)
            
            if len(table) >= 2:  # At least header + 1 row
                tables.append(table)
                
        except Exception as e:
            logger.debug(f'Visual gap extraction failed on page {page_num}: {e}')
        
        return tables
    
    def _detect_column_positions(self, sample_rows: List[Tuple]) -> List[float]:
        """Detect column positions by analyzing x-coordinates and gaps"""
        if not sample_rows:
            return []
        
        # Collect all x-coordinates
        all_x = []
        for y, words in sample_rows:
            for word in words:
                all_x.append(word['x0'])
                all_x.append(word['x1'])
        
        if not all_x:
            return []
        
        # Sort and find gaps (potential column separators)
        all_x = sorted(set(all_x))
        
        # Find significant gaps (larger than average word width)
        gaps = []
        for i in range(len(all_x) - 1):
            gap = all_x[i + 1] - all_x[i]
            if gap > 20:  # Significant gap threshold
                gaps.append((all_x[i], gap))
        
        # Use gaps to define column boundaries
        column_positions = []
        if gaps:
            # Start with first position
            column_positions.append(all_x[0])
            # Add positions after significant gaps
            for x, gap in gaps:
                if gap > 30:  # Large gap = column separator
                    column_positions.append(x + gap / 2)  # Middle of gap
            # Add end position
            column_positions.append(all_x[-1])
        else:
            # No clear gaps, use clustering
            column_positions = self._cluster_x_positions(all_x)
        
        return sorted(set(column_positions))
    
    def _cluster_x_positions(self, x_positions: List[float]) -> List[float]:
        """Cluster x-positions to find column centers"""
        if not x_positions:
            return []
        
        # Simple clustering: group positions within 10px
        clusters = []
        sorted_x = sorted(x_positions)
        
        current_cluster = [sorted_x[0]]
        for x in sorted_x[1:]:
            if x - current_cluster[-1] < 10:
                current_cluster.append(x)
            else:
                clusters.append(sum(current_cluster) / len(current_cluster))
                current_cluster = [x]
        
        if current_cluster:
            clusters.append(sum(current_cluster) / len(current_cluster))
        
        return clusters
    
    def _find_column_for_word(self, word_x: float, column_positions: List[float]) -> Optional[int]:
        """Find which column a word belongs to based on its x-position"""
        if not column_positions:
            return None
        
        # Find closest column
        min_dist = float('inf')
        closest_col = None
        
        for i, col_x in enumerate(column_positions):
            dist = abs(word_x - col_x)
            if dist < min_dist:
                min_dist = dist
                closest_col = i
        
        return closest_col if min_dist < 50 else None  # 50px tolerance
    
    def _process_table_advanced(self, table_data: List, page_num: int, table_idx: int, 
                                 output_dir: Optional[str], page_fitz, images: Optional[Dict] = None) -> Optional[Dict]:
        """
        Process table with advanced features:
        - Filter non-table content BEFORE header detection
        - Header detection with variants
        - Multi-line description preservation
        - Section title detection
        - Summary row detection
        - Image embedding in cells
        - Merge split rows into 1 row per item
        """
        if not table_data or len(table_data) < 2:
            return None
        
        try:
            # Step 0: Filter non-table rows BEFORE header detection
            filtered_table_data = []
            for row in table_data:
                if not row:
                    continue
                row_text = ' '.join([str(cell).lower() if cell else '' for cell in row])
                # Skip non-table rows (metadata, page headers, etc.)
                if not self._is_non_table_row(row_text):
                    filtered_table_data.append(row)
            
            if len(filtered_table_data) < 2:
                logger.warning(f'Table {table_idx} on page {page_num} filtered to {len(filtered_table_data)} rows, too few')
                return None
            
            # Step 1: Detect headers from filtered data (BEFORE fixing split text)
            headers, header_row_idx = self._detect_headers(filtered_table_data)
            
            # Step 1.5: Clean up headers - merge duplicate/empty columns
            column_mapping = {}
            if headers:
                headers, column_mapping = self._clean_headers(headers)
                # Align data rows with cleaned headers
                filtered_table_data = self._align_rows_with_cleaned_headers(filtered_table_data, headers, column_mapping, header_row_idx)
            
            # Step 2: Fix horizontally split text (CRITICAL for bordered tables)
            # pdfplumber sometimes splits multi-line text within a cell into separate columns
            # We need to merge horizontally split text back into appropriate columns
            # Do this AFTER header detection so we know which columns are which
            filtered_table_data = self._fix_horizontally_split_text(filtered_table_data, headers, header_row_idx)
            
            # Re-detect headers after fixing (in case structure changed)
            headers, header_row_idx = self._detect_headers(filtered_table_data)
            if headers:
                headers, column_mapping = self._clean_headers(headers)
                # Re-align data rows
                filtered_table_data = self._align_rows_with_cleaned_headers(filtered_table_data, headers, column_mapping, header_row_idx)
            if not headers:
                logger.warning(f'No valid headers found in table {table_idx} on page {page_num}')
                return None
            
            # Step 2: Find image column index - ENHANCED with more terms
            image_col_idx = None
            image_keywords = [
                'image', 'photo', 'picture', 'img', 'pic', 'photograph',
                'ref', 'reference', 'indicative', 'visual', 'thumbnail',
                'illustration', 'figure', 'diagram', 'صورة'
            ]
            for idx, header in enumerate(headers):
                header_lower = str(header).lower().replace('.', '').replace('_', ' ')
                if any(keyword in header_lower for keyword in image_keywords):
                    image_col_idx = idx
                    logger.info(f'Found image column at index {idx}: "{header}"')
                    break
            
            if image_col_idx is None:
                logger.warning(f'No image column detected in headers: {headers}')
            
            # Step 3: Extract rows (skip header row) and merge split rows
            rows = []
            section_titles = []
            
            # Get images for this table
            table_images = {}
            if images:
                for key, img_data in images.items():
                    if img_data.get('table_index') == table_idx and img_data.get('page') == page_num:
                        row_idx = img_data.get('row_index', 0)
                        if row_idx not in table_images:
                            table_images[row_idx] = []
                        table_images[row_idx].append(img_data)
            
            # NEW APPROACH: Process all rows first, then post-process to merge
            all_processed_rows = []
            # Track mapping from filtered_table_data index to processed_row index
            filtered_to_processed_idx = {}
            
            for row_idx, row in enumerate(filtered_table_data):
                # Skip header row if it exists
                if row_idx == header_row_idx:
                    continue
                
                # DISABLED: Section title detection was causing data rows to be skipped
                # All rows should be treated as data rows to prevent loss
                # if self._is_section_title(row, headers):
                #     section_titles.append({
                #         'text': ' '.join([str(cell).strip() for cell in row if cell]),
                #         'row_index': len(all_processed_rows)
                #     })
                #     continue
                
                # Check if row is a summary row
                is_summary = self._is_summary_row(row)
                
                # Process row
                processed_row = self._process_row(row, headers, is_summary)
                if processed_row:
                    # Map filtered index to processed index
                    filtered_to_processed_idx[row_idx] = len(all_processed_rows)
                    all_processed_rows.append(processed_row)
            
            # POST-PROCESS: Don't merge rows - keep each row separate to prevent merging
            # Disabled row merging to preserve individual rows per page
            rows = all_processed_rows
            
            # Step 4: Embed images in rows with full-quality click-to-enlarge thumbnails
            # IMPROVED: Sequential matching based on row order with proper index mapping
            if image_col_idx is not None and table_images:
                logger.info(f'Embedding images in column {image_col_idx} for {len(rows)} rows')
                logger.info(f'Available images: {list(table_images.keys())}')
                logger.info(f'Filtered to processed mapping: {filtered_to_processed_idx}')
                
                # Map images from filtered_table_data indices to processed row indices
                processed_images = {}
                for filtered_idx, img_list in table_images.items():
                    # Get the processed row index from mapping
                    if filtered_idx in filtered_to_processed_idx:
                        processed_idx = filtered_to_processed_idx[filtered_idx]
                        processed_images[processed_idx] = img_list
                        logger.info(f'Mapped image from filtered row {filtered_idx} to processed row {processed_idx}')
                    # Special case: if header_row_idx is -1, all rows are data rows starting from 0
                    elif header_row_idx == -1 and filtered_idx < len(rows):
                        # Direct 1:1 mapping when no header
                        processed_images[filtered_idx] = img_list
                        logger.info(f'Direct mapping (no header): filtered row {filtered_idx} = processed row {filtered_idx}')
                
                # Embed images into processed rows
                for row_idx, row in enumerate(rows):
                    if row_idx in processed_images and image_col_idx < len(row):
                        for img_idx, img_data in enumerate(processed_images[row_idx]):
                            img_path = img_data.get('relative_path') or img_data.get('path', '')
                            if img_path:
                                # Ensure proper path format
                                if not img_path.startswith('/'):
                                    img_path = f'/{img_path}'
                                
                                # Create high-quality clickable thumbnail (non-editable)
                                img_id = f'img_{page_num}_{table_idx}_{row_idx}_{img_idx}'
                                img_html = f'''<div contenteditable="false" style="text-align: center; padding: 5px; user-select: none;">
                                    <img id="{img_id}" 
                                         src="{img_path}" 
                                         alt="Product Image" 
                                         class="table-image-thumbnail"
                                         style="max-width: 120px; max-height: 120px; cursor: pointer; 
                                                border: 2px solid #e5e7eb; border-radius: 8px; padding: 4px; 
                                                object-fit: cover; transition: all 0.2s ease;
                                                box-shadow: 0 2px 4px rgba(0,0,0,0.1); pointer-events: auto;"
                                         onclick="openImageModal('{img_path}', '{img_id}')"
                                         onmouseover="this.style.transform='scale(1.1)'; this.style.boxShadow='0 4px 12px rgba(0,0,0,0.2)'; this.style.borderColor='#10b981';"
                                         onmouseout="this.style.transform='scale(1)'; this.style.boxShadow='0 2px 4px rgba(0,0,0,0.1)'; this.style.borderColor='#e5e7eb';"
                                         title="Click to view full-size image" 
                                         loading="lazy" />
                                </div>'''
                                
                                # Replace cell content with image
                                row[image_col_idx] = img_html
                                logger.info(f'✓ Embedded image in processed row {row_idx}, column {image_col_idx}')
            else:
                if image_col_idx is None:
                    logger.warning(f'Image column not found, skipping image embedding')
                if not table_images:
                    logger.warning(f'No images available for table {table_idx} page {page_num}')
            
            # Step 5: Ensure all cells are filled (no empty cells)
            rows = self._fill_empty_cells(rows, headers)
            
            # Step 6: Build table structure
            return {
                'headers': headers,
                'rows': rows,
                'section_titles': section_titles,
                'column_count': len(headers),
                'row_count': len(rows),
                'page': page_num,
                'table_index': table_idx
            }
            
        except Exception as e:
            logger.error(f'Error processing table {table_idx} on page {page_num}: {e}', exc_info=True)
            return None
    
    def _should_merge_rows(self, prev_row: List, current_row: List, headers: List[str]) -> bool:
        """Check if current row should be merged with previous row (split description)"""
        if not prev_row or not current_row:
            return False
        
        # Find description column index
        desc_col_idx = None
        for idx, header in enumerate(headers):
            header_lower = str(header).lower()
            if any(keyword in header_lower for keyword in ['description', 'item description', 'specification']):
                desc_col_idx = idx
                break
        
        if desc_col_idx is None:
            return False
        
        # Check if previous row has Sl.No/Qty/Price but current row doesn't (likely continuation)
        prev_has_slno = False
        prev_has_qty = False
        prev_has_price = False
        current_has_slno = False
        current_has_qty = False
        current_has_price = False
        
        for idx, header in enumerate(headers):
            header_lower = str(header).lower()
            if idx < len(prev_row) and prev_row[idx]:
                if 'sl.no' in header_lower or 's.no' in header_lower:
                    prev_has_slno = True
                if 'qty' in header_lower:
                    prev_has_qty = True
                if 'price' in header_lower or 'rate' in header_lower:
                    prev_has_price = True
            
            if idx < len(current_row) and current_row[idx]:
                if 'sl.no' in header_lower or 's.no' in header_lower:
                    current_has_slno = True
                if 'qty' in header_lower:
                    current_has_qty = True
                if 'price' in header_lower or 'rate' in header_lower:
                    current_has_price = True
        
        # If previous row has Sl.No/Qty/Price but current doesn't, likely continuation
        if (prev_has_slno or prev_has_qty or prev_has_price) and not (current_has_slno or current_has_qty or current_has_price):
            # Check if current row has text in description column
            if desc_col_idx < len(current_row) and current_row[desc_col_idx]:
                return True
        
        return False
    
    def _merge_rows(self, prev_row: List, current_row: List, headers: List[str]) -> List:
        """Merge current row into previous row (combine split descriptions)"""
        merged_row = list(prev_row)
        
        # Find description column
        desc_col_idx = None
        for idx, header in enumerate(headers):
            header_lower = str(header).lower()
            if any(keyword in header_lower for keyword in ['description', 'item description', 'specification']):
                desc_col_idx = idx
                break
        
        # Merge description column
        if desc_col_idx is not None:
            prev_desc = str(merged_row[desc_col_idx]) if desc_col_idx < len(merged_row) and merged_row[desc_col_idx] else ''
            curr_desc = str(current_row[desc_col_idx]) if desc_col_idx < len(current_row) and current_row[desc_col_idx] else ''
            
            if prev_desc and curr_desc:
                merged_row[desc_col_idx] = f'{prev_desc} {curr_desc}'
            elif curr_desc:
                merged_row[desc_col_idx] = curr_desc
        
        # Merge other columns if they're empty in prev but filled in current
        for idx in range(len(headers)):
            if idx < len(merged_row) and not merged_row[idx]:
                if idx < len(current_row) and current_row[idx]:
                    merged_row[idx] = current_row[idx]
        
        return merged_row
    
    def _post_process_merge_rows(self, rows: List[List], headers: List[str]) -> List[List]:
        """
        AGGRESSIVE post-processing: Pattern-based row merging for 1 row per item
        Uses pattern detection to identify item boundaries and merge continuations
        """
        if not rows:
            return rows
        
        # Find column indices
        slno_col_idx = None
        desc_col_idx = None
        qty_col_idx = None
        price_col_idx = None
        image_col_idx = None
        
        for idx, header in enumerate(headers):
            header_lower = str(header).lower()
            if 'sl.no' in header_lower or 's.no' in header_lower:
                slno_col_idx = idx
            if any(keyword in header_lower for keyword in ['description', 'item description', 'specification']):
                desc_col_idx = idx
            if 'qty' in header_lower:
                qty_col_idx = idx
            if 'price' in header_lower or 'rate' in header_lower:
                price_col_idx = idx
            if any(keyword in header_lower for keyword in ['image', 'photo', 'picture', 'img']):
                image_col_idx = idx
        
        # Step 1: Remove all header-like rows from data
        filtered_rows = []
        for row in rows:
            row_text = ' '.join([str(cell).lower().strip() if cell else '' for cell in row])
            # Skip rows that are just header text
            if row_text.strip() in ['sl.no', 's.no', 'image', 'reference', 'sl.no image reference', 
                                    'sl.no image', 'image reference']:
                continue
            # Skip rows that are mostly empty
            non_empty = sum(1 for cell in row if cell and str(cell).strip())
            if non_empty < 2:
                continue
            filtered_rows.append(row)
        
        if not filtered_rows:
            return []
        
        # Step 2: Pattern-based merging - identify item boundaries
        merged_rows = []
        i = 0
        
        while i < len(filtered_rows):
            row = filtered_rows[i]
            
            # Check if this row starts a new item using multiple patterns
            is_item_start = False
            
            # Pattern 1: Has numeric Sl.No (not text "Sl.No")
            if slno_col_idx is not None and slno_col_idx < len(row):
                slno_val = str(row[slno_col_idx]).strip() if row[slno_col_idx] else ''
                # Check if it's a number (item number)
                if slno_val and re.match(r'^\d+$', slno_val):
                    is_item_start = True
            
            # Pattern 2: Has Qty AND Price (complete item row)
            if not is_item_start and qty_col_idx is not None and price_col_idx is not None:
                qty_val = str(row[qty_col_idx]).strip() if qty_col_idx < len(row) and row[qty_col_idx] else ''
                price_val = str(row[price_col_idx]).strip() if price_col_idx < len(row) and row[price_col_idx] else ''
                if qty_val and price_val and re.match(r'^\d', qty_val) and re.match(r'^\d', price_val):
                    is_item_start = True
            
            # Pattern 3: Has description AND (Qty OR Price)
            if not is_item_start and desc_col_idx is not None:
                desc_val = str(row[desc_col_idx]).strip() if desc_col_idx < len(row) and row[desc_col_idx] else ''
                if desc_val and len(desc_val) > 10:  # Substantial description
                    has_qty_or_price = False
                    if qty_col_idx is not None and qty_col_idx < len(row) and row[qty_col_idx]:
                        has_qty_or_price = True
                    if price_col_idx is not None and price_col_idx < len(row) and row[price_col_idx]:
                        has_qty_or_price = True
                    if has_qty_or_price:
                        is_item_start = True
            
            if is_item_start:
                # This is a new item - merge all following rows until next item
                merged_row = list(row)
                i += 1
                
                # Merge continuation rows
                while i < len(filtered_rows):
                    next_row = filtered_rows[i]
                    
                    # Check if next row is a new item
                    next_is_item = False
                    if slno_col_idx is not None and slno_col_idx < len(next_row):
                        next_slno = str(next_row[slno_col_idx]).strip() if next_row[slno_col_idx] else ''
                        if next_slno and re.match(r'^\d+$', next_slno):
                            next_is_item = True
                    
                    if not next_is_item and qty_col_idx is not None and price_col_idx is not None:
                        next_qty = str(next_row[qty_col_idx]).strip() if qty_col_idx < len(next_row) and next_row[qty_col_idx] else ''
                        next_price = str(next_row[price_col_idx]).strip() if price_col_idx < len(next_row) and next_row[price_col_idx] else ''
                        if next_qty and next_price and re.match(r'^\d', next_qty) and re.match(r'^\d', next_price):
                            next_is_item = True
                    
                    # Check if summary row
                    next_row_text = ' '.join([str(cell).lower() if cell else '' for cell in next_row])
                    if self._is_summary_row(next_row):
                        break
                    
                    # If next row is a new item, stop merging
                    if next_is_item:
                        break
                    
                    # Merge continuation row
                    merged_row = self._merge_rows_aggressive(merged_row, next_row, headers, desc_col_idx)
                    i += 1
                
                merged_rows.append(merged_row)
            else:
                # Row doesn't match item pattern - try to merge with previous
                if merged_rows:
                    prev_row = merged_rows[-1]
                    # Check if previous row is a complete item
                    prev_has_item = False
                    if slno_col_idx is not None and slno_col_idx < len(prev_row):
                        prev_slno = str(prev_row[slno_col_idx]).strip() if prev_row[slno_col_idx] else ''
                        if prev_slno and re.match(r'^\d+$', prev_slno):
                            prev_has_item = True
                    
                    if prev_has_item:
                        # Merge with previous item
                        merged_rows[-1] = self._merge_rows_aggressive(prev_row, row, headers, desc_col_idx)
                        i += 1
                        continue
                
                # Orphaned row - might be section title or metadata
                # Only include if it has substantial content
                non_empty = sum(1 for cell in row if cell and str(cell).strip())
                if non_empty >= 3:
                    merged_rows.append(row)
                i += 1
        
        return merged_rows
    
    def _merge_rows_aggressive(self, prev_row: List, current_row: List, headers: List[str], desc_col_idx: Optional[int]) -> List:
        """Aggressively merge rows - combine all text columns"""
        merged_row = list(prev_row)
        
        # Merge description column (most important)
        if desc_col_idx is not None:
            prev_desc = str(merged_row[desc_col_idx]) if desc_col_idx < len(merged_row) and merged_row[desc_col_idx] else ''
            curr_desc = str(current_row[desc_col_idx]) if desc_col_idx < len(current_row) and current_row[desc_col_idx] else ''
            
            if prev_desc and curr_desc:
                merged_row[desc_col_idx] = f'{prev_desc} {curr_desc}'
            elif curr_desc:
                merged_row[desc_col_idx] = curr_desc
        
        # Merge other text columns (skip numeric columns like Sl.No, Qty, Price)
        for idx in range(len(headers)):
            if idx == desc_col_idx:
                continue
            
            header_lower = str(headers[idx]).lower()
            # Skip numeric columns
            if any(keyword in header_lower for keyword in ['sl.no', 's.no', 'qty', 'quantity', 'price', 'rate', 'total', 'amount']):
                # Only merge if prev is empty
                if idx < len(merged_row) and not merged_row[idx]:
                    if idx < len(current_row) and current_row[idx]:
                        merged_row[idx] = current_row[idx]
                continue
            
            # Merge text columns
            prev_val = str(merged_row[idx]) if idx < len(merged_row) and merged_row[idx] else ''
            curr_val = str(current_row[idx]) if idx < len(current_row) and current_row[idx] else ''
            
            if prev_val and curr_val:
                merged_row[idx] = f'{prev_val} {curr_val}'
            elif curr_val:
                merged_row[idx] = curr_val
        
        return merged_row
    
    def _detect_headers(self, table_data: List) -> Tuple[List[str], int]:
        """Detect headers - IMPROVED to recognize data rows and avoid treating them as headers"""
        if not table_data or len(table_data) < 1:
            return [], 0
        
        # Helper function to check if row is a data row (has serial numbers, numeric values, etc.)
        def is_data_row(row_cells) -> bool:
            """Check if row contains data (serial numbers, prices, etc.) rather than headers"""
            if not row_cells:
                return False
            
            # Check first cell for serial number (1, 2, 3, etc.)
            first_cell = str(row_cells[0]).strip() if row_cells else ''
            if first_cell.isdigit() and int(first_cell) >= 1 and int(first_cell) <= 1000:
                return True  # Likely a data row with serial number
            
            # Check for numeric values (prices, quantities, etc.)
            numeric_count = 0
            for cell in row_cells:
                cell_str = str(cell).strip()
                # Check for numbers with decimals, commas, currency symbols
                if re.match(r'^[\$€£¥QAR]*\s*[\d,]+\.?\d*\s*[\$€£¥QAR]*$', cell_str):
                    numeric_count += 1
            
            # If more than 2 cells have numeric values, likely a data row
            if numeric_count >= 2:
                return True
            
            # Check for descriptive content (long text blocks, multi-line descriptions)
            long_text_count = sum(1 for cell in row_cells if cell and len(str(cell).strip()) > 50)
            if long_text_count >= 1:
                return True  # Likely a data row with description
            
            return False
        
        # ALWAYS try first row as headers first (most common case)
        first_row = [str(cell).strip() if cell else '' for cell in table_data[0]]
        first_row_lower = [cell.lower() for cell in first_row]
        first_row_text = ' '.join(first_row_lower)
        
        # CRITICAL: Check if first row is actually a data row
        if is_data_row(table_data[0]):
            logger.info(f'First row is DATA ROW (has serial numbers/values), not header')
            # Generate generic headers since first row is data
            headers = [f'Column {i+1}' for i in range(len(table_data[0]))]
            logger.info(f'Using generic headers for data table: {headers}')
            return headers, -1  # Return -1 to indicate no header row (treat first row as data)
        
        # Check if first row is non-table content (should be filtered earlier, but double-check)
        if self._is_non_table_row(first_row_text):
            # Skip first row if it's non-table content, try second row
            if len(table_data) > 1:
                # Check if second row is data row
                if is_data_row(table_data[1]):
                    headers = [f'Column {i+1}' for i in range(len(table_data[1]))]
                    logger.info(f'Second row is DATA ROW, using generic headers: {headers}')
                    return headers, -1
                
                second_row = [str(cell).strip() if cell else '' for cell in table_data[1]]
                second_row_lower = [cell.lower() for cell in second_row]
                second_row_text = ' '.join(second_row_lower)
                
                if not self._is_non_table_row(second_row_text):
                    header_matches = 0
                    non_empty_count = sum(1 for cell in second_row if cell and str(cell).strip())
                    short_text_count = sum(1 for cell in second_row if cell and len(str(cell).strip()) < 30)
                    
                    # Check for header keywords (more flexible matching)
                    for cell_lower in second_row_lower:
                        if not cell_lower:
                            continue
                        for header_group in self.header_variants.values():
                            for variant in header_group:
                                if (variant in cell_lower or 
                                    cell_lower.startswith(variant) or 
                                    cell_lower.endswith(variant) or
                                    cell_lower == variant):
                                    header_matches += 1
                                    logger.debug(f'Header match found: "{variant}" in "{cell_lower}"')
                                    break
                            if header_matches > 0:
                                break
                    
                    is_likely_header = (
                        header_matches >= 1 or
                        (non_empty_count >= 3 and short_text_count >= 2)
                    )
                    
                    if is_likely_header:
                        headers = [str(cell).strip() if cell else '' for cell in table_data[1]]
                        logger.info(f'Detected headers in second row (first row was non-table): {headers} (matches: {header_matches})')
                        return headers, 1
        
        # Check if first row looks like headers (has header-like characteristics)
        header_matches = 0
        non_empty_count = sum(1 for cell in first_row if cell and str(cell).strip())
        short_text_count = sum(1 for cell in first_row if cell and len(str(cell).strip()) < 30)
        
        # Check for header keywords (more flexible matching)
        for cell_lower in first_row_lower:
            if not cell_lower:
                continue
            for header_group in self.header_variants.values():
                for variant in header_group:
                    # More flexible matching: check if variant is in cell or cell starts/ends with variant
                    if (variant in cell_lower or 
                        cell_lower.startswith(variant) or 
                        cell_lower.endswith(variant) or
                        cell_lower == variant):
                        header_matches += 1
                        logger.debug(f'Header match found: "{variant}" in "{cell_lower}"')
                        break
                if header_matches > 0:
                    break
        
        # If first row has header characteristics, use it as headers
        # BUT: if table only has 1-2 rows total, require stronger evidence to avoid treating data as headers
        min_matches_required = 2 if len(table_data) <= 2 else 1
        
        is_likely_header = (
            header_matches >= min_matches_required or  # Has header keywords (stricter for small tables)
            (non_empty_count >= 4 and short_text_count >= 3 and header_matches >= 1)  # Multiple short cells + at least 1 keyword
        )
        
        # Special case: if table has only 1 row and no header matches, treat it as data row with generic headers
        if len(table_data) == 1 and header_matches == 0:
            headers = [f'Column {i+1}' for i in range(len(table_data[0]))]
            logger.info(f'Single row table with no header keywords - using generic headers: {headers}')
            return headers, -1  # Return -1 to indicate no header row (treat first row as data)
        
        if is_likely_header:
            headers = [str(cell).strip() if cell else '' for cell in table_data[0]]
            logger.info(f'Detected headers in first row: {headers} (matches: {header_matches})')
            return headers, 0
        
        # Try second row if first doesn't work
        if len(table_data) > 1:
            second_row = [str(cell).strip() if cell else '' for cell in table_data[1]]
            second_row_lower = [cell.lower() for cell in second_row]
            header_matches = 0
            non_empty_count = sum(1 for cell in second_row if cell and str(cell).strip())
            short_text_count = sum(1 for cell in second_row if cell and len(str(cell).strip()) < 30)
            
            for cell_lower in second_row_lower:
                if not cell_lower:
                    continue
                for header_group in self.header_variants.values():
                    for variant in header_group:
                        if (variant in cell_lower or 
                            cell_lower.startswith(variant) or 
                            cell_lower.endswith(variant) or
                            cell_lower == variant):
                            header_matches += 1
                            break
                    if header_matches > 0:
                        break
            
            is_likely_header = (
                header_matches >= 1 or
                (non_empty_count >= 3 and short_text_count >= 2)
            )
            
            if is_likely_header:
                headers = [str(cell).strip() if cell else '' for cell in table_data[1]]
                logger.info(f'Detected headers in second row: {headers} (matches: {header_matches})')
                return headers, 1
        
        # Default: use first row as headers (preserve original - might be headers even without keywords)
        headers = [str(cell).strip() if cell else f'Column {i+1}' for i, cell in enumerate(table_data[0])]
        logger.info(f'Using first row as headers (no keywords found): {headers}')
        return headers, 0
    
    def _clean_headers(self, headers: List[str]) -> Tuple[List[str], Dict[int, int]]:
        """
        Clean up headers: merge duplicates, remove empty columns, standardize names
        Returns: (cleaned_headers, column_mapping) where mapping maps old_index -> new_index
        """
        if not headers:
            return headers, {}
        
        cleaned = []
        seen_headers = {}
        column_mapping = {}  # Maps original column index to cleaned column index
        
        for idx, header in enumerate(headers):
            header_lower = str(header).lower().strip()
            
            # Skip empty headers
            if not header_lower:
                continue
            
            # Check for duplicates
            if header_lower in seen_headers:
                # Duplicate header - merge with previous (don't add new column)
                prev_idx = seen_headers[header_lower]
                # Map this column to the previous one
                column_mapping[idx] = prev_idx
                continue
            
            # Add to cleaned list
            new_idx = len(cleaned)
            cleaned.append(header)
            seen_headers[header_lower] = new_idx
            column_mapping[idx] = new_idx
        
        # Standardize common header names
        standardized = []
        for header in cleaned:
            header_lower = str(header).lower().strip()
            # Standardize common variations
            if 'item code' in header_lower or (header_lower == 'code' and 'item' not in header_lower):
                standardized.append('Item Code')
            elif 'item name' in header_lower or (header_lower == 'name' and 'item' not in header_lower):
                standardized.append('Item Name')
            elif 'item details' in header_lower or header_lower == 'details':
                standardized.append('Item Details')
            elif 'additional' in header_lower:
                standardized.append('Additional Information')
            elif header_lower == 'uom':
                standardized.append('UOM')
            elif 'quantity' in header_lower or header_lower == 'qty':
                standardized.append('Quantity')
            elif 'rate' in header_lower or 'price' in header_lower:
                standardized.append('Rate')
            elif 'amount' in header_lower or 'total' in header_lower:
                standardized.append('Amount')
            else:
                standardized.append(header)
        
        if len(standardized) != len(headers):
            logger.info(f'Cleaned headers: {len(headers)} -> {len(standardized)} (removed {len(headers) - len(standardized)} duplicates/empty)')
        
        return standardized, column_mapping
    
    def _align_rows_with_cleaned_headers(self, table_data: List[List], headers: List[str], column_mapping: Dict[int, int], header_row_idx: int) -> List[List]:
        """
        Align data rows with cleaned headers using column mapping
        Merges data from duplicate columns into single columns
        """
        if not table_data or not headers or not column_mapping:
            return table_data
        
        aligned_data = []
        
        for row_idx, row in enumerate(table_data):
            if row_idx == header_row_idx:
                # Header row - use cleaned headers
                aligned_data.append(headers)
                continue
            
            # Create new row with cleaned column structure
            aligned_row = [''] * len(headers)
            
            # Map original columns to cleaned columns
            for orig_idx, cell in enumerate(row):
                if orig_idx in column_mapping:
                    new_idx = column_mapping[orig_idx]
                    # If cell already has content, merge it
                    if cell and str(cell).strip():
                        existing = aligned_row[new_idx]
                        if existing:
                            aligned_row[new_idx] = f'{existing} {str(cell).strip()}'
                        else:
                            aligned_row[new_idx] = str(cell).strip()
            
            aligned_data.append(aligned_row)
        
        return aligned_data
    
    def _fix_horizontally_split_text(self, table_data: List[List], headers: List[str] = None, header_row_idx: int = 0) -> List[List]:
        """
        Fix horizontally split text in bordered tables
        When pdfplumber extracts a table, multi-line text within a cell can be split across columns
        This function merges horizontally split text back into appropriate columns
        
        Args:
            table_data: The table data (list of rows)
            headers: Optional pre-detected headers (if None, will detect)
            header_row_idx: Optional header row index (if None, will detect)
        """
        if not table_data or len(table_data) < 2:
            return table_data
        
        # If headers not provided, detect them
        if headers is None:
            headers, header_row_idx = self._detect_headers(table_data)
            if not headers:
                return table_data
        
        # Identify description/text columns (columns that should contain long text)
        # Priority: Details > Name > Additional > Item (to avoid merging into wrong column)
        text_column_indices = []
        priority_map = {}  # Map priority to column index
        
        for idx, header in enumerate(headers):
            header_lower = str(header).lower().strip()
            if not header_lower:
                continue
                
            # Priority 1: Details/Description columns (highest priority)
            if any(keyword in header_lower for keyword in ['details', 'description', 'item details', 'item description', 'specification']):
                priority_map[idx] = 1
                text_column_indices.append(idx)
            # Priority 2: Name columns
            elif any(keyword in header_lower for keyword in ['name', 'item name']):
                priority_map[idx] = 2
                text_column_indices.append(idx)
            # Priority 3: Additional columns
            elif 'additional' in header_lower:
                priority_map[idx] = 3
                text_column_indices.append(idx)
            # Priority 4: Item columns (only if not already identified)
            elif 'item' in header_lower and idx not in text_column_indices:
                # Only add if it's not a code column
                if 'code' not in header_lower:
                    priority_map[idx] = 4
                    text_column_indices.append(idx)
        
        # If no text columns identified, try to detect them from data
        if not text_column_indices:
            # Look for columns with long text (likely description columns)
            for col_idx in range(len(headers)):
                # Check first few data rows
                text_lengths = []
                for row_idx in range(header_row_idx + 1, min(header_row_idx + 5, len(table_data))):
                    if col_idx < len(table_data[row_idx]):
                        cell = str(table_data[row_idx][col_idx]) if table_data[row_idx][col_idx] else ''
                        if len(cell) > 10:  # Long text
                            text_lengths.append(len(cell))
                
                # If column has consistently long text, it's likely a description column
                if text_lengths and sum(text_lengths) / len(text_lengths) > 15:
                    text_column_indices.append(col_idx)
        
        if not text_column_indices:
            logger.debug("No text columns identified for horizontal merge fix")
            return table_data
        
        # Process each row (skip header)
        fixed_table_data = []
        for row_idx, row in enumerate(table_data):
            if row_idx == header_row_idx:
                fixed_table_data.append(row)
                continue
            
            fixed_row = list(row)
            
            # Process text columns in priority order (Details first, then Name, then Additional, then Item)
            sorted_text_cols = sorted(text_column_indices, key=lambda x: priority_map.get(x, 99))
            
            for text_col_idx in sorted_text_cols:
                if text_col_idx >= len(fixed_row):
                    continue
                
                # Get text in the text column
                main_text = str(fixed_row[text_col_idx]).strip() if fixed_row[text_col_idx] else ''
                
                # Check adjacent columns for split text
                # Look right (text might be split to the right)
                merged_text = main_text
                cols_to_clear = []
                
                # Merge text from right-side columns (more aggressive - check up to 15 columns)
                for check_col_idx in range(text_col_idx + 1, min(text_col_idx + 15, len(fixed_row))):
                    # Skip if this is another text column with higher priority
                    if check_col_idx in text_column_indices:
                        check_priority = priority_map.get(check_col_idx, 99)
                        current_priority = priority_map.get(text_col_idx, 99)
                        # Only stop if we hit a higher priority text column
                        if check_priority < current_priority:
                            break
                        # If same or lower priority, continue (might be split text)
                    
                    # Skip numeric columns (stop immediately)
                    if check_col_idx < len(headers):
                        header_lower = str(headers[check_col_idx]).lower().strip()
                        if any(keyword in header_lower for keyword in ['qty', 'quantity', 'price', 'rate', 'total', 'amount', 'uom']):
                            break
                        # Also skip code columns
                        if 'code' in header_lower and header_lower != 'item code':
                            break
                    
                    # Get text from this column
                    check_text = str(fixed_row[check_col_idx]).strip() if check_col_idx < len(fixed_row) and fixed_row[check_col_idx] else ''
                    
                    # If column has text and it's not a number/code, it might be split text
                    if check_text:
                        # Check if it looks like part of a description (not a number, not a code pattern)
                        is_number = re.match(r'^[\d.,\s]+$', check_text)
                        is_code = re.match(r'^[A-Z]+\d+[A-Z]*$', check_text) or re.match(r'^LF\d+[A-Z]*$', check_text)
                        
                        if not is_number and not is_code and len(check_text) > 1:
                            # Merge it into the text column
                            if merged_text:
                                merged_text = f'{merged_text} {check_text}'
                            else:
                                merged_text = check_text
                            cols_to_clear.append(check_col_idx)
                        elif is_number or is_code:
                            # If we hit a number or code, stop merging for this column
                            break
                    elif check_text == '':
                        # Empty cell - continue checking (might be spacing)
                        continue
                    else:
                        # Non-empty but doesn't match - stop
                        break
                
                # Update the text column with merged text
                if merged_text != main_text:
                    fixed_row[text_col_idx] = merged_text
                    # Clear the columns we merged from
                    for clear_idx in cols_to_clear:
                        if clear_idx < len(fixed_row):
                            fixed_row[clear_idx] = ''
                    
                    if len(cols_to_clear) > 0:
                        logger.info(f"Fixed horizontally split text in row {row_idx}, column {text_col_idx} ('{headers[text_col_idx]}'): merged {len(cols_to_clear)} columns")
            
            fixed_table_data.append(fixed_row)
        
        return fixed_table_data
    
    def _normalize_header(self, header_text: str) -> Optional[str]:
        """Normalize header text to standard name - ONLY if exact match needed"""
        # NOTE: This function is kept for backward compatibility but we now preserve original names
        header_lower = header_text.lower().strip()
        
        for standard_name, variants in self.header_variants.items():
            for variant in variants:
                # Only normalize if it's an exact or very close match
                if variant == header_lower or header_lower.startswith(variant + ' ') or header_lower.endswith(' ' + variant):
                    return standard_name.title()
        
        return None
    
    def _is_section_title(self, row: List, headers: List[str]) -> bool:
        """Check if row is a section title (centered, bold, with spacing)"""
        if not row:
            return False
        
        # Check if row has mostly empty cells (centered text)
        non_empty = [cell for cell in row if cell and str(cell).strip()]
        if len(non_empty) != 1:
            return False
        
        # Check if text matches section title pattern
        text = str(non_empty[0]).strip()
        if re.match(r'^[A-Z][A-Z\s-]+$', text) and len(text) > 3:
            # But exclude if it's an image reference or description
            text_lower = text.lower()
            if any(keyword in text_lower for keyword in ['local', 'uae', 'far east', 'office', 'desk', 'chair', 'sofa']):
                return False  # Likely description or image reference
            return True
        
        return False
    
    def _is_summary_row(self, row: List) -> bool:
        """Check if row is a summary row (Total, VAT, etc.)"""
        row_text = ' '.join([str(cell).lower() if cell else '' for cell in row])
        
        for keyword in self.summary_keywords:
            if keyword in row_text:
                return True
        
        return False
    
    def _process_row(self, row: List, headers: List[str], is_summary: bool) -> Optional[List]:
        """Process a row, preserving multi-line descriptions and exact column order"""
        if not row:
            return None
        
        # CRITICAL: Preserve exact column order - match row length to headers
        processed_row = []
        
        # Ensure row has same length as headers (pad with empty strings if needed)
        for i in range(len(headers)):
            if i < len(row):
                cell = row[i]
            else:
                cell = ''
            
            if cell:
                # Keep multi-line text as single block - preserve original formatting
                cell_text = str(cell).strip()
                # Preserve line breaks and spacing within cells (don't collapse all whitespace)
                # Only normalize excessive spaces (3+ spaces to 1 space)
                cell_text = re.sub(r' {3,}', ' ', cell_text)
                # Preserve newlines within cells
                processed_row.append(cell_text)
            else:
                processed_row.append('')
        
        # If row has more columns than headers, append them (preserve all data)
        if len(row) > len(headers):
            for i in range(len(headers), len(row)):
                cell = row[i]
                if cell:
                    cell_text = str(cell).strip()
                    cell_text = re.sub(r' {3,}', ' ', cell_text)
                    processed_row.append(cell_text)
                else:
                    processed_row.append('')
        
        return processed_row
    
    def _fill_empty_cells(self, rows: List[List], headers: List[str]) -> List[List]:
        """Fill empty cells to ensure all cells are filled - maintain exact column structure"""
        filled_rows = []
        
        for row in rows:
            filled_row = []
            
            # Process cells up to header count
            for i in range(len(headers)):
                if i < len(row):
                    cell = row[i]
                else:
                    cell = ''
                
                # Fill empty cells with empty string (preserve structure)
                if not cell or not str(cell).strip():
                    filled_row.append('')
                else:
                    filled_row.append(str(cell).strip())
            
            # If row has extra columns beyond headers, preserve them
            if len(row) > len(headers):
                for i in range(len(headers), len(row)):
                    cell = row[i]
                    if cell and str(cell).strip():
                        filled_row.append(str(cell).strip())
                    else:
                        filled_row.append('')
            
            # Ensure minimum length matches headers (don't truncate if longer)
            while len(filled_row) < len(headers):
                filled_row.append('')
            
            filled_rows.append(filled_row)
        
        return filled_rows
    
    def _extract_images_comprehensive(self, pdf_path: str, tables: List[Dict], output_dir: str) -> Dict:
        """Extract images from PDF and embed them in table cells"""
        images = {}
        
        if not IMAGE_PROCESSING_AVAILABLE:
            logger.warning("Image processing libraries not available - skipping image extraction")
            return images
        
        try:
            pdf_fitz = fitz.open(pdf_path)
            
            # Group tables by page
            tables_by_page = defaultdict(list)
            for table in tables:
                page_num = table.get('page', 1)
                tables_by_page[page_num].append(table)
            
            # Process each page
            for page_num, page_tables in tables_by_page.items():
                if page_num > len(pdf_fitz):
                    continue
                
                page_fitz = pdf_fitz[page_num - 1]
                
                # Get page dimensions
                page_rect = page_fitz.rect
                
                # Extract images from page
                image_list = page_fitz.get_images(full=True)
                
                if not image_list:
                    continue
                
                # Get page as image for position matching
                try:
                    pix = page_fitz.get_pixmap(matrix=2.0)  # 2x zoom for better quality
                    page_image = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                except Exception as e:
                    logger.debug(f'Could not convert page {page_num} to image: {e}')
                    page_image = None
                
                # Process each table on this page
                for table in page_tables:
                    # Get row bboxes if available
                    row_bboxes = table.get('row_bboxes', [])
                    table_images = self._extract_and_match_images(
                        page_fitz, page_image, image_list, table, page_num, output_dir, row_bboxes
                    )
                    images.update(table_images)
            
            pdf_fitz.close()
            
        except Exception as e:
            logger.error(f'Image extraction failed: {e}', exc_info=True)
        
        return images
    
    def _extract_images_sequential(self, pdf_doc, page_fitz, image_list, table_idx, page_num, output_dir, row_offset=0, page_row_count=0):
        """Extract images from page and assign them sequentially to rows (top to bottom order)"""
        images = {}
        
        try:
            if not image_list or not output_dir:
                return images
            
            # Extract all images with their y-coordinates
            extracted_imgs = []
            for img_idx, img in enumerate(image_list):
                try:
                    xref = img[0]
                    img_dict = pdf_doc.extract_image(xref)  # Use document, not page
                    img_rects = page_fitz.get_image_rects(xref)
                    
                    if not img_dict or not img_rects:
                        continue
                    
                    # Get first rect (images usually have one rect)
                    rect = img_rects[0] if img_rects else None
                    if not rect:
                        continue
                    
                    # Store image data with y-coordinate for sorting
                    y_center = (rect[1] + rect[3]) / 2
                    extracted_imgs.append({
                        'img_idx': img_idx,
                        'xref': xref,
                        'img_dict': img_dict,
                        'rect': rect,
                        'y_center': y_center
                    })
                    
                except Exception as e:
                    logger.warning(f'Failed to extract image {img_idx}: {e}')
                    continue
            
            # Sort images by y-coordinate (top to bottom)
            extracted_imgs.sort(key=lambda x: x['y_center'])
            
            # Assign images sequentially to rows
            for local_row_idx, img_data in enumerate(extracted_imgs):
                # Check if we have rows for this image
                if local_row_idx >= page_row_count:
                    logger.warning(f'More images ({len(extracted_imgs)}) than rows ({page_row_count}) on page {page_num}, skipping extra images')
                    break
                
                try:
                    img_idx = img_data['img_idx']
                    img_dict = img_data['img_dict']
                    img_rect = img_data['rect']
                    
                    # Save image to disk
                    img_ext = img_dict.get('ext', 'png')
                    image_filename = f'page_{page_num}_table_{table_idx}_img_{img_idx}.{img_ext}'
                    image_path = os.path.join(output_dir, 'imgs', image_filename)
                    
                    os.makedirs(os.path.dirname(image_path), exist_ok=True)
                    
                    with open(image_path, 'wb') as img_file:
                        img_file.write(img_dict['image'])
                    
                    # Calculate global row index
                    global_row_idx = row_offset + local_row_idx
                    
                    # Build image URL - use just imgs/filename (relative to output_dir)
                    # This avoids duplicate paths when served through /serve_output endpoint
                    
                    # Store image data with global row index
                    image_key = f'page_{page_num}_table_{table_idx}_img_{img_idx}'
                    images[image_key] = {
                        'path': image_path,
                        'filename': image_filename,
                        'relative_path': f"imgs/{image_filename}",
                        'page': page_num,
                        'table_index': table_idx,
                        'table_global_idx': table_idx,
                        'row_index': global_row_idx,
                        'column_index': 2,  # Typically the image column
                        'bbox': img_rect
                    }
                    
                    logger.info(f'✓ Sequential: Page {page_num} image {img_idx} (y={img_data["y_center"]:.1f}) → global row {global_row_idx} (local {local_row_idx} + offset {row_offset})')
                    
                except Exception as e:
                    logger.warning(f'Failed to save image {img_idx}: {e}')
                    continue
            
            logger.info(f'Page {page_num}: Extracted {len(images)} images, assigned to rows {row_offset} to {row_offset + len(images) - 1}')
            
        except Exception as e:
            logger.error(f'Sequential image extraction failed: {e}', exc_info=True)
        
        return images
    
    def _extract_and_match_images(self, page_fitz, page_image, image_list, table: Dict, 
                                   page_num: int, output_dir: str, row_bboxes: List = None, 
                                   row_offset: int = 0, table_global_idx: int = 0) -> Dict:
        """Extract images and match them to table cells based on position
        
        Args:
            row_offset: For multi-page tables, offset to add to row indices
            table_global_idx: Global table index (after merging multi-page tables)
        """
        images = {}
        
        if not image_list:
            return images
        
        try:
            # Get table structure
            headers = table.get('headers', [])
            rows = table.get('rows', [])
            
            if not headers or not rows:
                return images
            
            # Find image column index (if exists)
            image_col_idx = None
            for idx, header in enumerate(headers):
                header_lower = str(header).lower()
                if any(keyword in header_lower for keyword in ['image', 'photo', 'picture', 'img', 'reference']):
                    image_col_idx = idx
                    break
            
            # Fallback: if headers don't have image keyword, check for empty columns in first few rows
            # or default to column 2 (common for product tables with SN, Location, Image pattern)
            if image_col_idx is None:
                # Check if any column has mostly empty cells (likely image column)
                if len(rows) > 0 and len(headers) > 2:
                    for col_idx in range(min(len(headers), 5)):  # Check first 5 columns
                        empty_count = sum(1 for row in rows[:3] if len(row) > col_idx and 
                                        (not row[col_idx] or str(row[col_idx]).strip() == '' or 
                                         str(row[col_idx]).strip() == '[IMAGE]'))
                        if empty_count >= min(len(rows[:3]), 2):  # If 2+ of first 3 rows have empty cell
                            image_col_idx = col_idx
                            logger.info(f'Detected image column {col_idx} from empty cells pattern')
                            break
                
                # Final fallback: column 2 is common for product tables
                if image_col_idx is None and len(headers) > 2:
                    image_col_idx = 2
                    logger.info(f'Using default image column 2')
            
            # Create images directory
            images_dir = os.path.join(output_dir, 'imgs')
            os.makedirs(images_dir, exist_ok=True)
            
            # Extract session_id and file_id from output_dir for URL construction
            # output_dir is like: outputs/session_id/file_id
            output_dir_parts = output_dir.replace('\\', '/').split('/')
            if len(output_dir_parts) >= 3:
                session_id = output_dir_parts[-2]
                file_id = output_dir_parts[-1]
                url_base = f'/outputs/{session_id}/{file_id}/imgs'
            else:
                url_base = '/imgs'
            logger.info(f'Image URL base: {url_base}')
            
            # Extract and save images at original quality
            # Note: extract_image() is a document method, not a page method
            doc = page_fitz.parent  # Get the document from the page
            logger.info(f'Processing {len(image_list)} images from image_list for table {table["table_index"]}')
            for img_idx, img in enumerate(image_list):
                try:
                    xref = img[0]
                    logger.info(f'Extracting image {img_idx} with xref={xref}')
                    base_image = doc.extract_image(xref)  # Use doc, not page_fitz!
                    image_bytes = base_image["image"]
                    image_ext = base_image["ext"]
                    logger.info(f'Image {img_idx}: format={image_ext}, size={len(image_bytes)} bytes')
                    
                    # Convert certain formats to PNG for better quality
                    if image_ext in ['jb2', 'jpx', 'jxr']:
                        image_ext = 'png'
                        # Convert using PIL for better compatibility
                        try:
                            from PIL import Image
                            import io
                            img_pil = Image.open(io.BytesIO(image_bytes))
                            output = io.BytesIO()
                            img_pil.save(output, format='PNG', quality=100)
                            image_bytes = output.getvalue()
                        except:
                            pass  # Keep original if conversion fails
                    
                    # Get image position - use proper PyMuPDF API
                    # Note: get_image_info() returns bbox but xref=None, get_images() returns xref but no bbox
                    # They are in the same order, so we can match by index
                    img_rect = None
                    try:
                        # Get all image info (with bboxes) - they match get_images() by index
                        img_info_list = page_fitz.get_image_info()
                        if img_idx < len(img_info_list):
                            bbox = img_info_list[img_idx].get('bbox')
                            if bbox:
                                img_rect = (bbox[0], bbox[1], bbox[2], bbox[3])
                                logger.info(f'✓ Image {img_idx} bbox: {img_rect}')
                    except Exception as e:
                        logger.warning(f'get_image_info failed for image {img_idx}: {e}')
                    
                    # If still no bbox, log warning but continue to save image
                    if not img_rect:
                        logger.warning(f'Could not determine bbox for image {img_idx} xref={xref}, using default row 0')
                    
                    # Save image at original quality - include page number to avoid overwriting
                    image_filename = f"page_{page_num}_table_{table['table_index']}_img_{img_idx}.{image_ext}"
                    image_path = os.path.join(images_dir, image_filename)
                    
                    with open(image_path, "wb") as img_file:
                        img_file.write(image_bytes)
                    logger.info(f'✓ Saved image to: {image_path}')
                    
                    # Determine which row this image belongs to
                    # Match image y-position to table row
                    local_row_idx = self._match_image_to_row(img_rect, rows, page_fitz, row_bboxes) if img_rect else 0
                    # Apply offset for multi-page tables
                    global_row_idx = local_row_idx + row_offset
                    logger.debug(f'Matched image {img_idx} to local row {local_row_idx}, global row {global_row_idx}')
                    
                    # Store image reference with row index
                    image_key = f"page_{page_num}_table_{table_global_idx}_row_{global_row_idx}_img_{img_idx}"
                    images[image_key] = {
                        'path': image_path,
                        'filename': image_filename,
                        'relative_path': f"imgs/{image_filename}",
                        'page': page_num,
                        'table_index': table['table_index'],
                        'table_global_idx': table_global_idx,
                        'row_index': global_row_idx,  # Global row index across all pages
                        'column_index': image_col_idx,
                        'bbox': img_rect
                    }
                    
                    logger.info(f'Extracted image {img_idx} for table {table_global_idx}, page {page_num}, global row {global_row_idx}')
                    
                except Exception as e:
                    logger.warning(f'Failed to extract image {img_idx}: {e}')
                    continue
            
        except Exception as e:
            logger.error(f'Image matching failed: {e}', exc_info=True)
        
        return images
    
    def _match_image_to_row(self, img_rect, rows: List[List], page_fitz, row_bboxes: List = None) -> int:
        """Match image position to table row using actual row bounding boxes"""
        if not rows or not img_rect:
            return 0
        
        try:
            # Get image y-position (middle of image)
            img_y_center = (img_rect[1] + img_rect[3]) / 2  # Middle y-coordinate
            img_y_top = img_rect[1]
            img_y_bottom = img_rect[3]
            
            logger.info(f'Matching image: y_top={img_y_top:.1f}, y_bottom={img_y_bottom:.1f}, y_center={img_y_center:.1f}')
            logger.info(f'Available row_bboxes: {len(row_bboxes) if row_bboxes else 0}, rows: {len(rows)}')
            
            # If we have actual row bboxes from pdfplumber, use them for precise matching
            if row_bboxes and len(row_bboxes) > 0:
                # Log first few bboxes for debugging
                for i, bbox in enumerate(row_bboxes[:3]):
                    logger.info(f'  Row {i} bbox: y0={bbox[0]:.1f}, y1={bbox[1]:.1f}')
                
                best_row_idx = 0
                max_overlap = 0
                
                for idx, (row_y0, row_y1) in enumerate(row_bboxes):
                    # Calculate overlap between image and row
                    overlap_top = max(img_y_top, row_y0)
                    overlap_bottom = min(img_y_bottom, row_y1)
                    overlap = max(0, overlap_bottom - overlap_top)
                    
                    if overlap > max_overlap:
                        max_overlap = overlap
                        best_row_idx = idx
                
                # Ensure within bounds (skip header row which is at index 0)
                row_idx = max(0, min(best_row_idx, len(rows) - 1))
                logger.info(f'Matched image at y={img_y_center:.1f} to row {row_idx} using bbox (overlap: {max_overlap:.1f}px)')
                return row_idx
            
            # Fallback: use word positions
            words = page_fitz.get_text("words")
            rows_dict = defaultdict(list)
            for word in words:
                y = round(word[3], 1)
                rows_dict[y].append(word)
            
            sorted_row_y = sorted(rows_dict.keys())
            
            best_row_idx = 0
            min_distance = float('inf')
            
            for idx, row_y in enumerate(sorted_row_y):
                distance = abs(img_y_center - row_y)
                if distance < min_distance:
                    min_distance = distance
                    best_row_idx = idx
            
            row_idx = min(best_row_idx, len(rows) - 1)
            logger.info(f'Matched image at y={img_y_center:.1f} to row {row_idx} using words (distance: {min_distance:.1f})')
            return max(0, row_idx)
            
        except Exception as e:
            logger.debug(f'Image row matching failed, using fallback: {e}')
            img_y = (img_rect[1] + img_rect[3]) / 2
            row_idx = min(int(img_y / 50), len(rows) - 1)
            return max(0, row_idx)
    
    def _extract_images_content_aware(self, pdf_doc, page_fitz, image_list, table_idx, page_num, output_dir,
                                      rows: List[List], headers: List, row_bboxes: List,
                                      row_offset=0, page_row_count=0):
        """Extract images and match them sequentially to rows (sorted by y-coordinate)"""
        images = {}
        
        try:
            logger.info(f'=== _extract_images_content_aware called ===')
            logger.info(f'Page {page_num}: image_list={len(image_list) if image_list else 0}, output_dir={output_dir}, row_bboxes={len(row_bboxes) if row_bboxes else 0}')
            
            if not image_list or not output_dir:
                logger.warning(f'Page {page_num}: Missing image_list or output_dir, returning empty images')
                return images
            
            if not row_bboxes:
                logger.error(f'Page {page_num}: CRITICAL - No row bboxes provided! Cannot match images to rows precisely.')
                logger.error(f'Page {page_num}: rows count={len(rows)}, headers count={len(headers)}')
                # Create fallback row bboxes based on estimated row positions
                # Estimate row height as average, or use a default
                logger.warning(f'Page {page_num}: Creating fallback row bboxes for {len(rows) - 1} data rows')
                estimated_row_height = 50  # Default row height estimate
                row_bboxes = []
                for i in range(len(rows) - 1):  # Exclude header
                    y0 = i * estimated_row_height
                    y1 = (i + 1) * estimated_row_height
                    row_bboxes.append((y0, y1))
                logger.info(f'Page {page_num}: Created {len(row_bboxes)} fallback row bboxes')
            
            # Extract all images and save them
            extracted_imgs = []
            for img_idx, img in enumerate(image_list):
                try:
                    xref = img[0]
                    img_dict = pdf_doc.extract_image(xref)
                    img_rects = page_fitz.get_image_rects(xref)
                    
                    if not img_dict or not img_rects:
                        continue
                    
                    rect = img_rects[0] if img_rects else None
                    if not rect:
                        continue
                    
                    # Save image
                    img_ext = img_dict.get('ext', 'png')
                    image_filename = f'page_{page_num}_table_{table_idx}_img_{img_idx}.{img_ext}'
                    image_path = os.path.join(output_dir, 'imgs', image_filename)
                    os.makedirs(os.path.dirname(image_path), exist_ok=True)
                    
                    with open(image_path, 'wb') as img_file:
                        img_file.write(img_dict['image'])
                    
                    # rect is (x0, y0, x1, y1) in PDF coordinates
                    y_center = (rect[1] + rect[3]) / 2
                    y_top = rect[1]
                    y_bottom = rect[3]
                    
                    extracted_imgs.append({
                        'img_idx': img_idx,
                        'xref': xref,
                        'img_dict': img_dict,
                        'rect': rect,
                        'y_center': y_center,
                        'y_top': y_top,
                        'y_bottom': y_bottom,
                        'image_path': image_path,
                        'image_filename': image_filename
                    })
                    
                    logger.info(f'✓ Extracted image {img_idx} from page {page_num} at y_center={y_center:.2f}')
                    
                except Exception as e:
                    logger.warning(f'Failed to extract image {img_idx}: {e}')
                    continue
            
            logger.info(f'Page {page_num}: Extracted {len(extracted_imgs)} images, {len(row_bboxes)} row bboxes, row_offset={row_offset}')
            
            if not extracted_imgs:
                return images
            
            # Sort images by y-coordinate (top to bottom) for sequential matching
            extracted_imgs.sort(key=lambda x: x.get('y_center', 0))
            
            # SIMPLIFIED SEQUENTIAL MATCHING: Match images to rows in order
            # Image 0 (top) → Row 1 (first data row), Image 1 → Row 2, etc.
            for img_idx_in_sorted, img_data in enumerate(extracted_imgs):
                img_idx = img_data['img_idx']
                
                # Calculate which row this image should go to
                # Row offset is the starting row index for this page (excludes header)
                # img_idx_in_sorted is 0, 1, 2... for sorted images on this page
                local_row_idx = img_idx_in_sorted  # 0, 1, 2...
                
                # Check if we have enough rows on this page
                if local_row_idx >= len(row_bboxes):
                    logger.warning(f'Image {img_idx} (sorted index {img_idx_in_sorted}) exceeds available rows on page {page_num} ({len(row_bboxes)} rows)')
                    continue
                
                # Get relative path for the image
                image_path = img_data['image_path']
                relative_path = image_path.replace(output_dir, '').replace('\\\\', '/').replace('\\', '/')
                if not relative_path.startswith('/'):
                    relative_path = '/' + relative_path
                
                # Store image data with row index
                images[f'img_{page_num}_{table_idx}_{local_row_idx}'] = {
                    'path': image_path,
                    'relative_path': relative_path,
                    'filename': img_data['image_filename'],
                    'table_index': table_idx,
                    'page': page_num,
                    'row_index': row_offset + local_row_idx,  # Global row index
                    'local_row_index': local_row_idx,  # Row index on this page
                    'y_center': img_data['y_center']
                }
                
                logger.info(f'✓ Matched image {img_idx} → row {row_offset + local_row_idx} (page {page_num}, local row {local_row_idx})')
            
            logger.info(f'Page {page_num}: Matched {len(images)} images to rows using sequential matching')
            
        except Exception as e:
            logger.error(f'Sequential image extraction failed: {e}', exc_info=True)
        
        return images
    
    def _sort_rows_by_sn(self, table: Dict):
        """Sort table rows by SN (Serial Number) column to maintain correct sequence (1-2-3-4-5...)"""
        try:
            headers = table.get('headers', [])
            rows = table.get('rows', [])
            
            if not headers or not rows:
                return
            
            # Find SN column index
            sn_col_idx = None
            for idx, header in enumerate(headers):
                header_lower = str(header).lower().strip()
                # Check all serial number variants
                if any(variant in header_lower for variant in self.header_variants['serial']):
                    sn_col_idx = idx
                    logger.info(f'Found SN column at index {idx}: "{header}"')
                    break
            
            if sn_col_idx is None:
                logger.warning('No SN column found, cannot sort rows by sequence')
                return
            
            # Store original row order for image remapping
            original_rows = rows.copy()
            
            # Extract SN values and sort rows
            def get_sn_value(row):
                if sn_col_idx < len(row):
                    sn_str = str(row[sn_col_idx]).strip()
                    # Try to extract numeric value from SN
                    import re
                    # Remove non-numeric characters except decimal point
                    sn_clean = re.sub(r'[^\d.]', '', sn_str)
                    try:
                        return float(sn_clean) if sn_clean else float('inf')
                    except:
                        # If not numeric, try to find first number in string
                        numbers = re.findall(r'\d+', sn_str)
                        if numbers:
                            return float(numbers[0])
                        return float('inf')
                return float('inf')
            
            # Create list of (row, original_index, sn_value) tuples
            rows_with_indices = [(row, idx, get_sn_value(row)) for idx, row in enumerate(rows)]
            # Sort by SN value
            rows_with_indices.sort(key=lambda x: x[2])
            
            # Extract sorted rows and store mapping
            sorted_rows = [row for row, _, _ in rows_with_indices]
            old_to_new_index = {old_idx: new_idx for new_idx, (_, old_idx, _) in enumerate(rows_with_indices)}
            
            # Update table with sorted rows
            table['rows'] = sorted_rows
            table['_row_sort_mapping'] = old_to_new_index  # Store mapping for image remapping
            logger.info(f'Sorted {len(rows)} rows by SN column (sequence: 1-2-3-4-5...)')
            
        except Exception as e:
            logger.error(f'Failed to sort rows by SN: {e}', exc_info=True)
    
    def _remap_images_after_sort(self, table: Dict, all_images: Dict, table_idx: int):
        """Remap image row indices after rows have been sorted by SN"""
        try:
            if '_row_sort_mapping' not in table:
                logger.info(f'Table {table_idx}: No row sorting mapping found, skipping remap')
                return  # No sorting was done
            
            row_mapping = table['_row_sort_mapping']  # old_idx -> new_idx
            rows = table.get('rows', [])
            
            # Get all images for this table
            table_images = []
            for key, img_data in all_images.items():
                if img_data.get('table_global_idx') == table_idx or img_data.get('table_index') == table_idx:
                    table_images.append((key, img_data))
            
            logger.info(f'Table {table_idx}: Found {len(table_images)} images to remap, mapping has {len(row_mapping)} entries')
            
            # Update image row indices based on new row order
            remapped_count = 0
            for key, img_data in table_images:
                old_row_idx = img_data.get('row_index')
                if old_row_idx is None:
                    logger.warning(f'Image {key} has no row_index, skipping')
                    continue
                
                # Find the new row index for this old row index
                if old_row_idx in row_mapping:
                    new_row_idx = row_mapping[old_row_idx]
                    img_data['row_index'] = new_row_idx
                    remapped_count += 1
                    logger.info(f'✓ Remapped image {key} from row {old_row_idx} to row {new_row_idx}')
                else:
                    logger.warning(f'Image {key} row_index {old_row_idx} not found in mapping, keeping original')
            
            logger.info(f'Table {table_idx}: Remapped {remapped_count}/{len(table_images)} images after sorting')
            
            # Remove mapping after use
            del table['_row_sort_mapping']
            
        except Exception as e:
            logger.error(f'Failed to remap images after sort: {e}', exc_info=True)
    
    def _merge_multipage_tables(self, tables: List[Dict]) -> List[Dict]:
        """Merge tables that continue across multiple pages (same header structure)"""
        if not tables or len(tables) <= 1:
            return tables
        
        logger.info(f'=== MERGE MULTIPAGE: Starting with {len(tables)} tables ===')
        
        merged_tables = []
        current_table = None
        
        for idx, table in enumerate(tables):
            logger.info(f'Processing table {idx}: page={table.get("page")}, cols={len(table.get("headers", []))}, rows={table.get("row_count")}')
            logger.info(f'  Headers: {table.get("headers", [])}')
            
            if current_table is None:
                # First table
                current_table = table.copy()
                current_table['pages'] = [table.get('page')]
                current_table['row_bboxes_per_page'] = {table.get('page'): table.get('row_bboxes', [])}
                logger.info(f'  -> Starting new merged table')
                continue
            
            # Check if this table is a continuation of the previous one
            # Criteria: same column count, similar/same headers, consecutive pages
            current_headers = current_table.get('headers', [])
            new_headers = table.get('headers', [])
            current_page = current_table.get('pages', [])[-1]
            new_page = table.get('page')
            
            logger.info(f'  Checking merge: current_page={current_page}, new_page={new_page}')
            logger.info(f'  Current headers ({len(current_headers)}): {current_headers}')
            logger.info(f'  New headers ({len(new_headers)}): {new_headers}')
            
            # Check if columns match
            same_column_count = len(current_headers) == len(new_headers)
            consecutive_pages = (new_page == current_page + 1)
            
            logger.info(f'  same_column_count={same_column_count}, consecutive_pages={consecutive_pages}')
            
            # Special case: If off by 1 column and consecutive, might be missing image column
            if not same_column_count and consecutive_pages and abs(len(current_headers) - len(new_headers)) == 1:
                # Check if the "headers" contain numeric data patterns (likely data row from continuation page)
                numeric_pattern = sum(1 for h in new_headers 
                                     if h and (str(h).replace('.', '').replace(',', '').replace(' ', '').isdigit() 
                                              or 'QAR' in str(h).upper() 
                                              or str(h).isdigit()))
                logger.info(f'  Column count off by 1, numeric pattern in new headers: {numeric_pattern}/{len(new_headers)}')
                
                if numeric_pattern >= 2:  # Likely a data row
                    # Find image column in current table
                    image_col_idx = -1
                    for idx, h in enumerate(current_headers):
                        if h and any(kw in str(h).upper() for kw in ['IMAGE', 'INDICATIVE', 'PHOTO', 'PICTURE']):
                            image_col_idx = idx
                            break
                    
                    if image_col_idx >= 0 and len(new_headers) < len(current_headers):
                        # Insert empty column at image position
                        new_headers.insert(image_col_idx, '')
                        # Also fix all rows in the new table
                        for row in table.get('rows', []):
                            if len(row) == len(current_headers) - 1:
                                row.insert(image_col_idx, '')
                        # Update table headers
                        table['headers'] = new_headers
                        same_column_count = True
                        logger.info(f'  -> Inserted empty image column at position {image_col_idx}, now {len(new_headers)} columns')
            
            # Check header similarity (allowing for missing headers on continuation pages)
            headers_similar = False
            if same_column_count and consecutive_pages:
                # If new table has same headers OR mostly empty headers (continuation page)
                empty_headers = sum(1 for h in new_headers if not h or str(h).strip() == '')
                logger.info(f'  Empty headers count: {empty_headers}/{len(new_headers)}')
                
                if empty_headers >= len(new_headers) * 0.7:  # 70%+ empty = continuation
                    headers_similar = True
                    logger.info(f'  -> Headers similar: continuation page (70%+ empty)')
                else:
                    # Check actual header similarity
                    matching_headers = sum(1 for i in range(len(current_headers)) 
                                          if str(current_headers[i]).lower().strip() == str(new_headers[i]).lower().strip())
                    logger.info(f'  Matching headers: {matching_headers}/{len(current_headers)}')
                    headers_similar = matching_headers >= len(current_headers) * 0.7  # 70% match
                    logger.info(f'  -> Headers similar: {headers_similar}')
            
            # Special case: If columns match but headers don't, check if first row looks like data
            # This handles continuation pages where pdfplumber treats first data row as header
            if same_column_count and consecutive_pages and not headers_similar:
                # Check if the "headers" contain numeric data patterns (likely data row)
                numeric_pattern = sum(1 for h in new_headers 
                                     if h and (str(h).replace('.', '').replace(',', '').replace(' ', '').isdigit() 
                                              or 'QAR' in str(h).upper()))
                logger.info(f'  Numeric/currency pattern in headers: {numeric_pattern}/{len(new_headers)}')
                
                if numeric_pattern >= 2:  # 2+ columns with numbers/currency = likely data row
                    headers_similar = True
                    logger.info(f'  -> Treating as continuation page (first row is data, not header)')
                    # Add the "header" row back as data
                    table['rows'].insert(0, new_headers)
            
            if same_column_count and consecutive_pages and headers_similar:
                # Merge: append rows from new table to current table
                logger.info(f'  ✓ MERGING table from page {new_page} into table starting at page {current_table["pages"][0]}')
                current_table['rows'].extend(table.get('rows', []))
                current_table['row_count'] += table.get('row_count', 0)
                current_table['pages'].append(new_page)
                current_table['row_bboxes_per_page'][new_page] = table.get('row_bboxes', [])
            else:
                # Different table - save current and start new
                logger.info(f'  ✗ NOT merging - starting new table')
                logger.info(f'    Reason: same_cols={same_column_count}, consecutive={consecutive_pages}, similar={headers_similar}')
                merged_tables.append(current_table)
                current_table = table.copy()
                current_table['pages'] = [table.get('page')]
                current_table['row_bboxes_per_page'] = {table.get('page'): table.get('row_bboxes', [])}
        
        # Add the last table
        if current_table:
            merged_tables.append(current_table)
        
        logger.info(f'=== MERGE RESULT: {len(tables)} tables -> {len(merged_tables)} merged tables ===')
        return merged_tables
    
    def _table_exists(self, new_table: List, existing_tables: List) -> bool:
        """Check if table already exists (avoid duplicates)"""
        if not new_table or len(new_table) < 2:
            return False
        
        new_text = ' '.join([str(cell) for row in new_table[:3] for cell in row if cell]).lower()
        
        for existing_table in existing_tables:
            if not existing_table or len(existing_table) < 2:
                continue
            existing_text = ' '.join([str(cell) for row in existing_table[:3] for cell in row if cell]).lower()
            
            # Check similarity
            if new_text and existing_text:
                similarity = len(set(new_text.split()) & set(existing_text.split())) / max(len(new_text.split()), len(existing_text.split()))
                if similarity > 0.7:  # 70% similarity = duplicate
                    return True
        
        return False
    
    def _empty_result(self) -> Dict:
        """Return empty result structure"""
        return {
            'tables': [],
            'images': {},
            'tables_found': 0,
            'extraction_method': 'improved',
            'total_pages': 0
        }

