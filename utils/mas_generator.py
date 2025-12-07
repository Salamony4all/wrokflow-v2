import os
import re
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image as RLImage, PageBreak
from reportlab.pdfgen import canvas
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from datetime import datetime
from bs4 import BeautifulSoup

class MASGenerator:
    """Generate Material Approval Sheets (MAS) with company template"""
    
    def __init__(self):
        self.styles = getSampleStyleSheet()
        self.setup_custom_styles()
    
    def setup_custom_styles(self):
        """Setup custom styles for MAS"""
        self.title_style = ParagraphStyle(
            'MASTitle',
            parent=self.styles['Heading1'],
            fontSize=18,
            textColor=colors.HexColor('#1a365d'),
            spaceAfter=12,
            alignment=TA_CENTER,
            fontName='Helvetica-Bold'
        )
        
        self.header_style = ParagraphStyle(
            'MASHeader',
            fontSize=11,
            textColor=colors.black,
            fontName='Helvetica-Bold',
            spaceAfter=6
        )
        
        self.normal_style = ParagraphStyle(
            'MASNormal',
            fontSize=9,
            textColor=colors.black,
            leading=11,
            wordWrap='CJK'
        )

    def _get_logo_path(self):
        candidates = [
            os.path.join('static', 'images', 'AlShaya-Logo-color@2x.png'),
            os.path.join('static', 'images', 'LOGO.png'),
            os.path.join('static', 'images', 'al-shaya-logo-white@2x.png')
        ]
        for p in candidates:
            if os.path.exists(p):
                return p
        return None

    def _draw_header_footer(self, canv: canvas.Canvas, doc):
        """Draw properly placed header logo and footer website for MAS PDF."""
        page_width, page_height = doc.pagesize
        gold = colors.HexColor('#d4af37')
        dark = colors.HexColor('#1a365d')

        # Logo centered in header with proper spacing
        logo_path = self._get_logo_path()
        if logo_path and os.path.exists(logo_path):
            try:
                w = 140  # Increased width
                h = 50   # Increased height for full logo visibility
                x = (page_width - w) / 2  # Center horizontally
                y = page_height - 60  # More space from top
                canv.drawImage(logo_path, x, y, width=w, height=h, preserveAspectRatio=True, mask='auto')
            except Exception:
                pass

        # Header gold line - positioned below the logo with proper spacing
        canv.setStrokeColor(gold)
        canv.setLineWidth(2)
        canv.line(doc.leftMargin, page_height - 70, page_width - doc.rightMargin, page_height - 70)

        # Footer with gold line and website centered
        canv.setStrokeColor(gold)
        canv.setLineWidth(2)
        canv.line(doc.leftMargin, doc.bottomMargin + 15, page_width - doc.rightMargin, doc.bottomMargin + 15)
        
        canv.setFillColor(dark)
        canv.setFont('Helvetica', 10)
        footer_text = 'https://alshayaenterprises.com'
        canv.drawCentredString(page_width / 2, doc.bottomMargin + 5, footer_text)
    
    def generate(self, file_id, session):
        """
        Generate Material Approval Sheet
        Returns: path to generated PDF
        """
        # Get file info and extracted data
        uploaded_files = session.get('uploaded_files', [])
        file_info = None
        
        for f in uploaded_files:
            if f['id'] == file_id:
                file_info = f
                break
        
        if not file_info:
            raise Exception('File not found')
        
        # Priority: costed_data -> stitched_table -> extraction_result
        items = []
        session_id = session.get('session_id', '')
        
        if 'costed_data' in file_info:
            items = self.parse_items_from_costed_data(file_info['costed_data'], session, file_id)
        elif 'stitched_table' in file_info:
            items = self.parse_items_from_stitched_table(file_info['stitched_table'], session, file_id)
        elif 'extraction_result' in file_info:
            items = self.parse_items_from_extraction(file_info['extraction_result'], session, file_id)
        else:
            raise Exception('No data available. Please extract tables first.')
        
        if not items:
            raise Exception('No items found in the table. Please check your data.')
        
        # Create output directory
        output_dir = os.path.join('outputs', session_id, 'mas')
        os.makedirs(output_dir, exist_ok=True)
        
        # Generate PDF
        output_file = os.path.join(output_dir, f'mas_{file_id}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.pdf')
        
        doc = SimpleDocTemplate(output_file, pagesize=A4,
                                topMargin=0.9*inch, bottomMargin=0.7*inch,
                                leftMargin=0.6*inch, rightMargin=0.6*inch)
        story = []
        
        # Create MAS page for each item
        for idx, item in enumerate(items):
            if idx > 0:
                story.append(PageBreak())
            story.extend(self.create_mas_page(item, idx + 1, len(items)))
        
        # Build PDF
        doc.build(story, onFirstPage=self._draw_header_footer, onLaterPages=self._draw_header_footer)
        
        return output_file
    
    def parse_items_from_costed_data(self, costed_data, session, file_id):
        """Parse items from costed data"""
        items = []
        session_id = costed_data.get('session_id', session.get('session_id', ''))
        
        tables = costed_data.get('tables', [])
        for table in tables:
            rows = table.get('rows', [])
            headers = table.get('headers', [])
            
            for row in rows:
                description = ''
                qty = ''
                unit = ''
                image_path = None
                
                # Iterate through row dictionary items
                image_paths = []
                for header, cell_value in row.items():
                    # Ensure header is a string
                    header_str = str(header) if header else ''
                    header_lower = header_str.lower()
                    cell_value = str(cell_value) if cell_value else ''
                    
                    # Check for image in cell - extract ALL images
                    if '<img' in cell_value:
                        paths = self.extract_all_image_paths(cell_value, session_id, file_id)
                        if paths:
                            image_paths.extend(paths)
                    
                    # Extract data based on header
                    if any(h in header_lower for h in ['descript', 'discript', 'item', 'product']):  # Handle misspelling
                        description = re.sub(r'<[^>]+>', '', cell_value)
                    elif 'qty' in header_lower or 'quantity' in header_lower:
                        qty = re.sub(r'<[^>]+>', '', cell_value)
                    elif 'unit' in header_lower and 'rate' not in header_lower:
                        unit = re.sub(r'<[^>]+>', '', cell_value)
                
                if description:
                    brand = self.extract_brand(description)
                    specifications = self.extract_specifications(description)
                    
                    item = {
                        'description': description,
                        'qty': qty,
                        'unit': unit,
                        'brand': brand,
                        'specifications': specifications,
                        'image_path': image_paths[0] if image_paths else None,
                        'image_paths': image_paths,
                        'finish': 'As per manufacturer standard',
                        'warranty': '5 Years'
                    }
                    items.append(item)
        
        return items
    
    def parse_items_from_stitched_table(self, stitched_table, session, file_id):
        """Parse items from stitched HTML table"""
        items = []
        session_id = session.get('session_id', '')
        
        html_content = stitched_table.get('html', '')
        if not html_content:
            return items
        
        soup = BeautifulSoup(html_content, 'html.parser')
        table = soup.find('table')
        
        if not table:
            return items
        
        rows = table.find_all('tr')
        if len(rows) < 2:
            return items
        
        # Extract headers from first row
        header_row = rows[0]
        headers = []
        for th in header_row.find_all(['th', 'td']):
            header_text = th.get_text(strip=True).lower()
            # Exclude Product Selection and Actions columns
            if header_text not in ['action', 'actions', 'product selection', 'productselection']:
                headers.append(header_text)
        
        # Process data rows
        for row in rows[1:]:
            cells = row.find_all('td')
            if not cells:
                continue
            
            row_data = {}
            col_idx = 0
            for idx, cell in enumerate(cells):
                # Skip Product Selection and Actions cells
                if cell.find(class_='product-selection-dropdowns') or cell.find('button'):
                    continue
                text = cell.get_text(strip=True).lower()
                if 'product selection' in text or 'actions' in text:
                    continue
                
                if col_idx < len(headers):
                    # Keep HTML for image detection
                    cell_html = str(cell)
                    row_data[headers[col_idx]] = cell_html
                    col_idx += 1
            
            # Extract fields
            description = ''
            qty = ''
            unit = ''
            image_path = None
            
            for header, cell_value in row_data.items():
                # Ensure header is a string
                header_str = str(header).lower() if header else ''
                
                # Check for images
                if '<img' in str(cell_value):
                    img_path = self.extract_image_path(str(cell_value), session_id, file_id)
                    if img_path:
                        image_path = img_path
                
                # Clean text
                cell_text = re.sub(r'<[^>]+>', '', str(cell_value)).strip()
                
                # Map to fields
                if any(h in header_str for h in ['descript', 'discript', 'item', 'product']):  # Handle misspelling
                    description = cell_text
                elif 'qty' in header_str or 'quantity' in header_str:
                    qty = cell_text
                elif 'unit' in header_str and 'rate' not in header_str:
                    unit = cell_text
            
            if description:
                brand = self.extract_brand(description)
                specifications = self.extract_specifications(description)
                
                item = {
                    'description': description,
                    'qty': qty,
                    'unit': unit,
                    'brand': brand,
                    'specifications': specifications,
                    'image_path': image_path,
                    'finish': 'As per manufacturer standard',
                    'warranty': '5 Years'
                }
                items.append(item)
        
        return items
    
    def parse_items_from_extraction(self, extraction_result, session, file_id):
        """Parse items from raw extraction result"""
        items = []
        session_id = session.get('session_id', '')
        
        for layout_result in extraction_result.get('layoutParsingResults', []):
            markdown_text = layout_result.get('markdown', {}).get('text', '')
            images = layout_result.get('markdown', {}).get('images', {})
            
            # Parse tables
            rows = self.extract_table_rows(markdown_text)
            
            for row in rows:
                description = row.get('description', row.get('item', row.get('product', 'N/A')))
                qty = row.get('qty', row.get('quantity', 'N/A'))
                unit = row.get('unit', '')
                
                brand = self.extract_brand(description)
                specifications = self.extract_specifications(description)
                
                # Get first image if available
                image_path = None
                if images:
                    first_img = list(images.values())[0]
                    # Ensure first_img is a string, not a list
                    if isinstance(first_img, (list, tuple)):
                        first_img = first_img[0] if first_img else ''
                    if isinstance(first_img, str) and first_img:
                        image_path = os.path.join('outputs', session_id, file_id, first_img)
                
                item = {
                    'description': description,
                    'qty': qty,
                    'unit': unit,
                    'brand': brand,
                    'specifications': specifications,
                    'image_path': image_path,
                    'finish': 'As per manufacturer standard',
                    'warranty': '5 Years'
                }
                items.append(item)
        
        return items
    
    def create_mas_page(self, item, item_num, total_items):
        """Create complete MAS page for one item"""
        story = []
        
        # Header with logo placeholder
        header_data = [
            ['Material Approval Sheet', f'Item {item_num} of {total_items}']
        ]
        header_table = Table(header_data, colWidths=[5.5*inch, 1.5*inch])
        header_table.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (0, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (0, 0), 14),
            ('FONTNAME', (1, 0), (1, 0), 'Helvetica'),
            ('FONTSIZE', (1, 0), (1, 0), 9),
            ('ALIGN', (1, 0), (1, 0), 'RIGHT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
        ]))
        story.append(header_table)
        
        # Horizontal line
        line_data = [['']]
        line_table = Table(line_data, colWidths=[7*inch])
        line_table.setStyle(TableStyle([
            ('LINEBELOW', (0, 0), (-1, -1), 2, colors.HexColor('#d4af37')),
        ]))
        story.append(line_table)
        story.append(Spacer(1, 0.15*inch))
        
        # Project info - more compact
        project_data = [
            ['Project:', '[Project Name]', 'MAS No:', f'MAS-{str(item_num).zfill(3)}'],
            ['Date:', datetime.now().strftime('%d/%m/%Y'), 'Rev:', '00'],
        ]
        project_table = Table(project_data, colWidths=[1*inch, 3*inch, 0.8*inch, 2.2*inch])
        project_table.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTNAME', (2, 0), (2, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#f0f0f0')),
            ('BACKGROUND', (2, 0), (2, -1), colors.HexColor('#f0f0f0')),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('LEFTPADDING', (0, 0), (-1, -1), 4),
            ('RIGHTPADDING', (0, 0), (-1, -1), 4),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
        ]))
        story.append(project_table)
        story.append(Spacer(1, 0.15*inch))
        
        # Item details section
        details_title = Paragraph('<b>ITEM DETAILS</b>', self.header_style)
        story.append(details_title)
        story.append(Spacer(1, 0.08*inch))
        
        # Wrap long descriptions
        description_text = item.get('description', 'N/A')
        if len(description_text) > 80:
            description_text = description_text[:200] + ('...' if len(description_text) > 200 else '')
        
        details_data = [
            ['Description:', Paragraph(description_text, self.normal_style)],
            ['Brand:', item.get('brand', 'To be specified')],
            ['Quantity:', f"{item.get('qty', 'N/A')} {item.get('unit', '')}"],
            ['Finish:', item.get('finish', 'As per manufacturer standard')],
            ['Warranty:', item.get('warranty', '5 Years')],
        ]
        
        details_table = Table(details_data, colWidths=[1.5*inch, 5.5*inch])
        details_table.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#f8f9fa')),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('LEFTPADDING', (0, 0), (-1, -1), 6),
            ('RIGHTPADDING', (0, 0), (-1, -1), 6),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
        ]))
        story.append(details_table)
        story.append(Spacer(1, 0.15*inch))
        
        # Product image section - smaller to fit page
        image_title = Paragraph('<b>PRODUCT IMAGE</b>', self.header_style)
        story.append(image_title)
        story.append(Spacer(1, 0.08*inch))
        
        image_path = item.get('image_path')
        if image_path:
            # If it's a URL, download it first
            if image_path.startswith('http'):
                from utils.image_helper import download_image
                cached_path = download_image(image_path)
                if cached_path:
                    image_path = cached_path
            
            if image_path and os.path.exists(image_path):
                try:
                    from PIL import Image as PILImage
                    # Get original image dimensions
                    pil_img = PILImage.open(image_path)
                    img_width, img_height = pil_img.size
                    aspect_ratio = img_height / img_width
                    
                    # Set target width and calculate height to preserve aspect ratio
                    target_width = 2.2 * inch
                    target_height = target_width * aspect_ratio
                    
                    # Create image with proper aspect ratio
                    img = RLImage(image_path, width=target_width, height=target_height)
                    img.hAlign = 'CENTER'
                    story.append(img)
                except Exception as e:
                    # Silently skip if image fails
                    pass
        
        story.append(Spacer(1, 0.15*inch))
        
        # Technical specifications - compact
        spec_title = Paragraph('<b>SPECIFICATIONS</b>', self.header_style)
        story.append(spec_title)
        story.append(Spacer(1, 0.06*inch))
        
        specifications = item.get('specifications', [])
        if specifications:
            # Limit to 4 specs to fit on page
            specs_to_show = specifications[:4]
            spec_text = '<br/>'.join([f'• {spec}' for spec in specs_to_show])
            spec_para = Paragraph(spec_text, self.normal_style)
            story.append(spec_para)
        else:
            compact_specs = '• As per manufacturer standard specifications<br/>• Comply with relevant standards'
            story.append(Paragraph(compact_specs, self.normal_style))
        
        story.append(Spacer(1, 0.15*inch))
        
        # Approval section - more compact
        approval_title = Paragraph('<b>APPROVAL</b>', self.header_style)
        story.append(approval_title)
        story.append(Spacer(1, 0.06*inch))
        
        approval_data = [
            ['Submitted By:', '', 'Date:', ''],
            ['', '', '', ''],
            ['Approved By:', '', 'Date:', ''],
            ['', '', '', ''],
        ]
        
        approval_table = Table(approval_data, colWidths=[1.3*inch, 2.7*inch, 0.8*inch, 2.2*inch])
        approval_table.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (0, 0), 'Helvetica-Bold'),
            ('FONTNAME', (0, 2), (0, 2), 'Helvetica-Bold'),
            ('FONTNAME', (2, 0), (2, 0), 'Helvetica-Bold'),
            ('FONTNAME', (2, 2), (2, 2), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('LINEBELOW', (1, 1), (1, 1), 1, colors.black),
            ('LINEBELOW', (3, 1), (3, 1), 1, colors.black),
            ('LINEBELOW', (1, 3), (1, 3), 1, colors.black),
            ('LINEBELOW', (3, 3), (3, 3), 1, colors.black),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('TOPPADDING', (0, 0), (-1, -1), 2),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 2),
        ]))
        story.append(approval_table)
        story.append(Spacer(1, 0.1*inch))
        
        # Remarks - compact
        remarks = Paragraph('<b>Remarks:</b> _______________________________________________', self.normal_style)
        story.append(remarks)
        
        return story
    
    def extract_all_image_paths(self, html_content, session_id, file_id):
        """Extract ALL image paths from HTML content (supports multiple images)"""
        image_paths = []
        matches = re.findall(r'src=["\']([^"\']+ )["\']', html_content)
        
        for src in matches:
            # Handle URLs (http/https)
            if src.startswith('http://') or src.startswith('https://'):
                image_paths.append(src)
                continue
            
            # Handle leading slash
            if src.startswith('/'):
                src = src[1:]
            
            # Handle local paths
            if src.startswith('outputs/'):
                image_paths.append(src)
                continue
            
            # Handle relative path - ensure all parts are strings
            if isinstance(session_id, (list, tuple)):
                session_id = session_id[0] if session_id else ''
            if isinstance(file_id, (list, tuple)):
                file_id = file_id[0] if file_id else ''
            if isinstance(src, (list, tuple)):
                src = src[0] if src else ''
            
            full_path = os.path.join('outputs', str(session_id), str(file_id), str(src))
            if os.path.exists(full_path):
                image_paths.append(full_path)
            elif os.path.exists(str(src)):
                image_paths.append(str(src))
            else:
                image_paths.append(full_path)
        
        return image_paths if image_paths else None
    
    def extract_image_path(self, html_content, session_id, file_id):
        """Extract first image path from HTML content (for backward compatibility)"""
        paths = self.extract_all_image_paths(html_content, session_id, file_id)
        return paths[0] if paths else None
        
    def _extract_image_path_old(self, html_content, session_id, file_id):
        """Original single image extraction (kept for reference)"""
        match = re.search(r'src=["\']([^"\']+ )["\']', html_content)
        if match:
            src = match.group(1)
            
            # Handle URLs (http/https) - return as-is for download logic to handle
            if src.startswith('http://') or src.startswith('https://'):
                return src
            
            # Handle leading slash
            if src.startswith('/'):
                src = src[1:]
            
            # Handle local paths
            if src.startswith('outputs/'):
                return src
            
            # Handle relative path - ensure all parts are strings
            if isinstance(session_id, (list, tuple)):
                session_id = session_id[0] if session_id else ''
            if isinstance(file_id, (list, tuple)):
                file_id = file_id[0] if file_id else ''
            if isinstance(src, (list, tuple)):
                src = src[0] if src else ''
            
            full_path = os.path.join('outputs', str(session_id), str(file_id), str(src))
            if os.path.exists(full_path):
                return full_path
            # Also check if src exists as-is
            if os.path.exists(str(src)):
                return str(src)
            return full_path  # Return even if doesn't exist yet, let download logic handle it
        return None
    
    def extract_table_rows(self, markdown_text):
        """Extract table rows from markdown"""
        lines = markdown_text.split('\n')
        rows = []
        headers = []
        
        for line in lines:
            if '|' in line:
                cells = [cell.strip() for cell in line.split('|') if cell.strip()]
                
                if not headers and cells:
                    headers = [h.lower() for h in cells]
                elif headers and cells and not all(c in '-: ' for c in ''.join(cells)):
                    if len(cells) == len(headers):
                        row = dict(zip(headers, cells))
                        rows.append(row)
        
        return rows
    
    def extract_brand(self, description):
        """Extract brand from description"""
        brands = ['Sedus', 'Narbutas', 'Sokoa', 'B&T', 'Herman Miller', 'Steelcase', 'Vitra', 'Knoll', 'Haworth']
        for brand in brands:
            if brand.lower() in description.lower():
                return brand
        
        # Try to find capitalized words as potential brands
        words = description.split()
        for word in words:
            if word and len(word) > 2 and word[0].isupper():
                return word
        
        return 'To be specified'
    
    def extract_specifications(self, description):
        """Extract specifications from description"""
        specs = []
        
        # Try to extract key specifications from description
        desc_lower = description.lower()
        
        # Material
        if any(mat in desc_lower for mat in ['wood', 'metal', 'fabric', 'leather', 'plastic', 'steel', 'aluminum']):
            specs.append('Material: As specified')
        
        # Finish
        if any(fin in desc_lower for fin in ['polished', 'matte', 'glossy', 'powder coated', 'chrome']):
            specs.append('Finish: As specified')
        
        # Always add these compact specs
        if len(specs) < 2:
            specs.append('Material/Finish: Per manufacturer standard')
        
        specs.append('Color: As per approved sample')
        specs.append('Compliance: Meet relevant standards')
        
        return specs[:4]  # Limit to 4 specs maximum
