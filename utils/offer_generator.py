import os
from reportlab.lib.pagesizes import letter, A4
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image as RLImage
from reportlab.pdfgen import canvas
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_RIGHT, TA_LEFT
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
import json
from datetime import datetime
import re

class OfferGenerator:
    """Generate offer documents with costing factors applied"""
    
    def __init__(self):
        self.styles = getSampleStyleSheet()
        self.setup_custom_styles()
    
    def setup_custom_styles(self):
        """Setup custom paragraph styles with Arabic support"""
        # Try to register Arabic font (fallback to Helvetica if not available)
        try:
            # Register DejaVu Sans which supports Arabic
            pdfmetrics.registerFont(TTFont('Arabic', 'DejaVuSans.ttf'))
            arabic_font = 'Arabic'
        except:
            # Fallback to Helvetica if Arabic font not available
            arabic_font = 'Helvetica'
        
        self.arabic_font = arabic_font
        
        self.title_style = ParagraphStyle(
            'CustomTitle',
            parent=self.styles['Heading1'],
            fontSize=24,
            textColor=colors.HexColor('#1a365d'),
            spaceAfter=30,
            alignment=TA_CENTER,
            fontName=arabic_font
        )
        
        self.header_style = ParagraphStyle(
            'CustomHeader',
            parent=self.styles['Heading2'],
            fontSize=16,
            textColor=colors.HexColor('#1a365d'),
            spaceAfter=12
        )
        
        # Compact style for table cells with Arabic support
        self.table_cell_style = ParagraphStyle(
            'TableCell',
            parent=self.styles['Normal'],
            fontSize=8,
            leading=10,
            spaceAfter=0,
            spaceBefore=0,
            leftIndent=0,
            rightIndent=0,
            fontName=arabic_font,
            wordWrap='CJK'  # Better word wrapping for all languages
        )
        
        # Smaller style for headers to fit in 1-2 lines
        self.table_header_style = ParagraphStyle(
            'TableHeader',
            parent=self.styles['Normal'],
            fontSize=7,
            leading=8,
            spaceAfter=0,
            spaceBefore=0,
            leftIndent=0,
            rightIndent=0,
            fontName=arabic_font,
            alignment=TA_CENTER,
            wordWrap='CJK'
        )
        
        # Smaller style for heavy text content (descriptions)
        self.table_description_style = ParagraphStyle(
            'TableDescription',
            parent=self.styles['Normal'],
            fontSize=6,
            leading=7,
            spaceAfter=0,
            spaceBefore=0,
            leftIndent=0,
            rightIndent=0,
            fontName=arabic_font,
            wordWrap='CJK'
        )

    def _get_logo_path(self):
        """Return the best available logo path."""
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
        """Draw properly placed header logo and footer website."""
        page_width, page_height = doc.pagesize
        gold = colors.HexColor('#d4af37')
        dark = colors.HexColor('#1a365d')

        # Logo centered top header with proper spacing
        logo_path = self._get_logo_path()
        if logo_path and os.path.exists(logo_path):
            try:
                logo_w = 150  # Increased width
                logo_h = 54   # Increased height for full logo visibility
                # Center horizontally
                x = (page_width - logo_w) / 2
                y = page_height - 65  # More space from top for complete logo
                canv.drawImage(logo_path, x, y, width=logo_w, height=logo_h, preserveAspectRatio=True, mask='auto')
            except Exception:
                pass

        # Top separator line positioned below the logo with proper spacing
        canv.setStrokeColor(gold)
        canv.setLineWidth(2)
        canv.line(doc.leftMargin, page_height - 75, page_width - doc.rightMargin, page_height - 75)

        # Footer with gold line and website centered
        canv.setStrokeColor(gold)
        canv.setLineWidth(2)
        canv.line(doc.leftMargin, doc.bottomMargin + 15, page_width - doc.rightMargin, doc.bottomMargin + 15)
        
        canv.setFillColor(dark)
        canv.setFont('Helvetica', 10)
        footer_text = 'https://alshayaenterprises.com'
        # Center the website in footer
        canv.drawCentredString(page_width / 2, doc.bottomMargin + 5, footer_text)
    
    def generate(self, file_id, session):
        """
        Generate offer document
        Returns: path to generated PDF
        """
        # Get file info and costed data
        uploaded_files = session.get('uploaded_files', [])
        file_info = None
        
        for f in uploaded_files:
            if f['id'] == file_id:
                file_info = f
                break
        
        if not file_info:
            raise Exception('File info not found')
        
        # Handle multi-budget tables with stitched_table (excludes Product Selection and Actions columns)
        if 'stitched_table' in file_info and file_info.get('multibudget'):
            from bs4 import BeautifulSoup
            html = file_info['stitched_table']['html']
            soup = BeautifulSoup(html, 'html.parser')
            table = soup.find('table')
            
            if not table:
                raise Exception('No table found in stitched data')
            
            # Parse table to costed_data format (excluding Product Selection and Actions columns)
            headers = []
            header_row = table.find('tr')
            if header_row:
                for th in header_row.find_all(['th', 'td']):
                    header_text = th.get_text(strip=True)
                    # Exclude Product Selection and Actions columns
                    if header_text.lower() not in ['action', 'actions', 'product selection', 'productselection']:
                        headers.append(header_text)
            
            rows = []
            for row in table.find_all('tr')[1:]:
                cells = row.find_all('td')
                if len(cells) == 0:
                    continue
                
                row_data = {}
                col_idx = 0
                for i, cell in enumerate(cells):
                    # Skip Product Selection and Actions cells
                    if cell.find(class_='product-selection-dropdowns') or cell.find('button'):
                        continue
                    text = cell.get_text(strip=True).lower()
                    if 'product selection' in text or 'actions' in text:
                        continue
                    
                    if col_idx < len(headers):
                        # Keep image HTML if present
                        img = cell.find('img')
                        if img:
                            row_data[headers[col_idx]] = str(cell)
                        else:
                            row_data[headers[col_idx]] = cell.get_text(strip=True)
                        col_idx += 1
                
                if row_data:
                    rows.append(row_data)
            
            # Create costed_data structure
            costed_data = {
                'tables': [{
                    'headers': headers,
                    'rows': rows
                }],
                'factors': {},
                'session_id': session.get('session_id', '')
            }
        elif 'costed_data' not in file_info:
            raise Exception('Costed data not found. Please apply costing first.')
        else:
            costed_data = file_info['costed_data']
        
        # Create output directory
        session_id = session['session_id']
        output_dir = os.path.join('outputs', session_id, 'offers')
        os.makedirs(output_dir, exist_ok=True)
        
        # Generate PDF
        output_file = os.path.join(output_dir, f'offer_{file_id}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.pdf')
        
        doc = SimpleDocTemplate(output_file, pagesize=A4,
                    topMargin=1.0*inch, bottomMargin=0.8*inch,
                    leftMargin=0.6*inch, rightMargin=0.6*inch)
        story = []
        
        # Title
        title = Paragraph('<font color="#1a365d">COMMERCIAL OFFER</font>', self.title_style)
        story.append(title)
        story.append(Spacer(1, 0.3*inch))
        
        # Company info (placeholder)
        company_info = Paragraph(
            """
            <b><font color="#1a365d">ALSHAYA ENTERPRISES</font></b><br/>
            <font color="#475569">P.O. Box 4451, Kuwait City</font><br/>
            <font color="#475569">Tel: +965 XXX XXXX | Email: info@alshayaenterprises.com</font>
        """,
            self.styles['Normal'])
        story.append(company_info)
        story.append(Spacer(1, 0.3*inch))
        
        # Date
        date_text = Paragraph(f"Date: {datetime.now().strftime('%B %d, %Y')}", self.styles['Normal'])
        story.append(date_text)
        story.append(Spacer(1, 0.5*inch))
        
        # Costing factors removed - confidential information
        
        # Tables with images
        for idx, table_data in enumerate(costed_data['tables']):
            header = Paragraph(f"<b><font color='#1a365d'>Item List {idx + 1}</font></b>", self.header_style)
            story.append(header)
            story.append(Spacer(1, 0.2*inch))
            
            # Get session and file info for images
            session_id = session['session_id']
            file_info = None
            uploaded_files = session.get('uploaded_files', [])
            for f in uploaded_files:
                if f['id'] == file_id:
                    file_info = f
                    break
            
            # Prepare table data with images
            table_rows = []
            
            # Headers - clean and format, exclude Action and Product Selection columns
            headers = table_data['headers']
            # Create mapping of original headers to clean string versions
            header_mapping = {}
            filtered_headers = []
            for h in headers:
                h_str = str(h).strip()
                h_lower = h_str.lower()
                # Exclude action columns and original price columns
                if h_lower not in ['action', 'actions', 'product selection', 'productselection'] and '_original' not in h_str:
                    filtered_headers.append(h_str)
                    header_mapping[h_str] = h  # Map clean string to original header
            
            header_row = [Paragraph(f"<b>{h}</b>", self.table_header_style) for h in filtered_headers]
            table_rows.append(header_row)
            
            # Data rows - show only final costed prices with images
            for row in table_data['rows']:
                table_row = []
                
                for h in filtered_headers:
                    # Use original header key for lookup
                    original_h = header_mapping.get(h, h)
                    cell_value = row.get(original_h, '')
                    
                    # Ensure cell_value is never a Paragraph object
                    if hasattr(cell_value, '__class__') and 'Paragraph' in str(type(cell_value)):
                        cell_value = str(cell_value)
                    
                    # Check if this cell contains an image reference
                    if self.contains_image(cell_value):
                        # Extract image path or URL and create image element
                        image_path = self.extract_image_path(cell_value, session_id, file_id)
                        
                        # If image_path is a URL, download it first
                        if image_path and image_path.startswith('http'):
                            from utils.image_helper import download_image
                            cached_path = download_image(image_path)
                            if cached_path:
                                image_path = cached_path
                        
                        if image_path and os.path.exists(image_path):
                            try:
                                # Create image with LARGER sizing to match Excel preview
                                from PIL import Image as PILImage
                                pil_img = PILImage.open(image_path)
                                img_width, img_height = pil_img.size
                                
                                # Much larger images like in Excel - 1.5" x 1.5" max
                                max_width = 1.5 * inch
                                max_height = 1.5 * inch
                                
                                # Scale to fit within bounds while preserving aspect ratio
                                width_ratio = max_width / img_width
                                height_ratio = max_height / img_height
                                scale_ratio = min(width_ratio, height_ratio)
                                
                                final_width = img_width * scale_ratio
                                final_height = img_height * scale_ratio
                                
                                # Ensure reasonable minimum size
                                if final_width < 0.8 * inch:
                                    final_width = 0.8 * inch
                                if final_height < 0.8 * inch:
                                    final_height = 0.8 * inch
                                
                                img = RLImage(image_path, width=final_width, height=final_height)
                                table_row.append(img)
                            except Exception as e:
                                # If image fails, show placeholder text
                                table_row.append(Paragraph("[Image]", self.table_cell_style))
                        else:
                            # Image not found, show placeholder
                            table_row.append(Paragraph("[Image]", self.table_cell_style))
                    else:
                        # Regular text cell - use final costed value only
                        # Strip any HTML tags that might remain
                        final_value = re.sub(r'<[^>]+>', '', str(cell_value))
                        final_value = final_value.strip()
                        
                        # Remove excessive newlines and normalize whitespace
                        final_value = re.sub(r'\n+', ' ', final_value)
                        final_value = re.sub(r'\s+', ' ', final_value)
                        
                        # Format numbers nicely
                        if self.is_numeric_column(h):
                            try:
                                num_val = float(re.sub(r'[^\d.-]', '', final_value))
                                final_value = f"{num_val:,.2f}"
                            except:
                                pass
                        
                        # Limit very long text to prevent cell overflow (max ~60 lines at 6pt font)
                        if len(final_value) > 800:
                            final_value = final_value[:797] + '...'
                        
                        # Use smaller font for description/item columns with heavy text
                        h_lower = h.lower()
                        if ('descript' in h_lower or 'item' in h_lower) and len(final_value) > 200:
                            cell_style = self.table_description_style
                        else:
                            cell_style = self.table_cell_style
                        
                        table_row.append(Paragraph(final_value, cell_style))
                
                table_rows.append(table_row)
            
            # Create ReportLab table with appropriate column widths using filtered headers
            col_widths = self.calculate_column_widths(filtered_headers, len(filtered_headers))
            t = Table(table_rows, colWidths=col_widths, repeatRows=1, rowHeights=None)
            
            # Enhanced table styling
            table_style = TableStyle([
                # Header styling
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#d4af37')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 9),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
                ('TOPPADDING', (0, 0), (-1, 0), 8),
                
                # Data rows styling
                ('BACKGROUND', (0, 1), (-1, -1), colors.white),
                ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
                ('ALIGN', (0, 1), (-1, -1), 'CENTER'),
                ('VALIGN', (0, 1), (-1, -1), 'MIDDLE'),
                ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
                ('FONTSIZE', (0, 1), (-1, -1), 8),
                ('TOPPADDING', (0, 1), (-1, -1), 6),
                ('BOTTOMPADDING', (0, 1), (-1, -1), 6),
                ('LEFTPADDING', (0, 1), (-1, -1), 4),
                ('RIGHTPADDING', (0, 1), (-1, -1), 4),
                
                # Grid
                ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                ('BOX', (0, 0), (-1, -1), 1, colors.black),
                
                # Alternating row colors for better readability
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f8f9fa')]),
            ])
            
            t.setStyle(table_style)
            story.append(t)
            story.append(Spacer(1, 0.4*inch))
        
        # Summary with updated VAT (5%)
        summary_header = Paragraph("<b><font color='#1a365d'>SUMMARY</font></b>", self.header_style)
        story.append(summary_header)
        story.append(Spacer(1, 0.2*inch))
        
        # Calculate totals
        subtotal = self.calculate_subtotal(costed_data['tables'])
        vat = subtotal * 0.05  # 5% VAT
        grand_total = subtotal + vat
        
        summary_data = [
            ['Subtotal:', f'{subtotal:,.2f}'],
            ['VAT (5%):', f'{vat:,.2f}'],
            ['', ''],  # Empty row for spacing
            ['Grand Total:', f'{grand_total:,.2f}']
        ]
        
        summary_table = Table(summary_data, colWidths=[4*inch, 2*inch])
        summary_style = TableStyle([
            ('ALIGN', (0, 0), (0, -1), 'RIGHT'),
            ('ALIGN', (1, 0), (1, -1), 'RIGHT'),
            ('FONTNAME', (0, 0), (-1, 2), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, 2), 11),
            ('FONTNAME', (0, 3), (-1, 3), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 3), (-1, 3), 14),
            ('TEXTCOLOR', (0, 3), (-1, 3), colors.HexColor('#1a365d')),
            ('LINEABOVE', (0, 3), (-1, 3), 2, colors.HexColor('#d4af37')),
            ('TOPPADDING', (0, 3), (-1, 3), 10),
        ])
        summary_table.setStyle(summary_style)
        story.append(summary_table)
        
        # Terms and conditions
        story.append(Spacer(1, 0.5*inch))
        terms = Paragraph("""
            <b>Terms and Conditions:</b><br/>
            1. Prices are valid for 30 days from the date of this offer.<br/>
            2. Delivery time: 4-6 weeks from order confirmation.<br/>
            3. Payment terms: 50% advance, 50% before delivery.<br/>
            4. Warranty: As per manufacturer's warranty.<br/>
        """, self.styles['Normal'])
        story.append(terms)
        
        # Build PDF
        doc.build(story, onFirstPage=self._draw_header_footer, onLaterPages=self._draw_header_footer)
        
        return output_file
    
    def calculate_subtotal(self, tables):
        """Calculate subtotal from all tables"""
        subtotal = 0.0
        
        for table in tables:
            for row in table['rows']:
                for key, value in row.items():
                    # Look for total/amount columns, exclude original values
                    if ('total' in key.lower() or 'amount' in key.lower()) and '_original' not in key:
                        try:
                            num_value = float(str(value).replace(',', '').replace('OMR', '').replace('$', '').strip())
                            subtotal += num_value
                        except:
                            pass
        
        return subtotal
    
    def contains_image(self, cell_value):
        """Check if cell contains an image reference"""
        return '<img' in str(cell_value).lower() or 'img_in_' in str(cell_value).lower()
    
    def extract_image_path(self, cell_value, session_id, file_id):
        """Extract image path or URL from cell value"""
        try:
            # Look for img src pattern
            import re
            match = re.search(r'src=["\']([^"\']+)["\']', str(cell_value))
            if match:
                img_path_or_url = match.group(1)
                # If it's a URL, return it as-is (will be downloaded later)
                if img_path_or_url.startswith('http://') or img_path_or_url.startswith('https://'):
                    return img_path_or_url
                
                # Remove leading slash if present
                img_path_or_url = img_path_or_url.lstrip('/')
                # Build absolute path from workspace root
                if img_path_or_url.startswith('outputs'):
                    img_path = img_path_or_url
                else:
                    img_path = os.path.join('outputs', session_id, file_id, img_path_or_url)
                return img_path
            
            # Try to find image reference in text
            if 'img_in_' in str(cell_value):
                match = re.search(r'(imgs/img_in_[^"\s<>]+\.jpg)', str(cell_value))
                if match:
                    img_relative_path = match.group(1)
                    img_path = os.path.join('outputs', session_id, file_id, img_relative_path)
                    return img_path
        except Exception as e:
            pass
        
        return None
    
    def is_numeric_column(self, header):
        """Check if column likely contains numeric values"""
        numeric_keywords = ['qty', 'quantity', 'rate', 'price', 'amount', 'total', 'cost']
        return any(keyword in header.lower() for keyword in numeric_keywords)
    
    def calculate_column_widths(self, headers, num_cols):
        """Calculate dynamic column widths based on content - match Excel layout with larger images"""
        total_width = 7.5 * inch  # A4 page width minus margins
        
        # Identify column types and assign appropriate widths
        widths = []
        has_description = False
        has_image = False
        
        for header in headers:
            h_lower = header.lower()
            
            # Serial number column - minimal (SN)
            if 'sn' in h_lower or 'sl' in h_lower or 'si' in h_lower or (h_lower in ['no', '#']) or 'serial' in h_lower:
                widths.append(0.3 * inch)
            
            # Location column - small
            elif 'location' in h_lower or 'loc' in h_lower:
                widths.append(0.55 * inch)
            
            # Image/reference column - MUCH LARGER to match Excel (1.6")
            elif 'img' in h_lower or 'image' in h_lower or 'indicative' in h_lower or 'ref' in h_lower:
                widths.append(1.6 * inch)
                has_image = True
            
            # Item/Product name - small if description exists
            elif 'item' in h_lower or 'product' in h_lower:
                widths.append(0.8 * inch if has_description else 2.5 * inch)
            
            # Description column - LARGE for detailed text (3.5")
            elif 'descript' in h_lower or 'discript' in h_lower:
                widths.append(3.5 * inch)
                has_description = True
            
            # Unit column - minimal
            elif 'unit' in h_lower and 'rate' not in h_lower and 'price' not in h_lower and 'total' not in h_lower:
                widths.append(0.35 * inch)
            
            # Quantity columns - small
            elif 'qty' in h_lower or 'quantity' in h_lower or 'office' in h_lower:
                widths.append(0.4 * inch)
            
            # Rate/Price - compact numbers (0.6")
            elif 'rate' in h_lower or 'price' in h_lower:
                widths.append(0.6 * inch)
            
            # Total/Amount - medium for numbers (0.7")
            elif 'amount' in h_lower or 'total' in h_lower:
                widths.append(0.7 * inch)
            
            # Supplier/Brand/Model - medium
            elif 'supplier' in h_lower or 'brand' in h_lower or 'model' in h_lower:
                widths.append(0.7 * inch)
            
            # Default for unknown columns - medium
            else:
                widths.append(0.6 * inch)
        
        # Normalize to fit total width
        current_total = sum(widths)
        if current_total > total_width:
            scale_factor = total_width / current_total
            widths = [w * scale_factor for w in widths]
        elif current_total < total_width * 0.95:  # If too small, expand proportionally
            scale_factor = (total_width * 0.98) / current_total
            widths = [w * scale_factor for w in widths]
        
        return widths
                widths.append(0.85 * inch)
            
            # Supplier/Brand - medium
            elif 'supplier' in h_lower or 'brand' in h_lower or 'model' in h_lower:
                widths.append(0.8 * inch)
            
            # Default for unknown columns
            else:
                widths.append(0.7 * inch)
        
        # Normalize to fit total width
        current_total = sum(widths)
        if current_total > total_width:
            scale_factor = total_width / current_total
            widths = [w * scale_factor for w in widths]
        elif current_total < total_width * 0.95:  # If too small, expand proportionally
            scale_factor = (total_width * 0.98) / current_total
            widths = [w * scale_factor for w in widths]
        
        return widths
