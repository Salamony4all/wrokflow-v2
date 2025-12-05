# Automated Workflow v2 - Furniture BOQ Processing Platform

> **AI-Powered Bill of Quantities extraction and automated commercial offer generation for furniture projects**

## 🎯 App Features

**Automated Document Processing**
- 📄 Extract BOQ tables from PDF documents using AI-powered table detection
- 🖼️ Process product images and match them to specifications
- 📊 Convert Excel/PDF quotes into structured data

**Interactive Editing & Costing**
- ✏️ Fully editable tables with real-time calculations
- 💰 Apply margins, freight, customs, and exchange rates automatically
- 🔢 Multiple costing tiers: budgetary, mid-range, and high-end alternatives

**Smart Brand Database**
- 🌐 Web scraping for 100+ furniture brands with automatic fallback detection
- 🔍 Intelligent product matching and enrichment
- 💾 Persistent storage with Railway volume support

**Professional Output Generation**
- 📋 Technical presentations (PPTX/PDF) with product specifications and images
- 💼 Commercial offers with branded styling and detailed breakdowns
- 📊 Material Approval Submittals (MAS) with complete product documentation

**Production Ready**
- ☁️ Deployed on Railway with automatic scaling
- 🔒 Secure environment variable management
- 📦 LibreOffice integration for cross-platform PDF generation

---

## 🚀 Quick Start

### Prerequisites

- Python 3.8 or higher
- pip (Python package manager)
- PP-StructureV3 API access token

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/Salamony4all/BOQ-platform1.git
   cd BOQ-platform1
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure API Token**
   
   Edit `app.py` and add your PP-StructureV3 API token:
   ```python
   TOKEN = "your_api_token_here"
   ```

4. **Run the application**
   ```bash
   python app.py
   ```

5. **Access the application**
   
   Open your browser and navigate to: `http://127.0.0.1:5000`

---

## 📖 User Guide

### Workflow Overview

Questemate offers four main workflows:

#### 1. **Quote with Price List** 💰
Upload BOQ and price list documents to generate instant quotations.

**Steps:**
1. Upload PDF/Excel files (BOQ + Price List)
2. Automatic extraction using PP-StructureV3 API
3. Tables are stitched across multiple pages
4. Apply costing factors (margins, freight, etc.)
5. Generate professional PDF offer

#### 2. **Multi-Budget Offers** 📊
Create three-tier pricing alternatives (Budgetary, Mid-Range, High-End).

**Steps:**
1. Upload BOQ document
2. System extracts items and quantities
3. Select products from brand database for each tier
4. Generate comparative offer with all three options

#### 3. **Presentation Generator** 🎨
Create professional PowerPoint presentations from BOQ data.

**Steps:**
1. Upload BOQ with product images
2. System extracts and organizes content
3. Generates branded PPTX with product showcase

#### 4. **MAS Generator** 📋
Generate Material Approval Submissions (MAS) for project approvals.

**Steps:**
1. Upload BOQ and specifications
2. System formats data for MAS requirements
3. Generates compliant submission documents

---

## 🎨 Features in Detail

### AI-Powered Table Extraction

- **Intelligent Detection**: Automatically identifies tables in PDF documents
- **Multi-Page Stitching**: Combines tables split across multiple pages
- **Empty Row Filtering**: Removes separator rows automatically
- **Header Recognition**: Identifies and preserves table headers
- **7-Step Progress Tracking**:
  1. File upload (with size display)
  2. Initialization
  3. PP-StructureV3 API call
  4. Content processing
  5. Table structure detection
  6. Table stitching
  7. Interactive rendering

### Interactive Table Editor

- ✏️ **Cell Editing**: Click any cell to edit content
- ➕ **Add Rows**: Insert new rows below any existing row
- 🗑️ **Delete Rows**: Remove unwanted rows
- 🖼️ **Drag & Drop Images**: Move product images between cells
- 🎨 **Alternating Row Colors**: Better readability
- 💾 **Auto-Save**: Changes are preserved in session

### Smart Costing Engine

Apply multiple financial factors:

- **Net Margin** (0-100%): Your profit margin
- **Freight** (0-50%): Shipping costs
- **Customs** (0-30%): Import duties
- **Installation** (0-40%): Setup costs
- **Exchange Rate**: Currency conversion
- **Additional Costs**: Fixed amount additions

All factors are applied with real-time calculation and preview.

### Professional PDF Generation

- 📄 Company branding (logo, colors)
- 📊 Formatted tables with images
- 💵 VAT calculation (5% default)
- 📝 Terms and conditions
- 🎨 Modern, clean design
- 📱 Responsive layout

---

## 🛠️ Technical Architecture

### Backend Stack

- **Framework**: Flask 3.0+
- **Session Management**: Flask-Session (filesystem)
- **PDF Processing**: PyMuPDF, pdfplumber, pdf2image
- **Image Processing**: OpenCV, Pillow, pytesseract
- **Data Processing**: Pandas, NumPy
- **Document Generation**: ReportLab, python-pptx, openpyxl
- **Web Scraping**: BeautifulSoup4, Selenium

### Frontend Stack

- **HTML5/CSS3**: Modern, responsive design
- **JavaScript**: Vanilla JS (no frameworks)
- **Animations**: CSS animations, transitions
- **Color Scheme**: Royal Blue (#1a365d) + Gold (#d4af37)
- **Icons**: Emoji-based for universal compatibility

### API Integration

**PP-StructureV3 API**
- Endpoint: `https://wfk3ide9lcd0x0k9.aistudio-hub.baidu.com/layout-parsing`
- Features: Table recognition, seal detection, formula recognition
- Retry logic: 3 attempts with exponential backoff
- Timeout: 60-120 seconds based on file size

### File Structure

```
quque1/
├── app.py                 # Main Flask application
├── requirements.txt       # Python dependencies
├── README.md             # This file
├── QUICK_START.md        # Quick start guide
├── templates/
│   └── index.html        # Main UI template
├── static/
│   ├── css/
│   ├── js/
│   │   └── table_manager.js  # Table manipulation logic
│   └── images/
├── utils/
│   ├── pdf_processor.py      # PDF extraction utilities
│   ├── costing_engine.py     # Costing calculations
│   ├── offer_generator.py    # PDF offer generation
│   ├── value_engineering.py  # Alternative suggestions
│   └── brand_scraper.py      # Web scraping for brands
├── uploads/              # Uploaded files (session-based)
├── outputs/              # Generated documents
└── brands_data/          # Brand catalog database
```

---

## ⚙️ Configuration

### Extraction Settings

Configure in the UI under "Extraction Settings":

- ✅ **Table Recognition** (Required): Detects table structures
- ☑️ **Seal Recognition**: Identifies stamps and seals
- ☑️ **Region Detection**: Analyzes document layout
- ☑️ **Format Block Content**: Preserves formatting
- ☐ **Formula Recognition**: Detects mathematical formulas (disabled by default)
- ☐ **Chart Recognition**: Identifies charts and graphs
- ☐ **Visualize (Debug)**: Shows detection overlays (disabled by default)

### Session Management

- **Storage**: Filesystem-based sessions
- **Cleanup**: Automatic cleanup of files older than 24 hours
- **Session ID**: UUID-based unique identifiers
- **File Limits**: 50MB max file size

---

## 📊 Supported File Formats

### Input Formats
- **PDF**: Primary format for BOQ documents
- **Excel**: .xlsx, .xls for price lists
- **Images**: .jpg, .jpeg, .png for product photos

### Output Formats
- **PDF**: Commercial offers, quotations
- **Excel**: Extracted tables, costing sheets
- **PowerPoint**: Presentations (.pptx)
- **Word**: MAS documents (.docx)

---

## 🔧 Troubleshooting

### Common Issues

**1. "File not found" error after successful extraction**
- **Cause**: Session timeout or race condition
- **Solution**: Refresh the page and re-upload the file

**2. Empty rows appearing between products**
- **Status**: ✅ Fixed in latest version
- **Solution**: Update to latest code (empty row filtering implemented)

**3. Duplicate add/remove buttons**
- **Status**: ✅ Fixed in latest version
- **Solution**: Update to latest code (button cleanup implemented)

**4. API extraction fails**
- **Check**: API token is valid
- **Check**: File size is under 50MB
- **Check**: Internet connection is stable
- **Try**: Reduce file size or split into multiple files

**5. Progress bar shows fake errors**
- **Status**: ✅ Fixed in latest version
- **Solution**: Update to latest code (proper error state management)

### Debug Mode

Enable detailed logging:
```python
# In app.py
logging.basicConfig(level=logging.DEBUG)
```

Check logs in `server.log` for detailed error messages.

---

## 🎯 Best Practices

### For Best Extraction Results

1. **File Quality**: Use high-resolution PDFs (300 DPI+)
2. **Table Format**: Clear borders and consistent formatting
3. **File Size**: Keep under 20MB for faster processing
4. **Page Count**: Split large documents (50+ pages) into batches
5. **Language**: English text works best with current OCR

### For Accurate Costing

1. **Verify Extracted Data**: Review tables before applying costs
2. **Check Units**: Ensure quantities and units are correct
3. **Test Factors**: Start with small margins to verify calculations
4. **Save Frequently**: Download Excel after each major edit

---

## 🚧 Roadmap

### Planned Features

- [ ] Multi-language support (Arabic, French)
- [ ] Cloud storage integration (AWS S3, Google Drive)
- [ ] User authentication and multi-tenancy
- [ ] Advanced analytics dashboard
- [ ] Email integration for sending offers
- [ ] Template library for different industries
- [ ] Mobile-responsive design improvements
- [ ] Batch processing for multiple files
- [ ] API for third-party integrations

---

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 👥 Support

For issues, questions, or feature requests:

- **GitHub Issues**: [Create an issue](https://github.com/Salamony4all/BOQ-platform1/issues)
- **Email**: support@questemate.com
- **Documentation**: See `QUICK_START.md` for detailed guides

---

## 🙏 Acknowledgments

- **PP-StructureV3 API** by Baidu for intelligent document analysis
- **Flask** framework for robust web application foundation
- **ReportLab** for professional PDF generation
- **OpenCV** for image processing capabilities

---

