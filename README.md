# KMFK Data Pipeline

Automated data collection and processing pipeline for Korean fund data from KOFIA (Korea Financial Investment Association).

---

## 🚀 Quick Start

### Installation

1. **Install Python dependencies:**
```bash
pip install -r requirements.txt
```

2. **Run the pipeline:**
```bash
python orchestrator.py
```

That's it! The orchestrator will:
- Download 11 datasets from KOFIA website
- Process and format the data
- Generate `KMFK_DATA_YYYYMMDD.xlsx`

---

## 📁 Project Structure

```
KMFK/
├── orchestrator.py          # Main entry point - runs entire pipeline
├── scraper.py               # Web scraper (downloads data)
├── map.py                   # Data mapper (processes data)
├── config.py                # Configuration settings
├── column_template.json     # Output format template
├── requirements.txt         # Python dependencies
├── .gitignore              # Git exclusions
├── downloads/              # Downloaded Excel files (auto-created)
├── logs/                   # Timestamped log files (auto-created)
└── KMFK_DATA_YYYYMMDD.xlsx # Final output
```

---

## 🔧 Components

### 1. **orchestrator.py** - Main Pipeline Controller

Coordinates the entire data pipeline execution.

**Usage:**
```bash
python orchestrator.py
```

**Features:**
- Runs scraper and mapper in sequence
- Comprehensive error handling
- Timestamped logging: `logs/orchestrator_YYYYMMDD_HHMMSS.log`
- Reports total execution time
- Exits with proper error codes on failure

**Output:**
- Console output with progress
- Dedicated log file
- Final data file: `KMFK_DATA_YYYYMMDD.xlsx`

---

### 2. **scraper.py** - Web Scraper

Downloads 11 datasets from KOFIA website using Selenium.

**Usage:**
```bash
python scraper.py
```

**Features:**
- ✅ Automated browser control (headless Chrome)
- ✅ Stealth mode to avoid detection
- ✅ Handles dynamic dropdowns and lazy-loading
- ✅ Processes 11 datasets in correct order
- ✅ Retry logic for failed operations
- ✅ Screenshots on errors
- ✅ Timestamped logging: `logs/scraper_YYYYMMDD_HHMMSS.log`

**Downloads:** 11 Excel files → `downloads/` folder
- Equity.xls
- DomesticEquity.xls
- HybridEquity.xls
- HybridDomesticEquity.xls
- HybridBond.xls
- HybridDomesticBond.xls
- Bond.xls
- DomesticBond.xls
- MoneyMarket.xls
- HybridAsset.xls
- DomesticHybridAsset.xls

---

### 3. **map.py** - Data Mapper

Processes downloaded Excel files into standardized format.

**Usage:**
```bash
python map.py
```

**Features:**
- ✅ Universal Korean header reading
- ✅ Intelligent column mapping (Korean → English)
- ✅ Template-based consistent output
- ✅ Missing data filled with "N.A."
- ✅ Works regardless of input file order
- ✅ Timestamped logging: `logs/mapper_YYYYMMDD_HHMMSS.log`

**Input:** `downloads/*.xls` (11 files)

**Output:** `KMFK_DATA_YYYYMMDD.xlsx`
- **Row 1:** CODE (e.g., `KMFK.EQUITY.TOTALASSET.M`)
- **Row 2:** DESCRIPTION (e.g., `Equity: Total assets`)
- **Rows 3-62:** Data (60 months of historical data)
- **Column 1:** Date (YYYY-MM format)
- **Columns 2-144:** Data columns (143 total)

---

### 4. **config.py** - Configuration

Centralized configuration for the scraper.

**Key Settings:**

```python
# Datasets to download (11 total)
DATASET_CONFIGS = [...]

# Fund type mappings
FUND_TYPES = {
    'Equity Funds': '주식형',
    'Bond': '채권형',
    'Money Market': '단기금융',
    ...
}

# Region mappings
REGIONS = {
    'All/Total': '전체',
    'Domestic': '국내',
    ...
}

# Directories
DOWNLOAD_DIR = "downloads"
LOG_DIR = "logs"
```

**Customization:**
- Modify time windows (default: 5 years)
- Add/remove datasets
- Adjust retry settings
- Configure browser options

---

## 📊 Output Format

**KMFK_DATA_YYYYMMDD.xlsx:**

| Date    | KMFK.EQUITY.TOTALASSET.M | KMFK.EQUITY.STOCK.AMOUNT.M | ... |
|---------|--------------------------|----------------------------|-----|
|         | Equity: Total assets      | Equity: Stock: Amount      | ... |
| 2020-11 | 925903                   | 820129                     | ... |
| 2020-12 | 997016                   | 882603                     | ... |
| ...     | ...                      | ...                        | ... |

- **144 columns total** (1 Date + 143 data columns)
- **62 rows total** (2 headers + 60 data rows)
- **Missing values:** Filled with "N.A."

---

## 🔍 Column Mapping

The mapper intelligently converts Korean headers to English codes:

| Korean Header | English Code | Example Full Code |
|---------------|--------------|-------------------|
| 자산총액 | TotalAsset | KMFK.EQUITY.TOTALASSET.M |
| 주식 | Stock | KMFK.EQUITY.STOCK.AMOUNT.M |
| 채권 | Bonds | KMFK.EQUITY.BONDS.WEIGHT.M |
| 예금 | Deposit | KMFK.EQUITY.DEPOSIT.AMOUNT.M |
| 콜론 | CallLoan | KMFK.EQUITY.CALLLOAN.WEIGHT.M |
| 기타 | Other | KMFK.EQUITY.OTHER.AMOUNT.M |

**Metrics:**
- 금액 → Amount
- 비중 → Weight

---

## 📝 Logging

All scripts follow consistent logging requirements:

### Directory Setup
```python
# Auto-created at startup
os.makedirs('downloads', exist_ok=True)
os.makedirs('logs', exist_ok=True)
```

### Logging Configuration
```python
# Timestamped log files
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/script_name_{timestamp}.log'),
        logging.StreamHandler()  # Console output
    ]
)
```

### Log Files Generated
- `logs/orchestrator_YYYYMMDD_HHMMSS.log` - Pipeline execution
- `logs/scraper_YYYYMMDD_HHMMSS.log` - Download progress
- `logs/mapper_YYYYMMDD_HHMMSS.log` - Processing details
- `logs/*.png` - Error screenshots (scraper only)

---

## 🛠️ Workflow Options

### Option 1: Full Pipeline (Recommended)
```bash
python orchestrator.py
```
Runs everything automatically.

### Option 2: Step-by-Step
```bash
# Step 1: Download data
python scraper.py

# Step 2: Process data
python map.py
```
Useful for debugging or running stages separately.

---

## ⚙️ Requirements

**Python 3.8+**

**Dependencies:**
```
pandas>=2.0.0
openpyxl>=3.0.0
selenium>=4.0.0
undetected-chromedriver>=3.5.0
selenium-stealth>=1.0.6
webdriver-manager>=4.0.0
```

Install with:
```bash
pip install -r requirements.txt
```

**System Requirements:**
- Chrome browser installed
- Internet connection (for scraping)
- ~100MB disk space for downloads and logs

---

## 🐛 Troubleshooting

### Issue: Scraper fails to navigate
**Solution:** Website structure may have changed. Check error screenshots in `logs/` folder.

### Issue: "No dataset files found"
**Solution:** Run `scraper.py` first to download data, then run `map.py`.

### Issue: Missing columns in output
**Solution:** Check Korean-to-English mappings in `config.py` and `map.py`.

### Issue: Chrome driver errors
**Solution:** `undetected-chromedriver` auto-updates. Delete cached drivers and retry.

---

## 📂 .gitignore

The following are excluded from version control:
```
# Directories
downloads/
logs/
__pycache__/

# Output files
KMFK_DATA_*.xlsx

# Python
*.pyc
*.pyo
```

---

## 🔐 Data Privacy

- No credentials are stored in code
- All data is downloaded from public KOFIA website
- Data is stored locally only

---

## 📄 License

Internal use only.

---

## 📧 Support

For issues or questions, check:
1. Error logs in `logs/` folder
2. Error screenshots (if scraper fails)
3. Console output for immediate feedback

---

## 🎯 Key Features Summary

✅ **Fully automated** - Single command execution
✅ **Robust error handling** - Retry logic and detailed logging
✅ **Universal mapping** - Reads Korean headers dynamically
✅ **Consistent output** - Template-based formatting
✅ **Production-ready** - Proper logging and directory structure
✅ **Maintainable** - Clean code with configuration separation

---

**Version:** 1.0
**Last Updated:** November 2024
