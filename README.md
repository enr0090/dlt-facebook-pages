# Facebook Pages DLT Data Extraction Pipeline

A robust data extraction pipeline that pulls Facebook Pages data using DLT (Data Load Tool), providing clean, structured data ready for analysis or transformation with tools like dbt.

## 🚀 Features

- **Comprehensive Data Extraction**: Extracts 4 comprehensive tables for complete Facebook Pages analytics
- **Modern Data Stack**: Built with DLT for reliable, scalable data extraction
- **Production Ready**: Includes validation, error handling, and monitoring
- **Multiple Output Formats**: Supports DuckDB, Parquet, and other formats
- **Real-time Extraction**: Uses Facebook Graph API v22.0 for up-to-date data

## 📊 Extracted Data Tables

The pipeline extracts:

1. **page** - Page information and metadata
2. **post_history** - Individual posts and content  
3. **daily_page_metrics_total** - Daily aggregated page metrics
4. **lifetime_post_metrics_total** - Post-level performance data

## 🛠 Quick Start

### Prerequisites

- Python 3.8+
- Facebook Page access token with appropriate permissions
- DuckDB (included in requirements)

### Installation

1. **Clone and setup environment:**
   ```bash
   git clone git@github.com:enr0090/dlt-facebook-pages.git
   cd dlt-facebook-pages
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. **Configure Facebook credentials:**
   ```bash
   cp secrets_template.toml .dlt/secrets.toml
   # Edit .dlt/secrets.toml with your Facebook credentials
   ```

3. **Run the pipeline:**
   ```bash
   make pipeline
   ```

## 🏃‍♂️ Usage

### Make Commands (Recommended)

```bash
# Run the complete extraction pipeline
make pipeline

# Individual steps
make extract         # Extract data from Facebook API only
make validate        # Validate pipeline output
make clean          # Clean temporary files and cache

# Setup and development
make dev-setup      # Setup development environment
make check-credentials  # Verify Facebook API credentials
```

### Manual Commands

```bash
# Extract data from Facebook API
python facebook_pages_pipeline.py

# Validate the extraction
python validate_pipeline.py

# Setup with interactive prompts
python setup_pipeline.py
```

## 📁 Project Structure

```
dlt-facebook-pages/
├── facebook_pages_pipeline.py      # Main DLT pipeline
├── setup_pipeline.py               # Setup and validation script
├── validate_pipeline.py            # Data validation
├── Makefile                        # Easy management commands
├── requirements.txt                # Python dependencies
├── secrets_template.toml           # Template for credentials
├── .dlt/                           # DLT configuration
│   ├── config.toml                 # Pipeline configuration
│   └── secrets.toml                # Facebook API credentials (create from template)
└── facebook_pages_pipeline.duckdb  # Output database
```

## ⚙️ Configuration

### Facebook API Setup

1. Create a Facebook App at [developers.facebook.com](https://developers.facebook.com)
2. Generate a Page Access Token with these permissions:
   - `pages_read_engagement`
   - `pages_show_list`
   - `read_insights`
3. Add credentials to `.dlt/secrets.toml`:

```toml
[sources.facebook_pages]
facebook_refresh_token = "your_access_token_here"
facebook_page_id = "your_page_id_here"
```

### DLT Configuration

Edit `.dlt/config.toml` to adjust:
- Date ranges for data extraction (default: 30 days)
- API rate limiting
- Output destination (DuckDB, BigQuery, Snowflake, etc.)

## 📤 Data Export

The extracted data is available in multiple formats:

### DuckDB Database
```bash
# Connect to the database
duckdb facebook_pages_pipeline.duckdb

# Query your data
SELECT * FROM page LIMIT 10;
```

### Parquet Files
DLT can export to Parquet format for use with other tools:
```bash
# Configure parquet destination in .dlt/config.toml
# Data will be available as individual parquet files
```

### Integration with dbt
For data transformation, use our companion dbt project:
👉 **[dbt-facebook-pages](https://github.com/enr0090/dbt-facebook-pages)** - Transform the extracted data with dbt models

## 🔍 Monitoring & Validation

- Pipeline logs are available in the console output
- DLT state is tracked in `_dlt_*` tables
- Validation script checks data quality and completeness
- Run `make validate` after extraction to verify data integrity

## 🛠 Development

### Environment Setup
```bash
make dev-setup
source venv/bin/activate
```

### Testing
```bash
make test
```

### Code Formatting
```bash
black facebook_pages_pipeline.py
```

## 🆘 Troubleshooting

### Common Issues

1. **Invalid Access Token**
   - Ensure your token has the required permissions
   - Check token expiration (use 60+ day tokens)
   - Verify the Page ID is correct

2. **Rate Limiting**
   - DLT handles rate limiting automatically
   - For large datasets, consider running during off-peak hours

3. **Permission Errors**
   - Verify your token has access to the specific page
   - Check that all required permissions are granted

### Getting Help
- Check the setup script output for specific error messages
- Review Facebook API documentation for permission requirements
- Ensure credentials are properly configured in `.dlt/secrets.toml`

## 🔗 Related Projects

- **[dbt-facebook-pages](https://github.com/enr0090/dbt-facebook-pages)** - Transform this data with dbt models
- [dlt](https://dlthub.com/) - Data Load Tool documentation
- [Facebook Graph API](https://developers.facebook.com/docs/graph-api/) - API documentation

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

---

**Next Step**: After extracting data, transform it with the **[dbt-facebook-pages](https://github.com/enr0090/dbt-facebook-pages)** project for analytics-ready models and dashboards.
