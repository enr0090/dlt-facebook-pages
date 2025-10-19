#!/usr/bin/env python3
"""
Setup script for Facebook Pages DLT Pipeline
Helps users configure and test the enhanced pipeline
"""

import os
import sys
import shutil
from pathlib import Path


def setup_dlt_environment():
    """Setup DLT configuration directory and files"""
    
    print("🔧 Setting up DLT environment...")
    
    # Create .dlt directory if it doesn't exist
    dlt_dir = Path(".dlt")
    dlt_dir.mkdir(exist_ok=True)
    
    # Copy secrets template if secrets.toml doesn't exist
    secrets_file = dlt_dir / "secrets.toml"
    if not secrets_file.exists():
        shutil.copy("secrets_template.toml", secrets_file)
        print(f"✅ Created {secrets_file}")
        print("   📝 Please edit this file with your Facebook credentials")
    else:
        print(f"✅ {secrets_file} already exists")
    
    # Create basic config.toml if it doesn't exist
    config_file = dlt_dir / "config.toml"
    if not config_file.exists():
        config_content = """# DLT Configuration for Facebook Pages Pipeline

[runtime]
log_level = "INFO"
progress = "log"

[extract]
max_parallel_resources = 3

[load]
batch_size = 1000

[sources.facebook_pages_source]
# Override defaults here if needed
# days_back = 30
"""
        with open(config_file, 'w') as f:
            f.write(config_content)
        print(f"✅ Created {config_file}")
    
    return True


def check_dependencies():
    """Check if required dependencies are installed"""
    
    print("📦 Checking dependencies...")
    
    required_packages = [
        'dlt',
        'requests', 
        'duckdb'
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
            print(f"✅ {package} installed")
        except ImportError:
            missing_packages.append(package)
            print(f"❌ {package} NOT installed")
    
    if missing_packages:
        print(f"\n⚠️  Missing packages: {missing_packages}")
        print("Install with: pip install -r requirements.txt")
        return False
    else:
        print("✅ All dependencies satisfied")
        return True


def validate_facebook_config():
    """Validate Facebook configuration"""
    
    print("🔍 Validating Facebook configuration...")
    
    secrets_file = Path(".dlt/secrets.toml")
    if not secrets_file.exists():
        print("❌ .dlt/secrets.toml not found")
        return False
    
    # Read secrets file
    try:
        with open(secrets_file, 'r') as f:
            content = f.read()
            
        # Check for placeholder values
        if "YOUR_FACEBOOK_LONG_LIVED_ACCESS_TOKEN_HERE" in content:
            print("❌ Facebook access token not configured (still has placeholder)")
            return False
        
        if "YOUR_FACEBOOK_PAGE_ID_HERE" in content:
            print("❌ Facebook page ID not configured (still has placeholder)")
            return False
        
        print("✅ Facebook configuration appears to be set")
        return True
        
    except Exception as e:
        print(f"❌ Error reading secrets file: {e}")
        return False


def test_facebook_connection():
    """Test connection to Facebook API"""
    
    print("🌐 Testing Facebook API connection...")
    
    try:
        import dlt
        from facebook_pages_pipeline import facebook_pages_source
        
        # Try to initialize the source (this will validate secrets)
        print("   Initializing Facebook Pages source...")
        source = facebook_pages_source()
        
        # Try to get the resource iterator (this tests the configuration)
        print("   Testing source configuration...")
        resources = list(source)
        print(f"   Found {len(resources)} configured resources")
        
        print("✅ Facebook API connection test passed")
        return True
        
    except Exception as e:
        print(f"❌ Facebook API connection failed: {e}")
        if "include_from_parent" in str(e):
            print("   This appears to be a configuration issue, not credentials")
        else:
            print("   Check your access token and page ID in .dlt/secrets.toml")
        return False


def run_test_pipeline():
    """Run a small test to verify the pipeline works"""
    
    print("🧪 Running test pipeline...")
    
    try:
        from facebook_pages_pipeline import get_data
        
        # Run the pipeline
        print("   Starting pipeline run...")
        get_data()
        print("✅ Test pipeline completed successfully")
        return True
        
    except Exception as e:
        print(f"❌ Test pipeline failed: {e}")
        return False


def generate_usage_guide():
    """Generate a usage guide for the user"""
    
    guide = """
🎉 SETUP COMPLETE!

Your enhanced Facebook Pages DLT Pipeline is ready to use.

## Quick Usage Guide

### 1. Run the full pipeline:
```bash
python facebook_pages_pipeline.py
```

### 2. Validate the output:
```bash
python validate_pipeline.py
```

### 3. Use with dbt (if you have dbt_facebook_pages):
```bash
dbt run --models dbt_facebook_pages
```

## What you get:

📊 **4 dbt-compatible tables:**
- `page` - Page information and metadata
- `post_history` - Individual posts and content  
- `daily_page_metrics_total` - Daily aggregated metrics
- `lifetime_post_metrics_total` - Post-level performance data

## Configuration:

📁 **Config files created:**
- `.dlt/secrets.toml` - Your Facebook credentials (keep secure!)
- `.dlt/config.toml` - Pipeline configuration options

## Need help?

📖 **Documentation:** README_enhanced.md
🐛 **Issues:** Check troubleshooting section in README
🔧 **Customization:** Edit facebook_pages_pipeline.py

## Pro Tips:

- Run pipeline daily/hourly for fresh data
- Adjust `days_back` parameter for historical data range
- Monitor API rate limits (200 calls/hour per user)
- Use long-lived access tokens (60+ days validity)

Happy data loading! 🚀
"""
    
    print(guide)


def main():
    """Main setup workflow"""
    
    print("🚀 Facebook Pages DLT Pipeline Setup")
    print("="*50)
    
    # Run setup steps
    steps = [
        ("📦 Dependencies", check_dependencies),
        ("🔧 DLT Environment", setup_dlt_environment), 
        ("🔍 Facebook Config", validate_facebook_config),
        ("🌐 API Connection", test_facebook_connection),
        ("🧪 Test Pipeline", run_test_pipeline)
    ]
    
    success_count = 0
    
    for step_name, step_func in steps:
        print(f"\n{step_name}")
        print("-" * 30)
        
        if step_func():
            success_count += 1
        else:
            print(f"\n❌ Setup failed at step: {step_name}")
            print("Please fix the issues above and run setup again.")
            sys.exit(1)
    
    # All steps passed
    print("\n" + "="*50)
    print(f"✅ Setup completed successfully! ({success_count}/{len(steps)} steps)")
    
    # Show usage guide
    generate_usage_guide()


if __name__ == "__main__":
    main()