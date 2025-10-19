#!/usr/bin/env python3
"""
Validation script for Facebook Pages DLT Pipeline
Checks that the pipeline produces tables compatible with dbt_facebook_pages
"""

import dlt
import sys
from typing import Dict, List


def validate_pipeline_output():
    """Validate that the pipeline creates the expected tables with correct schemas"""
    
    try:
        # Connect to the pipeline's destination
        pipeline = dlt.pipeline(
            pipeline_name='facebook_pages_pipeline',
            destination='duckdb',
            dataset_name='facebook_pages'
        )
        
        expected_tables = {
            'page': ['id', 'name', 'category', 'fan_count', '_extracted_at'],
            'post_history': ['id', 'message', 'created_time', 'page_id', '_extracted_at'],
            'daily_page_metrics_total': ['page_id', 'date', 'page_impressions', 'page_fan_adds'],
            'lifetime_post_metrics_total': ['post_id', 'date', 'post_impressions', 'post_clicks']
        }
        
        results = {}
        
        with pipeline.sql_client() as client:
            # Check that all expected tables exist
            with client.execute_query("SHOW TABLES") as cursor:
                actual_tables = [row[0] for row in cursor.fetchall()]
                
            print("🔍 Validating pipeline output tables...")
            print(f"Found tables: {actual_tables}")
            
            for table_name, required_columns in expected_tables.items():
                print(f"\n📊 Validating table: {table_name}")
                
                if table_name not in actual_tables:
                    print(f"❌ Table '{table_name}' not found!")
                    results[table_name] = {'exists': False, 'row_count': 0, 'columns': []}
                    continue
                
                # Check row count
                with client.execute_query(f"SELECT COUNT(*) FROM {table_name}") as cursor:
                    row_count = cursor.fetchone()[0]
                
                # Check columns
                with client.execute_query(f"DESCRIBE {table_name}") as cursor:
                    actual_columns = [row[0] for row in cursor.fetchall()]
                
                # Validate required columns exist
                missing_columns = [col for col in required_columns if col not in actual_columns]
                
                results[table_name] = {
                    'exists': True,
                    'row_count': row_count,
                    'columns': actual_columns,
                    'missing_columns': missing_columns
                }
                
                print(f"✅ Table exists: {len(actual_columns)} columns, {row_count} rows")
                
                if missing_columns:
                    print(f"⚠️  Missing required columns: {missing_columns}")
                else:
                    print(f"✅ All required columns present")
                
                # Show sample data if available
                if row_count > 0:
                    with client.execute_query(f"SELECT * FROM {table_name} LIMIT 3") as cursor:
                        sample_data = cursor.fetchall()
                        print(f"📝 Sample data (first 3 rows):")
                        for i, row in enumerate(sample_data, 1):
                            print(f"   Row {i}: {dict(zip(actual_columns, row))}")
        
        # Generate summary report
        print("\n" + "="*60)
        print("📈 VALIDATION SUMMARY")
        print("="*60)
        
        total_tables = len(expected_tables)
        existing_tables = sum(1 for r in results.values() if r['exists'])
        total_rows = sum(r['row_count'] for r in results.values())
        
        print(f"Tables created: {existing_tables}/{total_tables}")
        print(f"Total rows: {total_rows:,}")
        
        # Check compatibility with dbt_facebook_pages
        compatibility_score = 0
        max_score = 0
        
        for table_name, result in results.items():
            max_score += 2  # 1 for existence, 1 for required columns
            if result['exists']:
                compatibility_score += 1
                if not result.get('missing_columns', []):
                    compatibility_score += 1
        
        compatibility_pct = (compatibility_score / max_score) * 100 if max_score > 0 else 0
        
        print(f"dbt compatibility: {compatibility_pct:.1f}%")
        
        if compatibility_pct == 100:
            print("🎉 SUCCESS: Pipeline output is fully compatible with dbt_facebook_pages!")
            return True
        elif compatibility_pct >= 75:
            print("⚠️  WARNING: Pipeline output is mostly compatible but has some issues")
            return False
        else:
            print("❌ ERROR: Pipeline output has significant compatibility issues")
            return False
            
    except Exception as e:
        print(f"❌ ERROR: Failed to validate pipeline output: {e}")
        return False


def validate_dbt_compatibility():
    """Check if the current setup can work with dbt_facebook_pages models"""
    
    print("\n🔧 Checking dbt compatibility...")
    
    # Check if dbt_facebook_pages directory exists
    import os
    dbt_path = "/Users/enrico/Documents/Scripts/dlthub/facebook_pages/dbt_facebook_pages"
    
    if not os.path.exists(dbt_path):
        print("⚠️  dbt_facebook_pages directory not found")
        return False
        
    # Check key dbt files
    key_files = [
        "dbt_project.yml",
        "models/staging/src_facebook_pages.yml",
        "models/facebook_pages__pages_report.sql",
        "models/facebook_pages__posts_report.sql"
    ]
    
    missing_files = []
    for file_path in key_files:
        full_path = os.path.join(dbt_path, file_path)
        if not os.path.exists(full_path):
            missing_files.append(file_path)
    
    if missing_files:
        print(f"❌ Missing dbt files: {missing_files}")
        return False
    else:
        print("✅ All key dbt files found")
        return True


if __name__ == "__main__":
    print("🚀 Facebook Pages Pipeline Validation")
    print("="*50)
    
    # Run validations
    pipeline_valid = validate_pipeline_output()
    dbt_valid = validate_dbt_compatibility()
    
    # Final status
    print("\n" + "="*50)
    if pipeline_valid and dbt_valid:
        print("🎉 OVERALL STATUS: READY FOR PRODUCTION")
        print("✅ Pipeline produces compatible data")
        print("✅ dbt models are available") 
        print("\nNext steps:")
        print("1. Run 'dbt run --models dbt_facebook_pages'")
        print("2. Check the generated analytics tables")
        sys.exit(0)
    else:
        print("❌ OVERALL STATUS: ISSUES FOUND")
        if not pipeline_valid:
            print("❌ Pipeline output has compatibility issues")
        if not dbt_valid:
            print("❌ dbt setup is incomplete")
        print("\nPlease fix the issues above before proceeding")
        sys.exit(1)