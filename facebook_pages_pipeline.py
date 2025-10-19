"""Facebook Graph API REST API Source for Pages and Posts data"""

import dlt
from dlt.sources.rest_api import (
    RESTAPIConfig,
    rest_api_resources,
)
from typing import Iterator, Dict, Any
from datetime import datetime, timedelta
import requests


@dlt.source
def facebook_pages_source(
    access_token: str = dlt.secrets["facebook_refresh_token"],
    page_id: str = dlt.secrets["facebook_page_id"],
    days_back: int = 30  # How many days of historical data to fetch
):
    """
    Facebook Pages source that extracts comprehensive data from Facebook Graph API.
    
    Produces 4 tables for complete Facebook Pages analytics:
    - page: Page information and metadata
    - post_history: Individual posts and content  
    - daily_page_metrics_total: Daily aggregated page metrics
    - lifetime_post_metrics_total: Post-level performance data
    """
    
    # Ensure all secrets are strings to avoid type issues
    access_token = str(access_token)
    page_id = str(page_id)
    
    # Calculate date range for historical data
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)
    
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://graph.facebook.com/v22.0",
            "auth": {
                "type": "bearer",
                "token": access_token,
            },
            "headers": {
                "Accept": "application/json",
            },
        },
        "resource_defaults": {
            "write_disposition": "replace",
        },
        "resources": [
            # 1. Page Information (page)
            {
                "name": "page",
                "endpoint": {
                    "path": f"/{page_id}",
                    "method": "GET",
                    "params": {
                        "fields": "id,name,category,fan_count,about,phone,website,username,description,is_published,is_permanently_closed,is_unclaimed,overall_star_rating,rating_count,talking_about_count,were_here_count,single_line_address,checkins,can_checkin,can_post,is_always_open,is_chain,is_community_page,mission,general_info,price_range,founded,company_overview"
                    }
                },
                "primary_key": "id",
                "table_name": "page"
            },
            
            # 2. Post History (post_history)
            {
                "name": "post_history",
                "endpoint": {
                    "path": f"/{page_id}/posts",
                    "method": "GET",
                    "params": {
                        "fields": "id,message,created_time,updated_time,status_type,is_published,is_hidden,permalink_url",
                        "limit": 100,
                        "since": start_date.strftime("%Y-%m-%d"),
                        "until": end_date.strftime("%Y-%m-%d")
                    },
                    "paginator": {
                        "type": "cursor",
                        "cursor_path": "paging.cursors.after",
                        "cursor_param": "after"
                    }
                },
                "primary_key": "id",
                "table_name": "post_history"
            },
            
            # 3. Daily Page Metrics (daily_page_metrics_total)
            {
                "name": "daily_page_metrics_total",
                "endpoint": {
                    "path": f"/{page_id}/insights",
                    "method": "GET",
                    "params": {
                        "metric": "page_impressions,page_impressions_unique,page_impressions_nonviral,page_impressions_paid,page_impressions_viral,page_fans,page_fan_adds,page_fan_removes,page_post_engagements,page_actions_post_reactions_total,page_actions_post_reactions_anger_total,page_actions_post_reactions_haha_total,page_actions_post_reactions_like_total,page_actions_post_reactions_love_total,page_actions_post_reactions_sorry_total,page_actions_post_reactions_wow_total,page_posts_impressions,page_posts_impressions_nonviral,page_posts_impressions_organic,page_posts_impressions_paid,page_posts_impressions_viral,page_video_views,page_total_actions,page_views_total",
                        "period": "day",
                        "since": start_date.strftime("%Y-%m-%d"),
                        "until": end_date.strftime("%Y-%m-%d")
                    },
                    "data_selector": "data"
                },
                "primary_key": ["name", "end_time"],
                "table_name": "daily_page_metrics_total"
            },
            
            # Note: lifetime_post_metrics_total is handled as a separate resource
        ],
    }

    # Get base resources
    resources = rest_api_resources(config)
    
    # Get base resources and add transformations where needed
    resources = rest_api_resources(config)
    
    for resource in resources:
        if resource.name == "page":
            yield resource | add_page_fields
        elif resource.name == "post_history":
            yield resource | add_post_fields
        elif resource.name == "daily_page_metrics_total":
            yield resource | daily_metrics_with_page_id(page_id)
        else:
            yield resource
    
    # Add post metrics as a separate resource
    yield post_metrics_resource(access_token, page_id, days_back)


def daily_metrics_with_page_id(page_id: str):
    """Create a transformer with page_id captured"""
    @dlt.transformer(name="daily_page_metrics_total", write_disposition="replace")
    def flatten_daily_metrics_inner(items):
        """Transform Facebook Insights API response into flat daily metrics structure"""
        
        # Group metrics by date
        metrics_by_date = {}
        
        for metric_response in items:
            metric_name = metric_response.get("name")
            values = metric_response.get("values", [])
            period = metric_response.get("period", "day")
            
            if period == "day" and values:
                for value_entry in values:
                    end_time = value_entry.get("end_time")
                    value = value_entry.get("value")
                    
                    if end_time:
                        # Parse date from end_time
                        try:
                            date_obj = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
                            date_str = date_obj.date().isoformat()
                        except:
                            continue
                        
                        # Initialize date entry if needed
                        if date_str not in metrics_by_date:
                            metrics_by_date[date_str] = {
                                "page_id": page_id,
                                "date": date_str,
                                "_extracted_at": datetime.now().isoformat()
                            }
                        
                        # Add this metric to the date entry
                        metrics_by_date[date_str][metric_name] = value
        
        # Yield one record per date with all metrics
        for date_str, metrics in metrics_by_date.items():
            # Add missing metrics with null values to match dbt schema
            missing_metrics = {
                "page_negative_feedback": None,  # Not available in API v22.0
                "page_impressions_organic": None,  # Not available in API v22.0
                "page_fans_online_per_day": None,  # Not available in API v22.0
                "page_places_checkin_total": None,  # Not available in API v22.0
                # Video metrics that might be missing
                "page_video_complete_views_30_s": None,
                "page_video_complete_views_30_s_autoplayed": None,
                "page_video_complete_views_30_s_click_to_play": None,
                "page_video_complete_views_30_s_organic": None,
                "page_video_complete_views_30_s_paid": None,
                "page_video_complete_views_30_s_repeat_views": None,
                "page_video_repeat_views": None,
                "page_video_view_time": None,
                "page_video_views_10_s": None,
                "page_video_views_10_s_autoplayed": None,
                "page_video_views_10_s_click_to_play": None,
                "page_video_views_10_s_organic": None,
                "page_video_views_10_s_paid": None,
                "page_video_views_10_s_repeat": None,
                "page_video_views_autoplayed": None,
                "page_video_views_click_to_play": None,
                "page_video_views_organic": None,
                "page_video_views_paid": None
            }
            
            # Add missing metrics only if they don't already exist
            for metric_name, default_value in missing_metrics.items():
                if metric_name not in metrics:
                    metrics[metric_name] = default_value
            
            yield metrics
    
    return flatten_daily_metrics_inner


@dlt.transformer(name="page", write_disposition="replace")
def add_page_fields(items):
    """Add page_id and extraction timestamp to page records"""
    for item in items:
        # Use the page's own ID if available, otherwise use current page_id
        if "id" in item:
            item["page_id"] = item["id"]
        item["_extracted_at"] = datetime.now().isoformat()
        yield item


@dlt.transformer(name="post_history", write_disposition="replace")
def add_post_fields(items):
    """Add page_id and extraction timestamp to post records"""
    for item in items:
        # Extract page_id from post id (format: page_id_post_id)
        post_id = item.get("id", "")
        if "_" in post_id:
            page_id = post_id.split("_")[0]
            item["page_id"] = page_id
        
        item["_extracted_at"] = datetime.now().isoformat()
        yield item


@dlt.resource(
    name="lifetime_post_metrics_total",
    table_name="lifetime_post_metrics_total",
    primary_key=["post_id", "date"],
    write_disposition="replace"
)
def post_metrics_resource(access_token: str, page_id: str, days_back: int):
    """
    Resource that fetches post metrics separately.
    This approach is more reliable than using transformers.
    """
    # Calculate date range for historical data
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)
    
    # First, get all posts from the page
    posts_url = f"https://graph.facebook.com/v22.0/{page_id}/posts"
    posts_params = {
        "fields": "id,created_time",
        "limit": 100,
        "since": start_date.strftime("%Y-%m-%d"),
        "until": end_date.strftime("%Y-%m-%d"),
        "access_token": access_token
    }
    
    posts_response = requests.get(posts_url, params=posts_params)
    posts_response.raise_for_status()
    posts_data = posts_response.json()
    
    # Metrics to fetch for each post (only working metrics in API v22.0)
    post_metrics = [
        "post_impressions",
        "post_impressions_unique", 
        "post_clicks",
        "post_reactions_like_total",
        "post_reactions_love_total",
        "post_reactions_wow_total",
        "post_reactions_haha_total",
        "post_reactions_sorry_total",
        "post_reactions_anger_total"
    ]
    
    for post in posts_data.get("data", []):
        post_id = post["id"]
        post_created_time = post.get("created_time")
        
        # Convert created_time to date for the date field
        try:
            if post_created_time:
                date_obj = datetime.fromisoformat(post_created_time.replace('Z', '+00:00'))
                date_str = date_obj.date().isoformat()
            else:
                date_str = datetime.now().date().isoformat()
        except:
            date_str = datetime.now().date().isoformat()
        
        # Create base record structure first
        base_record = {
            "post_id": post_id,
            "date": date_str,
            "_extracted_at": datetime.now().isoformat(),
            # Initialize all expected metrics to None
            "post_clicks": None,
            "post_engaged_fan": None,
            "post_engaged_users": None,
            "post_impressions": None,
            "post_impressions_fan": None,
            "post_impressions_nonviral": None,
            "post_impressions_organic": None,
            "post_impressions_paid": None,
            "post_impressions_viral": None,
            "post_negative_feedback": None,
            "post_reactions_anger_total": None,
            "post_reactions_haha_total": None,
            "post_reactions_like_total": None,
            "post_reactions_love_total": None,
            "post_reactions_sorry_total": None,
            "post_reactions_wow_total": None,
            "post_video_avg_time_watched": None,
            "post_video_complete_views_30_s_autoplayed": None,
            "post_video_complete_views_30_s_clicked_to_play": None,
            "post_video_complete_views_30_s_organic": None,
            "post_video_complete_views_30_s_paid": None,
            "post_video_complete_views_organic": None,
            "post_video_complete_views_paid": None,
            "post_video_length": None,
            "post_video_view_time": None,
            "post_video_view_time_organic": None,
            "post_video_views": None,
            "post_video_views_10_s": None,
            "post_video_views_10_s_autoplayed": None,
            "post_video_views_10_s_clicked_to_play": None,
            "post_video_views_10_s_organic": None,
            "post_video_views_10_s_paid": None,
            "post_video_views_10_s_sound_on": None,
            "post_video_views_15_s": None,
            "post_video_views_autoplayed": None,
            "post_video_views_clicked_to_play": None,
            "post_video_views_organic": None,
            "post_video_views_paid": None,
            "post_video_views_sound_on": None
        }
        
        # Fetch metrics for this specific post
        try:
            url = f"https://graph.facebook.com/v22.0/{post_id}/insights"
            params = {
                "metric": ",".join(post_metrics),
                "access_token": access_token
            }
            
            response = requests.get(url, params=params)
            response.raise_for_status()
            metrics_data = response.json()
            
            # Update the base record with actual metric values
            for metric in metrics_data.get("data", []):
                metric_name = metric.get("name")
                metric_values = metric.get("values", [])
                
                if metric_values and len(metric_values) > 0:
                    # Get the metric value - handle both single values and arrays
                    metric_value = metric_values[0].get("value")
                    if metric_name in base_record:
                        base_record[metric_name] = metric_value
                        
        except requests.RequestException as e:
            # Log error but continue processing other posts with default values
            print(f"Error fetching metrics for post {post_id}: {e}")
        
        # Always yield the record (with None values if API call failed)
        yield base_record




def get_data() -> None:
    """Main pipeline execution function"""
    import os
    
    # Set the database path to current directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(current_dir, "facebook_pages_pipeline.duckdb")
    
    pipeline = dlt.pipeline(
        pipeline_name='facebook_pages_pipeline',
        destination=dlt.destinations.duckdb(db_path),
        dataset_name='facebook_pages',
        progress="log"
    )

    # Load data from Facebook Graph API v22.0
    # Configure secrets.toml with:
    # facebook_refresh_token = "your_long_lived_access_token"
    # facebook_page_id = "your_page_id"
    
    load_info = pipeline.run(facebook_pages_source())
    print(load_info)
    
    # Print table info for verification
    with pipeline.sql_client() as client:
        with client.execute_query("SHOW TABLES") as cursor:
            tables = cursor.fetchall()
            print(f"Created tables: {tables}")
            
        # Print sample data from each table
        table_names = ["page", "post_history", "daily_page_metrics_total", "lifetime_post_metrics_total"]
        for table_name in table_names:
            try:
                with client.execute_query(f"SELECT COUNT(*) as count FROM {table_name}") as cursor:
                    result = cursor.fetchone()
                    print(f"Table '{table_name}': {result[0]} rows")
            except Exception as e:
                print(f"Table '{table_name}': Not found or error - {e}")


if __name__ == "__main__":
    get_data()
