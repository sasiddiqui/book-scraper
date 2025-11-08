#!/usr/bin/env python3
"""
Script to fetch usage data from MongoDB and display a graph of usage per hour.
Groups data by hour and optionally by type.
"""

import os
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from urllib.parse import urlparse
import matplotlib.pyplot as plt
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def connect_to_db():
    """Connect to MongoDB using connection string from environment or mongourl.txt"""
    mongodb_uri = os.getenv('MONGODB_URI')
    
    if not mongodb_uri:
        # Try reading from mongourl.txt if env var not set
        mongourl_path = os.path.join(os.path.dirname(__file__), 'mongourl.txt')
        if os.path.exists(mongourl_path):
            with open(mongourl_path, 'r') as f:
                mongodb_uri = f.read().strip()
        else:
            raise ValueError("MONGODB_URI not set and mongourl.txt not found")
    
    client = MongoClient(mongodb_uri)
    db = client['data']
    return db


def parse_timestamp(timestamp_str):
    """Parse timestamp string to datetime object and convert from UTC to local time.
    Expected format: 'YYYY-MM-DDTHH:MM' (from .slice(0, 16))
    """
    try:
        # Parse as UTC (since MongoDB stores timestamps in UTC)
        dt_utc = datetime.fromisoformat(timestamp_str).replace(tzinfo=timezone.utc)
        # Convert to local time
        dt_local = dt_utc.astimezone()
        return dt_local
    except ValueError:
        # Try parsing with different formats if needed
        try:
            dt_utc = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M').replace(tzinfo=timezone.utc)
            dt_local = dt_utc.astimezone()
            return dt_local
        except ValueError:
            dt_utc = datetime.strptime(timestamp_str[:16], '%Y-%m-%dT%H:%M').replace(tzinfo=timezone.utc)
            dt_local = dt_utc.astimezone()
            return dt_local


def get_usage_data(db, hours=24):
    """Fetch documents from the usage collection within the last N hours"""
    collection = db['usage']
    
    # Calculate cutoff time (24 hours ago in UTC)
    cutoff_utc = datetime.now(timezone.utc) - timedelta(hours=hours)
    # Format as ISO string for MongoDB query (without timezone info, MongoDB treats as UTC)
    cutoff_str = cutoff_utc.strftime('%Y-%m-%dT%H:%M')
    
    # Query for documents with timestamp >= cutoff
    query = {'timestamp': {'$gte': cutoff_str}}
    documents = list(collection.find(query, {'type': 1, 'timestamp': 1, 'ip': 1, 'bookSource': 1}))
    print(f"Found {len(documents)} documents in usage collection from the last {hours} hours")
    return documents


def group_by_hour(documents):
    """Group documents by hour and optionally by type"""
    hourly_data = defaultdict(lambda: defaultdict(int))
    hourly_total = defaultdict(int)
    hourly_unique_ips = defaultdict(set)  # Track unique IPs per hour
    
    for doc in documents:
        if 'timestamp' not in doc or 'type' not in doc:
            continue
        
        try:
            dt = parse_timestamp(doc['timestamp'])
            hour_key = dt.strftime('%Y-%m-%d %H:00')
            
            # Count by type
            usage_type = doc.get('type', 'unknown')
            hourly_data[hour_key][usage_type] += 1
            
            # Count total
            hourly_total[hour_key] += 1
            
            # Track unique IPs
            if 'ip' in doc and doc['ip']:
                hourly_unique_ips[hour_key].add(doc['ip'])
        except Exception as e:
            print(f"Error parsing timestamp {doc.get('timestamp')}: {e}")
            continue
    
    # Convert sets to counts
    hourly_unique_user_count = {hour: len(ips) for hour, ips in hourly_unique_ips.items()}
    
    # Get all unique IPs across all hours for summary
    all_unique_ips = set()
    for ips_set in hourly_unique_ips.values():
        all_unique_ips.update(ips_set)
    
    return hourly_data, hourly_total, hourly_unique_user_count, all_unique_ips


def get_book_clicks_by_store(documents):
    """Count book clicks per store using bookSource field"""
    store_clicks = defaultdict(int)
    
    for doc in documents:
        # Only count documents where type is 'book_clicked' and has bookSource
        if doc.get('type') == 'book_clicked' and 'bookSource' in doc and doc['bookSource']:
            store = doc['bookSource']
            # Clean up the store name
            if store:
                # Remove query params if any
                store = store.split('?')[0]
                # If it's a URL, extract domain or last path segment
                if store.startswith('http://') or store.startswith('https://'):
                    parsed = urlparse(store)
                    # Use domain name, removing www. if present
                    store = parsed.netloc.replace('www.', '').split(':')[0]
                    if not store:
                        # Fallback to last path segment
                        store = parsed.path.strip('/').split('/')[-1] if parsed.path else 'Unknown'
                else:
                    # Not a URL, use as-is
                    store = store.split('/')[-1] if '/' in store else store
            else:
                store = 'Unknown'
            
            store_clicks[store] += 1
    
    return store_clicks


def create_store_clicks_chart(store_clicks):
    """Create a bar chart showing book clicks per store"""
    if not store_clicks:
        print("No book click data to display")
        return
    
    # Sort stores by click count (descending)
    sorted_stores = sorted(store_clicks.items(), key=lambda x: x[1], reverse=True)
    stores = [store for store, _ in sorted_stores]
    counts = [count for _, count in sorted_stores]
    
    # Create figure
    fig, ax = plt.subplots(figsize=(14, 8))
    
    # Use a nice color gradient based on count
    colors = plt.cm.viridis([count / max(counts) for count in counts])
    
    # Create bar chart
    bars = ax.bar(range(len(stores)), counts, color=colors, alpha=0.8, edgecolor='black', linewidth=1)
    
    # Add value labels on bars
    for i, (bar, count) in enumerate(zip(bars, counts)):
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
               f'{count}',
               ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    ax.set_xlabel('Store', fontsize=12)
    ax.set_ylabel('Book Clicks', fontsize=12)
    ax.set_title('Book Clicks by Store', fontsize=14, fontweight='bold')
    ax.set_xticks(range(len(stores)))
    ax.set_xticklabels(stores, rotation=45, ha='right')
    ax.grid(True, alpha=0.3, axis='y', linestyle=':', linewidth=0.8)
    ax.tick_params(labelsize=10)
    
    plt.tight_layout()
    
    # Display the graph
    plt.show()


def create_graph(hourly_data, hourly_total, hourly_unique_user_count):
    """Create a graph showing usage per hour"""
    # Sort by hour
    sorted_hours = sorted(hourly_total.keys())
    
    if not sorted_hours:
        print("No data to display")
        return
    
    # Extract all unique types
    all_types = set()
    for hour_data in hourly_data.values():
        all_types.update(hour_data.keys())
    
    # Prepare data for plotting
    hours = sorted_hours
    
    # Create single figure
    fig, ax = plt.subplots(figsize=(14, 8))
    
    # Use nicer color palette - tab10 is a good default with distinct colors
    # For more types, we can extend with tab20
    nice_colors = ['#2E86AB', '#A23B72', '#F18F01', '#C73E1D', '#6A994E', 
                   '#BC4749', '#7209B7', '#F72585', '#4CC9F0', '#4361EE',
                   '#06FFA5', '#FB8500', '#8338EC', '#FF006E', '#06A77D']
    
    # Plot one line for each usage type
    if all_types:
        sorted_types = sorted(all_types)
        color_map = {t: nice_colors[i % len(nice_colors)] for i, t in enumerate(sorted_types)}
        
        for usage_type in sorted_types:
            values = [hourly_data[h].get(usage_type, 0) for h in hours]
            ax.plot(range(len(hours)), values, marker='o', linewidth=2.5, 
                   markersize=7, label=usage_type, color=color_map[usage_type], alpha=0.8)
        
        # Add unique users line with a distinct style
        unique_user_values = [hourly_unique_user_count.get(h, 0) for h in hours]
        ax.plot(range(len(hours)), unique_user_values, marker='s', linewidth=3,
               markersize=7, label='Unique Users (by IP)', color='#FF006E', 
               linestyle='--', alpha=0.9)
        
        ax.set_xlabel('Hour (Local Time)', fontsize=12)
        ax.set_ylabel('Usage Count', fontsize=12)
        ax.set_title('Usage Per Hour by Type & Unique Users (Last 24 Hours)', fontsize=14, fontweight='bold')
        ax.set_xticks(range(len(hours)))
        # Format labels: show "MM-DD HH:00" for better readability
        hour_labels = []
        for h in hours:
            if ' ' in h:
                date_part, time_part = h.split()
                # Show as "MM-DD HH:00"
                date_obj = datetime.strptime(date_part, '%Y-%m-%d')
                hour_labels.append(f"{date_obj.strftime('%m-%d')} {time_part}")
            else:
                hour_labels.append(h[-5:] if len(h) >= 5 else h)
        ax.set_xticklabels(hour_labels, rotation=45, ha='right')
        ax.legend(loc='upper left', fontsize=10, framealpha=0.9)
        ax.grid(True, alpha=0.3, linestyle=':', linewidth=0.8)
        ax.tick_params(labelsize=10)
    else:
        # Fallback if no types found
        totals = [hourly_total[h] for h in hours]
        ax.plot(range(len(hours)), totals, marker='o', linewidth=2.5, markersize=7, 
               color=nice_colors[0], alpha=0.8)
        
        # Still add unique users line
        unique_user_values = [hourly_unique_user_count.get(h, 0) for h in hours]
        ax.plot(range(len(hours)), unique_user_values, marker='s', linewidth=3,
               markersize=7, label='Unique Users (by IP)', color='#FF006E', 
               linestyle='--', alpha=0.9)
        
        ax.set_xlabel('Hour (Local Time)', fontsize=12)
        ax.set_ylabel('Usage Count', fontsize=12)
        ax.set_title('Usage Per Hour (Last 24 Hours)', fontsize=14, fontweight='bold')
        ax.set_xticks(range(len(hours)))
        # Format labels: show "MM-DD HH:00" for better readability
        hour_labels = []
        for h in hours:
            if ' ' in h:
                date_part, time_part = h.split()
                # Show as "MM-DD HH:00"
                date_obj = datetime.strptime(date_part, '%Y-%m-%d')
                hour_labels.append(f"{date_obj.strftime('%m-%d')} {time_part}")
            else:
                hour_labels.append(h[-5:] if len(h) >= 5 else h)
        ax.set_xticklabels(hour_labels, rotation=45, ha='right')
        ax.legend(loc='upper left', fontsize=10, framealpha=0.9)
        ax.grid(True, alpha=0.3, linestyle=':', linewidth=0.8)
        ax.tick_params(labelsize=10)
    
    plt.tight_layout()
    
    # Display the graph
    plt.show()


def print_summary(hourly_data, hourly_total, hourly_unique_user_count, all_unique_ips):
    """Print a summary of the data"""
    print("\n" + "="*60)
    print("Usage Summary (Last 24 Hours - Local Time)")
    print("="*60)
    
    sorted_hours = sorted(hourly_total.keys())
    
    print(f"\nTotal hours with data: {len(sorted_hours)}")
    print(f"Total usage events: {sum(hourly_total.values())}")
    print(f"Total unique users (IPs): {len(all_unique_ips)}")
    if hourly_unique_user_count:
        max_unique = max(hourly_unique_user_count.values())
        avg_unique = sum(hourly_unique_user_count.values()) / len(hourly_unique_user_count)
        print(f"Max unique users in a single hour: {max_unique}")
        print(f"Average unique users per hour: {avg_unique:.1f}")
    
    # Get all types
    all_types = set()
    for hour_data in hourly_data.values():
        all_types.update(hour_data.keys())
    
    print(f"\nUsage types found: {', '.join(sorted(all_types))}")
    
    # Print hourly breakdown
    print("\n" + "-"*60)
    print("Hourly Breakdown:")
    print("-"*60)
    print(f"{'Hour':<20} {'Total':<10} {'By Type':<30}")
    print("-"*60)
    
    for hour in sorted_hours[:20]:  # Show first 20 hours
        type_str = ', '.join([f"{t}: {c}" for t, c in sorted(hourly_data[hour].items())])
        print(f"{hour:<20} {hourly_total[hour]:<10} {type_str[:30]}")
    
    if len(sorted_hours) > 20:
        print(f"\n... and {len(sorted_hours) - 20} more hours")


def main():
    """Main function"""
    try:
        # Connect to database
        print("Connecting to MongoDB...")
        db = connect_to_db()
        print("Connected successfully!")
        
        # Fetch data (last 24 hours by default)
        print("\nFetching usage data from the last 24 hours...")
        documents = get_usage_data(db, hours=24000)
        
        if not documents:
            print("No documents found in usage collection")
            return
        
        # Group by hour
        print("\nGrouping data by hour...")
        hourly_data, hourly_total, hourly_unique_user_count, all_unique_ips = group_by_hour(documents)
        
        # Print summary
        print_summary(hourly_data, hourly_total, hourly_unique_user_count, all_unique_ips)
        
        # Get book clicks by store
        print("\nAnalyzing book clicks by store...")
        store_clicks = get_book_clicks_by_store(documents)
        
        # Print store clicks summary
        if store_clicks:
            print(f"\nFound {len(store_clicks)} stores with book clicks")
            print("\nTop stores by clicks:")
            sorted_stores = sorted(store_clicks.items(), key=lambda x: x[1], reverse=True)
            for store, count in sorted_stores[:10]:  # Show top 10
                print(f"  {store}: {count} clicks")
        
        # Create and display graphs
        print("\nGenerating graphs...")
        create_graph(hourly_data, hourly_total, hourly_unique_user_count)
        
        if store_clicks:
            create_store_clicks_chart(store_clicks)
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()

