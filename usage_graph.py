#!/usr/bin/env python3
"""
Script to fetch usage data from MongoDB and display a graph of usage per hour.
Groups data by hour and optionally by type.
"""

import os
from datetime import datetime
from collections import defaultdict
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
    """Parse timestamp string to datetime object.
    Expected format: 'YYYY-MM-DDTHH:MM' (from .slice(0, 16))
    """
    try:
        return datetime.fromisoformat(timestamp_str)
    except ValueError:
        # Try parsing with different formats if needed
        try:
            return datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M')
        except ValueError:
            return datetime.strptime(timestamp_str[:16], '%Y-%m-%dT%H:%M')


def get_usage_data(db):
    """Fetch all documents from the usage collection"""
    collection = db['usage']
    documents = list(collection.find({}, {'type': 1, 'timestamp': 1, 'ip': 1}))
    print(f"Found {len(documents)} documents in usage collection")
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
        
        ax.set_xlabel('Hour', fontsize=12)
        ax.set_ylabel('Usage Count', fontsize=12)
        ax.set_title('Usage Per Hour by Type & Unique Users', fontsize=14, fontweight='bold')
        ax.set_xticks(range(len(hours)))
        ax.set_xticklabels([h.split()[1] if ' ' in h else h[-5:] for h in hours], rotation=45, ha='right')
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
        
        ax.set_xlabel('Hour', fontsize=12)
        ax.set_ylabel('Usage Count', fontsize=12)
        ax.set_title('Usage Per Hour', fontsize=14, fontweight='bold')
        ax.set_xticks(range(len(hours)))
        ax.set_xticklabels([h.split()[1] if ' ' in h else h[-5:] for h in hours], rotation=45, ha='right')
        ax.legend(loc='upper left', fontsize=10, framealpha=0.9)
        ax.grid(True, alpha=0.3, linestyle=':', linewidth=0.8)
        ax.tick_params(labelsize=10)
    
    plt.tight_layout()
    
    # Display the graph
    plt.show()


def print_summary(hourly_data, hourly_total, hourly_unique_user_count, all_unique_ips):
    """Print a summary of the data"""
    print("\n" + "="*60)
    print("Usage Summary")
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
        
        # Fetch data
        print("\nFetching usage data...")
        documents = get_usage_data(db)
        
        if not documents:
            print("No documents found in usage collection")
            return
        
        # Group by hour
        print("\nGrouping data by hour...")
        hourly_data, hourly_total, hourly_unique_user_count, all_unique_ips = group_by_hour(documents)
        
        # Print summary
        print_summary(hourly_data, hourly_total, hourly_unique_user_count, all_unique_ips)
        
        # Create and display graph
        print("\nGenerating graph...")
        create_graph(hourly_data, hourly_total, hourly_unique_user_count)
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()

