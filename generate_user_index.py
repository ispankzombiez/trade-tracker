#!/usr/bin/env python3
"""
Generate an index.json file for the web interface to dynamically discover available users
"""

import json
import os
from glob import glob

def generate_user_index():
    """Generate an index of available users based on existing JSON files"""
    
    web_data_dir = "web/data"
    
    if not os.path.exists(web_data_dir):
        print(f"❌ Directory {web_data_dir} not found")
        return
    
    # Find all JSON files except summary.json
    json_files = glob(os.path.join(web_data_dir, "*.json"))
    users = []
    
    for json_file in json_files:
        filename = os.path.basename(json_file)
        username = os.path.splitext(filename)[0]
        
        # Skip summary.json and other non-user files
        if username not in ['summary']:
            users.append(username)
    
    # Sort alphabetically
    users.sort()
    
    # Create index data
    index_data = {
        "generated": "2026-01-30T23:00:00Z",
        "total_users": len(users),
        "users": users
    }
    
    # Write index file
    index_path = os.path.join(web_data_dir, "index.json")
    with open(index_path, 'w') as f:
        json.dump(index_data, f, indent=2)
    
    print(f"✅ Generated user index with {len(users)} users")
    print(f"📁 Saved to: {index_path}")
    print(f"👤 Users: {', '.join(users[:10])}{'...' if len(users) > 10 else ''}")

if __name__ == "__main__":
    generate_user_index()