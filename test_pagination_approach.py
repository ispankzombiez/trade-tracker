#!/usr/bin/env python3
"""
Test the pagination approach - fetch all farms and filter for our specific ones
"""

import json
import requests
import os
from typing import Dict, List, Set

API_KEY = "sfl.MTEyODk3NjMwMTU4MzUwOA.YKi8l48T3jH_mPYvpxgCrkeS_IUt3uWQFgTUQg40JCE"

def load_our_farm_ids() -> Set[str]:
    """Load farm IDs from farm_ids.txt and convert to set for fast lookup"""
    try:
        with open('farm_ids.txt', 'r') as f:
            lines = [line.strip() for line in f if line.strip()]
        
        # Skip the first line if it's the wait time setting
        if lines and lines[0].startswith('WAIT_TIME_SECONDS='):
            farm_ids = lines[1:]
        else:
            farm_ids = lines
        
        print(f"📋 Loaded {len(farm_ids)} target farm IDs from farm_ids.txt")
        return set(farm_ids)
    except FileNotFoundError:
        print("❌ farm_ids.txt not found!")
        return set()

def load_farm_id_mapping() -> Dict[str, str]:
    """Load the farm ID mapping for short->long conversion"""
    try:
        with open('farm_id_mapping.json', 'r') as f:
            mapping = json.load(f)
        print(f"📋 Loaded {len(mapping)} farm ID mappings")
        return mapping
    except FileNotFoundError:
        print("❌ farm_id_mapping.json not found!")
        return {}

def fetch_all_farms() -> List[Dict]:
    """Fetch all farms using the pagination endpoint"""
    headers = {'x-api-key': API_KEY}
    url = "https://api.sunflower-land.com/community/farms"
    
    print("🌍 Fetching ALL farms from pagination endpoint...")
    print(f"🔗 URL: {url}")
    
    try:
        response = requests.get(url, headers=headers, timeout=60)
        print(f"📡 Response status: {response.status_code}")
        print(f"📏 Response size: {len(response.text):,} bytes ({len(response.text)/1024/1024:.1f} MB)")
        
        if response.status_code == 200:
            data = response.json()
            farms = data.get('farms', [])
            print(f"✅ Successfully fetched {len(farms):,} total farms")
            return farms
        else:
            print(f"❌ API error: {response.status_code}")
            print(f"📄 Response: {response.text[:200]}")
            return []
            
    except Exception as e:
        print(f"❌ Error fetching farms: {e}")
        return []

def filter_farms_for_our_list(all_farms: List[Dict], target_farm_ids: Set[str], farm_mapping: Dict[str, str]) -> Dict[str, Dict]:
    """Filter the farms list to only include our target farms"""
    
    print(f"\n🔍 Filtering {len(all_farms):,} farms for our {len(target_farm_ids)} target farms...")
    
    # Create sets of all possible IDs we're looking for
    target_long_ids = set()
    
    for short_id in target_farm_ids:
        # Add the short ID itself
        target_long_ids.add(short_id)
        
        # Add the mapped long ID if available
        if short_id in farm_mapping:
            long_id = farm_mapping[short_id]
            target_long_ids.add(long_id)
    
    print(f"🎯 Looking for farms with IDs in: {list(target_long_ids)[:10]}{'...' if len(target_long_ids) > 10 else ''}")
    
    # Filter farms
    matching_farms = {}
    found_ids = set()
    
    for farm in all_farms:
        farm_id = str(farm.get('id', ''))
        nft_id = str(farm.get('nftId', ''))
        
        # Check if this farm matches any of our target IDs
        if farm_id in target_long_ids or nft_id in target_long_ids:
            username = farm.get('username', f'farm_{farm_id}')
            matching_farms[farm_id] = farm
            found_ids.add(farm_id)
            print(f"✅ Found target farm: ID {farm_id} -> {username}")
    
    print(f"\n📊 Results:")
    print(f"   Found: {len(matching_farms)} farms")
    print(f"   Missing: {len(target_farm_ids) - len(matching_farms)} farms")
    
    # Show which farms we're still missing
    missing = target_farm_ids - found_ids
    if missing:
        print(f"❓ Missing farm IDs: {list(missing)[:10]}{'...' if len(missing) > 10 else ''}")
    
    return matching_farms

def save_filtered_farms(filtered_farms: Dict[str, Dict]) -> None:
    """Save the filtered farms to individual JSON files"""
    
    if not filtered_farms:
        print("❌ No farms to save")
        return
    
    # Ensure raw pull directory exists
    os.makedirs('raw pull', exist_ok=True)
    
    print(f"\n💾 Saving {len(filtered_farms)} farms to raw pull folder...")
    
    saved_count = 0
    for farm_id, farm_data in filtered_farms.items():
        try:
            username = farm_data.get('username', f'farm_{farm_id}')
            filename = f"raw pull/{username.lower()}.json"
            
            # Save as pretty-printed JSON
            with open(filename, 'w') as f:
                json.dump(farm_data, f, indent=2)
            
            print(f"✅ Saved {username} -> {filename}")
            saved_count += 1
            
        except Exception as e:
            print(f"❌ Error saving farm {farm_id}: {e}")
    
    print(f"\n🎉 Successfully saved {saved_count}/{len(filtered_farms)} farms!")

def main():
    print("🌻 Sunflower Land - Pagination Approach Test")
    print("=" * 60)
    
    # Load our target farm IDs
    target_farm_ids = load_our_farm_ids()
    if not target_farm_ids:
        print("❌ No target farm IDs loaded. Exiting.")
        return
    
    # Load farm ID mapping
    farm_mapping = load_farm_id_mapping()
    
    # Fetch all farms
    all_farms = fetch_all_farms()
    if not all_farms:
        print("❌ Failed to fetch farms. Exiting.")
        return
    
    # Filter for our farms
    filtered_farms = filter_farms_for_our_list(all_farms, target_farm_ids, farm_mapping)
    
    # Save filtered farms
    save_filtered_farms(filtered_farms)
    
    print(f"\n✅ Pagination approach complete!")
    print(f"📈 Efficiency: Got {len(filtered_farms)} farms in 1 API call vs {len(target_farm_ids)} individual calls")

if __name__ == "__main__":
    main()