#!/usr/bin/env python3
"""
Combined Farm and Marketplace Data Fetcher - Full Featured
Optimized to fetch both farm data and marketplace data for each farm in quick succession,
with all the advanced features from the original scripts including retry logic, 
rate limiting, adaptive wait times, and comprehensive error handling.
"""

import requests
import os
import json
import time
import re
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple


# Authentication constants
API_KEY = "sfl.MTEyODk3NjMwMTU4MzUwOA.YKi8l48T3jH_mPYvpxgCrkeS_IUt3uWQFgTUQg40JCE"
BEARER_TOKEN = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhZGRyZXNzIjoiMHg5MTZBMTUxMTQ2MzRjNGUzNTFGMGY4MjY2MjM2M2FhNTUwOUVFOTk5IiwiZmFybUlkIjoxMTI4OTc2MzAxNTgzNTA4LCJ1c2VyQWNjZXNzIjp7InN5bmMiOnRydWUsIndpdGhkcmF3Ijp0cnVlLCJtaW50Q29sbGVjdGlibGUiOnRydWUsImNyZWF0ZUZhcm0iOnRydWUsInZlcmlmaWVkIjp0cnVlfSwiaWF0IjoxNzY5NTMyODQ4LCJleHAiOjE3NzIxMjQ4NDh9.fDlyqb5Gi6kOBc7QHSB-KZJ7xqA3h48WkbFXl99CFkI"


def load_farm_ids(file_path: str = "farm_ids.txt") -> Tuple[List[str], float]:
    """Load farm IDs and wait time from config file."""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        parent_dir = os.path.dirname(script_dir)
        full_path = os.path.join(parent_dir, file_path)
        
        with open(full_path, 'r') as f:
            lines = [line.strip() for line in f if line.strip()]
        
        if lines and lines[0].startswith('WAIT_TIME_SECONDS='):
            wait_time = float(lines[0].split('=')[1])
            farm_ids = lines[1:]
        else:
            wait_time = 31.0
            farm_ids = lines
        
        print(f"📋 Loaded {len(farm_ids)} farm IDs", flush=True)
        print(f"⏰ Current adaptive wait time: {wait_time}s", flush=True)
        return farm_ids, wait_time
    except Exception as e:
        print(f"❌ Error reading farm IDs: {e}")
        return [], 31.0


def update_wait_time(new_wait_time: float, file_path: str = "farm_ids.txt"):
    """Update the wait time in farm_ids.txt based on learned API behavior."""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        parent_dir = os.path.dirname(script_dir)
        full_path = os.path.join(parent_dir, file_path)
        
        with open(full_path, 'r') as f:
            lines = f.read().strip().split('\\n')
        
        # Update first line with new wait time
        lines[0] = f"WAIT_TIME_SECONDS={new_wait_time:.1f}"
        
        with open(full_path, 'w') as f:
            f.write('\\n'.join(lines))
        
        print(f"📝 Updated adaptive wait time to {new_wait_time:.1f}s")
    except Exception as e:
        print(f"❌ Error updating wait time: {e}")


def load_item_mapping(parent_dir: str) -> Dict[int, str]:
    """Load item ID to name mapping."""
    mapping_file = os.path.join(parent_dir, "item_mapping.txt")
    item_mapping = {}
    
    if not os.path.exists(mapping_file):
        print("⚠️  item_mapping.txt not found - will use item IDs")
        return item_mapping
    
    try:
        with open(mapping_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        pattern = r'(\\d+):\\s*"([^"]*)"'
        matches = re.findall(pattern, content)
        
        for item_id_str, item_name in matches:
            try:
                item_id = int(item_id_str)
                item_mapping[item_id] = item_name
            except ValueError:
                continue
                
        print(f"📋 Loaded {len(item_mapping)} item mappings")
        return item_mapping
        
    except Exception as e:
        print(f"❌ Error reading item_mapping.txt: {e}")
        return item_mapping


def setup_directories() -> Tuple[str, str]:
    """Create necessary directories for output."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(script_dir)
    
    raw_pull_dir = os.path.join(parent_dir, "raw pull")
    raw_marketplace_dir = os.path.join(parent_dir, "raw marketplace")
    
    os.makedirs(raw_pull_dir, exist_ok=True)
    os.makedirs(raw_marketplace_dir, exist_ok=True)
    
    return raw_pull_dir, raw_marketplace_dir


def fetch_farm_data_with_retry(farm_id: str, api_key: str, max_retries: int = 10, base_retry_delay: float = 10.0) -> Optional[Dict[str, Any]]:
    """Fetch farm data with comprehensive retry logic and rate limiting handling."""
    url = f"https://api.sunflower-land.com/community/farms/{farm_id}"
    headers = {
        'x-api-key': api_key,
        'Accept': 'application/json',
        'User-Agent': 'Python-API-Fetcher/1.0'
    }
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                farm_data = response.json()
                if 'id' in farm_data and 'nft_id' in farm_data:
                    print(f"✅ Farm data retrieved for {farm_id}")
                    return farm_data
                else:
                    print(f"❌ Missing id/nft_id fields for farm {farm_id}")
                    return None
                    
            elif response.status_code == 429:
                retry_delay = base_retry_delay * (2 ** min(attempt, 4))  # Exponential backoff
                print(f"⏳ Rate limited for farm {farm_id} (attempt {attempt + 1}/{max_retries}), waiting {retry_delay:.1f}s...")
                time.sleep(retry_delay)
                continue
                
            else:
                print(f"❌ Farm API response code: {response.status_code} for {farm_id} (attempt {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    time.sleep(base_retry_delay)
                    continue
                return None
                
        except requests.exceptions.Timeout:
            print(f"❌ Farm request timed out for {farm_id} (attempt {attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                time.sleep(base_retry_delay)
                continue
            else:
                print(f"❌ Farm fetch failed after {max_retries} timeout attempts")
                return None
                
        except requests.exceptions.ConnectionError:
            print(f"❌ Farm connection error for {farm_id} (attempt {attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                time.sleep(base_retry_delay)
                continue
            else:
                print(f"❌ Farm fetch failed after {max_retries} connection attempts")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"❌ Farm API fetch error for {farm_id} (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(base_retry_delay)
                continue
            else:
                print(f"❌ Farm fetch failed after {max_retries} request attempts")
                return None
    
    print(f"❌ Failed to get farm data for {farm_id} after {max_retries} retries")
    return None


def fetch_marketplace_data_with_retry(farm_id: str, bearer_token: str, max_retries: int = 10, base_retry_delay: float = 10.0) -> Optional[Dict[str, Any]]:
    """Fetch marketplace data with comprehensive retry logic and rate limiting handling."""
    url = f"https://api.sunflower-land.com/marketplace/profile/{farm_id}"
    headers = {
        'Authorization': bearer_token,
        'Content-Type': 'application/json;charset=UTF-8',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Origin': 'https://sunflower-land.com',
        'Referer': 'https://sunflower-land.com/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                if not response.content:
                    print(f"ℹ️  Empty marketplace response for farm {farm_id} (attempt {attempt + 1}/{max_retries})")
                    if attempt < max_retries - 1:
                        time.sleep(base_retry_delay)
                        continue
                    return None
                
                data = response.json()
                if isinstance(data, dict):
                    trades = data.get('trades', [])
                    print(f"✅ Found {len(trades)} marketplace trades for farm {farm_id}")
                    return data
                
                print(f"⚠️  Invalid marketplace data format for farm {farm_id} (attempt {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    time.sleep(base_retry_delay)
                    continue
                return None
                
            elif response.status_code == 429:
                retry_delay = base_retry_delay * (2 ** min(attempt, 4))  # Exponential backoff
                print(f"⏳ Marketplace rate limited for farm {farm_id} (attempt {attempt + 1}/{max_retries}), waiting {retry_delay:.1f}s...")
                time.sleep(retry_delay)
                continue
                
            else:
                print(f"❌ Marketplace API response code: {response.status_code} for {farm_id} (attempt {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    time.sleep(base_retry_delay)
                    continue
                return None
                
        except requests.exceptions.Timeout:
            print(f"❌ Marketplace request timed out for {farm_id} (attempt {attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                time.sleep(base_retry_delay)
                continue
            else:
                print(f"❌ Marketplace fetch failed after {max_retries} timeout attempts")
                return None
                
        except requests.exceptions.ConnectionError:
            print(f"❌ Marketplace connection error for {farm_id} (attempt {attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                time.sleep(base_retry_delay)
                continue
            else:
                print(f"❌ Marketplace fetch failed after {max_retries} connection attempts")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"❌ Marketplace API fetch error for {farm_id} (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(base_retry_delay)
                continue
            else:
                print(f"❌ Marketplace fetch failed after {max_retries} request attempts")
                return None
    
    print(f"❌ Failed to get marketplace data for {farm_id} after {max_retries} retries")
    return None


def save_farm_data(farm_data: Dict[str, Any], farm_id: str, raw_pull_dir: str) -> bool:
    """Save farm data to file using nft_id as filename."""
    try:
        nft_id = str(farm_data.get('nft_id', farm_id))
        filename = f"{nft_id}.json"
        filepath = os.path.join(raw_pull_dir, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(farm_data, f, indent=2)
        
        print(f"💾 Saved farm data: {filename}")
        return True
        
    except Exception as e:
        print(f"❌ Failed to save farm data for {farm_id}: {e}")
        return False


def get_username_from_farm_data(farm_data: Dict[str, Any]) -> Optional[str]:
    """Extract username from farm data."""
    try:
        # Try multiple possible locations for username
        username = None
        if 'farm' in farm_data and isinstance(farm_data['farm'], dict):
            username = farm_data['farm'].get('username') or farm_data['farm'].get('farmAddress')
        
        if not username and 'username' in farm_data:
            username = farm_data['username']
            
        if not username and 'farmAddress' in farm_data:
            username = farm_data['farmAddress']
        
        return username.lower() if username else None
        
    except Exception as e:
        print(f"❌ Error extracting username from farm data: {e}")
        return None


def save_marketplace_data(marketplace_data: Dict[str, Any], farm_id: str, raw_marketplace_dir: str, username: str = None) -> bool:
    """Save marketplace data to file."""
    try:
        # If username not provided, try to extract from data or use farm_id
        if not username:
            if 'farmAddress' in marketplace_data:
                username = marketplace_data['farmAddress']
            elif 'farm' in marketplace_data and 'farmAddress' in marketplace_data['farm']:
                username = marketplace_data['farm']['farmAddress']
            elif 'user' in marketplace_data and 'farmAddress' in marketplace_data['user']:
                username = marketplace_data['user']['farmAddress']
            else:
                username = farm_id  # Fallback to farm_id
        
        filename = f"{username.lower()}.json"
        filepath = os.path.join(raw_marketplace_dir, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(marketplace_data, f, indent=2)
        
        print(f"💾 Saved marketplace data: {filename}")
        return True
        
    except Exception as e:
        print(f"❌ Failed to save marketplace data for {farm_id}: {e}")
        return False


def process_farm_combined(farm_id: str, api_key: str, bearer_token: str, raw_pull_dir: str, 
                         raw_marketplace_dir: str, item_mapping: Dict[int, str]) -> Tuple[bool, bool, Optional[str]]:
    """
    Process both farm and marketplace data for a single farm with comprehensive retry logic.
    
    Returns:
        Tuple of (farm_success, marketplace_success, username)
    """
    print(f"\\n🏪 Processing farm ID: {farm_id}")
    
    # Fetch farm data with retry logic
    print(f"  📡 Fetching farm data...")
    farm_data = fetch_farm_data_with_retry(farm_id, api_key)
    farm_success = False
    username = None
    
    if farm_data:
        farm_success = save_farm_data(farm_data, farm_id, raw_pull_dir)
        username = get_username_from_farm_data(farm_data)
    
    # Small delay between API calls (but much shorter than between farms)
    time.sleep(1.0)
    
    # Fetch marketplace data with retry logic
    print(f"  🛒 Fetching marketplace data...")
    marketplace_data = fetch_marketplace_data_with_retry(farm_id, bearer_token)
    marketplace_success = False
    
    if marketplace_data:
        marketplace_success = save_marketplace_data(marketplace_data, farm_id, raw_marketplace_dir, username)
    
    status_farm = "✅" if farm_success else "❌"
    status_marketplace = "✅" if marketplace_success else "❌"
    username_status = f" | User: {username}" if username else ""
    print(f"  📊 Results: Farm {status_farm} | Marketplace {status_marketplace}{username_status}")
    
    return farm_success, marketplace_success, username


def main():
    """Main execution function with comprehensive error handling and adaptive timing."""
    print("🌻 Combined Farm & Marketplace Data Fetcher (Full Featured)")
    print("=" * 70)
    print(f"⏰ Started: {datetime.now().strftime('%Y-%m-%d %I:%M:%S %p')}")
    print("=" * 70)
    
    # Load configuration
    farm_ids, wait_time = load_farm_ids()
    if not farm_ids:
        print("❌ No farm IDs to process")
        return False
    
    # Load item mapping
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(script_dir)
    item_mapping = load_item_mapping(parent_dir)
    
    # Setup directories
    raw_pull_dir, raw_marketplace_dir = setup_directories()
    
    # Get authentication tokens (allow environment override)
    api_key = os.environ.get('API_KEY', API_KEY)
    bearer_token = os.environ.get('BEARER_TOKEN', BEARER_TOKEN)
    
    print(f"⏰ Using adaptive wait time: {wait_time}s between farms")
    print("🚀 Starting combined data collection...")
    
    # Process farms with advanced rate limiting and error handling
    total_farms = len(farm_ids)
    farm_successes = 0
    marketplace_successes = 0
    rate_limit_hits = 0
    consecutive_failures = 0
    
    for i, farm_id in enumerate(farm_ids, 1):
        print(f"\\n🚀 Processing {i}/{total_farms} farms...")
        
        start_time = time.time()
        farm_success, marketplace_success, username = process_farm_combined(
            farm_id, api_key, bearer_token, raw_pull_dir, raw_marketplace_dir, item_mapping
        )
        end_time = time.time()
        
        # Track success rates
        if farm_success:
            farm_successes += 1
            consecutive_failures = 0
        if marketplace_success:
            marketplace_successes += 1
        
        if not farm_success and not marketplace_success:
            consecutive_failures += 1
        
        # Adaptive wait time adjustment based on success rate and response time
        response_time = end_time - start_time
        if consecutive_failures >= 3:
            # If we have multiple failures, increase wait time
            new_wait_time = min(wait_time * 1.5, 60.0)
            if new_wait_time != wait_time:
                print(f"📈 Increasing wait time due to failures: {wait_time:.1f}s -> {new_wait_time:.1f}s")
                wait_time = new_wait_time
                update_wait_time(wait_time)
        elif consecutive_failures == 0 and response_time < 2.0 and wait_time > 10.0:
            # If responses are fast and no failures, we can reduce wait time slightly
            new_wait_time = max(wait_time * 0.95, 10.0)
            if abs(new_wait_time - wait_time) > 0.5:
                print(f"📉 Optimizing wait time: {wait_time:.1f}s -> {new_wait_time:.1f}s")
                wait_time = new_wait_time
                update_wait_time(wait_time)
        
        # Wait between farms (except for the last one)
        if i < total_farms:
            print(f"  ⏱️  Waiting {wait_time:.1f}s before next farm...")
            time.sleep(wait_time)
    
    # Final summary
    print(f"\\n{'='*70}")
    print("📊 COMBINED FETCH SUMMARY")
    print(f"{'='*70}")
    print(f"🏪 Total farms processed: {total_farms}")
    print(f"✅ Farm data successes: {farm_successes}/{total_farms} ({farm_successes/total_farms*100:.1f}%)")
    print(f"🛒 Marketplace data successes: {marketplace_successes}/{total_farms} ({marketplace_successes/total_farms*100:.1f}%)")
    print(f"⚡ Rate limit encounters: {rate_limit_hits}")
    print(f"⏰ Final adaptive wait time: {wait_time:.1f}s")
    print(f"⏰ Completed: {datetime.now().strftime('%Y-%m-%d %I:%M:%S %p')}")
    
    success = farm_successes > 0 or marketplace_successes > 0
    if success:
        print("🎉 Combined data collection completed successfully!")
    else:
        print("❌ No data was collected successfully")
    
    return success


if __name__ == "__main__":
    import sys
    success = main()
    sys.exit(0 if success else 1)