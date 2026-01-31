#!/usr/bin/env python3
"""
Combined Farm and Marketplace Data Fetcher
Optimized to fetch both farm data and marketplace data for each farm in quick succession,
dramatically reducing total execution time.
"""

import requests
import os
import json
import time
import re
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple


def format_timestamp(timestamp_str: str) -> str:
    """Format timestamp from ISO format to readable format."""
    if not timestamp_str:
        return "Unknown Date"
    
    try:
        # Parse ISO format: 2025-01-31T18:47:35.000Z
        if 'T' in timestamp_str:
            dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            return dt.strftime('%Y-%m-%d %I:%M %p')
        else:
            # Try Unix timestamp (milliseconds)
            timestamp = int(timestamp_str) / 1000
            dt = datetime.fromtimestamp(timestamp)
            return dt.strftime('%Y-%m-%d %I:%M %p')
    except (ValueError, TypeError):
        return "Unknown Date"


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
        print(f"⏰ Wait time between farms: {wait_time}s", flush=True)
        return farm_ids, wait_time
    except Exception as e:
        print(f"❌ Error reading farm IDs: {e}")
        return [], 31.0


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
        
        pattern = r'(\d+):\s*"([^"]*)"'
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


def fetch_farm_data(farm_id: str, api_key: str, max_retries: int = 3, base_delay: float = 2.0) -> Optional[Dict[str, Any]]:
    """Fetch farm data for a specific farm ID using x-api-key authentication with retry logic."""
    url = f"https://api.sunflower-land.com/community/farms/{farm_id}"
    headers = {
        'x-api-key': api_key,
        'Content-Type': 'application/json'
    }
    
    for attempt in range(max_retries + 1):
        try:
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                if attempt < max_retries:
                    delay = base_delay * (2 ** attempt)  # Exponential backoff
                    print(f"   ⏳ Rate limited (429), retrying in {delay:.1f}s... (attempt {attempt + 1}/{max_retries + 1})", flush=True)
                    time.sleep(delay)
                    continue
                else:
                    print(f"   ❌ Rate limited after {max_retries + 1} attempts, giving up", flush=True)
                    return None
            else:
                if attempt < max_retries:
                    delay = base_delay * (2 ** attempt)
                    print(f"   ⚠️  HTTP {response.status_code}, retrying in {delay:.1f}s... (attempt {attempt + 1}/{max_retries + 1})", flush=True)
                    time.sleep(delay)
                    continue
                else:
                    print(f"   ❌ HTTP {response.status_code} after {max_retries + 1} attempts", flush=True)
                    return None
                    
        except Exception as e:
            if attempt < max_retries:
                delay = base_delay * (2 ** attempt)
                print(f"   ⚠️  Request error: {e}, retrying in {delay:.1f}s... (attempt {attempt + 1}/{max_retries + 1})", flush=True)
                time.sleep(delay)
                continue
            else:
                print(f"   ❌ Request failed after {max_retries + 1} attempts: {e}", flush=True)
                return None
    
    return None


def fetch_marketplace_data(farm_id: str, bearer_token: str, max_retries: int = 3, base_delay: float = 2.0) -> Optional[Dict[str, Any]]:
    """Fetch marketplace data for a specific farm ID using Bearer token authentication with retry logic."""
    url = f"https://api.sunflower-land.com/marketplace/profile/{farm_id}"
    headers = {
        'Authorization': bearer_token,
        'Content-Type': 'application/json'
    }
    
    for attempt in range(max_retries + 1):
        try:
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                if attempt < max_retries:
                    delay = base_delay * (2 ** attempt)  # Exponential backoff
                    print(f"   ⏳ Rate limited (429), retrying in {delay:.1f}s... (attempt {attempt + 1}/{max_retries + 1})", flush=True)
                    time.sleep(delay)
                    continue
                else:
                    print(f"   ❌ Rate limited after {max_retries + 1} attempts, giving up", flush=True)
                    return None
            else:
                if attempt < max_retries:
                    delay = base_delay * (2 ** attempt)
                    print(f"   ⚠️  HTTP {response.status_code}, retrying in {delay:.1f}s... (attempt {attempt + 1}/{max_retries + 1})", flush=True)
                    time.sleep(delay)
                    continue
                else:
                    print(f"   ❌ HTTP {response.status_code} after {max_retries + 1} attempts", flush=True)
                    return None
                    
        except Exception as e:
            if attempt < max_retries:
                delay = base_delay * (2 ** attempt)
                print(f"   ⚠️  Request error: {e}, retrying in {delay:.1f}s... (attempt {attempt + 1}/{max_retries + 1})", flush=True)
                time.sleep(delay)
                continue
            else:
                print(f"   ❌ Request failed after {max_retries + 1} attempts: {e}", flush=True)
                return None
    
    return None


def save_farm_data(farm_data: Dict[str, Any], farm_id: str, raw_pull_dir: str) -> bool:
    """Save farm data to file using username from response - following original batch_fetch.py format."""
    try:
        # Extract username from the farm object (following original batch_fetch.py format)
        farm_obj = farm_data.get('farm', {})
        raw_username = farm_obj.get('username')
        
        if raw_username:
            username = raw_username.lower()  # Convert to lowercase
            filename = f"{username}.json"
            print(f"   📝 Username found: {raw_username} -> {filename}", flush=True)
        else:
            print(f"   ⚠️  No username found for farm {farm_id}, using fallback filename", flush=True)
            filename = f"farm_{farm_id}.json"
        
        filepath = os.path.join(raw_pull_dir, filename)
        
        # Save as pretty-printed JSON (following original format)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(farm_data, f, indent=2, ensure_ascii=False)
        
        if raw_username:
            print(f"   ✅ Saved {raw_username} -> {filename}", flush=True)
        else:
            print(f"   ✅ Saved farm_{farm_id} -> {filename}", flush=True)
        return True
        
    except Exception as e:
        print(f"   ❌ Failed to save farm data for {farm_id}: {e}", flush=True)
        return False


def save_marketplace_data(marketplace_data: Dict[str, Any], farm_id: str, raw_marketplace_dir: str, item_mapping: Dict[int, str], farm_data: Optional[Dict[str, Any]] = None) -> bool:
    """Save marketplace data to file using username from farm data - following original marketplace_fetch.py format."""
    try:
        # Get username from farm data (exactly like original marketplace_fetch.py does)
        username = None
        raw_username = None
        if farm_data and 'farm' in farm_data and 'username' in farm_data['farm']:
            raw_username = farm_data['farm']['username']
            username = raw_username.lower()
        
        if not username:
            print(f"   ⚠️  No username available for marketplace data for farm ID {farm_id}", flush=True)
            return False
        
        # Save raw marketplace data (exactly like original script)
        filename = f"{username}.json"
        filepath = os.path.join(raw_marketplace_dir, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(marketplace_data, f, indent=2, ensure_ascii=False)
        
        # Extract trades and update history (exactly like original script)
        trades = marketplace_data.get('trades', [])
        print(f"   📝 Found {len(trades)} trades in API response for {raw_username}", flush=True)
        
        # Update marketplace history (cumulative, filters for new trades only)
        script_dir = os.path.dirname(os.path.abspath(__file__))
        parent_dir = os.path.dirname(script_dir)
        history_dir = os.path.join(parent_dir, "marketplace history")
        os.makedirs(history_dir, exist_ok=True)
        
        new_trade_count = update_marketplace_history(username, trades, history_dir, item_mapping)
        if new_trade_count > 0:
            print(f"   📈 Added {new_trade_count} NEW trades to {raw_username} history", flush=True)
        else:
            print(f"   ℹ️  No new trades found for {raw_username} (all {len(trades)} trades already exist)", flush=True)
        
        print(f"   ✅ Saved raw marketplace data for {raw_username} -> {filename}", flush=True)
        
        return True
        
    except Exception as e:
        print(f"   ❌ Failed to save marketplace data for {farm_id}: {e}", flush=True)
        return False


def get_item_name(item_id: str, collection: str, item_mapping: Dict[int, str]) -> str:
    """Get the item name from ID and collection using the mapping, with smart fallbacks."""
    try:
        item_id_int = int(item_id)
        
        # First try to get from mapping
        if item_id_int in item_mapping:
            return item_mapping[item_id_int]
        
        # If not in mapping, use collection-specific formatting
        if collection.lower() == "pets":
            return f"Pet #{item_id}"
        elif collection.lower() == "wearables":
            return f"Wearable #{item_id}"
        elif collection.lower() == "collectibles":
            return f"Collectible #{item_id}"
        else:
            return f"Item #{item_id}"
            
    except (ValueError, TypeError):
        return f"Item {item_id}"


def format_trade_entry(trade: Dict[str, Any], item_mapping: Dict[int, str]) -> str:
    """Format a single trade with correct field names - following original marketplace_fetch.py format."""
    trade_id = trade.get('id', 'Unknown')
    fulfilled_at = trade.get('fulfilledAt', 0)
    sfl_amount = trade.get('sfl', 0)
    quantity = trade.get('quantity', 1)
    item_id = trade.get('itemId', 'Unknown')
    collection = trade.get('collection', 'Unknown')
    source = trade.get('source', 'Unknown')
    
    # Convert item ID to name
    if item_id != 'Unknown':
        item_name = get_item_name(str(item_id), collection, item_mapping)
    else:
        item_name = 'Unknown Item'
    
    initiated_by = trade.get('initiatedBy', {})
    fulfilled_by = trade.get('fulfilledBy', {})
    
    seller_username = initiated_by.get('username', 'Unknown')
    seller_id = initiated_by.get('id', 'Unknown')
    buyer_username = fulfilled_by.get('username', 'Unknown')
    buyer_id = fulfilled_by.get('id', 'Unknown')
    
    # Convert timestamp
    if fulfilled_at:
        try:
            timestamp = datetime.fromtimestamp(fulfilled_at / 1000)
            date_str = timestamp.strftime("%Y-%m-%d %I:%M %p")
        except:
            date_str = "Unknown Date"
    else:
        date_str = "Unknown Date"
    
    entry = f"""{"=" * 60}
🔄 TRADE #{trade_id} - {date_str}
{"=" * 60}
📦 Item: {item_name} (ID: {item_id}) | Collection: {collection}
📊 Quantity: {quantity}
💰 Amount: {sfl_amount} SFL
🔄 Source: {source}
👤 Seller: {seller_username} (ID: {seller_id})
👤 Buyer: {buyer_username} (ID: {buyer_id})

"""
    return entry


def update_marketplace_history(username: str, trades: List[Dict[str, Any]], history_folder: str, item_mapping: Dict[int, str]) -> int:
    """Update marketplace history file with new trades only - exactly like original marketplace_fetch.py."""
    if not trades:
        return 0
    
    history_file = os.path.join(history_folder, f"{username}_marketplace_history.txt")
    
    # Get existing trade IDs (exactly like original script lines 267-290)
    existing_ids = set()
    if os.path.exists(history_file):
        try:
            with open(history_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line.startswith('🔄 TRADE #'):
                        # Extract trade ID from line format: "🔄 TRADE #1279d424 - 2026-01-31 01:47 PM"
                        trade_id = line.split(' - ')[0].replace('🔄 TRADE #', '')
                        existing_ids.add(trade_id)
        except Exception as e:
            print(f"   ⚠️  Error reading history file {history_file}: {e}", flush=True)
    
    # Filter for new trades only (exactly like original script lines 293-297)
    new_trades = []
    for trade in trades:
        trade_id = trade.get('id', '')
        if trade_id and trade_id not in existing_ids:
            new_trades.append(trade)
    
    # If no new trades, return 0 (exactly like original script lines 299-301)
    if not new_trades:
        return 0
    
    # Append new trades to history file (exactly like original script lines 303-332)
    try:
        with open(history_file, 'a', encoding='utf-8') as f:
            for trade in new_trades:
                trade_id = trade.get('id', '')
                timestamp = trade.get('createdAt', '')
                item_id = trade.get('itemId', 0)
                sfl = trade.get('sfl', 0)
                supply = trade.get('supply', 1)
                
                # Format timestamp (2025-01-31T18:47:35.000Z -> 2026-01-31 01:47 PM)
                formatted_timestamp = format_timestamp(timestamp)
                
                # Get item name from mapping
                item_name = item_mapping.get(item_id, f"Item {item_id}")
                
                # Write trade line (same format as original)
                f.write(f"🔄 TRADE #{trade_id} - {formatted_timestamp}\n")
                f.write(f"   Item: {item_name} (ID: {item_id})\n")
                f.write(f"   SFL: {sfl}\n")
                f.write(f"   Supply: {supply}\n")
                f.write(f"\n")
        
        return len(new_trades)
        
    except Exception as e:
        print(f"   ❌ Failed to update history file {history_file}: {e}", flush=True)
        return 0


def process_farm_combined(farm_id: str, api_key: str, bearer_token: str, raw_pull_dir: str, raw_marketplace_dir: str, item_mapping: Dict[int, str], current_num: int, total_farms: int) -> Tuple[bool, bool]:
    """
    Process both farm and marketplace data for a single farm in quick succession.
    
    Returns:
        Tuple of (farm_success, marketplace_success)
    """
    print(f"\n----- FARM {current_num}/{total_farms} -----", flush=True)
    print(f"Farm ID: {farm_id}", flush=True)
    
    # Fetch farm data with retry logic
    print(f"📡 [STEP 1/2] Fetching farm data from API...", flush=True)
    farm_data = fetch_farm_data(farm_id, api_key, max_retries=3, base_delay=2.0)
    farm_success = False
    if farm_data:
        print(f"    ✅ Farm data received successfully", flush=True)
        farm_success = save_farm_data(farm_data, farm_id, raw_pull_dir)
        if farm_success:
            print(f"    💾 Farm data saved successfully", flush=True)
    else:
        print(f"    ❌ Farm data fetch failed after all retries", flush=True)
    
    # Small delay between API calls
    print(f"⏱️  Brief delay between API calls (1.0s)...", flush=True)
    time.sleep(1.0)  # Slightly longer delay to be safer
    
    # Fetch marketplace data with retry logic
    print(f"🛒 [STEP 2/2] Fetching marketplace data from API...", flush=True)
    marketplace_data = fetch_marketplace_data(farm_id, bearer_token, max_retries=3, base_delay=2.0)
    marketplace_success = False
    if marketplace_data:
        print(f"    ✅ Marketplace data received successfully", flush=True)
        # Pass farm_data to marketplace save function (like original script does)
        marketplace_success = save_marketplace_data(marketplace_data, farm_id, raw_marketplace_dir, item_mapping, farm_data)
        if marketplace_success:
            print(f"    💾 Marketplace data saved successfully", flush=True)
    else:
        print(f"    ❌ Marketplace data fetch failed after all retries", flush=True)
    
    # Summary for this farm
    farm_status = "✅ SUCCESS" if farm_success else "❌ FAILED"
    marketplace_status = "✅ SUCCESS" if marketplace_success else "❌ FAILED"
    print(f"\n----- RESULTS -----", flush=True)
    print(f"Farm Data:        {farm_status}", flush=True)
    print(f"Marketplace Data: {marketplace_status}", flush=True)
    
    return farm_success, marketplace_success


def main():
    """Main execution function."""
    print("\n===== SUNFLOWER LAND COMBINED DATA FETCHER =====", flush=True)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %I:%M:%S %p')}", flush=True)
    print("===============================================", flush=True)
    # Load configuration
    print("\n----- INITIALIZATION PHASE -----", flush=True)
    farm_ids, wait_time = load_farm_ids()
    if not farm_ids:
        print("No farm IDs to process", flush=True)
        return False
    print(f"Loaded {len(farm_ids)} farm IDs for processing", flush=True)
    print(f"Rate limiting: {wait_time}s wait between farms", flush=True)
    # Load item mapping
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(script_dir)
    item_mapping = load_item_mapping(parent_dir)
    print(f"Loaded {len(item_mapping)} item name mappings", flush=True)
    # Setup directories
    raw_pull_dir, raw_marketplace_dir = setup_directories()
    print(f"Output directories configured:", flush=True)
    print(f"   📁 Farm data: {raw_pull_dir}", flush=True)
    print(f"   📁 Marketplace data: {raw_marketplace_dir}", flush=True)
    # Get authentication tokens
    api_key = "sfl.MTEyODk3NjMwMTU4MzUwOA.YKi8l48T3jH_mPYvpxgCrkeS_IUt3uWQFgTUQg40JCE"
    bearer_token = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhZGRyZXNzIjoiMHg5MTZBMTUxMTQ2MzRjNGUzNTFGMGY4MjY2MjM2M2FhNTUwOUVFOTk5IiwiZmFybUlkIjoxMTI4OTc2MzAxNTgzNTA4LCJ1c2VyQWNjZXNzIjp7InN5bmMiOnRydWUsIndpdGhkcmF3Ijp0cnVlLCJtaW50Q29sbGVjdGlibGUiOnRydWUsImNyZWF0ZUZhcm0iOnRydWUsInZlcmlmaWVkIjp0cnVlfSwiaWF0IjoxNzY5NTMyODQ4LCJleHAiOjE3NzIxMjQ4NDh9.fDlyqb5Gi6kOBc7QHSB-KZJ7xqA3h48WkbFXl99CFkI"
    # Allow override from environment variables
    api_key = os.environ.get('API_KEY', api_key)
    bearer_token = os.environ.get('BEARER_TOKEN', bearer_token)
    print(f"Authentication tokens configured", flush=True)
    print(f"\n----- STARTING DATA COLLECTION FOR {len(farm_ids)} FARMS -----", flush=True)
    
    # Process farms
    total_farms = len(farm_ids)
    farm_successes = 0
    marketplace_successes = 0
    
    for i, farm_id in enumerate(farm_ids, 1):
        farm_success, marketplace_success = process_farm_combined(
            farm_id, api_key, bearer_token, raw_pull_dir, raw_marketplace_dir, item_mapping, i, total_farms
        )
        
        if farm_success:
            farm_successes += 1
        if marketplace_success:
            marketplace_successes += 1
        
        # Wait between farms (except for the last one)
        if i < total_farms:
            print(f"\n----- RATE LIMIT -----", flush=True)
            print(f"Waiting {wait_time}s before processing next farm", flush=True)
            print(f"Progress: {i}/{total_farms} farms completed ({(i/total_farms)*100:.1f}%)", flush=True)
            print(f"Next: Farm {i+1} ({farm_ids[i]})", flush=True)
            # Animated loading bar
            bar_width = 30  # Total width of the progress bar
            update_interval = 0.5  # Update every 0.5 seconds
            total_updates = int(wait_time / update_interval)
            for update in range(total_updates + 1):
                progress = update / total_updates
                filled_width = int(progress * bar_width)
                arrow = ">" * filled_width
                empty = " " * (bar_width - filled_width)
                remaining_time = wait_time - (update * update_interval)
                print(f"\r    [{arrow}{empty}] {progress*100:5.1f}% ({remaining_time:4.1f}s)", end="", flush=True)
                if update < total_updates:
                    time.sleep(update_interval)
            print(f"\n    Wait complete - proceeding to next farm\n====Complete====\n", flush=True)
    
    # Final summary
    print(f"\n{'='*80}", flush=True)
    print("🏁 FINAL SUMMARY", flush=True)
    print(f"{'='*80}", flush=True)
    print(f"⏰ Started:  {datetime.now().strftime('%Y-%m-%d %I:%M:%S %p')}", flush=True) 
    print(f"⏰ Completed: {datetime.now().strftime('%Y-%m-%d %I:%M:%S %p')}", flush=True)
    print(f"🏪 Total farms processed: {total_farms}", flush=True)
    print("-" * 50, flush=True)
    
    farm_success_rate = (farm_successes / total_farms * 100) if total_farms > 0 else 0
    marketplace_success_rate = (marketplace_successes / total_farms * 100) if total_farms > 0 else 0
    
    print(f"🏠 FARM DATA RESULTS:", flush=True)
    print(f"   ✅ Successful: {farm_successes}/{total_farms} ({farm_success_rate:.1f}%)", flush=True)
    print(f"   ❌ Failed:     {total_farms - farm_successes}/{total_farms} ({100-farm_success_rate:.1f}%)", flush=True)
    
    print(f"\n🛒 MARKETPLACE DATA RESULTS:", flush=True)
    print(f"   ✅ Successful: {marketplace_successes}/{total_farms} ({marketplace_success_rate:.1f}%)", flush=True)
    print(f"   ❌ Failed:     {total_farms - marketplace_successes}/{total_farms} ({100-marketplace_success_rate:.1f}%)", flush=True)
    
    overall_success = farm_successes > 0 or marketplace_successes > 0
    if overall_success:
        print(f"\n🎉 DATA COLLECTION COMPLETED SUCCESSFULLY!", flush=True)
        if farm_successes == total_farms and marketplace_successes == total_farms:
            print("   💯 Perfect run - all farms processed successfully!", flush=True)
        else:
            print("   ⚠️  Some farms had issues - check logs above for details", flush=True)
    else:
        print(f"\n❌ DATA COLLECTION FAILED - NO DATA COLLECTED", flush=True)
        print("   🔍 Check API credentials and network connectivity", flush=True)
    
    print(f"{'='*80}", flush=True)
    
    return overall_success


if __name__ == "__main__":
    import sys
    success = main()
    sys.exit(0 if success else 1)