#!/usr/bin/env python3
"""
Fixed Marketplace Trade History Fetcher for Sunflower Land
"""

import json
import os
import time
import requests
from datetime import datetime
from typing import Dict, List, Any, Optional

def load_item_mapping(parent_dir: str) -> Dict[int, str]:
    """
    Load item ID to name mapping from the item_mapping.txt file.
    
    Args:
        parent_dir: Parent directory containing the item_mapping.txt file
        
    Returns:
        Dictionary mapping item ID (int) to item name (str)
    """
    mapping_file = os.path.join(parent_dir, "item_mapping.txt")
    item_mapping = {}
    
    if not os.path.exists(mapping_file):
        print("⚠️  item_mapping.txt not found - will use item IDs")
        return item_mapping
    
    try:
        import re
        with open(mapping_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Parse TypeScript format: 123: "Item Name",
        pattern = r'(\d+):\s*"([^"]*)"'
        matches = re.findall(pattern, content)
        
        for item_id_str, item_name in matches:
            try:
                item_id = int(item_id_str)
                item_mapping[item_id] = item_name
            except ValueError:
                continue
            
        print(f"📋 Loaded {len(item_mapping)} item mappings")
        
    except Exception as e:
        print(f"❌ Error reading item_mapping.txt: {e}")
        print("⚠️  Will use item IDs instead")
    
    return item_mapping

def get_item_name(item_id: str, collection: str, item_mapping: Dict[int, str]) -> str:
    """
    Get the item name from ID and collection using the mapping, with smart fallbacks.
    
    Args:
        item_id: The item ID as string
        collection: The collection type (pets, collectibles, wearables)
        item_mapping: Dictionary mapping item ID to name
        
    Returns:
        Item name with appropriate formatting
    """
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

# Configuration
X_API_KEY = "sfl.MTEyODk3NjMwMTU4MzUwOA.YKi8l48T3jH_mPYvpxgCrkeS_IUt3uWQFgTUQg40JCE"
BEARER_TOKEN = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhZGRyZXNzIjoiMHg5MTZBMTUxMTQ2MzRjNGUzNTFGMGY4MjY2MjM2M2FhNTUwOUVFOTk5IiwiZmFybUlkIjoxMTI4OTc2MzAxNTgzNTA4LCJ1c2VyQWNjZXNzIjp7InN5bmMiOnRydWUsIndpdGhkcmF3Ijp0cnVlLCJtaW50Q29sbGVjdGlibGUiOnRydWUsImNyZWF0ZUZhcm0iOnRydWUsInZlcmlmaWVkIjp0cnVlfSwiaWF0IjoxNzY5NTMyODQ4LCJleHAiOjE3NzIxMjQ4NDh9.fDlyqb5Gi6kOBc7QHSB-KZJ7xqA3h48WkbFXl99CFkI"
MARKETPLACE_API_URL = "https://api.sunflower-land.com/marketplace/profile/"
REQUEST_DELAY = 31

def get_username_from_raw_data(farm_id: str) -> Optional[str]:
    """Get username from existing raw data files."""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        parent_dir = os.path.dirname(script_dir)  # Go up one level to main directory
        raw_pull_folder = os.path.join(parent_dir, "raw pull")
        
        if os.path.exists(raw_pull_folder):
            for filename in os.listdir(raw_pull_folder):
                if filename.endswith('.json'):
                    file_path = os.path.join(raw_pull_folder, filename)
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                        
                        file_farm_id = data.get('id') or data.get('nft_id')
                        if str(file_farm_id) == str(farm_id):
                            username = data.get('farm', {}).get('username', '').lower()
                            if username:
                                print(f"✅ Found username: {username} for farm {farm_id}")
                                return username
                    except:
                        continue
        return None
    except Exception as e:
        print(f"❌ Error reading raw data for farm {farm_id}: {e}")
        return None

def get_marketplace_data(farm_id: str, max_retries: int = 3, retry_delay: float = 10.0) -> Optional[Dict[str, Any]]:
    """Get marketplace data for a farm ID with retry logic."""
    headers = {
        'Authorization': BEARER_TOKEN,
        'Content-Type': 'application/json;charset=UTF-8',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Origin': 'https://sunflower-land.com',
        'Referer': 'https://sunflower-land.com/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    for attempt in range(max_retries):
        try:
            response = requests.get(f"{MARKETPLACE_API_URL}{farm_id}", headers=headers, timeout=30)
            response.raise_for_status()
            
            if not response.content:
                print(f"ℹ️  Empty response for farm {farm_id} (attempt {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    print(f"⏳ Waiting {retry_delay}s before retry...")
                    time.sleep(retry_delay)
                    continue
                return None
            
            data = response.json()
            if isinstance(data, dict):
                trades = data.get('trades', [])
                print(f"✅ Found {len(trades)} trades for farm {farm_id}")
                return data
            
            print(f"⚠️  Invalid data format for farm {farm_id} (attempt {attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                print(f"⏳ Waiting {retry_delay}s before retry...")
                time.sleep(retry_delay)
                continue
            return None
            
        except Exception as e:
            print(f"❌ Error fetching marketplace data for farm {farm_id} (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                print(f"⏳ Waiting {retry_delay}s before retry...")
                time.sleep(retry_delay)
            else:
                print(f"❌ Failed after {max_retries} attempts, skipping farm {farm_id}")
                return None

def create_folders():
    """Create necessary folders."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(script_dir)  # Go up one level to main directory
    
    history_folder = os.path.join(parent_dir, "marketplace history")
    raw_folder = os.path.join(parent_dir, "raw marketplace")
    
    os.makedirs(history_folder, exist_ok=True)
    os.makedirs(raw_folder, exist_ok=True)
    
    return history_folder, raw_folder

def save_raw_marketplace_data(data: dict, username: str, raw_folder: str):
    """Save raw marketplace data (overwrites)."""
    filename = f"{username}.json"
    file_path = os.path.join(raw_folder, filename)
    
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"✅ Saved raw marketplace data for {username}")
    except Exception as e:
        print(f"❌ Error saving raw marketplace data for {username}: {e}")

def format_trade_entry(trade: Dict[str, Any], item_mapping: Dict[int, str]) -> str:
    """Format a single trade with correct field names."""
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

def update_marketplace_history(username: str, trades: List[Dict[str, Any]], history_folder: str, item_mapping: Dict[int, str]):
    """Update marketplace history file (cumulative)."""
    filename = f"{username}_marketplace_history.txt"
    file_path = os.path.join(history_folder, filename)
    
    if not trades:
        if not os.path.exists(file_path):
            try:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(f"📈 MARKETPLACE TRADE HISTORY - {username.upper()}\n")
                    f.write(f"🔄 Last Updated: {datetime.now().strftime('%Y-%m-%d %I:%M %p')}\n")
                    f.write("=" * 60 + "\n\n")
                    f.write("ℹ️  No trades found for this farm.\n\n")
                print(f"✅ Created empty marketplace history for {username}")
            except Exception as e:
                print(f"❌ Error creating file for {username}: {e}")
        return
    
    # Load existing trade IDs
    existing_ids = set()
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            lines = content.split('\n')
            for line in lines:
                if line.startswith('🔄 TRADE #'):
                    try:
                        parts = line.split(' - ')
                        if parts:
                            trade_part = parts[0].replace('🔄 TRADE #', '')
                            existing_ids.add(trade_part)
                    except:
                        continue
        except Exception as e:
            print(f"❌ Error reading existing trades: {e}")
    
    # Filter new trades
    new_trades = []
    for trade in trades:
        trade_id = str(trade.get('id', ''))
        if trade_id not in existing_ids:
            new_trades.append(trade)
    
    if not new_trades:
        print(f"ℹ️  No new trades found for {username}")
        return
    
    # Sort new trades by timestamp (newest first)
    new_trades.sort(key=lambda x: x.get('fulfilledAt', 0), reverse=True)
    
    try:
        # Read existing content
        existing_content = ""
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                lines = content.split('\n')
                # Skip old header
                start_idx = 0
                for i, line in enumerate(lines):
                    if line.startswith('=') and len(line) > 10:
                        start_idx = i + 1
                        break
                existing_content = '\n'.join(lines[start_idx:]).strip()
        
        # Write new content
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(f"📈 MARKETPLACE TRADE HISTORY - {username.upper()}\n")
            f.write(f"🔄 Last Updated: {datetime.now().strftime('%Y-%m-%d %I:%M %p')}\n")
            f.write("=" * 60 + "\n\n")
            
            for trade in new_trades:
                f.write(format_trade_entry(trade, item_mapping))
            
            if existing_content:
                f.write(existing_content)
        
        print(f"✅ Added {len(new_trades)} new trades to {username} marketplace history")
        
    except Exception as e:
        print(f"❌ Error updating marketplace history for {username}: {e}")

def load_farm_ids() -> tuple[List[str], float]:
    """Load farm IDs from farm_ids.txt, skipping the first line (wait time setting)."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(script_dir)  # Go up one level to main directory
    farm_ids_file = os.path.join(parent_dir, "farm_ids.txt")
    
    if not os.path.exists(farm_ids_file):
        print("❌ farm_ids.txt file not found!")
        return [], 31.0
    
    try:
        with open(farm_ids_file, 'r') as f:
            lines = [line.strip() for line in f if line.strip()]
        
        # Skip first line (wait time setting), return rest as farm IDs
        if lines and lines[0].startswith('WAIT_TIME_SECONDS='):
            wait_time = float(lines[0].split('=')[1])
            farm_ids = lines[1:]
        else:
            wait_time = 31.0  # Default fallback
            farm_ids = lines
        
        print(f"📄 Loaded {len(farm_ids)} farm IDs")
        print(f"⏰ Using adaptive wait time: {wait_time}s")
        return farm_ids, wait_time
    except Exception as e:
        print(f"❌ Error reading farm_ids.txt: {e}")
        return [], 31.0

def main():
    """Main function."""
    print("🚀 Starting marketplace trade history processing...")
    
    farm_ids, wait_time = load_farm_ids()
    if not farm_ids:
        return
    
    history_folder, raw_folder = create_folders()
    print(f"📁 Marketplace history folder: {history_folder}")
    print(f"📁 Raw marketplace folder: {raw_folder}")
    
    # Load item mapping
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(script_dir)
    item_mapping = load_item_mapping(parent_dir)
    
    successful_updates = 0
    
    for i, farm_id in enumerate(farm_ids, 1):
        print(f"\n🔄 Processing farm {i}/{len(farm_ids)}: {farm_id}")
        
        username = get_username_from_raw_data(farm_id)
        if not username:
            print(f"⏭️  Skipping farm {farm_id} - could not get username")
            continue
        
        full_response = get_marketplace_data(farm_id)
        if full_response is None:
            print(f"⏭️  Skipping farm {farm_id} - could not get marketplace data")
            continue
        
        # Save raw data (overwrites)
        save_raw_marketplace_data(full_response, username, raw_folder)
        
        # Extract trades and update history (cumulative)
        trades = full_response.get('trades', [])
        update_marketplace_history(username, trades, history_folder, item_mapping)
        successful_updates += 1
        
        # Add delay
        if i < len(farm_ids):
            print(f"⏳ Waiting {wait_time} seconds...")
            time.sleep(wait_time)
    
    print(f"\n🎉 Processing complete!")
    print(f"📊 Successfully updated {successful_updates}/{len(farm_ids)} farms")

if __name__ == "__main__":
    main()