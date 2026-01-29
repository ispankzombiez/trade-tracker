#!/usr/bin/env python3
"""
Trade History Processor
Processes JSON files from the raw pull folder and creates/updates human-readable trade history files.
Each username gets its own trade history file that never gets overwritten, only appended to.
"""

import os
import json
from datetime import datetime
from typing import Dict, List, Any
import glob
import re


def load_item_mapping(parent_dir: str) -> Dict[int, str]:
    """
    Load item ID to name mapping from the item_mapping.txt file.
    
    Args:
        parent_dir: Parent directory containing the item_mapping.txt file
        
    Returns:
        Dictionary mapping item ID to item name
    """
    mapping_file = os.path.join(parent_dir, "item_mapping.txt")
    item_mapping = {}
    
    if os.path.exists(mapping_file):
        try:
            with open(mapping_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Extract all number: "name" patterns from the file
            # This will catch both wearables and collectibles
            pattern = r'(\d+):\s*"([^"]+)"'
            matches = re.findall(pattern, content)
            
            for item_id_str, item_name in matches:
                item_id = int(item_id_str)
                # If we already have this ID, keep the last one found (collectibles usually come after)
                item_mapping[item_id] = item_name
            
            print(f"📋 Loaded {len(item_mapping)} item mappings")
            
        except Exception as e:
            print(f"⚠️  Error loading item mapping: {e}")
    else:
        print("⚠️  item_mapping.txt not found - will use item IDs")
    
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
            return f"{collection.title()} #{item_id}" if collection != "Unknown" else f"Item #{item_id}"
            
    except (ValueError, TypeError):
        # If item_id is not a number, return a descriptive fallback
        if collection != "Unknown":
            return f"{collection.title()} {item_id}"
        return f"Item {item_id}" if item_id != "Unknown" else "Unknown Item"


def load_raw_data(raw_pull_folder: str) -> Dict[str, Dict]:
    """
    Load all JSON files from the raw pull folder.
    
    Args:
        raw_pull_folder: Path to the raw pull folder
        
    Returns:
        Dictionary mapping username to farm data
    """
    farm_data = {}
    
    try:
        json_files = glob.glob(os.path.join(raw_pull_folder, "*.json"))
        
        for file_path in json_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # Extract username from farm data
                farm_info = data.get('farm', {})
                username = farm_info.get('username')
                
                if username:
                    farm_data[username.lower()] = data
                    print(f"📄 Loaded data for: {username}")
                else:
                    print(f"⚠️  No username found in {os.path.basename(file_path)}")
                    
            except Exception as e:
                print(f"❌ Error loading {file_path}: {e}")
                
    except Exception as e:
        print(f"❌ Error accessing raw pull folder: {e}")
    
    return farm_data


def extract_trade_info(farm_data: Dict) -> Dict[str, Any]:
    """
    Extract trade-related information from farm data.
    
    Args:
        farm_data: The complete farm data JSON
        
    Returns:
        Dictionary with trade information
    """
    farm_info = farm_data.get('farm', {})
    
    trade_info = {
        'timestamp': datetime.now(),
        'farm_id': farm_data.get('id', 'Unknown'),
        'nft_id': farm_data.get('nft_id', 'Unknown'),
        'username': farm_info.get('username', 'Unknown'),
        'balance': farm_info.get('balance', '0'),
        'coins': farm_info.get('coins', 0),
        'gems': farm_info.get('gems', 0),
        'trades': farm_info.get('trades', {})
    }
    
    # Extract earned/spent totals from farmActivity
    farm_activity = farm_info.get('farmActivity', {})
    trade_info['sfl_earned'] = farm_activity.get('SFL Earned', 0)
    trade_info['sfl_spent'] = farm_activity.get('SFL Spent', 0)
    trade_info['coins_earned'] = farm_activity.get('Coins Earned', 0)
    trade_info['coins_spent'] = farm_activity.get('Coins Spent', 0)
    
    return trade_info


def format_trade_history_entry(trade_info: Dict[str, Any]) -> str:
    """
    Format trade information into pipe-separated compact format.
    
    Args:
        trade_info: Dictionary with trade information
        
    Returns:
        Formatted text entry as pipe-separated values
    """
    timestamp = trade_info['timestamp'].strftime("%Y-%m-%d %I:%M %p")
    username = trade_info['username']
    farm_id = trade_info['farm_id']
    balance = trade_info.get('balance', '0')
    
    # Ensure proper type conversion
    try:
        coins = int(trade_info.get('coins', 0))
    except (ValueError, TypeError):
        coins = 0
        
    try:
        gems = int(trade_info.get('gems', 0))
    except (ValueError, TypeError):
        gems = 0
    
    # Get trade data
    trades = trade_info.get('trades', {})
    sold_count = trades.get('soldCount', 0)
    trade_points = trades.get('tradePoints', 0.0)
    
    # Count active listings
    listings = trades.get('listings', {})
    active_listings = len(listings) if listings else 0
    
    # Get recent sales and purchases counts
    weekly_sales = trades.get('weeklySales', {})
    weekly_purchases = trades.get('weeklyPurchases', {})
    
    recent_sales_count = 0
    recent_purchases_count = 0
    
    # Count sales from last 7 days
    if weekly_sales:
        sorted_dates = sorted(weekly_sales.keys(), reverse=True)[:7]
        for date in sorted_dates:
            sales_data = weekly_sales[date]
            recent_sales_count += sum(sales_data.values()) if isinstance(sales_data, dict) else 0
    
    # Count purchases from last 7 days  
    if weekly_purchases:
        sorted_dates = sorted(weekly_purchases.keys(), reverse=True)[:7]
        for date in sorted_dates:
            purchases_data = weekly_purchases[date]
            recent_purchases_count += sum(purchases_data.values()) if isinstance(purchases_data, dict) else 0
    
    # Format as pipe-separated line
    entry = (f"{timestamp} | {username} | {farm_id} | {balance} SFL | "
            f"{coins:,} Coins | {gems:,} Gems | {sold_count} Total Sold | "
            f"{trade_points:.2f} Trade Points | {active_listings} Active Listings | "
            f"{recent_sales_count} Recent Sales | {recent_purchases_count} Recent Purchases")
    
    return entry


def get_trade_history_header() -> str:
    """
    Get the header row for trade history files with proper column alignment.
    
    Returns:
        Header string with column labels properly aligned
    """
    return f"{'Date':<12} | {'Time':<8} | {'Type':<7} | {'Item':<15} | {'Qty':<6} | {'Price':<12} | {'Counterparty':<15} | {'Farm ID':<8} | {'Trade ID':<8}"


def format_active_listings(trade_info: Dict[str, Any]) -> str:
    """
    Format active listings section with proper column alignment.
    
    Args:
        trade_info: Dictionary with trade information
        
    Returns:
        Formatted active listings section with aligned columns
    """
    trades = trade_info.get('trades', {})
    listings = trades.get('listings', {})
    
    if not listings:
        return "No active listings\n"
    
    lines = []
    for listing_id, listing in listings.items():
        items = listing.get('items', {})
        price = listing.get('sfl', 0)
        created_at = listing.get('createdAt', 0)
        
        # Convert timestamp to readable date
        try:
            from datetime import datetime
            date_obj = datetime.fromtimestamp(created_at / 1000)
            date_str = date_obj.strftime("%Y-%m-%d")
            time_str = date_obj.strftime("%I:%M %p")
        except:
            date_str = "Unknown"
            time_str = "Unknown"
        
        for item_name, quantity in items.items():
            # Truncate long item names and format with proper alignment
            item_display = item_name[:15] if len(item_name) > 15 else item_name
            price_display = f"{price} SFL"
            trade_id_short = listing_id[:8] if len(listing_id) > 8 else listing_id
            
            line = f"{date_str:<12} | {time_str:<8} | {'LISTING':<7} | {item_display:<15} | {quantity:<6} | {price_display:<12} | {'-':<15} | {'-':<8} | {trade_id_short:<8}"
            lines.append(line)
    
    return "\n".join(lines) + "\n"


def load_marketplace_history(username: str, parent_dir: str) -> tuple[List[str], List[str]]:
    """
    Load existing marketplace history for a user and separate into buys and sells.
    
    Args:
        username: Username to load history for
        parent_dir: Parent directory containing marketplace history
        
    Returns:
        Tuple of (buy_lines, sell_lines) with trade history
    """
    # Load item mapping
    item_mapping = load_item_mapping(parent_dir)
    
    marketplace_folder = os.path.join(parent_dir, "marketplace history")
    history_file = os.path.join(marketplace_folder, f"{username}_marketplace_history.txt")
    
    buy_lines = []
    sell_lines = []
    
    if os.path.exists(history_file):
        try:
            with open(history_file, 'r', encoding='utf-8') as f:
                content = f.read()
                
            # Parse the marketplace history file
            trades = content.split('🔄 TRADE #')
            
            for trade in trades[1:]:  # Skip the header section
                lines = trade.strip().split('\n')
                if len(lines) < 8:
                    continue
                    
                # Extract trade information
                trade_id = lines[0].split(' - ')[0].strip()
                date_time = lines[0].split(' - ')[1].strip() if ' - ' in lines[0] else "Unknown"
                
                # Parse date and time
                try:
                    date_part = date_time.split(' ')[0] if ' ' in date_time else "Unknown"
                    time_part = ' '.join(date_time.split(' ')[1:]) if ' ' in date_time else "Unknown"
                except:
                    date_part = "Unknown"
                    time_part = "Unknown"
                
                item_id = "Unknown"
                collection = "Unknown"
                quantity = "Unknown"
                amount = "Unknown"
                source = "Unknown"
                seller = "Unknown"
                buyer = "Unknown"
                
                # Parse each line
                for line in lines[1:]:
                    line = line.strip()
                    if line.startswith('📦 Item:') or line.startswith('📦 Item ID:'):
                        parts = line.split(' | ')
                        if len(parts) >= 2:
                            if line.startswith('📦 Item:'):
                                # New format: "📦 Item: Milk (ID: 645) | Collection: collectibles"
                                item_part = parts[0].replace('📦 Item: ', '').strip()
                                if ' (ID: ' in item_part:
                                    item_name = item_part.split(' (ID: ')[0].strip()
                                    item_id = item_part.split(' (ID: ')[1].replace(')', '').strip()
                                else:
                                    item_name = item_part
                                    item_id = "Unknown"
                            else:
                                # Old format: "📦 Item ID: 645 | Collection: collectibles"
                                item_id = parts[0].replace('📦 Item ID: ', '').strip()
                                item_name = None  # Will be resolved later
                            collection = parts[1].replace('Collection: ', '').strip()
                    elif line.startswith('📊 Quantity:'):
                        quantity = line.replace('📊 Quantity: ', '').strip()
                    elif line.startswith('💰 Amount:'):
                        amount = line.replace('💰 Amount: ', '').strip()
                    elif line.startswith('🔄 Source:'):
                        source = line.replace('🔄 Source: ', '').strip()
                    elif line.startswith('👤 Seller:'):
                        seller = line.replace('👤 Seller: ', '').strip()
                        seller = seller.split(' (ID: ')[0] if ' (ID: ' in seller else seller
                    elif line.startswith('👤 Buyer:'):
                        buyer = line.replace('👤 Buyer: ', '').strip()  
                        buyer = buyer.split(' (ID: ')[0] if ' (ID: ' in buyer else buyer
                
                # Determine trade type and counterparty
                if seller.lower() == username.lower():
                    trade_type = "SELL"
                    counterparty = buyer
                else:
                    trade_type = "BUY"
                    counterparty = seller
                
                # Format item display using mapping and collection
                if 'item_name' in locals() and item_name is not None:
                    # Use the directly parsed name from new format
                    final_item_name = item_name
                else:
                    # Use mapping for old format
                    final_item_name = get_item_name(item_id, collection, item_mapping)
                item_display = final_item_name[:15] if len(final_item_name) > 15 else final_item_name
                    
                # Format counterparty display
                counterparty_display = counterparty[:15] if len(counterparty) > 15 else counterparty
                
                # Format price display
                price_display = amount if len(str(amount)) <= 12 else str(amount)[:12]
                
                # Format trade ID
                trade_id_short = trade_id[:8] if len(trade_id) > 8 else trade_id
                
                # Format the trade line with proper alignment
                trade_line = f"{date_part:<12} | {time_part:<8} | {trade_type:<7} | {item_display:<15} | {quantity:<6} | {price_display:<12} | {counterparty_display:<15} | {'-':<8} | {trade_id_short:<8}"
                
                # Add to appropriate list
                if trade_type == "BUY":
                    buy_lines.append(trade_line)
                else:
                    sell_lines.append(trade_line)
                
        except Exception as e:
            print(f"❌ Error parsing marketplace history for {username}: {e}")
    
    return buy_lines, sell_lines


def parse_existing_trade_overview(username: str, history_folder: str) -> tuple[List[str], List[str]]:
    """
    Parse existing Trade Overview files to extract existing buy and sell trades.
    
    Args:
        username: Username to load history for
        history_folder: Path to the trade overview folder
        
    Returns:
        Tuple of (existing_buys, existing_sells) trade lines
    """
    user_folder = os.path.join(history_folder, username.lower())
    buys_file = os.path.join(user_folder, "buys.txt")
    sells_file = os.path.join(user_folder, "sells.txt")
    
    existing_buys = []
    existing_sells = []
    
    # Read existing buys
    if os.path.exists(buys_file):
        try:
            with open(buys_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                for line in lines:
                    line = line.strip()
                    # Skip headers, separators, and empty lines
                    if (line and not line.startswith("Date") and 
                        not line.startswith("=") and not line.startswith("-") and
                        " | " in line and len(line.split(" | ")) >= 8):
                        existing_buys.append(line)
        except Exception as e:
            print(f"⚠️  Error reading existing buys for {username}: {e}")
    
    # Read existing sells
    if os.path.exists(sells_file):
        try:
            with open(sells_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                for line in lines:
                    line = line.strip()
                    # Skip headers, separators, and empty lines
                    if (line and not line.startswith("Date") and 
                        not line.startswith("=") and not line.startswith("-") and
                        " | " in line and len(line.split(" | ")) >= 8):
                        existing_sells.append(line)
        except Exception as e:
            print(f"⚠️  Error reading existing sells for {username}: {e}")
    
    return existing_buys, existing_sells


def merge_trade_histories(new_buys: List[str], new_sells: List[str], 
                         existing_buys: List[str], existing_sells: List[str]) -> tuple[List[str], List[str]]:
    """
    Merge new trades with existing trades, removing duplicates and sorting chronologically.
    
    Args:
        new_buys: New buy trades from marketplace data
        new_sells: New sell trades from marketplace data
        existing_buys: Existing buy trades from previous runs
        existing_sells: Existing sell trades from previous runs
        
    Returns:
        Tuple of (merged_buys, merged_sells) with newest first
    """
    
    def extract_trade_id(trade_line: str) -> str:
        """Extract trade ID from the end of a trade line"""
        try:
            return trade_line.split(" | ")[-1].strip()
        except:
            return ""
    
    def get_datetime_from_line(trade_line: str) -> datetime:
        """Extract datetime from trade line for sorting"""
        try:
            parts = trade_line.split(" | ")
            if len(parts) >= 2:
                date_str = parts[0].strip()
                time_str = parts[1].strip()
                datetime_str = f"{date_str} {time_str}"
                return datetime.strptime(datetime_str, "%Y-%m-%d %I:%M %p")
        except:
            pass
        return datetime.min  # Fallback for unparseable dates
    
    # Combine and deduplicate buys
    all_buys = []
    seen_buy_ids = set()
    
    # Add new buys first (they take priority)
    for trade in new_buys:
        trade_id = extract_trade_id(trade)
        if trade_id and trade_id not in seen_buy_ids:
            all_buys.append(trade)
            seen_buy_ids.add(trade_id)
    
    # Add existing buys if not already seen
    for trade in existing_buys:
        trade_id = extract_trade_id(trade)
        if trade_id and trade_id not in seen_buy_ids:
            all_buys.append(trade)
            seen_buy_ids.add(trade_id)
    
    # Combine and deduplicate sells
    all_sells = []
    seen_sell_ids = set()
    
    # Add new sells first (they take priority)
    for trade in new_sells:
        trade_id = extract_trade_id(trade)
        if trade_id and trade_id not in seen_sell_ids:
            all_sells.append(trade)
            seen_sell_ids.add(trade_id)
    
    # Add existing sells if not already seen
    for trade in existing_sells:
        trade_id = extract_trade_id(trade)
        if trade_id and trade_id not in seen_sell_ids:
            all_sells.append(trade)
            seen_sell_ids.add(trade_id)
    
    # Sort both lists by datetime (newest first)
    all_buys.sort(key=get_datetime_from_line, reverse=True)
    all_sells.sort(key=get_datetime_from_line, reverse=True)
    
    return all_buys, all_sells


def create_trade_history_folder() -> str:
    """
    Create the trade overview folder if it doesn't exist.
    
    Returns:
        Path to the trade overview folder
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(script_dir)  # Go up one level from scripts folder
    folder_path = os.path.join(parent_dir, "Trade Overview")
    
    try:
        os.makedirs(folder_path, exist_ok=True)
        return folder_path
    except Exception as e:
        print(f"❌ Error creating Trade Overview folder: {e}")
        return os.path.dirname(script_dir)


def update_trade_history(username: str, trade_info: Dict[str, Any], history_folder: str):
    """
    Update the trade history file for a username.
    
    Args:
        username: The username (lowercase)
        trade_info: Trade information to add
        history_folder: Path to trade history folder
    """
    filename = f"{username}_trade_history.txt"
    file_path = os.path.join(history_folder, filename)
    
def update_trade_history(username: str, trade_info: Dict[str, Any], history_folder: str):
    """
    Update the trade history file for a username with three sections:
    1. Overview summary
    2. Active listings  
    3. Trade history (permanent record)
    
    Args:
        username: The username (lowercase)
        trade_info: Trade information to add
        history_folder: Path to trade history folder
    """
    filename = f"{username}_trade_history.txt"
    file_path = os.path.join(history_folder, filename)
    
    # Calculate summary stats
    try:
        balance = float(trade_info['balance'])
    except:
        balance = 0.0
    
    try:
        coins = int(trade_info.get('coins', 0))
    except (ValueError, TypeError):
        coins = 0
        
    try:
        gems = int(trade_info.get('gems', 0))
    except (ValueError, TypeError):
        gems = 0
    
    try:
        sfl_earned = float(trade_info['sfl_earned'])
        sfl_spent = float(trade_info['sfl_spent'])
        coins_earned = float(trade_info['coins_earned'])
        coins_spent = float(trade_info['coins_spent'])
    except:
        sfl_earned = sfl_spent = coins_earned = coins_spent = 0.0
        
    # Get trade data
    trades = trade_info.get('trades', {})
    sold_count = trades.get('soldCount', 0)
    trade_points = trades.get('tradePoints', 0.0)
    active_listings = len(trades.get('listings', {}))
    
    # Create the three sections
    timestamp = trade_info['timestamp'].strftime("%Y-%m-%d %I:%M %p")
    
    # Section 1: Overview Summary
    overview_section = [
        f"📊 TRADE OVERVIEW - {trade_info['username'].upper()}",
        f"🔄 Last Updated: {timestamp}",
        "=" * 80,
        f"💰 Balance: {balance:.2f} SFL | 🪙 {coins:,} Coins | 💎 {gems:,} Gems",
        f"📈 Lifetime Earned: {sfl_earned:.2f} SFL | {coins_earned:,} Coins",
        f"📉 Lifetime Spent: {sfl_spent:.2f} SFL | {coins_spent:,} Coins",
        f"🏪 Total Items Sold: {sold_count:,} | ⭐ Trade Points: {trade_points:.2f}",
        f"📋 Active Listings: {active_listings}",
        ""
    ]
    
    # Section 2: Active Listings
    active_listings_section = [
        "📋 ACTIVE LISTINGS",
        "=" * 80,
        get_trade_history_header(),
        "-" * 80,
        format_active_listings(trade_info),
    ]
    
def update_trade_history(username: str, trade_info: Dict[str, Any], history_folder: str):
    """
    Update the trade history files for a username with separate files:
    1. overview.txt - Summary and active listings
    2. buys.txt - Purchase history  
    3. sells.txt - Sales history
    
    Args:
        username: The username (lowercase)
        trade_info: Trade information to add
        history_folder: Path to trade history folder
    """
    # Create user folder
    user_folder = os.path.join(history_folder, username.lower())
    try:
        os.makedirs(user_folder, exist_ok=True)
    except Exception as e:
        print(f"❌ Error creating user folder for {username}: {e}")
        return
    
    overview_file = os.path.join(user_folder, "overview.txt")
    buys_file = os.path.join(user_folder, "buys.txt")
    sells_file = os.path.join(user_folder, "sells.txt")
    
    # Calculate summary stats
    try:
        balance = float(trade_info['balance'])
    except:
        balance = 0.0
    
    try:
        coins = int(trade_info.get('coins', 0))
    except (ValueError, TypeError):
        coins = 0
        
    try:
        gems = int(trade_info.get('gems', 0))
    except (ValueError, TypeError):
        gems = 0
    
    try:
        sfl_earned = float(trade_info['sfl_earned'])
        sfl_spent = float(trade_info['sfl_spent'])
        coins_earned = float(trade_info['coins_earned'])
        coins_spent = float(trade_info['coins_spent'])
    except:
        sfl_earned = sfl_spent = coins_earned = coins_spent = 0.0
        
    # Get trade data
    trades = trade_info.get('trades', {})
    sold_count = trades.get('soldCount', 0)
    trade_points = trades.get('tradePoints', 0.0)
    active_listings = len(trades.get('listings', {}))
    
    # Create the overview content
    timestamp = trade_info['timestamp'].strftime("%Y-%m-%d %I:%M %p")
    
    # Section 1: Overview Summary and Active Listings
    overview_content = [
        f"📊 TRADE OVERVIEW - {trade_info['username'].upper()}",
        f"🔄 Last Updated: {timestamp}",
        "=" * 80,
        f"💰 Balance: {balance:.2f} SFL | 🪙 {coins:,} Coins | 💎 {gems:,} Gems",
        f"📈 Lifetime Earned: {sfl_earned:.2f} SFL | {coins_earned:,} Coins",
        f"📉 Lifetime Spent: {sfl_spent:.2f} SFL | {coins_spent:,} Coins",
        f"🏪 Total Items Sold: {sold_count:,} | ⭐ Trade Points: {trade_points:.2f}",
        f"📋 Active Listings: {active_listings}",
        "",
        "📋 ACTIVE LISTINGS",
        "=" * 80,
        get_trade_history_header(),
        "-" * 80,
        format_active_listings(trade_info)
    ]
    
    # Load and merge trade history
    parent_dir = os.path.dirname(history_folder)
    
    # Get new trades from marketplace data
    new_buy_trades, new_sell_trades = load_marketplace_history(username, parent_dir)
    
    # Get existing trades from previous files
    existing_buys, existing_sells = parse_existing_trade_overview(username, history_folder)
    
    # Merge new and existing trades
    merged_buys, merged_sells = merge_trade_histories(
        new_buy_trades, new_sell_trades, existing_buys, existing_sells
    )
    
    try:
        # Write overview file
        with open(overview_file, 'w', encoding='utf-8') as f:
            f.write("\n".join(overview_content))
        
        # Write buys file
        with open(buys_file, 'w', encoding='utf-8') as f:
            f.write("📈 PURCHASES (BUYS)\n")
            f.write("=" * 80 + "\n")
            f.write(get_trade_history_header() + "\n")
            f.write("-" * 80 + "\n")
            if merged_buys:
                for trade in merged_buys:
                    f.write(trade + "\n")
            else:
                f.write("No purchase history found\n")
        
        # Write sells file
        with open(sells_file, 'w', encoding='utf-8') as f:
            f.write("📉 SALES (SELLS)\n")
            f.write("=" * 80 + "\n")
            f.write(get_trade_history_header() + "\n")
            f.write("-" * 80 + "\n")
            if merged_sells:
                for trade in merged_sells:
                    f.write(trade + "\n")
            else:
                f.write("No sales history found\n")
        
        print(f"✅ Updated trade history for: {trade_info['username']}")
        
    except Exception as e:
        print(f"❌ Error updating trade history for {username}: {e}")


def process_trade_histories():
    """
    Main function to process all farm data and update trade histories.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(script_dir)  # Go up one level from scripts folder  
    raw_pull_folder = os.path.join(parent_dir, "raw pull")
    
    # Check if raw pull folder exists
    if not os.path.exists(raw_pull_folder):
        print("❌ Raw pull folder not found. Please run the batch fetch script first.")
        return
    
    print("🚀 Starting trade history processing...")
    print(f"📂 Reading from: {raw_pull_folder}")
    
    # Load all farm data
    farm_data_dict = load_raw_data(raw_pull_folder)
    
    if not farm_data_dict:
        print("❌ No farm data found to process.")
        return
    
    # Create trade history folder
    history_folder = create_trade_history_folder()
    print(f"📁 Trade history folder: {history_folder}")
    
    # Process each farm
    for username, farm_data in farm_data_dict.items():
        print(f"\\n🔄 Processing: {username}")
        
        # Extract trade information
        trade_info = extract_trade_info(farm_data)
        
        # Update trade history file
        update_trade_history(username, trade_info, history_folder)
    
    print(f"\\n🎉 Trade history processing complete!")
    print(f"📊 Processed {len(farm_data_dict)} farms")
    print(f"📁 History files updated in: Trade Overview")


if __name__ == "__main__":
    process_trade_histories()