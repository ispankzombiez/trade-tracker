#!/usr/bin/env python3
"""
Dashboard Data Generator
Processes trade data and generates JSON files for the web dashboard.
"""

import os
import json
import glob
from datetime import datetime, timedelta
from typing import Dict, List, Any
import re


def parse_trade_line(line: str) -> Dict[str, Any]:
    """Parse a trade line into structured data."""
    parts = [part.strip() for part in line.split('|')]
    if len(parts) < 9:
        return None
    
    try:
        return {
            'date': parts[0],
            'time': parts[1],
            'type': parts[2],
            'item': parts[3],
            'quantity': int(parts[4]),
            'price_text': parts[5],
            'price': float(parts[5].replace(' SFL', '')),
            'counterparty': parts[6],
            'farm_id': parts[7],
            'trade_id': parts[8]
        }
    except (ValueError, IndexError):
        return None


def load_inventory_data(username: str) -> Dict[str, Dict[str, float]]:
    """Load and categorize inventory data from raw pull files."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(script_dir)
    raw_file = os.path.join(parent_dir, "raw pull", f"{username.lower()}.json")
    
    inventory_categories = {
        'crops': {},
        'fruits': {},
        'resources': {}
    }
    
    # Define item categories
    crops = {
        'Sunflower', 'Potato', 'Pumpkin', 'Carrot', 'Cabbage', 'Beetroot', 'Cauliflower', 
        'Parsnip', 'Radish', 'Wheat', 'Kale', 'Eggplant', 'Corn', 'Soybean', 'Rice', 
        'Barley', 'Rhubarb', 'Zucchini', 'Yam', 'Broccoli', 'Pepper', 'Onion', 'Turnip', 
        'Artichoke', 'Olive'
    }
    
    fruits = {
        'Apple', 'Blueberry', 'Orange', 'Banana', 'Grape', 'Lemon', 'Tomato', 
        'Duskberry', 'Lunara', 'Celestine'
    }
    
    resources = {
        'Stone', 'Iron', 'Gold', 'Diamond', 'Wood', 'Crimstone', 'Obsidian'
        # Excluded: 'Axe', 'Pickaxe', 'Shovel', 'Petting Hand', 'Brush', 'Gold Egg' - not for chart display
    }
    
    if os.path.exists(raw_file):
        try:
            with open(raw_file, 'r', encoding='utf-8') as f:
                raw_data = json.load(f)
            
            inventory = raw_data.get('farm', {}).get('inventory', {})
            
            for item_name, quantity_str in inventory.items():
                try:
                    quantity = float(quantity_str)
                    if quantity > 0:  # Only include items with positive quantities
                        if item_name in crops:
                            inventory_categories['crops'][item_name] = quantity
                        elif item_name in fruits:
                            inventory_categories['fruits'][item_name] = quantity
                        elif item_name in resources:
                            inventory_categories['resources'][item_name] = quantity
                except (ValueError, TypeError):
                    continue
                    
        except Exception as e:
            print(f"❌ Error loading inventory data for {username}: {e}")
    
    return inventory_categories


def load_active_listings_and_offers(username: str) -> Dict[str, List[Dict[str, Any]]]:
    """Load active listings and offers for a user from raw marketplace data."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(script_dir)
    marketplace_file = os.path.join(parent_dir, "raw marketplace", f"{username.lower()}.json")
    
    listings = []
    offers = []
    
    if os.path.exists(marketplace_file):
        try:
            with open(marketplace_file, 'r', encoding='utf-8') as f:
                marketplace_data = json.load(f)
            
            # Load listings
            raw_listings = marketplace_data.get('listings', {})
            for listing_id, listing_data in raw_listings.items():
                items = listing_data.get('items', {})
                for item_name, quantity in items.items():
                    listings.append({
                        'id': listing_id,
                        'item': item_name,
                        'quantity': quantity,
                        'price': listing_data.get('sfl', 0),
                        'created_at': listing_data.get('createdAt'),
                        'collection': listing_data.get('collection', 'unknown'),
                        'trade_type': listing_data.get('tradeType', 'unknown')
                    })
            
            # Load offers
            raw_offers = marketplace_data.get('offers', {})
            for offer_id, offer_data in raw_offers.items():
                items = offer_data.get('items', {})
                for item_name, quantity in items.items():
                    offers.append({
                        'id': offer_id,
                        'item': item_name,
                        'quantity': quantity,
                        'price': offer_data.get('sfl', 0),
                        'created_at': offer_data.get('createdAt'),
                        'collection': offer_data.get('collection', 'unknown'),
                        'trade_type': offer_data.get('tradeType', 'unknown')
                    })
                    
        except Exception as e:
            print(f"❌ Error loading marketplace data for {username}: {e}")
    
    return {'listings': listings, 'offers': offers}


def load_user_data() -> Dict[str, Dict]:
    """Load all user data from Trade Overview folders."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(script_dir)
    trade_overview_path = os.path.join(parent_dir, "Trade Overview")
    
    users_data = {}
    
    if not os.path.exists(trade_overview_path):
        print("❌ Trade Overview folder not found")
        return users_data
    
    # Get all user folders
    for user_folder in os.listdir(trade_overview_path):
        folder_path = os.path.join(trade_overview_path, user_folder)
        if not os.path.isdir(folder_path):
            continue
            
        user_data = {
            'username': user_folder,
            'overview': {},
            'buys': [],
            'sells': [],
            'last_updated': None,
            'stats': {}
        }
        
        # Load overview data
        overview_file = os.path.join(folder_path, "overview.txt")
        if os.path.exists(overview_file):
            user_data['overview'] = parse_overview_file(overview_file)
        
        # Load buys data
        buys_file = os.path.join(folder_path, "buys.txt")
        if os.path.exists(buys_file):
            user_data['buys'] = parse_trades_file(buys_file)
        
        # Load sells data
        sells_file = os.path.join(folder_path, "sells.txt")
        if os.path.exists(sells_file):
            user_data['sells'] = parse_trades_file(sells_file)
        
        # Calculate stats
        user_data['stats'] = calculate_user_stats(user_data)
        
        users_data[user_folder] = user_data
    
    return users_data


def parse_overview_file(file_path: str) -> Dict[str, Any]:
    """Parse overview file to extract key metrics."""
    overview_data = {}
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Extract key metrics using regex
        patterns = {
            'balance': r'💰 Balance: ([\d,]+\.?\d*) SFL',
            'coins': r'🪙 ([\d,]+) Coins',
            'gems': r'💎 ([\d,]+) Gems',
            'earned_sfl': r'📈 Lifetime Earned: ([\d,]+\.?\d*) SFL',
            'earned_coins': r'([\d,]+\.?\d*) Coins',
            'spent_sfl': r'📉 Lifetime Spent: ([\d,]+\.?\d*) SFL',
            'items_sold': r'🏪 Total Items Sold: ([\d,]+)',
            'trade_points': r'⭐ Trade Points: ([\d,]+\.?\d*)',
            'active_listings': r'📋 Active Listings: (\\d+)',
            'last_updated': r'🔄 Last Updated: (.+)'
        }
        
        for key, pattern in patterns.items():
            match = re.search(pattern, content)
            if match:
                value = match.group(1).replace(',', '')
                if key in ['last_updated']:
                    overview_data[key] = value
                else:
                    try:
                        overview_data[key] = float(value)
                    except ValueError:
                        overview_data[key] = value
        
        # Count active listings
        listings_count = content.count('LISTING |')
        overview_data['active_listings'] = listings_count
        
    except Exception as e:
        print(f"❌ Error parsing overview file {file_path}: {e}")
    
    return overview_data


def parse_trades_file(file_path: str) -> List[Dict[str, Any]]:
    """Parse trades file and return list of trade data."""
    trades = []
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        for line in lines:
            line = line.strip()
            if line and '|' in line and not line.startswith('Date') and not line.startswith('=') and not line.startswith('-'):
                trade = parse_trade_line(line)
                if trade:
                    trades.append(trade)
    
    except Exception as e:
        print(f"❌ Error parsing trades file {file_path}: {e}")
    
    return trades


def calculate_user_stats(user_data: Dict) -> Dict[str, Any]:
    """Calculate trading statistics for a user."""
    buys = user_data['buys']
    sells = user_data['sells']
    overview = user_data['overview']
    
    stats = {
        'total_buys': len(buys),
        'total_sells': len(sells),
        'buy_volume': sum(trade['price'] for trade in buys),
        'sell_volume': sum(trade['price'] for trade in sells),
        'gross_profit': sum(trade['price'] for trade in sells) - sum(trade['price'] for trade in buys),
        'avg_buy_price': 0,
        'avg_sell_price': 0,
        'most_traded_items': {},
        'recent_activity': [],
        'profit_margin': 0
    }
    
    # Calculate averages
    if buys:
        stats['avg_buy_price'] = stats['buy_volume'] / len(buys)
    if sells:
        stats['avg_sell_price'] = stats['sell_volume'] / len(sells)
    
    # Calculate profit margin
    if stats['buy_volume'] > 0:
        stats['profit_margin'] = (stats['gross_profit'] / stats['buy_volume']) * 100
    
    # Find most bought items
    buy_item_counts = {}
    for trade in buys:
        item = trade['item']
        buy_item_counts[item] = buy_item_counts.get(item, 0) + trade['quantity']
    
    stats['most_bought_items'] = dict(sorted(buy_item_counts.items(), key=lambda x: x[1], reverse=True)[:10])
    
    # Find most sold items
    sell_item_counts = {}
    for trade in sells:
        item = trade['item']
        sell_item_counts[item] = sell_item_counts.get(item, 0) + trade['quantity']
    
    stats['most_sold_items'] = dict(sorted(sell_item_counts.items(), key=lambda x: x[1], reverse=True)[:10])
    
    # Get recent activity (last 24 hours)
    now = datetime.now()
    recent_trades = []
    for trade in (buys + sells)[:20]:  # Last 20 trades
        recent_trades.append({
            'type': trade['type'],
            'item': trade['item'],
            'quantity': trade['quantity'],
            'price': trade['price'],
            'date': trade['date'],
            'time': trade['time']
        })
    stats['recent_activity'] = recent_trades
    
    return stats


def generate_dashboard_data():
    """Generate JSON data files for the web dashboard."""
    print("🚀 Generating dashboard data...")
    
    # Load all user data
    users_data = load_user_data()
    
    if not users_data:
        print("❌ No user data found")
        return
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(script_dir)
    web_dir = os.path.join(parent_dir, "web")
    data_dir = os.path.join(web_dir, "data")
    
    # Create web directories
    os.makedirs(data_dir, exist_ok=True)
    
    # Generate summary data
    summary_data = {
        'last_updated': datetime.now().isoformat(),
        'total_users': len(users_data),
        'top_traders': [],
        'market_summary': {},
        'recent_activity': []
    }
    
    # Calculate top traders by profit
    user_profits = []
    all_recent_activity = []
    
    for username, data in users_data.items():
        stats = data['stats']
        user_profits.append({
            'username': username,
            'profit': stats['gross_profit'],
            'profit_margin': stats['profit_margin'],
            'total_volume': stats['buy_volume'] + stats['sell_volume'],
            'total_trades': stats['total_buys'] + stats['total_sells']
        })
        
        # Add to global recent activity
        for activity in stats['recent_activity'][:5]:  # Top 5 per user
            activity['username'] = username
            all_recent_activity.append(activity)
    
    # Sort top traders
    summary_data['top_traders'] = sorted(user_profits, key=lambda x: x['profit'], reverse=True)[:10]
    
    # Recent activity across all users
    summary_data['recent_activity'] = sorted(all_recent_activity, 
                                           key=lambda x: f"{x['date']} {x['time']}", 
                                           reverse=True)[:20]
    
    # Save individual user data
    for username, data in users_data.items():
        # Add active listings and offers data
        marketplace_data = load_active_listings_and_offers(username)
        data['active_listings'] = marketplace_data['listings']
        data['active_offers'] = marketplace_data['offers']
        
        # Add inventory data
        inventory_data = load_inventory_data(username)
        data['inventory'] = inventory_data
        
        user_file = os.path.join(data_dir, f"{username}.json")
        with open(user_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    
    # Save summary data
    summary_file = os.path.join(data_dir, "summary.json")
    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump(summary_data, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Generated dashboard data for {len(users_data)} users")
    print(f"📁 Data saved to: {data_dir}")


if __name__ == "__main__":
    generate_dashboard_data()