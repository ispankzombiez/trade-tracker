#!/usr/bin/env python3
"""
Batch Farm Data Fetcher
Reads a list of farm IDs and fetches data for all of them in batch,
saving each response with the username as filename in a timestamped folder.
"""

import requests
import os
import json
from datetime import datetime
from typing import Optional, List
import time


def load_farm_ids(file_path: str = "farm_ids.txt") -> tuple[List[str], float]:
    """
    Load farm IDs from a text file, skipping the first line which contains wait time.
    
    Args:
        file_path: Path to the farm IDs file
        
    Returns:
        Tuple of (farm_ids_list, wait_time_seconds)
    """
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        parent_dir = os.path.dirname(script_dir)  # Go up one level to main directory
        full_path = os.path.join(parent_dir, file_path)
        
        with open(full_path, 'r') as f:
            lines = [line.strip() for line in f if line.strip()]
        
        # First line contains wait time setting
        if lines and lines[0].startswith('WAIT_TIME_SECONDS='):
            wait_time = float(lines[0].split('=')[1])
            farm_ids = lines[1:]
        else:
            wait_time = 31.0  # Default fallback
            farm_ids = lines
        
        print(f"📋 Loaded {len(farm_ids)} farm IDs from {file_path}")
        print(f"⏰ Current adaptive wait time: {wait_time}s")
        return farm_ids, wait_time
    except FileNotFoundError:
        print(f"❌ Farm IDs file not found: {file_path}")
        return [], 31.0
    except Exception as e:
        print(f"❌ Error reading farm IDs file: {e}")
        return [], 31.0


def update_wait_time(new_wait_time: float, file_path: str = "farm_ids.txt"):
    """
    Update the wait time in farm_ids.txt based on learned API behavior.
    """
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        parent_dir = os.path.dirname(script_dir)
        full_path = os.path.join(parent_dir, file_path)
        
        with open(full_path, 'r') as f:
            lines = f.read().strip().split('\n')
        
        # Update first line with new wait time
        lines[0] = f"WAIT_TIME_SECONDS={new_wait_time:.1f}"
        
        with open(full_path, 'w') as f:
            f.write('\n'.join(lines))
        
        print(f"📝 Updated adaptive wait time to {new_wait_time:.1f}s")
    except Exception as e:
        print(f"❌ Error updating wait time: {e}")


def fetch_api_data(url: str, api_key: str, timeout: int = 10) -> Optional[str]:
    """
    Fetch data from API with x-api-key authentication.
    
    Args:
        url: The API endpoint URL
        api_key: The API key for authentication
        timeout: Request timeout in seconds (default: 10)
        
    Returns:
        Response data as string, or None if failed
    """
    try:
        headers = {
            'x-api-key': api_key,
            'Accept': 'application/json',
            'User-Agent': 'Python-API-Fetcher/1.0'
        }
        
        response = requests.get(
            url, 
            headers=headers, 
            timeout=timeout
        )
        
        if response.status_code == 200:
            return response.text
        else:
            print(f"❌ API response code: {response.status_code} for {url}")
            return None
            
    except requests.exceptions.Timeout:
        print(f"❌ Request timed out for {url}")
        return None
    except requests.exceptions.ConnectionError:
        print(f"❌ Connection error for {url}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"❌ API fetch error for {url}: {e}")
        return None


def create_raw_pull_folder() -> str:
    """
    Create/clear the raw pull folder for this batch.
    
    Returns:
        Path to the raw pull folder
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(script_dir)  # Go up one level to main directory
    folder_name = "raw pull"
    folder_path = os.path.join(parent_dir, folder_name)
    
    try:
        # Create folder if it doesn't exist
        os.makedirs(folder_path, exist_ok=True)
        
        # Clear existing files in the folder
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            if os.path.isfile(file_path):
                os.remove(file_path)
        
        print(f"📁 Prepared raw pull folder: {folder_name}")
        return folder_path
    except Exception as e:
        print(f"❌ Error preparing raw pull folder: {e}")
        return os.path.dirname(script_dir)  # Return main directory as fallback


def save_farm_data(data: str, farm_id: str, folder_path: str) -> bool:
    """
    Save the fetched farm data using the username as filename.
    
    Args:
        data: The JSON data to save
        farm_id: The farm ID (used as fallback)
        folder_path: The folder to save the file in
        
    Returns:
        True if saved successfully, False otherwise
    """
    try:
        # Extract username from the JSON data
        json_data = json.loads(data)
        farm_data = json_data.get('farm', {})
        raw_username = farm_data.get('username')
        
        if raw_username:
            username = raw_username.lower()  # Convert to lowercase
            filename = f"{username}.json"
        else:
            filename = f"farm_{farm_id}.json"
            print(f"⚠️  No username found for farm {farm_id}, using fallback filename")
        
        file_path = os.path.join(folder_path, filename)
        
        # Save as pretty-printed JSON
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False)
        
        if raw_username:
            print(f"✅ Saved {raw_username} -> {filename}")
        else:
            print(f"✅ Saved farm_{farm_id} -> {filename}")
        return True
        
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON for farm {farm_id}: {e}")
        return False
    except Exception as e:
        print(f"❌ Error saving farm {farm_id}: {e}")
        return False


def process_batch(farm_ids: List[str], api_key: str, delay: float = 31.0) -> None:
    """
    Process a batch of farm IDs.
    
    Args:
        farm_ids: List of farm IDs to process
        api_key: API key for authentication
        delay: Delay between requests in seconds (default: 31.0)
    """
    if not farm_ids:
        print("❌ No farm IDs to process")
        return
    
    # Create/clear raw pull folder
    raw_pull_folder = create_raw_pull_folder()
    
    successful = 0
    failed = 0
    
    print(f"🚀 Starting batch processing of {len(farm_ids)} farms...")
    print(f"⏱️ Using {delay}s delay between requests to respect rate limits")
    
    for i, farm_id in enumerate(farm_ids, 1):
        print(f"\n📊 Processing {i}/{len(farm_ids)}: Farm {farm_id}")
        
        # Build API URL
        url = f"https://api.sunflower-land.com/community/farms/{farm_id}"
        
        # Fetch data
        data = fetch_api_data(url, api_key)
        
        if data:
            # Save data
            if save_farm_data(data, farm_id, raw_pull_folder):
                successful += 1
            else:
                failed += 1
        else:
            failed += 1
            print(f"❌ Failed to fetch data for farm {farm_id}")
        
        # Add delay between requests to be respectful to the API
        if i < len(farm_ids):  # Don't delay after the last request
            print(f"⏸️ Waiting {delay}s before next request...")
            time.sleep(delay)
    
    print(f"\n📈 Batch processing complete!")
    print(f"✅ Successful: {successful}")
    print(f"❌ Failed: {failed}")
    print(f"📁 Results saved in: raw pull")


def process_batch_adaptive(farm_ids: List[str], api_key: str, initial_wait_time: float, file_path: str) -> None:
    """
    Process a batch of farm IDs with adaptive rate limiting.
    
    Args:
        farm_ids: List of farm IDs to process
        api_key: API key for authentication
        initial_wait_time: Initial wait time between requests
        file_path: Path to farm_ids.txt for updating wait time
    """
    if not farm_ids:
        print("❌ No farm IDs to process")
        return
    
    # Create/clear raw pull folder
    raw_pull_folder = create_raw_pull_folder()
    
    successful = 0
    failed = 0
    total_wait_times = []
    current_wait_time = initial_wait_time
    
    print(f"🚀 Starting adaptive batch processing of {len(farm_ids)} farms...")
    print(f"⏱️ Initial adaptive delay: {current_wait_time}s between requests")
    
    for i, farm_id in enumerate(farm_ids, 1):
        print(f"\n📊 Processing {i}/{len(farm_ids)}: Farm {farm_id}")
        
        # Record start time for adaptive timing
        request_start_time = time.time()
        
        # Build API URL
        url = f"https://api.sunflower-land.com/community/farms/{farm_id}"
        
        # Fetch data
        data = fetch_api_data(url, api_key)
        
        # Record end time
        request_end_time = time.time()
        actual_request_time = request_end_time - request_start_time
        
        if data:
            # Save data
            if save_farm_data(data, farm_id, raw_pull_folder):
                successful += 1
            else:
                failed += 1
        else:
            failed += 1
            print(f"❌ Failed to fetch data for farm {farm_id}")
        
        # Adaptive wait time logic
        if i < len(farm_ids):  # Don't wait after last request
            # Analyze response time and adjust wait time
            if actual_request_time > current_wait_time:
                # API took longer than expected, increase wait time
                new_wait_time = min(actual_request_time * 1.2, 60.0)  # Cap at 60s
                if new_wait_time > current_wait_time + 2:
                    print(f"📈 API response slower than expected ({actual_request_time:.1f}s), increasing wait time to {new_wait_time:.1f}s")
                    current_wait_time = new_wait_time
                    update_wait_time(current_wait_time, file_path)
            elif actual_request_time < current_wait_time * 0.5 and len(total_wait_times) >= 3:
                # API consistently faster, try reducing wait time
                avg_recent_time = sum(total_wait_times[-3:]) / 3
                if avg_recent_time < current_wait_time * 0.6:
                    new_wait_time = max(avg_recent_time * 1.5, 15.0)  # Minimum 15s
                    if new_wait_time < current_wait_time - 2:
                        print(f"📉 API consistently fast ({avg_recent_time:.1f}s avg), reducing wait time to {new_wait_time:.1f}s")
                        current_wait_time = new_wait_time
                        update_wait_time(current_wait_time, file_path)
            
            total_wait_times.append(actual_request_time)
            
            print(f"⏸️ Waiting {current_wait_time:.1f}s before next request (last request: {actual_request_time:.1f}s)...")
            time.sleep(current_wait_time)
        else:
            total_wait_times.append(actual_request_time)
    
    # Final statistics
    avg_request_time = sum(total_wait_times) / len(total_wait_times) if total_wait_times else 0
    print(f"\n📈 Adaptive batch processing complete!")
    print(f"✅ Successful: {successful}")
    print(f"❌ Failed: {failed}")
    print(f"⏱️ Average request time: {avg_request_time:.1f}s")
    print(f"🎯 Final adaptive wait time: {current_wait_time:.1f}s")
    print(f"📁 Results saved in: raw pull")


def main():
    """
    Main function to run batch farm data fetching with adaptive rate limiting.
    """
    
    # 🔧 CONFIGURATION - Update these values
    API_KEY = "sfl.MTEyODk3NjMwMTU4MzUwOA.YKi8l48T3jH_mPYvpxgCrkeS_IUt3uWQFgTUQg40JCE"
    FARM_IDS_FILE = "farm_ids.txt"
    
    # Optional: Read from environment variables
    API_KEY = os.getenv('API_KEY', API_KEY)
    FARM_IDS_FILE = os.getenv('FARM_IDS_FILE', FARM_IDS_FILE)
    
    # Validate configuration
    if not API_KEY.startswith("sfl."):
        print("⚠️  Please check the API_KEY format")
        return
    
    # Load farm IDs and current wait time
    farm_ids, current_wait_time = load_farm_ids(FARM_IDS_FILE)
    
    if not farm_ids:
        print("❌ No farm IDs to process. Please check your farm_ids.txt file.")
        return
    
    # Process the batch with adaptive timing
    process_batch_adaptive(farm_ids, API_KEY, current_wait_time, FARM_IDS_FILE)


if __name__ == "__main__":
    main()