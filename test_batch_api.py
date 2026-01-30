#!/usr/bin/env python3

import requests
import json
import os

def test_batch_api():
    """Test just the batch API portion with existing mapped farms"""
    
    API_KEY = "sfl.MTEyODk3NjMwMTU4MzUwOA.YKi8l48T3jH_mPYvpxgCrkeS_IUt3uWQFgTUQg40JCE"
    
    # Load existing mappings
    with open("farm_id_mapping.json", 'r') as f:
        mappings = json.load(f)
    
    print(f"🔍 Testing batch API with {len(mappings)} mapped farms...")
    
    # Get first 5 long IDs for testing
    long_ids = list(mappings.values())[:5]
    print(f"📦 Testing with long IDs: {long_ids}")
    
    # Convert to integers for the API
    long_ids_int = [int(id_str) for id_str in long_ids]
    
    url = "https://api.sunflower-land.com/community/getFarms"
    headers = {
        'x-api-key': API_KEY,
        'Content-Type': 'application/json'
    }
    
    payload = {
        "ids": long_ids_int
    }
    
    print(f"🚀 Sending batch request...")
    
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=60)
        
        print(f"📡 Response status: {response.status_code}")
        
        if response.status_code == 200:
            batch_data = response.json()
            print(f"✅ Response type: {type(batch_data)}")
            
            if isinstance(batch_data, dict):
                print(f"✅ Successfully fetched {len(batch_data)} farms!")
                print("🎯 Sample farm IDs returned:")
                for i, farm_id in enumerate(list(batch_data.keys())[:3]):
                    print(f"  - {farm_id}")
                return True
            elif isinstance(batch_data, list):
                print(f"✅ Successfully fetched {len(batch_data)} farms (list format)!")
                return True
            else:
                print(f"⚠️ Unexpected response format: {type(batch_data)}")
                print(f"Response: {str(batch_data)[:200]}")
                
        else:
            print(f"❌ Request failed with status {response.status_code}")
            print(f"Response: {response.text[:300]}")
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Request error: {e}")
    
    return False

if __name__ == "__main__":
    test_batch_api()