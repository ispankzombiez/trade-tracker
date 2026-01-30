#!/usr/bin/env python3
"""
Test script for batch API functionality
Based on developer examples from chat history
"""

import json
import requests
import time
from typing import Dict, List, Any

# API Configuration
BASE_URL = "https://api.sunflower-land.com"
BATCH_ENDPOINT = "/community/getFarms"

def load_farm_mapping() -> Dict[str, str]:
    """Load the farm ID mapping"""
    try:
        with open('farm_id_mapping.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print("❌ farm_id_mapping.json not found!")
        return {}

def load_farm_ids() -> List[str]:
    """Load farm IDs from farm_ids.txt"""
    try:
        with open('farm_ids.txt', 'r') as f:
            return [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print("❌ farm_ids.txt not found!")
        return []

def test_batch_api_different_formats():
    """Test batch API with different payload formats based on developer examples"""
    
    print("🧪 Testing Batch API with Different Formats")
    print("=" * 60)
    
    # Load our data
    farm_mapping = load_farm_mapping()
    farm_ids = load_farm_ids()
    
    if not farm_mapping or not farm_ids:
        print("❌ Missing required data files")
        return
    
    # Get long farm IDs for our farms (use first 5 for testing)
    long_farm_ids = []
    for short_id in farm_ids[:5]:
        if short_id in farm_mapping and short_id != "WAIT_TIME_SECONDS=15.0":
            long_id = farm_mapping[short_id]
            long_farm_ids.append(long_id)
        else:
            print(f"⚠️ No mapping found for farm ID: {short_id}")
    
    print(f"📋 Found {len(long_farm_ids)} mapped farm IDs for testing")
    
    if not long_farm_ids:
        print("❌ No valid farm IDs found for testing")
        return
    
    # Test different payload formats
    test_formats = [
        {
            "name": "Array of strings (current approach)",
            "payload": long_farm_ids,
            "headers": {'Content-Type': 'application/json'}
        },
        {
            "name": "Object with farmIds array",
            "payload": {"farmIds": long_farm_ids},
            "headers": {'Content-Type': 'application/json'}
        },
        {
            "name": "Object with ids array",
            "payload": {"ids": long_farm_ids},
            "headers": {'Content-Type': 'application/json'}
        },
        {
            "name": "Array of objects with id field",
            "payload": [{"id": farm_id} for farm_id in long_farm_ids],
            "headers": {'Content-Type': 'application/json'}
        },
        {
            "name": "Form data with farmIds",
            "payload": {"farmIds": ",".join(long_farm_ids)},
            "headers": {'Content-Type': 'application/x-www-form-urlencoded'},
            "use_data": True
        }
    ]
    
    for i, test_format in enumerate(test_formats):
        print(f"\n🔍 Testing format {i+1}: {test_format['name']}")
        print("-" * 50)
        
        try:
            print(f"📦 Payload structure: {type(test_format['payload'])}")
            print(f"📄 Payload preview: {str(test_format['payload'])[:100]}{'...' if len(str(test_format['payload'])) > 100 else ''}")
            
            if test_format.get('use_data'):
                response = requests.post(
                    f"{BASE_URL}{BATCH_ENDPOINT}",
                    data=test_format['payload'],
                    headers=test_format['headers'],
                    timeout=30
                )
            else:
                response = requests.post(
                    f"{BASE_URL}{BATCH_ENDPOINT}",
                    json=test_format['payload'],
                    headers=test_format['headers'],
                    timeout=30
                )
            
            print(f"📡 Response Status: {response.status_code}")
            print(f"📏 Response Size: {len(response.text)} bytes")
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    print(f"✅ JSON Response Type: {type(data)}")
                    
                    if isinstance(data, dict):
                        print(f"📊 Dictionary Length: {len(data)}")
                        print(f"🔑 Keys: {list(data.keys())[:3]}{'...' if len(data) > 3 else ''}")
                    elif isinstance(data, list):
                        print(f"📊 List Length: {len(data)}")
                        if data and isinstance(data[0], dict):
                            print(f"🔑 First Item Keys: {list(data[0].keys())}")
                    
                    # Calculate success rate
                    farms_returned = len(data) if isinstance(data, (dict, list)) else 0
                    success_rate = (farms_returned / len(long_farm_ids)) * 100
                    print(f"📈 Success Rate: {success_rate:.1f}% ({farms_returned}/{len(long_farm_ids)})")
                    
                    if farms_returned > 0:
                        print(f"🎉 SUCCESS! This format works!")
                        return data  # Return successful data for further analysis
                    
                except json.JSONDecodeError:
                    print(f"❌ Invalid JSON response")
                    print(f"📄 Raw Response: {response.text[:200]}{'...' if len(response.text) > 200 else ''}")
            else:
                print(f"❌ HTTP Error: {response.status_code}")
                print(f"📄 Error Response: {response.text[:200]}{'...' if len(response.text) > 200 else ''}")
                
        except requests.exceptions.RequestException as e:
            print(f"❌ Request failed: {e}")
        
        # Wait between tests
        if i < len(test_formats) - 1:
            print("⏸️ Waiting 3 seconds before next test...")
            time.sleep(3)
    
    return None

def test_individual_vs_batch():
    """Compare individual API calls vs batch API for same farms"""
    
    print("\n🔬 Comparing Individual vs Batch API")
    print("=" * 60)
    
    # Load data
    farm_mapping = load_farm_mapping()
    farm_ids = [fid for fid in load_farm_ids()[:5] if fid != "WAIT_TIME_SECONDS=15.0"]  # Filter out the config line
    
    print(f"🎯 Testing with {len(farm_ids)} farms")
    
    # Test individual API calls first
    print("\n1️⃣ Testing Individual API Calls:")
    print("-" * 40)
    
    individual_results = {}
    for i, short_id in enumerate(farm_ids):
        if short_id in farm_mapping:
            long_id = farm_mapping[short_id]
            try:
                response = requests.get(
                    f"{BASE_URL}/community/{long_id}",
                    timeout=10
                )
                if response.status_code == 200:
                    data = response.json()
                    username = data.get('username', 'Unknown')
                    individual_results[long_id] = {
                        'username': username,
                        'data_size': len(str(data)),
                        'status': 'success'
                    }
                    print(f"✅ Farm {i+1}: {username} ({long_id}) - {len(str(data))} bytes")
                else:
                    print(f"❌ Farm {i+1}: HTTP {response.status_code}")
                    individual_results[long_id] = {'status': 'failed'}
            except Exception as e:
                print(f"❌ Farm {i+1}: Error - {e}")
                individual_results[long_id] = {'status': 'error'}
            
            if i < len(farm_ids) - 1:
                time.sleep(2)  # Brief wait between calls
    
    print(f"\n📊 Individual API Results: {len([r for r in individual_results.values() if r['status'] == 'success'])}/{len(farm_ids)} successful")
    return individual_results

if __name__ == "__main__":
    print("🌻 Sunflower Land Batch API Tester")
    print("=" * 60)
    
    # First test individual API to make sure our farm IDs are valid
    individual_results = test_individual_vs_batch()
    
    # Then run format tests to find the right batch API format
    successful_data = test_batch_api_different_formats()
    
    if successful_data:
        print(f"\n🎉 Found working batch format! Returned {len(successful_data)} farms")
        # Save sample of successful data for analysis
        with open('successful_batch_sample.json', 'w') as f:
            json.dump(successful_data, f, indent=2)
        print(f"💾 Saved successful batch data to successful_batch_sample.json")
    else:
        print("\n😞 No batch format worked successfully")
        print("🔍 The batch API might be disabled, broken, or require different authentication")
        print("💡 Suggestion: Continue using individual API calls as fallback")
    
    print("\n✅ Testing complete!")