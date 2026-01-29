#!/usr/bin/env python3
"""
Master Script - Sunflower Land Data Pipeline
Runs all data collection and processing scripts in sequence:
1. batch_fetch.py - Fetches farm data
2. scripts/marketplace_fetch.py - Fetches marketplace data
3. scripts/process_data.py - Processes and formats trade data
"""

import subprocess
import sys
import os
from datetime import datetime


def run_script(script_path: str, script_name: str) -> bool:
    """
    Run a Python script and return success status.
    
    Args:
        script_path: Path to the script relative to current directory
        script_name: Display name for the script
        
    Returns:
        True if script ran successfully, False otherwise
    """
    print(f"\n{'='*60}", flush=True)
    print(f"🚀 Running: {script_name}", flush=True)
    print(f"📁 Script: {script_path}", flush=True)
    print(f"⏰ Started: {datetime.now().strftime('%Y-%m-%d %I:%M:%S %p')}", flush=True)
    print(f"{'='*60}", flush=True)
    
    try:
        # Run the script with unbuffered output for live logging
        result = subprocess.run([sys.executable, "-u", script_path], 
                              capture_output=False, 
                              text=True, 
                              check=True)
        
        print(f"\n✅ {script_name} completed successfully!", flush=True)
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"\n❌ {script_name} failed with exit code: {e.returncode}", flush=True)
        return False
    except Exception as e:
        print(f"\n❌ Error running {script_name}: {e}", flush=True)
        return False


def main():
    """
    Main function to run the complete data pipeline.
    """
    print("🌻 Sunflower Land Data Pipeline", flush=True)
    print("=" * 60, flush=True)
    print(f"⏰ Pipeline started: {datetime.now().strftime('%Y-%m-%d %I:%M:%S %p')}", flush=True)
    
    # Define the scripts to run in order
    scripts = [
        ("scripts/batch_fetch.py", "Farm Data Batch Fetcher"),
        ("scripts/marketplace_fetch.py", "Marketplace Data Fetcher"), 
        ("scripts/process_data.py", "Trade Data Processor"),
        ("scripts/generate_dashboard_data.py", "Dashboard Data Generator")
    ]
    
    successful = 0
    failed = 0
    
    # Run each script in sequence
    for script_path, script_name in scripts:
        # Check if script exists
        if not os.path.exists(script_path):
            print(f"❌ Script not found: {script_path}", flush=True)
            failed += 1
            continue
            
        # Run the script
        if run_script(script_path, script_name):
            successful += 1
        else:
            failed += 1
            print(f"⚠️  Continuing with next script despite {script_name} failure...", flush=True)
    
    # Final summary
    print(f"\n{'='*60}", flush=True)
    print("📊 PIPELINE SUMMARY", flush=True)
    print(f"{'='*60}", flush=True)
    print(f"✅ Successful scripts: {successful}", flush=True)
    print(f"❌ Failed scripts: {failed}", flush=True)
    print(f"⏰ Pipeline completed: {datetime.now().strftime('%Y-%m-%d %I:%M:%S %p')}", flush=True)
    
    if failed == 0:
        print("🎉 All scripts completed successfully!", flush=True)
    else:
        print("⚠️  Some scripts failed - check output above for details", flush=True)
    
    return failed == 0


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
