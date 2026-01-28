# GitHub Runner Setup Guide

This guide will help you set up a self-hosted GitHub runner to automate the Sunflower Land trading intelligence system.

## Prerequisites

- Windows computer (ispank@192.168.1.51)
- Git installed
- Python 3.x installed
- Administrative access

## Step 1: Create GitHub Repository

1. Go to GitHub and create a new repository named `trade-tracker`
2. Make it public (required for GitHub Pages)
3. Clone this repository to your local machine

## Step 2: Push Your Code to GitHub

```bash
cd c:\Users\caleb\Documents\GitHub\api-data
git init
git add .
git commit -m "Initial commit - Sunflower Land Trading Intelligence"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/trade-tracker.git
git push -u origin main
```

## Step 3: Set Up GitHub Secrets

1. Go to your repository on GitHub
2. Click **Settings** > **Secrets and variables** > **Actions**
3. Click **New repository secret** and add:

### Required Secrets:
- **Name:** `SFL_API_KEY`
  - **Value:** Your Sunflower Land API key
- **Name:** `SFL_BEARER_TOKEN`
  - **Value:** Your Bearer token for API authentication

## Step 4: Set Up Self-Hosted Runner

### On GitHub:
1. Go to your repository **Settings** > **Actions** > **Runners**
2. Click **New self-hosted runner**
3. Select **Windows** as the operating system
4. Copy the download and configuration commands

### On Your Windows Machine (ispank@192.168.1.51):

1. **Create a folder for the runner:**
```cmd
mkdir C:\actions-runner
cd C:\actions-runner
```

2. **Download the runner (use the link from GitHub):**
```cmd
# Example command - use the actual one from GitHub
Invoke-WebRequest -Uri https://github.com/actions/runner/releases/download/v2.311.0/actions-runner-win-x64-2.311.0.zip -OutFile actions-runner-win-x64-2.311.0.zip
```

3. **Extract and configure:**
```cmd
Add-Type -AssemblyName System.IO.Compression.FileSystem
[System.IO.Compression.ZipFile]::ExtractToDirectory("$PWD\actions-runner-win-x64-2.311.0.zip", "$PWD")
```

4. **Configure the runner (use the command from GitHub):**
```cmd
# Example command - use the actual one from GitHub
.\config.cmd --url https://github.com/YOUR_USERNAME/trade-tracker --token YOUR_TOKEN
```

5. **When prompted:**
   - **Enter the name of the runner group:** Press Enter (default)
   - **Enter the name of runner:** `sfl-trading-runner`
   - **Enter any additional labels:** `sunflower,trading,windows`
   - **Enter name of work folder:** Press Enter (default)

6. **Install and start the runner as a Windows service:**
```cmd
.\svc.cmd install
.\svc.cmd start
```

## Step 5: Enable GitHub Pages

1. Go to your repository **Settings** > **Pages**
2. Under **Source**, select **GitHub Actions**
3. The site will be available at: `https://YOUR_USERNAME.github.io/trade-tracker`

## Step 6: Test the Setup

1. Go to your repository **Actions** tab
2. Click **Run workflow** on the "Trading Monitor" workflow
3. Watch the workflow execute

If successful, you should see:
- Data collection runs every 15 minutes
- Web dashboard updates automatically
- Trading intelligence available at your GitHub Pages URL

## File Structure After Setup

```
your-repo/
├── master.py                 # Main orchestration script
├── farm_ids.txt             # List of farms to monitor
├── item_mapping.txt         # Item ID to name mapping
├── scripts/
│   ├── batch_fetch.py           # Farm data collection
│   ├── marketplace_fetch.py     # Marketplace data collection
│   ├── process_data.py          # Data processing
│   └── generate_dashboard_data.py # Dashboard data generation
├── web/
│   ├── index.html              # Web dashboard
│   └── data/                   # Generated JSON files
├── .github/workflows/
│   └── trading-monitor.yml     # GitHub Actions workflow
├── Trade Overview/             # Processed trade data
├── raw pull/                   # Raw farm data
└── marketplace history/        # Raw marketplace data
```

## Monitoring and Maintenance

### Check Runner Status:
```cmd
C:\actions-runner\svc.cmd status
```

### Restart Runner Service:
```cmd
C:\actions-runner\svc.cmd stop
C:\actions-runner\svc.cmd start
```

### View Logs:
- GitHub Actions logs: Repository > Actions tab
- Runner logs: `C:\actions-runner\_diag\`

## Troubleshooting

### Common Issues:

1. **Runner offline:**
   - Check if the service is running
   - Restart the runner service
   - Check network connectivity

2. **API errors:**
   - Verify GitHub secrets are set correctly
   - Check API key validity

3. **Permissions errors:**
   - Ensure runner has write access to the directory
   - Check GitHub token permissions

4. **Python errors:**
   - Verify Python and pip are in PATH
   - Check if requests library is installed

## Expected Behavior

Once set up correctly:
- Every 15 minutes, the system automatically:
  1. Collects farm data from Sunflower Land API
  2. Fetches marketplace transaction data
  3. Processes and analyzes trading patterns
  4. Generates web dashboard data
  5. Updates the live website

## Dashboard Features

The web dashboard will show:
- 🏆 Top traders by profit
- 📊 Market summary statistics
- ⚡ Recent trading activity
- 👥 Individual trader analysis
- 📈 Profit/loss tracking
- 🎯 Most traded items

Perfect for identifying profitable trading patterns and successful traders to emulate!