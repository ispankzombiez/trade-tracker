# 🌻 trade-tracker

An automated system for tracking and analyzing marketplace activity in Sunflower Land to identify profitable trading patterns.

## Features

- **Automated Data Collection**: Runs every 15 minutes via GitHub Actions
- **Player Trade Tracking**: Monitors purchases, sales, and active listings
- **Profit Analysis**: Calculates trading performance and patterns
- **Web Dashboard**: Live view of trading data via GitHub Pages
- **Historical Data**: Maintains complete trading history

## Structure

```
├── scripts/                 # Core processing scripts
│   ├── batch_fetch.py      # Fetch farm data
│   ├── marketplace_fetch.py # Fetch marketplace data  
│   └── process_data.py     # Process and format data
├── data/                   # Generated data files
│   ├── raw pull/          # Raw farm data
│   ├── marketplace history/ # Raw marketplace data
│   └── Trade Overview/     # Processed trade summaries
├── web/                    # Dashboard interface
│   ├── index.html         # Main dashboard
│   ├── css/              # Styling
│   └── js/               # JavaScript for data visualization
├── .github/
│   └── workflows/
│       └── trading-monitor.yml # Automation workflow
├── master.py              # Main execution script
├── farm_ids.txt          # List of farms to monitor
└── item_mapping.txt      # Item ID to name mappings
```

## Setup

1. **Configure Farm IDs**: Add farm IDs to monitor in `farm_ids.txt`
2. **Set API Key**: Configure your SFL API key in GitHub Secrets
3. **Deploy**: Push to GitHub and enable Actions

## Dashboard

View live trading data at: `https://[username].github.io/[repo-name]`

## Data Collection

The system automatically:
- Fetches current farm states every 15 minutes
- Downloads recent marketplace transactions  
- Processes data into readable trade summaries
- Updates the web dashboard with latest insights

## Trading Intelligence

Analyze:
- **Profit/Loss**: Which players are making money?
- **Hot Items**: What's trading frequently at good margins?
- **Market Timing**: When do the best deals happen?
- **Trading Patterns**: Who to follow for profitable strategies?