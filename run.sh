#!/bin/bash
# Quick-start script — run this from the recruitment-sourcer folder

# 1. Create virtual environment if it doesn't exist
if [ ! -d ".venv" ]; then
  echo "Creating virtual environment..."
  python3 -m venv .venv
fi

# 2. Activate it
source .venv/bin/activate

# 3. Install dependencies
pip install -q -r requirements.txt

# 4. Copy .env if not present
if [ ! -f ".env" ]; then
  cp .env.example .env
  echo ""
  echo "⚠️  Add your Proxycurl API key to .env before running"
  echo "    Or just enter it in the app sidebar"
fi

# 5. Launch the app
streamlit run app.py
