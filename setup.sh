#!/bin/bash
set -e

echo "Creating Python virtual environment..."
python3 -m venv venv

echo "Activating virtual environment..."
source venv/bin/activate

echo "Installing required packages..."
pip install --upgrade pip
pip install requests beautifulsoup4 tqdm

echo "Setup complete."
echo "You can now run the script with:"
echo "source venv/bin/activate && python download_site.py <sitemap_url> [options]"
