#!/bin/bash
# Install script for funda_finder
# Handles funda-scraper dependency conflict workaround

set -e  # Exit on error

echo "🔧 Installing Funda Finder dependencies..."

# Check if we're in a virtual environment
if [[ -z "$VIRTUAL_ENV" ]]; then
    echo "⚠️  No virtual environment detected."
    echo "   Creating .venv..."
    python3 -m venv .venv
    source .venv/bin/activate
    echo "✓ Virtual environment created and activated"
else
    echo "✓ Using existing virtual environment: $VIRTUAL_ENV"
fi

# Install main dependencies (excluding funda-scraper)
echo ""
echo "📦 Installing main dependencies..."
grep -v "^#" requirements.txt | grep -v "^$" | grep -v "funda-scraper" | pip install -r /dev/stdin

# Install funda-scraper without its dependencies
echo ""
echo "📦 Installing funda-scraper (without dependency resolution)..."
pip install --no-deps funda-scraper

# Install funda-scraper's missing dependencies
echo ""
echo "📦 Installing funda-scraper dependencies..."
pip install beautifulsoup4 requests diot tqdm lxml

echo ""
echo "✅ Installation complete!"
echo ""
echo "ℹ️  Note: funda-scraper has a strict urllib3==1.26 pin that conflicts"
echo "   with our Python 3.13 requirement (urllib3>=1.26.18). This script"
echo "   works around that by installing funda-scraper without dependency"
echo "   resolution. urllib3 1.26.20 still contains the compatibility layer"
echo "   that funda-scraper needs."
echo ""
echo "To run tests: pytest -v"
