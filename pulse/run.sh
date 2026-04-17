#!/bin/bash
# PULSE Launcher Script

echo "🚀 Starting PULSE..."
echo ""

# Check Python
if ! command -v python3 &> /dev/null; then
    echo "❌ Python3 not found. Please install Python 3.9+"
    exit 1
fi

# Check if venv exists
if [ ! -d "venv" ]; then
    echo "📦 Creating virtual environment..."
    python3 -m venv venv
fi

# Activate venv
echo "🔌 Activating virtual environment..."
source venv/bin/activate

# Install requirements
if [ ! -f "venv/.installed" ]; then
    echo "📥 Installing dependencies..."
    pip install -r requirements.txt
    touch venv/.installed
fi

# Check .env
if [ ! -f ".env" ]; then
    echo "⚠️  .env file not found!"
    echo "📝 Creating from template..."
    cp .env.example .env
    echo ""
    echo "❗ Please edit .env with your Azure OpenAI credentials"
    echo "   Then run this script again"
    exit 1
fi

# Run PULSE
echo "✅ Launching PULSE..."
echo ""
streamlit run src/pulse/ui/app.py
