# PULSE Launcher Script (Windows)

Write-Host "🚀 Starting PULSE..." -ForegroundColor Green
Write-Host ""

# Check Python
if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    Write-Host "❌ Python not found. Please install Python 3.9+" -ForegroundColor Red
    exit 1
}

# Check if venv exists
if (-not (Test-Path "venv")) {
    Write-Host "📦 Creating virtual environment..." -ForegroundColor Yellow
    python -m venv venv
}

# Activate venv
Write-Host "🔌 Activating virtual environment..." -ForegroundColor Yellow
& venv\Scripts\Activate.ps1

# Install requirements
if (-not (Test-Path "venv\.installed")) {
    Write-Host "📥 Installing dependencies..." -ForegroundColor Yellow
    pip install -r requirements.txt
    New-Item -Path "venv\.installed" -ItemType File
}

# Check .env
if (-not (Test-Path ".env")) {
    Write-Host "⚠️  .env file not found!" -ForegroundColor Yellow
    Write-Host "📝 Creating from template..." -ForegroundColor Yellow
    Copy-Item .env.example .env
    Write-Host ""
    Write-Host "❗ Please edit .env with your Azure OpenAI credentials" -ForegroundColor Red
    Write-Host "   Then run this script again"
    exit 1
}

# Run PULSE
Write-Host "✅ Launching PULSE..." -ForegroundColor Green
Write-Host ""
streamlit run src/pulse/ui/app.py
