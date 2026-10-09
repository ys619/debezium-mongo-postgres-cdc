# PowerShell script to register Debezium connector on Windows
param (
    [string]$ConnectUrl = "http://localhost:8083",
    [string]$ConfigFile = "$PSScriptRoot\..\configs\mongodb-source-connector.json"
)

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "Registering Debezium MongoDB Source Connector" -ForegroundColor Cyan
Write-Host "Target Kafka Connect: $ConnectUrl" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# Wait for Kafka Connect REST API
Write-Host "Waiting for Kafka Connect REST API to be ready..." -ForegroundColor Yellow
$ready = $false
while (-not $ready) {
    try {
        $response = Invoke-RestMethod -Uri "$ConnectUrl/connectors" -Method Get -TimeoutSec 3 -ErrorAction Stop
        $ready = $true
    }
    catch {
        Write-Host "Kafka Connect is starting up, retrying in 3 seconds..."
        Start-Sleep -Seconds 3
    }
}
Write-Host "Kafka Connect is ready!" -ForegroundColor Green

# Read JSON configuration
$configJson = Get-Content -Raw -Path $ConfigFile
$configObj = $configJson | ConvertFrom-Json
$connectorName = $configObj.name

# Check if already registered
try {
    $existing = Invoke-RestMethod -Uri "$ConnectUrl/connectors/$connectorName" -Method Get -ErrorAction Stop
    Write-Host "Connector '$connectorName' already exists. Updating..." -ForegroundColor Yellow
    $configPayload = $configObj.config | ConvertTo-Json -Depth 10
    $updateResult = Invoke-RestMethod -Uri "$ConnectUrl/connectors/$connectorName/config" -Method Put -Body $configPayload -ContentType "application/json"
}
catch {
    Write-Host "Registering connector '$connectorName'..." -ForegroundColor Green
    $postResult = Invoke-RestMethod -Uri "$ConnectUrl/connectors" -Method Post -Body $configJson -ContentType "application/json"
}

Start-Sleep -Seconds 2
$status = Invoke-RestMethod -Uri "$ConnectUrl/connectors/$connectorName/status" -Method Get
Write-Host "Connector Status:" -ForegroundColor Cyan
$status | ConvertTo-Json -Depth 5
Write-Host "Registration completed successfully!" -ForegroundColor Green
