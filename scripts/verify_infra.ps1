$ErrorActionPreference = "Stop"

function Invoke-Checked {
    param(
        [string]$Label,
        [scriptblock]$Command
    )

    & $Command
    if ($LASTEXITCODE -ne 0) {
        throw "$Label failed with exit code $LASTEXITCODE"
    }
}

function Invoke-ExpectedFailure {
    param(
        [string]$Label,
        [scriptblock]$Command
    )

    & $Command
    if ($LASTEXITCODE -eq 0) {
        throw "$Label unexpectedly succeeded"
    }
    Write-Host "$Label failed as expected"
}

function Wait-HttpOk {
    param(
        [string]$Url,
        [int]$Attempts = 60
    )

    for ($i = 1; $i -le $Attempts; $i++) {
        curl.exe -fsS $Url
        if ($LASTEXITCODE -eq 0) {
            Write-Host ""
            return
        }
        Start-Sleep -Seconds 2
    }

    throw "HTTP endpoint did not become ready: $Url"
}

Invoke-Checked "docker compose config" { docker compose config }
Invoke-Checked "docker compose up" { docker compose up -d --build }
Invoke-Checked "docker compose ps" { docker compose ps }

Invoke-Checked "valid date check" {
    docker compose run --rm stock-python python -m stock_selector.cli validate-date --trade-date 2026-06-19
}

Invoke-ExpectedFailure "invalid date check" {
    docker compose run --rm stock-python python -m stock_selector.cli validate-date --trade-date "../bad-date"
}

Invoke-Checked "init-db" {
    docker compose run --rm stock-python python -m stock_selector.cli init-db
}
Invoke-Checked "init-storage" {
    docker compose run --rm stock-python python -m stock_selector.cli init-storage
}
Invoke-Checked "health-check" {
    docker compose run --rm stock-python python -m stock_selector.cli health-check
}
Invoke-Checked "storage-smoke" {
    docker compose run --rm stock-python python -m stock_selector.cli storage-smoke --trade-date 2026-06-19
}

Wait-HttpOk "http://localhost:18080/actuator/health"
Wait-HttpOk "http://localhost:18080/api/health"
