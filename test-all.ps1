# FlowMaster Automated Test Script for PowerShell
Write-Host "====================================================" -ForegroundColor Cyan
Write-Host "     FlowMaster Distributed Orchestrator Tests      " -ForegroundColor Cyan
Write-Host "====================================================" -ForegroundColor Cyan
Write-Host ""

$baseUrl = "http://localhost:3000"

# 1. Health Check
Write-Host "[1/5] Testing Health Endpoint..." -ForegroundColor Yellow
try {
    $health = Invoke-RestMethod -Uri "$baseUrl/health" -Method Get
    Write-Host "Health Check: OK (Workflow: $($health.workflow), Steps: $($health.steps -join ', '))" -ForegroundColor Green
} catch {
    Write-Host "Health Check FAILED: $_" -ForegroundColor Red
    exit 1
}
Write-Host ""

# Helper to poll workflow
function Wait-Workflow ($workflowId, $maxSeconds = 15) {
    for ($i = 0; $i -lt $maxSeconds; $i++) {
        Start-Sleep -Seconds 1
        $res = Invoke-RestMethod -Uri "$baseUrl/workflows/$workflowId" -Method Get
        $status = $res.workflow.status
        if ($status -eq "COMPLETED" -or $status -like "*FAIL*") {
            return $res
        }
    }
    throw "Workflow $workflowId timed out after $maxSeconds seconds"
}

# 2. Normal Successful Workflow
Write-Host "[2/5] Testing Normal Successful Workflow (Happy Path)..." -ForegroundColor Yellow
$bodyNormal = @{ customerId = "C-POWERSHELL-1"; failCredit = $false; failVerification = $false } | ConvertTo-Json
$resNormal = Invoke-RestMethod -Uri "$baseUrl/workflows" -Method Post -ContentType "application/json" -Body $bodyNormal
$idNormal = $resNormal.workflow.id
Write-Host "Submitted Workflow ID: $idNormal"

$finalNormal = Wait-Workflow $idNormal
Write-Host "Status: $($finalNormal.workflow.status) (Step: $($finalNormal.workflow.current_step))" -ForegroundColor Green
$eventsNormal = ($finalNormal.events | ForEach-Object { $_.event_type }) -join " -> "
Write-Host "Events: $eventsNormal"
if ($finalNormal.workflow.status -ne "COMPLETED") { throw "Expected COMPLETED" }
Write-Host ""

# 3. Saga Compensation (failCredit = $true)
Write-Host "[3/5] Testing Saga Compensation (failCredit = true)..." -ForegroundColor Yellow
$bodyComp = @{ customerId = "C-POWERSHELL-FAIL-CREDIT"; failCredit = $true; failVerification = $false } | ConvertTo-Json
$resComp = Invoke-RestMethod -Uri "$baseUrl/workflows" -Method Post -ContentType "application/json" -Body $bodyComp
$idComp = $resComp.workflow.id
Write-Host "Submitted Workflow ID: $idComp"

$finalComp = Wait-Workflow $idComp
Write-Host "Status: $($finalComp.workflow.status) (Step: $($finalComp.workflow.current_step))" -ForegroundColor Green
$eventsComp = ($finalComp.events | ForEach-Object { $_.event_type }) -join " -> "
Write-Host "Events: $eventsComp"
if ($finalComp.workflow.status -ne "FAILED_COMPENSATED") { throw "Expected FAILED_COMPENSATED" }
Write-Host ""

# 4. Early Verification Failure (failVerification = $true)
Write-Host "[4/5] Testing Early Failure (failVerification = true)..." -ForegroundColor Yellow
$bodyFailVerif = @{ customerId = "C-POWERSHELL-FAIL-VERIF"; failCredit = $false; failVerification = $true } | ConvertTo-Json
$resFailVerif = Invoke-RestMethod -Uri "$baseUrl/workflows" -Method Post -ContentType "application/json" -Body $bodyFailVerif
$idFailVerif = $resFailVerif.workflow.id
Write-Host "Submitted Workflow ID: $idFailVerif"

$finalFailVerif = Wait-Workflow $idFailVerif
Write-Host "Status: $($finalFailVerif.workflow.status) (Step: $($finalFailVerif.workflow.current_step))" -ForegroundColor Green
$eventsFailVerif = ($finalFailVerif.events | ForEach-Object { $_.event_type }) -join " -> "
Write-Host "Events: $eventsFailVerif"
if ($finalFailVerif.workflow.status -ne "FAILED") { throw "Expected FAILED" }
Write-Host ""

# 5. Idempotency-Key Header
Write-Host "[5/5] Testing HTTP Idempotency-Key Deduplication..." -ForegroundColor Yellow
$idemKey = "ps-idemp-" + [System.DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
$headers = @{ "Idempotency-Key" = $idemKey }
$bodyIdemp = @{ customerId = "C-POWERSHELL-IDEMP"; failCredit = $false } | ConvertTo-Json

$res1 = Invoke-RestMethod -Uri "$baseUrl/workflows" -Method Post -Headers $headers -ContentType "application/json" -Body $bodyIdemp
$res2 = Invoke-RestMethod -Uri "$baseUrl/workflows" -Method Post -Headers $headers -ContentType "application/json" -Body $bodyIdemp

Write-Host "First Submit ID: $($res1.workflow.id)"
Write-Host "Second Submit ID: $($res2.workflow.id) (Reused: $($res2.reused))" -ForegroundColor Green
if ($res1.workflow.id -ne $res2.workflow.id -or $res2.reused -ne $true) {
    throw "Idempotency key did not return reused workflow"
}
Write-Host ""

Write-Host "====================================================" -ForegroundColor Cyan
Write-Host "       ALL POWERSHELL VERIFICATION TESTS PASSED!    " -ForegroundColor Cyan
Write-Host "====================================================" -ForegroundColor Cyan
