param([long]$MinimumEBytes=90000000000,[long]$MinimumCBytes=10000000000,[switch]$RequireFreshSilverOutput)
$ErrorActionPreference='Stop'
$projectRoot=[IO.Path]::GetFullPath((Split-Path $PSScriptRoot -Parent))
Set-Location $projectRoot
function Assert-EBackedPath([string]$Path) {
    if ($Path -notmatch '^(?i)(E:[\\/]|/run/desktop/mnt/host/e/|/host_mnt/e/)') {
        throw ('Not verified E-backed: '+$Path)
    }
}
Assert-EBackedPath $projectRoot
$checks=[ordered]@{repository_on_e=$true;e_reserve=(([IO.DriveInfo]::new('E:\')).AvailableFreeSpace -ge $MinimumEBytes);c_absolute_reserve=(([IO.DriveInfo]::new('C:\')).AvailableFreeSpace -ge $MinimumCBytes)}
$settingsPath=Join-Path $env:APPDATA 'Docker\settings-store.json'
$settings=Get-Content -LiteralPath $settingsPath -Raw | ConvertFrom-Json
$checks.docker_storage_on_e=([string]$settings.CustomWslDistroDir -match '^(?i)E:\\DockerData\\')
$mounts=@()
foreach($container in @('spark-master','spark-worker-1','spark-worker-2')) {
    $raw=docker inspect $container
    if($LASTEXITCODE -ne 0){throw "Cannot inspect $container"}
    $info=($raw | ConvertFrom-Json)[0]
    $checks[($container+'_running')]=[bool]$info.State.Running
    foreach($destination in @('/app','/spark-local')) {
        $mount=@($info.Mounts | Where-Object Destination -eq $destination)
        if($mount.Count -ne 1){throw "Missing unique mount $container $destination"}
        Assert-EBackedPath $mount[0].Source
        $mounts+=@{container=$container;destination=$destination;source=$mount[0].Source}
    }
}
if($RequireFreshSilverOutput){foreach($name in @('silver','quarantine')){$checks[($name+'_absent')]= -not(Test-Path -LiteralPath (Join-Path $projectRoot "data/lake/benchmark/e1/tier_l/$name/events"))}}
$cluster=docker exec spark-master python3 -c "import json,urllib.request; d=json.load(urllib.request.urlopen('http://spark-master:8080/json/')); print(json.dumps({'workers_alive':sum(w['state']=='ALIVE' for w in d['workers']),'active_apps':len(d['activeapps'])}))"
if($LASTEXITCODE -ne 0){throw 'Cluster unavailable'}
$cluster=$cluster | ConvertFrom-Json
$checks.cluster_idle_healthy=($cluster.workers_alive -eq 2 -and $cluster.active_apps -eq 0)
$report=[ordered]@{utc=[DateTime]::UtcNow.ToString('o');checks=$checks;mounts=$mounts;disks=@(Get-PSDrive C,E | Select-Object Name,Free,Used);pagefile=@(Get-CimInstance Win32_PageFileUsage | Select-Object Name,AllocatedBaseSize,CurrentUsage,PeakUsage);warnings=@('Backup checksum NOT REVALIDATED; no backup content read','Pagefile on C remains system-managed');scope='Storage routes only; full Bronze, metrics isolation, protected content and fresh run gates remain mandatory in controller'}
$report | ConvertTo-Json -Depth 8
if(@($checks.Values | Where-Object {$_ -ne $true}).Count){exit 1}
