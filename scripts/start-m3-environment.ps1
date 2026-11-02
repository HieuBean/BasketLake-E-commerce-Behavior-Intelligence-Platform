param([string]$CacheRoot)
$ErrorActionPreference='Stop'
$projectRoot=[IO.Path]::GetFullPath((Split-Path $PSScriptRoot -Parent))
$projectDrive = Split-Path $projectRoot -Qualifier
if (-not $CacheRoot) {
    if (Test-Path 'E:\') { $CacheRoot = 'E:\DevCaches\BasketLake' }
    else { $CacheRoot = Join-Path $projectDrive '\DevCaches\BasketLake' }
}
$sparkLocalDirs = if (Test-Path 'E:\SparkTemp') { 'E:\SparkTemp\host-driver' } else { Join-Path $projectRoot '.tmp\spark-local' }
$routes=[ordered]@{TEMP=(Join-Path $projectRoot '.tmp');TMP=(Join-Path $projectRoot '.tmp');TMPDIR=(Join-Path $projectRoot '.tmp');PIP_CACHE_DIR=(Join-Path $CacheRoot 'pip');PYTHONPYCACHEPREFIX=(Join-Path $CacheRoot 'pycache');SPARK_LOCAL_DIRS=$sparkLocalDirs}
foreach($target in @($routes.Values)+@((Join-Path $CacheRoot 'pytest'),(Join-Path $projectRoot '.tmp\java'),(Join-Path $projectRoot '.tmp\checkpoints'))) {
    $cursor=[IO.Path]::GetFullPath($target)
    while($cursor) {
        if(Test-Path -LiteralPath $cursor) {if((Get-Item -LiteralPath $cursor).Attributes -band [IO.FileAttributes]::ReparsePoint){throw "Reparse point requires review: $cursor"}}
        $cursor=Split-Path $cursor -Parent
    }
    [IO.Directory]::CreateDirectory($target) | Out-Null
}
foreach($key in $routes.Keys){[Environment]::SetEnvironmentVariable($key,$routes[$key],'Process')}
$env:PYTEST_ADDOPTS=($env:PYTEST_ADDOPTS+' -o cache_dir="'+(Join-Path $CacheRoot 'pytest')+'"').Trim()
$javaOption='-Djava.io.tmpdir="'+(Join-Path $projectRoot '.tmp\java')+'"'
if($env:JAVA_TOOL_OPTIONS -match 'java\.io\.tmpdir='){throw 'Review existing JAVA_TOOL_OPTIONS tmpdir before overriding'}
$env:JAVA_TOOL_OPTIONS=($env:JAVA_TOOL_OPTIONS+' '+$javaOption).Trim()
$routes.GetEnumerator() | Format-Table Key,Value -AutoSize
Write-Output 'Process environment only. Dot-source this script in the terminal used to launch tools. Docker jobs require explicit container overrides; host E paths are not Linux paths.'
