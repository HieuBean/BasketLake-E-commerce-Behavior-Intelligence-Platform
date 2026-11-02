param([string]$RunDirectory='results/m3/e1/task06b/official-1',[string]$FigureDirectory='docs/figures/m3/e1-task06b')
$ErrorActionPreference='Stop'
Add-Type -AssemblyName System.Windows.Forms.DataVisualization
$root=Split-Path $PSScriptRoot -Parent
Set-Location $root
$data=Get-Content (Join-Path $RunDirectory 'analysis.json') -Raw | ConvertFrom-Json
$folder=Join-Path $root $FigureDirectory
if(Test-Path $folder){throw 'Figures already exist; do not replace'}
New-Item -ItemType Directory -Path $folder | Out-Null
function Chart($name,$title,$ylabel,$field,$scale) {
    $chart=New-Object System.Windows.Forms.DataVisualization.Charting.Chart
    $chart.Width=1400;$chart.Height=850;$chart.BackColor=[Drawing.Color]::White
    $area=New-Object System.Windows.Forms.DataVisualization.Charting.ChartArea
    $area.AxisX.Title='Actual sampled input (million rows)';$area.AxisY.Title=$ylabel
    $area.AxisX.TitleFont=New-Object Drawing.Font('Arial',14)
    $area.AxisY.TitleFont=New-Object Drawing.Font('Arial',14)
    $area.AxisX.LabelStyle.Font=New-Object Drawing.Font('Arial',12)
    $area.AxisY.LabelStyle.Font=New-Object Drawing.Font('Arial',12)
    $area.AxisX.Minimum=0;$area.AxisY.Minimum=0
    $area.AxisX.MajorGrid.LineColor=[Drawing.Color]::LightGray
    $area.AxisY.MajorGrid.LineColor=[Drawing.Color]::LightGray
    $chart.ChartAreas.Add($area);$heading=$chart.Titles.Add($title);$heading.Font=New-Object Drawing.Font('Arial',14)
    $series=New-Object System.Windows.Forms.DataVisualization.Charting.Series
    $series.Name='Median (3 successful runs)';$series.ChartType='Line';$series.BorderWidth=3;$series.MarkerStyle='Circle';$series.MarkerSize=9
    foreach($row in $data.rows){if($null -ne $row.$field){[void]$series.Points.AddXY($row.input_rows/1e6,$row.$field/$scale)}}
    $chart.Series.Add($series)
    if($field -eq 'median_s'){
        $runs=New-Object System.Windows.Forms.DataVisualization.Charting.Series
        $runs.Name='All measured runs';$runs.ChartType='Point';$runs.MarkerSize=7
        foreach($row in $data.records){[void]$runs.Points.AddXY($row.actual_input_rows/1e6,$row.wall_time_s)}
        $chart.Series.Add($runs)
    }
    $legend=New-Object System.Windows.Forms.DataVisualization.Charting.Legend
    $legend.Font=New-Object Drawing.Font('Arial',12)
    $legend.Docking='Bottom';$chart.Legends.Add($legend)
    $chart.SaveImage((Join-Path $folder ($name+'.png')),'Png');$chart.Dispose()
}
Chart 'wall-time' 'E1: compute-only workload; fixed 2 cores; MEMORY_AND_DISK + warmup' 'Wall time (seconds)' 'median_s' 1
Chart 'throughput' 'E1: effective input throughput (includes non-purchase input rows)' 'Input rows / second' 'median_throughput_rows_s' 1
Chart 'shuffle' 'E1: measured workload stages only; preparation/warmup excluded' 'Shuffle write (MiB)' 'median_shuffle_write_bytes' 1048576
Chart 'spill' 'E1: measured workload disk spill; cumulative bytes, not peak memory' 'Disk spill (MiB)' 'median_spill_bytes' 1048576
Chart 'scaling' 'E1: normalized throughput vs 1%; fixed resources (not parallel efficiency)' 'Normalized throughput (1% = 1)' 'normalized_throughput_vs_1pct' 1
Write-Output 'E1_CHARTS_CREATED 5 PNG; Windows .NET Charting; no dependencies installed'
