# collect_cpu_temp.ps1 — textfile collector for CPU/motherboard temperature
# Uses LibreHardwareMonitorLib.dll to read hardware sensors
# Writes .prom file for node-exporter textfile collector
# Must run as Administrator (sensor access requires ring-0 driver)

$ErrorActionPreference = "Stop"

$dllPath = "C:\Users\lc999\AppData\Local\Microsoft\WinGet\Packages\LibreHardwareMonitor.LibreHardwareMonitor_Microsoft.Winget.Source_8wekyb3d8bbwe\LibreHardwareMonitorLib.dll"
$outputDir = "F:\Dev\AIstock\monitoring\textfile_collector"
$tempFile = Join-Path $outputDir "cpu_temp.prom.tmp"
$finalFile = Join-Path $outputDir "cpu_temp.prom"

Add-Type -Path $dllPath

$computer = New-Object LibreHardwareMonitor.Hardware.Computer
$computer.IsCpuEnabled = $true
$computer.IsMotherboardEnabled = $true
$computer.Open()

$lines = @(
    "# HELP hw_temperature_celsius Hardware sensor temperature in Celsius"
    "# TYPE hw_temperature_celsius gauge"
)

foreach ($hardware in $computer.Hardware) {
    $hardware.Update()
    foreach ($sub in $hardware.SubHardware) {
        $sub.Update()
    }

    $hwType = $hardware.HardwareType.ToString()
    $hwName = $hardware.Name -replace '"', '\"'

    foreach ($sensor in $hardware.Sensors) {
        if ($sensor.SensorType -eq [LibreHardwareMonitor.Hardware.SensorType]::Temperature -and $null -ne $sensor.Value) {
            $sensorName = $sensor.Name -replace '"', '\"'
            $value = [math]::Round($sensor.Value, 1)
            $lines += "hw_temperature_celsius{hardware_type=`"$hwType`",hardware=`"$hwName`",sensor=`"$sensorName`"} $value"
        }
    }

    # Also check sub-hardware sensors
    foreach ($sub in $hardware.SubHardware) {
        $subName = $sub.Name -replace '"', '\"'
        $subType = $sub.HardwareType.ToString()
        foreach ($sensor in $sub.Sensors) {
            if ($sensor.SensorType -eq [LibreHardwareMonitor.Hardware.SensorType]::Temperature -and $null -ne $sensor.Value) {
                $sensorName = $sensor.Name -replace '"', '\"'
                $value = [math]::Round($sensor.Value, 1)
                $lines += "hw_temperature_celsius{hardware_type=`"$subType`",hardware=`"$subName`",sensor=`"$sensorName`"} $value"
            }
        }
    }
}

$computer.Close()

# Write UTF-8 without BOM, LF line endings (node-exporter requires plain UTF-8 + LF)
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
$content = ($lines -join "`n") + "`n"
[System.IO.File]::WriteAllText($tempFile, $content, $utf8NoBom)
Move-Item -Path $tempFile -Destination $finalFile -Force
