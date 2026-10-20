<#
.SYNOPSIS
    Builds the Tessera storage engine (C11) as a native shared library for Quorum.

.DESCRIPTION
    Windows: finds Visual Studio (or the Build Tools) with vswhere, loads the x64
    developer environment and compiles every Tessera source file with cl.exe into
    native/bin/tessera.dll.
    Other platforms: use native/build.sh instead.

    The .NET build calls this script automatically when the DLL is missing, so you
    only need to run it by hand after changing the C sources (or pass -Force).
#>
param(
    [switch]$Force,
    [switch]$DebugBuild
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$src = Join-Path $root 'tessera'
$out = Join-Path $root 'bin'
$obj = Join-Path $root 'obj'
$dll = Join-Path $out 'tessera.dll'

if ((Test-Path $dll) -and -not $Force) {
    $newest = Get-ChildItem -Recurse -Path $src -Include *.c, *.h | Sort-Object LastWriteTime -Descending | Select-Object -First 1
    if ($newest.LastWriteTime -le (Get-Item $dll).LastWriteTime) {
        Write-Host "tessera.dll is up to date."
        exit 0
    }
}

$vswhere = Join-Path ${env:ProgramFiles(x86)} 'Microsoft Visual Studio\Installer\vswhere.exe'
if (-not (Test-Path $vswhere)) {
    throw "vswhere.exe not found. Install Visual Studio 2022+ or the Build Tools with the 'Desktop development with C++' workload."
}
$vsPath = & $vswhere -latest -products * -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationPath
if (-not $vsPath) {
    throw "No Visual Studio installation with the C++ x64 tools was found."
}
$vcvars = Join-Path $vsPath 'VC\Auxiliary\Build\vcvars64.bat'

New-Item -ItemType Directory -Force -Path $out, $obj | Out-Null

$sources = Get-ChildItem -Recurse -Path (Join-Path $src 'src') -Filter *.c | ForEach-Object { '"' + $_.FullName + '"' }
$opt = if ($DebugBuild) { '/Od /Zi' } else { '/O2 /DNDEBUG' }
$def = Join-Path $root 'tessera.def'

# A batch file avoids cmd.exe quoting problems with paths that contain spaces.
# vcvars64.bat looks for vswhere.exe on PATH, so put the installer folder there.
$bat = Join-Path $obj 'build-tessera.bat'
@(
    '@echo off',
    "set `"PATH=$(Split-Path -Parent $vswhere);%PATH%`"",
    "call `"$vcvars`" >nul || exit /b 1",
    ("cl /nologo /LD $opt /std:c11 /W3 /utf-8 /D_CRT_SECURE_NO_WARNINGS /DWIN32_LEAN_AND_MEAN /DNOMINMAX " +
     "/I`"$src\include`" /I`"$src\src`" /Fo`"$obj\\`" $($sources -join ' ') " +
     "/Fe`"$dll`" /link /DEF:`"$def`" /NOLOGO")
) | Set-Content -Encoding ascii -Path $bat

Write-Host "Building tessera.dll with $(Split-Path -Leaf $vsPath) ..."
& cmd.exe /c "`"$bat`""
if ($LASTEXITCODE -ne 0) { throw "cl.exe failed with exit code $LASTEXITCODE" }
Write-Host "Built $dll"
