@echo off
REM P2P Gossip Load Balancer - Windows Cluster Yonetim Scripti
REM Kullanim: run_cluster.bat [build|start|stop|status]

setlocal enabledelayedexpansion

set "PROJECT_ROOT=%~dp0.."
set "BIN_DIR=%PROJECT_ROOT%\bin"
set "PID_DIR=%TEMP%\p2p-cluster"
set "LOG_DIR=%TEMP%\p2p-cluster\logs"

if "%1"=="" goto :help
if "%1"=="build" goto :build
if "%1"=="start" goto :start
if "%1"=="stop" goto :stop
if "%1"=="status" goto :status
goto :help

:build
echo === Derleniyor ===
if not exist "%BIN_DIR%" mkdir "%BIN_DIR%"
cd /d "%PROJECT_ROOT%"
go build -o "%BIN_DIR%\node.exe" .\cmd\node\
if errorlevel 1 ( echo HATA: node derlenemedi & exit /b 1 )
echo [OK] node.exe derlendi

go build -o "%BIN_DIR%\loadbalancer.exe" .\cmd\loadbalancer\
if errorlevel 1 ( echo HATA: loadbalancer derlenemedi & exit /b 1 )
echo [OK] loadbalancer.exe derlendi

go build -o "%BIN_DIR%\simulator.exe" .\cmd\simulator\
if errorlevel 1 ( echo HATA: simulator derlenemedi & exit /b 1 )
echo [OK] simulator.exe derlendi

echo Derleme tamamlandi.
goto :end

:start
if not exist "%BIN_DIR%\node.exe" (
    echo Binary bulunamadi. Once 'build' calistirin.
    exit /b 1
)

if not exist "%PID_DIR%" mkdir "%PID_DIR%"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

set "ENABLE_DEBUG=1"
echo === Cluster baslatiliyor === [ENABLE_DEBUG=1]

REM Node A - gossip adresi localhost ile belirtiliyor (Windows UDP icin zorunlu)
start /B "" "%BIN_DIR%\node.exe" -id=node-a -gossip=localhost:9001 -service=:8001 -seeds=localhost:9002,localhost:9003 > "%LOG_DIR%\node-a.log" 2>&1
echo [OK] node-a baslatildi (HTTP :8001, Gossip UDP localhost:9001)

REM Node B
start /B "" "%BIN_DIR%\node.exe" -id=node-b -gossip=localhost:9002 -service=:8002 -seeds=localhost:9001,localhost:9003 > "%LOG_DIR%\node-b.log" 2>&1
echo [OK] node-b baslatildi (HTTP :8002, Gossip UDP localhost:9002)

REM Node C
start /B "" "%BIN_DIR%\node.exe" -id=node-c -gossip=localhost:9003 -service=:8003 -seeds=localhost:9001,localhost:9002 > "%LOG_DIR%\node-c.log" 2>&1
echo [OK] node-c baslatildi (HTTP :8003, Gossip UDP localhost:9003)

REM Node D
start /B "" "%BIN_DIR%\node.exe" -id=node-d -gossip=localhost:9004 -service=:8004 -seeds=localhost:9001,localhost:9002 > "%LOG_DIR%\node-d.log" 2>&1
echo [OK] node-d baslatildi (HTTP :8004, Gossip UDP localhost:9004)

echo Gossip convergence bekleniyor (1s)...
timeout /T 1 /NOBREAK > nul

REM Load Balancer
start /B "" "%BIN_DIR%\loadbalancer.exe" -listen=:8080 -strategy=roundrobin -gossip=localhost:9000 -seeds=localhost:9001,localhost:9002,localhost:9003,localhost:9004 > "%LOG_DIR%\lb.log" 2>&1
echo [OK] loadbalancer baslatildi (HTTP :8080, Gossip UDP localhost:9000)

echo.
echo === Cluster hazir ===
echo Test: curl http://localhost:8080/
echo Gossip state: curl http://localhost:8001/members
echo Loglar: %LOG_DIR%
goto :end

:stop
echo === Cluster durduruluyor ===
REM Windows'ta start /B ile baslatilan processleri port bazli kapat
for %%p in (8001 8002 8003 8004 8080) do (
    for /f "tokens=5" %%a in ('netstat -ano 2^>nul ^| findstr ":%%p " ^| findstr "LISTENING"') do (
        echo Port :%%p - PID %%a durduruluyor...
        taskkill /PID %%a /F > nul 2>&1
    )
)
echo Cluster durduruldu.
goto :end

:status
echo === Cluster Durumu ===
for %%p in (8001 8002 8003 8004) do (
    curl -sf http://localhost:%%p/health > nul 2>&1
    if errorlevel 1 (
        echo [X] :%%p ERISILEMUYOR
    ) else (
        echo [OK] :%%p SAGLIKLI
    )
)
curl -sf http://localhost:8080/ > nul 2>&1
if errorlevel 1 (
    echo [X] :8080 LB ERISILEMUYOR
) else (
    echo [OK] :8080 LB CALISIYOR
)
goto :end

:help
echo Kullanim: run_cluster.bat [build^|start^|stop^|status]
echo.
echo  build   - Tum binary'leri derle
echo  start   - Cluster baslatildigi (4 node + LB)
echo  stop    - Cluster durdur
echo  status  - Cluster durumunu goster

:end
endlocal
