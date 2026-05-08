@echo off
set PYTHONUTF8=1
cd %~dp0..

echo [Conectado ao Servidor Prefect Cloud]

echo [1/3] Ligando o Operario Local...
start "Prefect Worker" cmd /k "uv run prefect worker start --pool tutor-workspace"

timeout /t 5 /nobreak >nul

echo [2/3] Ligando a IA Regente...
start "FastAPI Webhook" cmd /k "uv run uvicorn webhook:app --reload --port 8000"

echo [3/3] Ligando a Ponte...
start "Ngrok Tunnel" cmd /k "C:\ngrok-v3-stable-windows-amd64\ngrok.exe http 8000"

exit