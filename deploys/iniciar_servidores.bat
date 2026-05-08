@echo off
set PYTHONUTF8=1
cd %~dp0..

echo [1/4] Ligando a Sede...
:: O Servidor NUNCA pode ter a variavel PREFECT_API_URL setada, senao ele entra em loop
start "Prefect Server" cmd /k "uv run prefect server start"

timeout /t 15 /nobreak >nul

echo [2/4] Ligando o Operario...
:: Forca cirurgicamente o Worker a usar a rede, jamais o arquivo
start "Prefect Worker" cmd /k "set PREFECT_API_URL=http://127.0.0.1:4200/api&& uv run prefect worker start --pool worker-tutor"

timeout /t 8 /nobreak >nul

echo [3/4] Ligando a IA Regente...
:: Forca cirurgicamente o Webhook a usar a rede
start "FastAPI Webhook" cmd /k "set PREFECT_API_URL=http://127.0.0.1:4200/api&& uv run uvicorn webhook:app --reload --port 8000"

echo [4/4] Ligando a Ponte...
start "Ngrok Tunnel" cmd /k "C:\ngrok-v3-stable-windows-amd64\ngrok.exe http 8000"

exit