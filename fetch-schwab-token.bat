@echo off
for /F "tokens=*" %%A in (.env) do SET %%A
set CALLBACK_URL="https://127.0.0.1:8182"
set TOKEN_FILE="data/schwab-token.json"
mkdir data
schwab-generate-token.py --token_file %TOKEN_FILE% --api_key %SCHWAB_API_KEY% --app_secret %SCHWAB_API_SECRET% --callback_url %CALLBACK_URL%
pause
