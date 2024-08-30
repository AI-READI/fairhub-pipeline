$ErrorActionPreference = "Stop"
Set-Location C:\Users\b2aiUsr\.scripts\fairhubPipeline 

python -m venv .venv
.venv\Scripts\Activate

Copy-Item -Path C:\Users\b2aiUsr\.scripts\fairhubPipeline\requirements.txt -Destination C:\Users\b2aiUsr\.scripts\fairhubPipeline\aireadi\requirements.txt -Force

pip install -r requirements.txt

py.exe preprocess_cirrus_and_pool.py
