# Deploy on Render

## Files
- `main_v2_4_bybit_adapter_v0.py` — receiver + Bybit adapter
- `main.py` — receiver-only stub
- `requirements.txt` — Python dependencies
- `render.yaml` — optional Render Blueprint

## Recommended path
1. Create the GitHub repo `theblackmambaLINK`.
2. Upload all files from this folder.
3. In Render, create a new Web Service manually from the repo.
4. Region: Frankfurt.
5. Use:
   - Build Command: `pip install -r requirements.txt`
   - Start Command: `uvicorn main_v2_4_bybit_adapter_v0:app --host 0.0.0.0 --port $PORT`
6. Wait for deploy.
7. Check:
   - `/`
   - `/state`
   - `/actions`
   - `/adapter/state`
   - `/adapter/actions`
   - `/webhook/tradingview` should show Method Not Allowed on GET

## Notes
- Default adapter journal path is `/tmp/bybit_adapter_link_v0.json`
- Set symbol mapping through env, for example `{"LINKUSDT":"LINKUSDT"}`
- Keep the LINK service isolated from BTC / ETH / BCH / other contours
