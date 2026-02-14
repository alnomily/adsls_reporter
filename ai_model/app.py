import os
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse

from .predict_image_api import PredictImageAPI

app = FastAPI(title="YemenNet OCR", version="1.0")

_model: PredictImageAPI | None = None


def _get_model() -> PredictImageAPI:
    global _model
    if _model is None:
        model_path = os.getenv("MODEL_PATH", "ocr_crnn_model.keras")
        _model = PredictImageAPI(model_path=model_path)
    return _model


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/test")


@app.get("/favicon.ico", include_in_schema=False)
def favicon():
    # Avoid noisy 404s in logs when opening the test page.
    return HTMLResponse(content="", status_code=204)


def _render_test_page(*, result_text: str | None = None, error_text: str | None = None) -> str:
    # Minimal HTML (no external assets) so it works in isolated containers.
    parts: list[str] = [
        "<!doctype html>",
        "<html lang='en'>",
        "<head>",
        "  <meta charset='utf-8'/>",
        "  <meta name='viewport' content='width=device-width, initial-scale=1'/>",
        "  <title>YemenNet OCR – Test</title>",
        "  <style>",
        "    body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;max-width:900px;margin:24px auto;padding:0 16px;line-height:1.4}",
        "    .card{border:1px solid #ddd;border-radius:12px;padding:16px;margin:16px 0}",
        "    .muted{color:#555}",
        "    code{background:#f6f6f6;padding:2px 6px;border-radius:6px}",
        "    .ok{padding:12px;border-radius:10px;background:#eefbf0;border:1px solid #cfe9d3}",
        "    .err{padding:12px;border-radius:10px;background:#fff1f1;border:1px solid #f0c9c9}",
        "    input[type=file]{display:block;margin:10px 0}",
        "    button{padding:10px 14px;border-radius:10px;border:1px solid #ccc;background:#fafafa;cursor:pointer}",
        "    button:hover{background:#f2f2f2}",
        "  </style>",
        "</head>",
        "<body>",
        "  <h1>YemenNet OCR (AI Model)</h1>",
        "  <p class='muted'>This is a small test UI for the OCR microservice running in the <code>ai-model</code> container.</p>",
        "  <div class='card'>",
        "    <h2>What can this AI do?</h2>",
        "    <ul>",
        "      <li>Reads YemenNet captcha images and returns a short code (expected characters: digits <code>0-9</code> and <code>X</code>).</li>",
        "      <li>Used by the main scraper/bot via HTTP at <code>POST /predict</code>.</li>",
        "      <li>Test endpoint here accepts common image formats (PNG/JPG).</li>",
        "    </ul>",
        "  </div>",
    ]

    if error_text:
        parts.append(f"<div class='err'><strong>Error:</strong> {error_text}</div>")
    if result_text is not None:
        parts.append(f"<div class='ok'><strong>Result:</strong> <code>{result_text or ''}</code></div>")

    parts.extend(
        [
            "  <div class='card'>",
            "    <h2>Upload captcha image</h2>",
            "    <form action='/test' method='post' enctype='multipart/form-data'>",
            "      <input type='file' name='file' accept='image/*' required />",
            "      <button type='submit'>Process</button>",
            "    </form>",
            "    <p class='muted'>API: <code>GET /health</code>, <code>POST /predict</code> (multipart form field name: <code>file</code>).</p>",
            "  </div>",
            "</body>",
            "</html>",
        ]
    )
    return "\n".join(parts)


@app.get("/test", response_class=HTMLResponse)
def test_page():
    return _render_test_page()


@app.post("/predict")
async def predict(file: UploadFile = File(...)):
    try:
        content = await file.read()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"failed_to_read_file: {e}")

    if not content:
        raise HTTPException(status_code=400, detail="empty_file")

    try:
        model = _get_model()
        text = model.predict_image_bytes(content)
        return {"text": text}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"predict_failed: {e}")


@app.post("/test", response_class=HTMLResponse)
async def test_predict(file: UploadFile = File(...)):
    try:
        content = await file.read()
    except Exception as e:
        return _render_test_page(error_text=f"failed_to_read_file: {e}")

    if not content:
        return _render_test_page(error_text="empty_file")

    try:
        model = _get_model()
        text = model.predict_image_bytes(content)
        return _render_test_page(result_text=text)
    except Exception as e:
        return _render_test_page(error_text=f"predict_failed: {e}")
