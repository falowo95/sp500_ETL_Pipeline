"""
SP500 pipeline dashboard — a small, public, read-only FastAPI app on top of
the pipeline's own BigQuery marts. Runs on Cloud Run (scale-to-zero).
"""
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from . import queries

app = FastAPI(title="SP500 Pipeline Dashboard")
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "latest_trade_date": queries.latest_trade_date(),
            "top_movers": queries.top_movers(),
            "signal_distribution": queries.signal_distribution(),
            "pipeline_run": queries.latest_pipeline_run(),
        },
    )


@app.get("/tickers/{symbol}", response_class=HTMLResponse)
def ticker_detail(request: Request, symbol: str):
    symbol = symbol.upper()
    rows = queries.ticker_detail(symbol)
    if not rows:
        raise HTTPException(status_code=404, detail=f"No data found for symbol {symbol}")
    return templates.TemplateResponse(
        request,
        "ticker_detail.html",
        {
            "symbol": symbol,
            "rows": rows,
            "summary": queries.stock_summary(symbol),
        },
    )


@app.get("/about", response_class=HTMLResponse)
def about(request: Request):
    return templates.TemplateResponse(request, "about.html", {})


@app.get("/healthz")
def healthz():
    """Cloud Run startup/liveness probe target — deliberately does not
    touch BigQuery, so it stays fast and can't fail due to data issues."""
    return {"status": "ok"}
