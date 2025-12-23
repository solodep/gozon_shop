import httpx
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# Разрешаем запросы с фронтенда
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

ORDERS_URL = "http://orders:8000"
PAYMENTS_URL = "http://payments:8000"

async def proxy_request(url: str, request: Request):
    async with httpx.AsyncClient() as client:
        try:
            req_body = await request.body()
            resp = await client.request(
                method=request.method,
                url=url,
                content=req_body,
                headers=request.headers.raw,
                params=request.query_params
            )
            return Response(content=resp.content, status_code=resp.status_code, headers=dict(resp.headers))
        except httpx.RequestError:
            return Response(content="Service Unavailable", status_code=503)

@app.api_route("/orders/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def orders_proxy(path: str, request: Request):
    return await proxy_request(f"{ORDERS_URL}/orders/{path}", request)

@app.api_route("/accounts/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def payments_proxy(path: str, request: Request):
    return await proxy_request(f"{PAYMENTS_URL}/accounts/{path}", request)