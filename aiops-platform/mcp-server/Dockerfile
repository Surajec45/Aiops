FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Run with MCP SSE transport using a simple command
# FastMCP uses uvicorn/starlette internally when mcp.run() is called with SSE
# but for production Docker, we can be more explicit if needed.
# For now, we'll use a simple wrapper that starts the SSE server.
CMD ["python", "main.py"]
