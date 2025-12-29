from pathlib import Path
from mcp.server.fastmcp import FastMCP
from fastapi import FastAPI
from fastapi.responses import FileResponse

STATIC_DIR = Path(__file__).parent / "static"

# Create an MCP server
mcp = FastMCP("Cyber MCP Server on Databricks Apps")


@mcp.tool()
def dbx_disable_user(email: str) -> str:
    """Disables a user in Databricks"""
    return f"User {email} disabled successfully"


mcp_app = mcp.streamable_http_app()


app = FastAPI(
    lifespan=lambda _: mcp.session_manager.run(),
)


@app.get("/", include_in_schema=False)
async def serve_index():
    return FileResponse(STATIC_DIR / "index.html")


app.mount("/", mcp_app)
