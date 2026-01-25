"""Serve multiple workflows from one server CLI."""

import hashlib
import importlib.util
import inspect
import sys
from pathlib import Path
from typing import List, Optional, Type

import typer
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .workflow import BaseWorkflow
from .web_api.api import create_workflow_router

app = typer.Typer()


def load_workflow(path: str) -> Type[BaseWorkflow]:
    """Load workflow class from file path. Supports path:ClassName syntax."""
    file_path, class_name = (path.rsplit(":", 1) + [None])[:2]
    file_path = Path(file_path).resolve()
    
    if not file_path.exists():
        raise FileNotFoundError(f"Not found: {file_path}")
    
    # Use hash for unique module name
    module_name = f"_wf_{hashlib.md5(str(file_path).encode()).hexdigest()[:8]}"
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    
    if class_name:
        return getattr(module, class_name)
    
    # Find first BaseWorkflow subclass
    for _, obj in inspect.getmembers(module, inspect.isclass):
        if issubclass(obj, BaseWorkflow) and obj is not BaseWorkflow:
            return obj
    
    raise ValueError(f"No BaseWorkflow subclass in {file_path}")


def create_multi_app(
    workflows: List[BaseWorkflow],
    workflow_names: List[str],
    mcp_port: int = 8080,
    ui_mode: str = "auto",
    dev_url: Optional[str] = None,
) -> FastAPI:
    """Create FastAPI app serving multiple workflows."""
    mcp_lifespan = None
    mcp_app = None
    try:
        from dr_agent.mcp_backend.main import mcp
        mcp_app = mcp.http_app(path="/")
        mcp_lifespan = mcp_app.lifespan
    except Exception as e:
        print(f"⚠ MCP not available: {e}")
    
    app = FastAPI(title="DR-Agent Multi-Workflow API", lifespan=mcp_lifespan)
    app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
    
    workflow_info = [] # list of workflows
    for wf, name in zip(workflows, workflow_names):
        app.include_router(create_workflow_router(wf), prefix=f"/agent/{name}", tags=[name])
        workflow_info.append({"name": name, "class": wf.__class__.__name__})
    
    @app.get("/health")
    async def health():
        return {"status": "ok", "workflows": [w["name"] for w in workflow_info]}
    
    @app.get("/agent/")
    async def list_agents():
        return {"agents": workflow_info}
    
    if mcp_app:
        app.mount("/mcp", mcp_app)
    
    # Mount UI
    try:
        from dr_agent_ui.server import mount_ui
        ui_mounted = mount_ui(app, ui_mode=ui_mode, dev_url=dev_url)
        if ui_mounted:
            print(f"UI mounted successfully in '{ui_mode}' mode")
        else:
            print(f"UI not mounted (mode: {ui_mode}). API endpoints are available.")
    except ImportError:
        print("dr_agent_ui not installed. Run: pip install dr-agent-ui")
    except Exception as e:
        print(f"Failed to mount UI: {e}")
    
    return app


@app.command()
def serve(
    workflow_paths: List[str] = typer.Argument(..., help="Workflow file paths (supports path:config.yaml:alias)"),
    port: int = typer.Option(8080, "--port", "-p"),
    host: str = typer.Option("0.0.0.0", "--host"),
    config: Optional[str] = typer.Option(None, "--config", "-c", help="Config file (applies to all workflows)"),
    ui_mode: str = typer.Option("auto", "--ui-mode", help="UI mode: auto, precompiled, dev, or proxy"),
    dev_url: Optional[str] = typer.Option(None, "--dev-url", help="Dev server URL (for proxy mode)"),
):
    """Serve multiple workflows from one server."""
    workflows = []
    workflow_names = []  # track names in order
    names = set()
    
    for spec in workflow_paths:
        parts = spec.split(":")
        if len(parts) == 1:
            path, wf_config, alias = spec, None, None
        elif len(parts) == 2: # path:config.yaml
            path, wf_config, alias = parts[0], parts[1], None
        elif len(parts) == 3: # alias
            path = parts[0]
            wf_config = parts[1] if parts[1] else None
            alias = parts[2]
        else:
            raise typer.BadParameter(f"Invalid SPEC. Use path, path:config.yaml, path::alias, or path:config.yaml:alias")
        
        cls = load_workflow(path)
        name = alias or cls.get_workflow_name()
        if name in names:
            raise typer.BadParameter(f"Duplicate workflow name: {name}")
        names.add(name)
        
        config_path = wf_config or config or getattr(cls, '_default_configuration_path', None)
        workflows.append(cls(
            configuration=config_path,
            skip_service_check=True,
            skip_mcp_check=True,
            mcp_url=f"http://localhost:{port}/mcp/",
        ))
        workflow_names.append(name)
        print(f"✓ {cls.__name__} → /agent/{name}/ ({Path(config_path).name if config_path else 'default'})")
    
    app = create_multi_app(workflows, workflow_names, mcp_port=port, ui_mode=ui_mode, dev_url=dev_url)
    
    print(f"\n{'='*50}\nServing at http://localhost:{port}")
    for name in workflow_names:
        print(f"   POST /agent/{name}/chat")
        print(f"   POST /agent/{name}/chat/stream")
        print(f"   POST /agent/{name}/chat/background")
        print(f"   WS   /agent/{name}/chat/background/ws")
        print(f"   GET  /agent/{name}/jobs")
        print(f"   POST /agent/{name}/jobs/{{job_id}}/cancel")
    print(f"   GET  /health")
    print(f"   GET  /agent/")
    print(f"{'='*50}\n")
    
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    app()

