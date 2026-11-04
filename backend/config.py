import os
import logging
from typing import Optional, Dict, Any, List
from dotenv import load_dotenv

import sys
from pathlib import Path

# Load environment variables from .env
load_dotenv()

# Configure paths so all modular services in backend/ are importable
BACKEND_DIR = Path(__file__).resolve().parent
REPO_ROOT = BACKEND_DIR.parent

for sub in [BACKEND_DIR, BACKEND_DIR / "reconciliation", BACKEND_DIR / "adt", BACKEND_DIR / "retrieval"]:
    if str(sub) not in sys.path:
        sys.path.insert(0, str(sub))

# Add Windows npm global directory to PATH if needed
if os.name == "nt":
    npm_path = os.path.expandvars(r"%APPDATA%\npm")
    if os.path.isdir(npm_path) and npm_path.lower() not in os.environ.get("PATH", "").lower():
        os.environ["PATH"] = npm_path + os.pathsep + os.environ.get("PATH", "")

# Load environment variables explicitly from backend/.env or repo root .env
ENV_PATHS = [BACKEND_DIR / ".env", REPO_ROOT / ".env", Path.cwd() / ".env"]
LOADED_ENV_PATH = None
for p in ENV_PATHS:
    if p.is_file():
        load_dotenv(dotenv_path=str(p), override=False)
        if LOADED_ENV_PATH is None:
            LOADED_ENV_PATH = str(p)

# Logger setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("datasphere-agent")

# SAP Datasphere OData / BTP Configuration
DATASPHERE_API_URL = os.getenv("DATASPHERE_API_URL", "").strip()
DATASPHERE_TOKEN_URL = os.getenv("DATASPHERE_TOKEN_URL", "").strip()
DATASPHERE_AUTHORIZATION_URL = os.getenv("DATASPHERE_AUTHORIZATION_URL", "").strip()
DATASPHERE_MODELING_SPACE = os.getenv("DATASPHERE_MODELING_SPACE", "").strip()
DATASPHERE_SPACE_ID = os.getenv("DATASPHERE_SPACE_ID", DATASPHERE_MODELING_SPACE).strip()
if not DATASPHERE_MODELING_SPACE and DATASPHERE_SPACE_ID:
    DATASPHERE_MODELING_SPACE = DATASPHERE_SPACE_ID
DATASPHERE_REPLICATION_SPACE = os.getenv("DATASPHERE_REPLICATION_SPACE", "").strip()
OAUTH_CLIENT_ID = os.getenv("OAUTH_CLIENT_ID", "").strip()
OAUTH_CLIENT_SECRET = os.getenv("OAUTH_CLIENT_SECRET", "").strip()

# Datasphere CLI Settings
DATASPHERE_CLI_COMMAND = os.getenv("DATASPHERE_CLI_COMMAND", "datasphere").strip()
DATASPHERE_CLI_ACCEPT = os.getenv(
    "DATASPHERE_CLI_ACCEPT",
    "application/vnd.sap.datasphere.object.content+json"
).strip()

import shutil
def resolve_datasphere_cli_path(command_name: str = DATASPHERE_CLI_COMMAND) -> Optional[str]:
    """Resolves the actual Datasphere CLI executable on Windows or Linux."""
    cmd = (command_name or "datasphere").strip()
    resolved = shutil.which(cmd)
    if resolved:
        return resolved
    if os.name == "nt":
        for ext in [".cmd", ".CMD", ".bat", ".BAT", ".exe", ".EXE"]:
            resolved = shutil.which(cmd + ext)
            if resolved:
                return resolved
        appdata_npm = Path(os.path.expandvars(r"%APPDATA%\npm"))
        for candidate in ["datasphere.cmd", "datasphere.CMD", "datasphere.bat", "datasphere"]:
            candidate_p = appdata_npm / candidate
            if candidate_p.is_file():
                return str(candidate_p)
    return None

RESOLVED_CLI_PATH = resolve_datasphere_cli_path()

# LLM Providers Configuration
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "").strip()
ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-3-7-sonnet-20250219").strip()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-1.5-pro").strip()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o").strip()
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "auto").strip().casefold()
API_KEY = ANTHROPIC_API_KEY or GEMINI_API_KEY or OPENAI_API_KEY

# SAP Datasphere / HANA Cloud Direct SQL (Zero-Buffer Mode)
HANA_HOST = os.getenv("HANA_HOST", "").strip()
HANA_PORT = int(os.getenv("HANA_PORT", "443").strip() or "443")
HANA_USER = os.getenv("HANA_USER", "").strip()
HANA_PASSWORD = os.getenv("HANA_PASSWORD", "").strip()
HANA_ENCRYPT = os.getenv("HANA_ENCRYPT", "True").strip().lower() in ("true", "1", "yes")
HANA_SSL_VALIDATE_CERT = os.getenv("HANA_SSL_VALIDATE_CERT", "False").strip().lower() in ("true", "1", "yes")
USE_DIRECT_HANA = bool(HANA_HOST and HANA_USER and HANA_PASSWORD)

try:
    from hdbcli import dbapi as hana_dbapi
except ImportError:
    hana_dbapi = None

# SAP S/4HANA ADT (Eclipse Open SQL) Configuration
SAP_BASE_URL = os.getenv("SAP_BASE_URL", "").strip().rstrip("/")
SAP_USERNAME = os.getenv("SAP_USERNAME", "").strip()
SAP_PASSWORD = os.getenv("SAP_PASSWORD", "")
ADT_ROW_LIMIT = int(os.getenv("ADT_ROW_LIMIT", "100").strip() or "100")
USE_SAP_ADT = bool(SAP_BASE_URL and SAP_USERNAME and SAP_PASSWORD)

# API Server Settings
API_HOST = os.getenv("API_HOST", "0.0.0.0").strip()
API_PORT = int(os.getenv("API_PORT", "9050").strip() or "9050")

def get_config() -> Dict[str, Any]:
    """Compatibility config accessor for Data Reconciliation Agent modules."""
    return {
        "base_url": SAP_BASE_URL,
        "username": SAP_USERNAME,
        "password": SAP_PASSWORD,
        "row_limit": ADT_ROW_LIMIT,
    }

def get_system_diagnostics() -> Dict[str, Any]:
    """Returns safe diagnostic information without leaking passwords or secrets."""
    missing_vars = []
    if not DATASPHERE_SPACE_ID:
        missing_vars.append("DATASPHERE_SPACE_ID")
    if not (OAUTH_CLIENT_ID and OAUTH_CLIENT_SECRET and DATASPHERE_TOKEN_URL):
        missing_vars.append("OAUTH_CREDENTIALS")

    cli_found = bool(resolve_datasphere_cli_path())
    return {
        "loaded_env_path": LOADED_ENV_PATH,
        "space_id": DATASPHERE_SPACE_ID,
        "modeling_space": DATASPHERE_MODELING_SPACE,
        "replication_space": DATASPHERE_REPLICATION_SPACE,
        "datasphere_cli": {
            "available": cli_found,
            "resolved_path": RESOLVED_CLI_PATH,
        },
        "connectors": {
            "oauth_configured": bool(DATASPHERE_TOKEN_URL and OAUTH_CLIENT_ID and OAUTH_CLIENT_SECRET),
            "direct_hana": USE_DIRECT_HANA,
            "sap_adt": USE_SAP_ADT,
            "llm_ready": bool(API_KEY),
        },
        "missing_variables": missing_vars,
    }

# Session Database Path (Pure Local SQLite)
SESSION_DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "datasphere_chat_sessions.db")

# In-Memory Caches & Shared State
cached_oauth_token: Optional[Dict[str, Any]] = None
view_columns_cache: Dict[str, List[str]] = {}
view_parameters_cache: Dict[str, List[Dict[str, Any]]] = {}
view_working_endpoint_cache: Dict[str, str] = {}
space_views_cache: Dict[str, List[str]] = {}
space_assets_cache: Dict[str, Dict[str, Any]] = {}
cached_gemini_working: Optional[bool] = None
conversation_sessions: Dict[str, Dict[str, Any]] = {}
