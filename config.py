import os

# 禁用 PaddleOCR 联网模型检查，解决启动慢的问题
os.environ.setdefault("PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK", "True")

# MySQL Configuration
MYSQL_HOST = os.environ.get("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", "3306"))
MYSQL_USER = os.environ.get("MYSQL_USER", "root")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD", "030705")
MYSQL_DB = os.environ.get("MYSQL_DB", "rjt3")
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "888")

# Paths
ARCHIVE_DIR = "shipping_history"
CONTRACT_DIR = "data/contracts"
MACHINE_ARCHIVE_DIR = "machine_archives"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONTRACT_ABS_DIR = os.path.join(BASE_DIR, CONTRACT_DIR)
MACHINE_ARCHIVE_ABS_DIR = os.path.join(BASE_DIR, MACHINE_ARCHIVE_DIR)

# Optional dependency flags
try:
    from paddleocr import PaddleOCR
    import pdfplumber
    import docx
    OCR_AVAILABLE = True
except ImportError:
    PaddleOCR = None
    pdfplumber = None
    docx = None
    OCR_AVAILABLE = False

try:
    import mammoth
    MAMMOTH_AVAILABLE = True
except ImportError:
    mammoth = None
    MAMMOTH_AVAILABLE = False

try:
    import openpyxl
    OPENPYXL_AVAILABLE = True
except ImportError:
    openpyxl = None
    OPENPYXL_AVAILABLE = False

try:
    import plotly.express as px
    import plotly.graph_objects as go
    PLOTLY_AVAILABLE = True
except (ImportError, ModuleNotFoundError):
    px = None
    go = None
    PLOTLY_AVAILABLE = False

DEFAULT_USERS = {
    "boss": {"password": "888", "role": "Boss", "name": "老板"},
    "admin": {"password": "888", "role": "Admin", "name": "系统管理员"},
    "sales": {"password": "123", "role": "Sales", "name": "销售员"},
    "prod": {"password": "123", "role": "Prod", "name": "仓管/生产"},
}

DEFAULT_ROLE_PERMISSIONS = {
    "Boss": ["PLANNING", "CONTRACT", "QUERY", "ARCHIVE"],
    "Sales": ["PLANNING", "CONTRACT", "SALES_CREATE", "SALES_ALLOC", "INBOUND", "QUERY"],
    "Prod": ["INBOUND", "SHIP_CONFIRM", "QUERY", "MACHINE_EDIT", "ARCHIVE"],
}

PRESET_RATIOS = {
    "300C": (["FH-300C"], ["FH-300C", "FR-400G", "FR-500G", "FR-600G"]),
    "400G": (["FR-400G"], ["FH-300C", "FR-400G", "FR-500G", "FR-600G"]),
    "500G": (["FR-500G"], ["FH-300C", "FR-400G", "FR-500G", "FR-600G"]),
    "600G": (["FR-600G"], ["FH-300C", "FR-400G", "FR-500G", "FR-600G"]),
    "400XS": (["FR-400XS(PRO)"], ["FR-400XS(PRO)", "FR-500XS(PRO)", "FR-600XS(PRO)", "FR-7055XS(PRO)", "FR-8055XS(PRO)", "FR-8060XS(PRO)"]),
    "500XS": (["FR-500XS(PRO)"], ["FR-400XS(PRO)", "FR-500XS(PRO)", "FR-600XS(PRO)", "FR-7055XS(PRO)", "FR-8055XS(PRO)", "FR-8060XS(PRO)"]),
    "600XS": (["FR-600XS(PRO)"], ["FR-400XS(PRO)", "FR-500XS(PRO)", "FR-600XS(PRO)", "FR-7055XS(PRO)", "FR-8055XS(PRO)", "FR-8060XS(PRO)"]),
    "大机": (["FR-7055XS(PRO)", "FR-8055XS(PRO)", "FR-8060XS(PRO)"], ["FR-400XS(PRO)", "FR-500XS(PRO)", "FR-600XS(PRO)", "FR-7055XS(PRO)", "FR-8055XS(PRO)", "FR-8060XS(PRO)"]),
}

CUSTOM_MODEL_ORDER = [
    "FH-260C", "FH-300C",
    "FR-400G", "FR-400XS(PRO)", "FR-400AUTO",
    "FR-500G", "FR-500XS(PRO)", "FR-500AUTO",
    "FR-600G", "FR-600XS(PRO)", "FR-600AUTO",
    "FR-7055AUTO", "FR-7055XS(PRO)",
    "FR-8055XS(PRO)", "FR-8055AUTO", "FR-8060XS(PRO)",
    "FR-1100XS(PRO)", "FL-1390XS(PRO)", "FL-1610XS", "FR-1080Y",
]

FUNC_MAP = {
    "PLANNING": {"label": "👑 生产统筹", "page": "boss_planning", "class": "boss-btn"},
    "CONTRACT": {"label": "🏭 合同管理", "page": "production", "class": "production-btn"},
    "QUERY": {"label": "🔍 库存查询", "page": "query", "class": "query-btn"},
    "ARCHIVE": {"label": "📂 机台档案", "page": "machine_archive", "class": "machine-edit-btn"},
    "SALES_CREATE": {"label": "📝 销售下单", "page": "sales_create", "class": "sales-create-btn"},
    "INBOUND": {"label": "📥 成品入库", "page": "inbound", "class": "inbound-btn"},
    "SALES_ALLOC": {"label": "📦 订单配货", "page": "sales_alloc", "class": "sales-alloc-btn"},
    "SHIP_CONFIRM": {"label": "🚛 发货复核", "page": "ship_confirm", "class": "ship-btn"},
    "MACHINE_EDIT": {"label": "🛠️ 机台编辑", "page": "machine_edit", "class": "machine-edit-btn"},
}

GLOBAL_CSS = """
<style>
.block-container { padding-top: 2rem !important; max-width: 100% !important; }
html, body, [class*="css"] { font-family: 'Segoe UI', sans-serif; }
.stTextInput label, .stSelectbox label, .stNumberInput label, .stTextArea label, .stRadio label { font-size: 16px !important; font-weight: 600 !important; }
.big-btn button { height: 100px !important; width: 100% !important; font-size: 20px !important; border-radius: 8px !important; }
.boss-btn button { border: 2px solid #FFD700 !important; color: #DAA520 !important; background-color: #FFFACD !important;}
.inbound-btn button { border: 2px solid #4CAF50 !important; color: #4CAF50 !important; }
.sales-create-btn button { border: 2px solid #673AB7 !important; color: #673AB7 !important; }
.sales-alloc-btn button { border: 2px solid #9C27B0 !important; color: #9C27B0 !important; }
.ship-btn button { border: 2px solid #E91E63 !important; color: #E91E63 !important; }
.query-btn button { border: 2px solid #FF9800 !important; color: #FF9800 !important; }
.production-btn button { border: 2px solid #d32f2f !important; color: #d32f2f !important; }
.machine-edit-btn button { border: 2px solid #607D8B !important; color: #607D8B !important; }
.order-card { background-color: #f0f2f6; padding: 15px; border-radius: 8px; margin-bottom: 15px; border-left: 5px solid #9C27B0; }
.boss-plan-card { background-color: #FFF8DC; padding: 15px; border-radius: 8px; margin-bottom: 15px; border-left: 5px solid #FFD700; }
.sub-alloc-card { background-color: #ffffff; padding: 10px; border-radius: 6px; margin-top: 10px; border: 1px solid #e0e0e0; }
.urgent-alert { padding: 10px; background-color: #ffebee; color: #c62828; border-radius: 5px; border: 1px solid #ef9a9a; margin-bottom: 10px; font-weight: bold; text-align: center;}
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
</style>
"""


def ensure_storage_dirs() -> None:
    os.makedirs(CONTRACT_ABS_DIR, exist_ok=True)
    os.makedirs(MACHINE_ARCHIVE_ABS_DIR, exist_ok=True)
    os.makedirs(os.path.join(BASE_DIR, ARCHIVE_DIR), exist_ok=True)
