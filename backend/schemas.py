from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field
from config import DATASPHERE_SPACE_ID

class SchemaRequest(BaseModel):
    view_names: Optional[List[str]] = Field(default_factory=list, description="List of view names (or empty to discover all in space)")
    space_id: Optional[str] = Field(None, description="Datasphere Space ID")

class QueryRequest(BaseModel):
    user_prompt: str = Field(..., description="Natural language request from user")
    user_email: Optional[str] = Field(None, description="Optional User email for DAC")
    session_id: Optional[str] = Field(None, description="Conversational session ID for multi-turn chat")
    view_name: Optional[str] = Field(None, description="Target view name (single view mode)")
    view_names: Optional[List[str]] = Field(default_factory=list, description="Target view names (or empty for Space Auto-Discovery)")
    selected_fields: Optional[List[str]] = Field(default_factory=list, description="User-selected columns to retrieve and display")
    space_id: Optional[str] = Field(None, description="Datasphere Space ID")
    raw_view_preview: bool = Field(False, description="Run a direct read-only SELECT * against the explicitly selected live view")

class KPICard(BaseModel):
    label: str
    value: Any
    formatted_value: str
    metric_type: str  # sum, avg, max, min, count, text

class KPISummary(BaseModel):
    direct_answer: str
    target_metric: Optional[str] = None
    cards: List[KPICard] = []

class QueryResponse(BaseModel):
    session_id: str
    query_blueprint: Dict[str, Any]
    retrieved_view_metadata: Dict[str, Any]
    execution_info: Dict[str, Any]
    kpi_summary: Optional[KPISummary] = None
    data: List[Dict[str, Any]]
    views_data: Optional[Dict[str, List[Dict[str, Any]]]] = None

class SaveSessionRequest(BaseModel):
    session_id: str
    title: str
    space_id: Optional[str] = DATASPHERE_SPACE_ID
    views: Optional[List[str]] = []
    messages: List[Dict[str, Any]] = []

class RenameSessionRequest(BaseModel):
    title: str

class GoldenSQLRequest(BaseModel):
    natural_query: str
    sql_query: str
    space_id: Optional[str] = DATASPHERE_SPACE_ID
    target_views: Optional[List[str]] = None
    odata_filter: Optional[str] = None
    explanation: Optional[str] = ""

class GlossaryRequest(BaseModel):
    term: str
    definition: str
    formula: Optional[str] = None
    synonyms: Optional[str] = None

class DocumentRequest(BaseModel):
    title: str
    content: str
    category: Optional[str] = "general"

class RAGSearchRequest(BaseModel):
    query: str
    space_id: Optional[str] = None
    top_k: Optional[int] = 3

class ValidationRequest(BaseModel):
    view_name: str = Field(..., description="Target Datasphere view provided directly by the user")
    user_prompt: str = Field("", description="Natural language prompt specifying the KPI/dimension/filter")
    space_id: Optional[str] = Field(None, description="Datasphere Space ID")
    selected_fields: Optional[List[str]] = Field(None, description="Selected fields to validate")
    selected_metrics: Optional[List[str]] = Field(default_factory=list, description="User-selected KPI measures to validate (multiple measures supported)")
    custom_filters: Optional[str] = Field(None, description="Explicit SQL filter if provided")
    reconciliation_dimension: Optional[str] = Field(None, description="Explicit grain dimension e.g. CompanyCode")
    key_fields: Optional[List[str]] = Field(default_factory=list, description="One or more fields forming the row identity for reconciliation")
    source_cds_view: Optional[str] = Field(None, description="User-specified raw S/4HANA CDS source view (e.g. ZI_PurchaseKPIBase)")
    cds_parameters: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description="Explicit CDS parameter values; use a flat object for one source or an entity-keyed object for multiple sources",
    )
    formula_prompt: Optional[str] = Field(None, description="User-provided prompt or formula for calculated fields at CDS level")
    calculated_field_formulas: Optional[Dict[str, str]] = Field(default_factory=dict, description="Formulas for new fields not present in source")
    session_id: Optional[str] = Field(None, description="Chat session ID")
    human_approved: Optional[bool] = Field(False, description="Explicit approval to execute the planned SQL queries")
    approved_query_hash: Optional[str] = Field(None, description="SHA-256 hash or version of the query plan that was approved")
    sql_mode: str = Field("llm", description="SQL generation mode: llm or manual")
    prompt_only: bool = Field(False, description="Generate and execute the natural-language Datasphere fact query without running S/4HANA reconciliation")
    manual_sql: Optional[str] = Field(None, description="User-entered read-only SQL; never replaced by generated SQL")
    manual_sql_target: Optional[str] = Field(None, description="Execution target for manual SQL: datasphere or s4hana")

class CDSStageSource(BaseModel):
    cds_view: str = Field(..., description="CDS view selected from the resolved Datasphere lineage")
    key_fields: List[str] = Field(default_factory=list, description="Candidate stable pagination key fields from verified source metadata")

class CDSStageRequest(BaseModel):
    view_name: str = Field(..., description="Datasphere model whose resolved lineage lists the selected CDS sources")
    space_id: Optional[str] = Field(None, description="Datasphere space containing the selected model")
    selected_sources: List[CDSStageSource] = Field(..., description="Selected lineage-matched CDS sources to stage")
    force_refresh: bool = Field(False, description="Ignore a compatible staged snapshot and fetch fresh source data")

class StagedSQLPreviewRequest(BaseModel):
    view_name: str
    space_id: Optional[str] = None
    selected_fields: List[str] = Field(default_factory=list)
    key_fields: List[str] = Field(default_factory=list)

class StagedSQLExecutionRequest(BaseModel):
    sql: str
    approved_query_hash: str
    space_id: Optional[str] = None
    approval_context: Dict[str, Any] = Field(default_factory=dict)

class StagedCsvPromptRequest(BaseModel):
    export_id: str = Field(..., description="ID of the final CDS replica CSV export")
    user_prompt: str = Field(..., description="Natural-language query to run against the final CDS replica CSV")

class ValidationResponse(BaseModel):
    status: str
    view_name: str
    space_id: str
    source_cds_view: str
    target_metric: str
    target_metrics: List[str] = Field(default_factory=list)
    reconciliation_dimension: str
    user_prompt: str
    datasphere: Dict[str, Any]
    s4hana: Dict[str, Any]
    comparison: Dict[str, Any]
    metrics_comparison: List[Dict[str, Any]] = Field(default_factory=list)
    lineage_chain: List[Dict[str, Any]] = Field(default_factory=list)
    grain_breakdown: List[Dict[str, Any]] = Field(default_factory=list)
    field_projections: Optional[Dict[str, Any]] = None
    triage: Optional[Dict[str, Any]] = None
    architecture_metadata: Dict[str, Any] = Field(default_factory=dict)
    reconstruction: Dict[str, Any] = Field(default_factory=dict)
    approval_required: Optional[bool] = Field(False, description="Indicates whether execution is waiting for human approval")
    execution_plan: Optional[Dict[str, Any]] = Field(None, description="Structured query plan preview before execution")

class ViewField(BaseModel):
    name: str
    label: Optional[str] = None
    type: Optional[str] = "cds.String"
    category: str = "dimension"
    aggregation: Optional[str] = None
    is_key: Optional[bool] = False

class ViewSource(BaseModel):
    name: str
    type: str
    lineage_path: str
    matched_cds_view: Optional[str] = None
    replicated_from_adt: Optional[bool] = None
    replication_space: Optional[str] = None
    description: Optional[str] = None
    key_fields: List[str] = []

class LineageStep(BaseModel):
    step: int
    name: str
    type: str
    space: str
    role: str
    matched_cds_view: Optional[str] = None

class ViewDetailsResponse(BaseModel):
    view_name: str
    space_id: str
    label: str
    type: str
    supports_analytical_queries: bool = False
    has_parameters: bool = False
    fields: List[ViewField] = []
    measures: List[Dict[str, Any]] = []
    dimensions: List[Dict[str, Any]] = []
    sources: List[ViewSource] = []
    lineage_chain: List[LineageStep] = []
    lineage_analysis: Dict[str, Any] = {}
    impact_analysis: Dict[str, Any] = {}
    metadata_json: Dict[str, Any] = {}
