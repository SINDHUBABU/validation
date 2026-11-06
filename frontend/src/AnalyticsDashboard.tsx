export interface FieldComparisonData {
  view_name: string;
  source_cds_view: string;
  matched_source_fields: Array<{
    name: string;
    source_col: string;
    category: string;
    type?: string;
  }>;
  new_fields: Array<{
    name: string;
    category: string;
    type?: string;
    suggested_formula?: string;
    source_object?: string;
    referenced_columns?: string[];
    transformation_category?: string;
    is_calculated?: boolean;
  }>;
  raw_cds_columns_count: number;
  raw_cds_columns: string[];
}

import React, { useState, useEffect, useMemo, useRef } from 'react';
import {
  ShieldCheck,
  Table as TableIcon,
  RefreshCw,
  Download,
  CheckCircle2,
  Layers,
  ChevronDown,
  ChevronUp,
  ChevronLeft,
  ChevronRight,
  Search,
  Copy,
  Plus,
  X,
  Link2,
  SlidersHorizontal,
  Check,
  Tag,
  Database,
  Terminal,
  Cpu,
  Zap,
  ArrowRight,
  Filter,
  Eye,
  FileCode2,
  FileJson,
  AlertCircle,
  AlertTriangle,
  GitBranch,
  Boxes,
  ExternalLink,
  Lock,
  Clock,
  Sparkles,
  CheckCheck,
  Trash2,
  Maximize2
} from 'lucide-react';

// ==============================================================================
// 1. DATA CONTRACTS & INTERFACES
// ==============================================================================

export interface ViewField {
  name: string;
  label?: string;
  type?: string;
  category: 'measure' | 'dimension' | 'unknown';
  aggregation?: string | null;
  is_key?: boolean;
}

export interface ViewSource {
  name: string;
  type: string;
  lineage_path: string;
  matched_cds_view?: string | null;
  replicated_from_adt?: boolean | null;
  replication_space?: string | null;
  space?: string | null;
  description?: string;
  key_fields: string[];
}

interface CdsStagingResult {
  source: string;
  status: string;
  table_name: string;
  columns: Array<{ name: string; type?: string }>;
  key_fields: string[];
  row_count: number;
  total_rows: number;
  batch_count: number;
  staged_at?: number;
  expires_at?: number;
  cache_age_seconds?: number;
  cache_status?: string;
  extraction_duration_seconds?: number;
  snapshot_id?: string;
  preview: Array<Record<string, unknown>>;
  issues: string[];
}

export interface LineageStep {
  step: number;
  name: string;
  type: string;
  space: string;
  role: string;
  matched_cds_view?: string;
}

export interface ViewDetails {
  view_name: string;
  space_id: string;
  label: string;
  type: string;
  supports_analytical_queries: boolean;
  has_parameters: boolean;
  fields: ViewField[];
  measures: Array<Record<string, any>>;
  dimensions: Array<Record<string, any>>;
  sources: ViewSource[];
  lineage_chain: LineageStep[];
  metadata_json: Record<string, any>;
  lineage_analysis?: { status: string; items: Array<Record<string, any>>; raw_sources: Array<Record<string, any>>; message?: string | null };
  impact_analysis?: { status: string; items: Array<Record<string, any>>; message?: string | null };
}

function getCalculatedFieldFormulas(details?: ViewDetails | null): Record<string, string> {
  const formulas: Record<string, string> = {};
  if (!details) return formulas;

  // Datasphere metadata can place a calculation on the output field, its
  // calculated-column record, a nested query definition, or a lineage node.
  // Walk the complete response and associate expressions only with an
  // explicitly named output field so unrelated SQL expressions are not used.
  const formulaKeys = new Set(['expression', 'formula', 'calculation', 'calculatedexpression', 'calculated_expression', 'calculation_expression', 'calc_expression', 'sql_expression', 'sqlexpression']);
  const nameKeys = ['name', 'field_name', 'fieldName', 'output_name', 'outputName', 'alias', 'target', 'column_name', 'columnName', 'technical_name', 'technicalName', 'field', 'column', 'member'];
  const visited = new WeakSet<object>();
  const scan = (value: unknown, parentKey = ''): void => {
    // Some connectors embed sections of the metadata as JSON text. Parse
    // those too; ordinary formula strings simply fail JSON parsing and are
    // ignored by this branch.
    if (typeof value === 'string') {
      const text = value.trim();
      if ((text.startsWith('{') && text.endsWith('}')) || (text.startsWith('[') && text.endsWith(']'))) {
        try { scan(JSON.parse(text), parentKey); } catch { /* Not a serialized metadata block. */ }
      }
      return;
    }
    if (!value || typeof value !== 'object') return;
    if (visited.has(value as object)) return;
    visited.add(value as object);
    if (Array.isArray(value)) {
      value.forEach((item) => scan(item, parentKey));
      return;
    }
    const record = value as Record<string, unknown>;
    if (/calculation|formula|expression/i.test(parentKey)) {
      Object.entries(record).forEach(([fieldName, expression]) => {
        if (typeof expression === 'string' && expression.trim() && !formulas[fieldName]) {
          formulas[fieldName] = expression.trim();
        }
      });
    }
    const outputName = nameKeys.map((key) => record[key]).find((candidate) => typeof candidate === 'string' && candidate.trim());
    if (typeof outputName === 'string') {
      const formulaEntry = Object.entries(record).find(([key, candidate]) =>
        formulaKeys.has(key.toLowerCase()) && typeof candidate === 'string' && candidate.trim(),
      );
      if (formulaEntry) {
        const fieldName = String(outputName).trim();
        if (!formulas[fieldName]) formulas[fieldName] = String(formulaEntry[1]).trim();
      }
    }
    Object.entries(record).forEach(([key, child]) => scan(child, key));
  };
  scan(details);

  // 1. From metadata_json.calculated_columns
  const metaCalcs = details.metadata_json?.calculated_columns;
  if (Array.isArray(metaCalcs)) {
    for (const item of metaCalcs) {
      if (item?.name && (item?.expression || item?.formula)) {
        formulas[item.name] = item.expression || item.formula;
      }
    }
  }

  // 2. From fields
  if (Array.isArray(details.fields)) {
    for (const field of details.fields) {
      if ((field as any)?.is_calculated && (field as any)?.calculation && field.name) {
        if (!formulas[field.name]) {
          formulas[field.name] = (field as any).calculation;
        }
      }
    }
  }

  // 3. From measures and dimensions
  if (Array.isArray(details.measures)) {
    for (const m of details.measures) {
      if (m?.is_calculated && m?.calculation && m?.name) {
        if (!formulas[m.name]) formulas[m.name] = m.calculation;
      }
    }
  }
  if (Array.isArray(details.dimensions)) {
    for (const d of details.dimensions) {
      if (d?.is_calculated && d?.calculation && d?.name) {
        if (!formulas[d.name]) formulas[d.name] = d.calculation;
      }
    }
  }

  // 4. From lineage_analysis items (upstream views field_catalog and calculated_columns)
  const items = details.lineage_analysis?.items || [];
  for (let index = items.length - 1; index >= 0; index -= 1) {
    const fields = items[index]?.field_catalog;
    if (Array.isArray(fields)) {
      for (const field of fields) {
        if (
          field?.is_calculated === true
          && typeof field.name === 'string'
          && typeof field.calculation === 'string'
          && field.calculation.trim()
        ) {
          if (!formulas[field.name]) {
            formulas[field.name] = field.calculation;
          }
        }
      }
    }
    const calcs = items[index]?.calculated_columns;
    if (Array.isArray(calcs)) {
      for (const c of calcs) {
        if (c?.name && (c.expression || c.formula)) {
          if (!formulas[c.name]) {
            formulas[c.name] = c.expression || c.formula;
          }
        }
      }
    }
  }

  return formulas;
}

export interface MetricComparisonItem {
  metric_name: string;
  aggregation: string | null;
  datasphere_total: number | null;
  formatted_datasphere: string;
  s4hana_total: number | null;
  formatted_s4hana: string;
  delta: number | null;
  variance_pct: number | null;
  is_match: boolean | null;
  relative_difference_pct?: number | null;
  status?: 'MATCH' | 'MISMATCH' | 'INDETERMINATE' | 'NOT_COMPUTED';
  completeness?: 'COMPLETE' | 'TRUNCATED' | 'UNKNOWN';
  group_status?: 'MATCH' | 'MISMATCH' | 'NOT_APPLICABLE';
  reason?: string | null;
}

export interface TransformationCoverageReport {
  overall_status: 'VERIFIED' | 'SEMANTIC_EQUIVALENCE_UNVERIFIED';
  diagnostic?: string | null;
  items?: Array<{
    transformation_id?: string;
    transformation: string;
    owner?: string;
    datasphere_path?: string;
    datasphere_expression?: unknown;
    s4hana_expression?: string | null;
    status: 'VERIFIED' | 'UNVERIFIED';
    state?: 'VERIFIED' | 'METADATA_UNAVAILABLE' | 'SUPPORTED_NOT_COMPILED' | 'UNASSESSED' | 'TRANSLATION_PENDING' | 'TRANSLATION_UNSUPPORTED';
    source_object?: string | null;
    source_fields?: string[];
    metadata_status?: 'RETRIEVED' | 'UNAVAILABLE';
    metadata_source?: string | null;
    diagnostic?: string | null;
  }>;
  [key: string]: unknown;
}

export interface ValidationResponse {
  status: 'VALIDATED' | 'DISCREPANCY_DETECTED' | 'ERROR' | 'INCOMPLETE' | 'MATCH' | 'MISMATCH' | 'INDETERMINATE' | 'NOT_RUN';
  view_name: string;
  space_id: string;
  source_cds_view: string;
  target_metric: string;
  target_metrics?: string[];
  reconciliation_dimension: string;
  user_prompt: string;
  key_fields?: string[];
  approval_required?: boolean;
  execution_plan?: any;
  datasphere: {
    view: string;
    sql: string;
    total_kpi: number | null;
    formatted_kpi: string;
    record_count: number | null;
    total_rows?: number | null;
    truncated?: boolean | null;
    latency_seconds: number;
    execution_engine: string;
    query_status?: string;
    columns?: Array<Record<string, any>>;
    queries?: Array<Record<string, any>>;
    source_results?: Array<Record<string, any>>;
    data?: Array<Record<string, any>>;
    error?: string;
  };
  s4hana: {
    cds_view: string;
    sql: string;
    total_kpi: number | null;
    formatted_kpi: string;
    record_count: number | null;
    total_rows?: number | null;
    truncated?: boolean | null;
    latency_seconds: number;
    execution_engine: string;
    query_status?: string;
    metadata_status?: string;
    columns?: Array<Record<string, any>>;
    queries?: Array<Record<string, any>>;
    source_results?: Array<Record<string, any>>;
    data?: Array<Record<string, any>>;
    error?: string;
  };
  field_projections?: {
    datasphere_view: string;
    datasphere_fields: string[];
    raw_cds_view: string;
    raw_cds_fields_count: number;
    raw_cds_fields: string[];
    projected_fields: string[];
    calculated_fields: Record<string, string>;
    filtered_out_count: number;
    filtered_out_fields: string[];
  };
  comparison: {
    target_metric: string;
    delta: number | null;
    variance_pct: number | null;
    is_match: boolean;
    summary_text: string;
    security: string;
  };
  metrics_comparison?: MetricComparisonItem[];
  lineage_chain: Array<{
    name: string;
    type: string;
    space?: string;
    role: string;
    matched_cds_view?: string;
  }>;
  grain_breakdown: Array<{
    grain_key: string;
    datasphere_kpi: number;
    s4hana_kpi: number;
    delta: number | null;
    variance_pct: number;
    datasphere_count: number;
    s4hana_count: number;
    metrics?: Record<string, {
      datasphere_kpi: number;
      s4hana_kpi: number;
      delta: number;
      variance_pct: number;
    }>;
    status: string;
  }>;
  triage?: {
    root_cause: string;
    orphan_keys_count: number;
    orphan_sample: string[];
    cardinality_status: string;
    action_recommended: string;
  };
  architecture_metadata?: Record<string, any>;
  reconstruction?: {
    status: string;
    message?: string;
    datasphere_sql?: string;
    s4hana_sql?: string;
    output_fields?: string[];
    key_fields?: string[];
    logic_summary?: string;
    assumptions?: string[];
    generation_diagnostics?: Array<{
      provider: string;
      model?: string;
      status_code?: number | null;
      message: string;
    }>;
    llm_provider?: string | null;
    llm_model?: string | null;
    unsupported_transformations?: string[];
    query_hash?: string;
    semantic_equivalence_status?: string;
    transformation_coverage?: TransformationCoverageReport;
  };
}

interface ValidationHistoryItem {
  id: string;
  timestamp: string;
  view_name: string;
  metric: string;
  dimension: string;
  status: ValidationResponse['status'];
  delta: number | null;
  response: ValidationResponse;
}

type PromptResult = { rows: Array<Record<string, unknown>>; metric: string; groupField?: string };

const normalizePromptName = (value: string) => value.replace(/[^a-z0-9]/gi, '').toLowerCase();

function parseCsvRows(csv: string): Array<Record<string, unknown>> {
  const records: string[][] = [];
  let record: string[] = [];
  let cell = '';
  let quoted = false;
  for (let index = 0; index < csv.length; index += 1) {
    const char = csv[index];
    if (quoted) {
      if (char === '"' && csv[index + 1] === '"') { cell += '"'; index += 1; }
      else if (char === '"') quoted = false;
      else cell += char;
    } else if (char === '"') quoted = true;
    else if (char === ',') { record.push(cell); cell = ''; }
    else if (char === '\n') {
      record.push(cell.replace(/\r$/, ''));
      records.push(record);
      record = [];
      cell = '';
    } else cell += char;
  }
  if (cell || record.length) { record.push(cell.replace(/\r$/, '')); records.push(record); }
  const headers = (records.shift() || []).map((header) => header.replace(/^\uFEFF/, '').trim());
  return records.filter((row) => row.some((value) => value !== '')).map((row) =>
    Object.fromEntries(headers.map((header, index) => [header, row[index] ?? ''])),
  );
}

function evaluatePromptOnRows(prompt: string, sourceRows: Array<Record<string, unknown>>): PromptResult {
  if (!sourceRows.length) throw new Error('The retrieved dataset has no rows to validate.');
  const fields = Object.keys(sourceRows[0]);
  const findField = (text: string) => {
    const normalized = normalizePromptName(text);
    return fields.find((field) => normalizePromptName(field) === normalized)
      || fields.filter((field) => normalized.includes(normalizePromptName(field)))
        .sort((left, right) => right.length - left.length)[0];
  };
  const quotedValues = Array.from(prompt.matchAll(/["']([^"']+)["']/g)).map((match) => match[1].trim());
  const quotedField = quotedValues.map(findField).find(Boolean);
  const promptName = normalizePromptName(prompt);
  const targetField = quotedField || fields
    .filter((field) => promptName.includes(normalizePromptName(field)))
    .sort((left, right) => right.length - left.length)[0];
  const aggregate = /\b(count|number\s+of|how\s+many)\b/i.test(prompt) ? 'count'
    : /\b(avg|average|mean)\b/i.test(prompt) ? 'avg'
    : /\b(min|minimum|lowest)\b/i.test(prompt) ? 'min'
    : /\b(max|maximum|highest)\b/i.test(prompt) ? 'max'
    : /\b(sum|total)\b/i.test(prompt) ? 'sum' : null;
  if (!aggregate) throw new Error('Use a prompt with count, sum, average, minimum, or maximum.');
  const groupMatch = prompt.match(/\b(?:by|group(?:ed)?\s+by)\s+["']?([A-Za-z][A-Za-z0-9_ ]*)["']?\s*$/i);
  const groupField = groupMatch ? findField(groupMatch[1].trim()) : undefined;
  const valueLiteral = quotedValues.find((value) => !findField(value));
  let filterField: string | undefined;
  let filterValue: string | undefined;
  if (valueLiteral && targetField) { filterField = targetField; filterValue = valueLiteral; }
  const explicitFilter = prompt.match(/\b(?:where|with)\s+["']?([A-Za-z][A-Za-z0-9_]*)["']?\s*(?:=|is|equals?)\s*(?:["']([^"']+)["']|([A-Za-z0-9_.-]+))/i);
  if (explicitFilter) {
    filterField = findField(explicitFilter[1]);
    filterValue = explicitFilter[2] ?? explicitFilter[3];
  }

  const valueFor = (row: Record<string, unknown>, field: string) => {
    const key = Object.keys(row).find((candidate) => candidate.toLowerCase() === field.toLowerCase());
    return key ? row[key] : undefined;
  };
  const filteredRows = sourceRows.filter((row) => {
    if (!filterField) return true;
    return String(valueFor(row, filterField) ?? '').trim().toLowerCase() === String(filterValue ?? '').trim().toLowerCase();
  });
  if (aggregate !== 'count' && !targetField) {
    throw new Error(`Could not find a field to ${aggregate} in the retrieved dataset (${fields.join(', ')}).`);
  }
  const aggregateValue = (rows: Array<Record<string, unknown>>) => {
    if (aggregate === 'count') return rows.length;
    const values = rows.map((row) => Number(valueFor(row, targetField!))).filter(Number.isFinite);
    if (!values.length) return 0;
    if (aggregate === 'sum') return values.reduce((total, value) => total + value, 0);
    if (aggregate === 'avg') return values.reduce((total, value) => total + value, 0) / values.length;
    return aggregate === 'min' ? Math.min(...values) : Math.max(...values);
  };
  const metric = aggregate === 'count' ? (targetField ? `count(${targetField})` : 'count(*)') : `${aggregate}(${targetField})`;
  if (!groupField) return { rows: [{ [metric]: aggregateValue(filteredRows) }], metric };
  const groups = new Map<string, Array<Record<string, unknown>>>();
  for (const row of filteredRows) {
    const value = String(valueFor(row, groupField) ?? '');
    groups.set(value, [...(groups.get(value) || []), row]);
  }
  return {
    rows: Array.from(groups, ([value, rows]) => ({ [groupField]: value, [metric]: aggregateValue(rows) }))
      .sort((left, right) => String(left[groupField]).localeCompare(String(right[groupField]))),
    metric,
    groupField,
  };
}

function compareRetrievedPromptResults(
  prompt: string,
  datasphereRows: Array<Record<string, unknown>>,
  cdsRows: Array<Record<string, unknown>>,
  viewName: string,
  spaceId: string,
  sourceCdsView: string,
): ValidationResponse {
  const datasphereResult = evaluatePromptOnRows(prompt, datasphereRows);
  const cdsResult = evaluatePromptOnRows(prompt, cdsRows);
  const resultFields = Array.from(new Set([...Object.keys(datasphereResult.rows[0] || {}), ...Object.keys(cdsResult.rows[0] || {})]));
  const valueKey = resultFields.find((field) => normalizePromptName(field) === normalizePromptName(datasphereResult.metric));
  const metricField = valueKey || datasphereResult.metric;
  const normalizeValue = (value: unknown) => {
    const numeric = Number(value);
    return value !== '' && value != null && Number.isFinite(numeric) ? numeric : String(value ?? '').trim().toLowerCase();
  };
  const rowsMatch = datasphereResult.rows.length === cdsResult.rows.length
    && datasphereResult.rows.every((row, index) => resultFields.every((field) =>
      normalizeValue(row[field]) === normalizeValue(cdsResult.rows[index]?.[field]),
    ));
  const dsValue = datasphereResult.rows.length === 1 ? Number(datasphereResult.rows[0][metricField]) : null;
  const cdsValue = cdsResult.rows.length === 1 ? Number(cdsResult.rows[0][metricField]) : null;
  const delta = dsValue != null && cdsValue != null ? Math.abs(dsValue - cdsValue) : null;
  const resultStatus = rowsMatch ? 'VALIDATED' : 'MISMATCH';
  const summary = `${datasphereResult.metric}: Datasphere ${datasphereResult.rows.length === 1 ? datasphereResult.rows[0][metricField] : `${datasphereResult.rows.length} grouped results`}; final CDS ${cdsResult.rows.length === 1 ? cdsResult.rows[0][metricField] : `${cdsResult.rows.length} grouped results`}. ${rowsMatch ? 'Results match.' : 'Results differ.'}`;
  return {
    status: resultStatus,
    view_name: viewName,
    space_id: spaceId,
    source_cds_view: sourceCdsView,
    target_metric: metricField,
    target_metrics: [metricField],
    reconciliation_dimension: datasphereResult.groupField || 'Prompt aggregate',
    user_prompt: prompt,
    approval_required: false,
    datasphere: {
      view: 'Retrieved Datasphere fact view', sql: 'Evaluated against retrieved rows in memory',
      total_kpi: dsValue, formatted_kpi: String(dsValue ?? 'Grouped results'),
      record_count: datasphereRows.length, total_rows: datasphereRows.length, truncated: false,
      latency_seconds: 0, execution_engine: 'Retrieved Datasphere rows (in memory)',
      query_status: 'EVALUATED_FROM_RETRIEVED_DATA', data: datasphereResult.rows, error: undefined,
    },
    s4hana: {
      cds_view: sourceCdsView, sql: 'Evaluated against final CDS CSV rows in memory',
      total_kpi: cdsValue, formatted_kpi: String(cdsValue ?? 'Grouped results'),
      record_count: cdsRows.length, total_rows: cdsRows.length, truncated: false,
      latency_seconds: 0, execution_engine: 'Final CDS CSV (in memory)',
      query_status: 'EVALUATED_FROM_RETRIEVED_DATA', data: cdsResult.rows, error: undefined,
    },
    comparison: {
      target_metric: metricField, delta, variance_pct: null, is_match: rowsMatch,
      summary_text: summary, security: 'Prompt evaluated locally against the two already retrieved datasets.',
    },
    metrics_comparison: [{
      metric_name: metricField,
      aggregation: null,
      datasphere_total: dsValue,
      formatted_datasphere: String(dsValue ?? 'Grouped results'),
      s4hana_total: cdsValue,
      formatted_s4hana: String(cdsValue ?? 'Grouped results'),
      delta,
      variance_pct: null,
      is_match: rowsMatch,
      status: rowsMatch ? 'MATCH' : 'MISMATCH',
    }],
    lineage_chain: [], grain_breakdown: [], architecture_metadata: {},
    reconstruction: { status: 'EVALUATED_FROM_RETRIEVED_DATA', logic_summary: summary },
  };
}

// ==============================================================================
// 2. MAIN COMPONENT: AnalyticsDashboard (Data Sphere Validation Agent)
// ==============================================================================

export default function AnalyticsDashboard() {
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const [workflowStep, setWorkflowStep] = useState(0);
  const [workflowRunning, setWorkflowRunning] = useState(false);
  const [workflowComplete, setWorkflowComplete] = useState(false);
  // ---------------------------------------------------------------------------
  // Workspace & Catalog State
  // ---------------------------------------------------------------------------
  const [spaceId, setSpaceId] = useState<string>("");
  const [discoveredViews, setDiscoveredViews] = useState<string[]>([]);
  const [loadingViews, setLoadingViews] = useState<boolean>(false);
  const [viewSearch, setViewSearch] = useState<string>("");
  const [spaceSource, setSpaceSource] = useState<string>("datasphere_consumption_catalog");

  // ---------------------------------------------------------------------------
  // Selected View & Details State
  // ---------------------------------------------------------------------------
  const [selectedViewName, setSelectedViewName] = useState<string | null>(null);
  const [loadingDetails, setLoadingDetails] = useState<boolean>(false);
  const [viewDetails, setViewDetails] = useState<ViewDetails | null>(null);
  const [detailsError, setDetailsError] = useState<string | null>(null);

  // ---------------------------------------------------------------------------
  // Active Workbench Tab
  // ---------------------------------------------------------------------------
  const [activeTab, setActiveTab] = useState<'validation' | 'lineage' | 'fields' | 'json'>('lineage');

  // ---------------------------------------------------------------------------
  // Field Selector States
  // ---------------------------------------------------------------------------
  const [selectedFields, setSelectedFields] = useState<string[]>([]);
  const [fieldSearch, setFieldSearch] = useState<string>("");
  const [fieldFilterTab, setFieldFilterTab] = useState<'all' | 'measures' | 'dimensions'>('all');

  // ---------------------------------------------------------------------------
  // Source Selection State
  // ---------------------------------------------------------------------------
  const [selectedSourceCds, setSelectedSourceCds] = useState<string | null>(null);
  const [selectedCdsForStaging, setSelectedCdsForStaging] = useState<string[]>([]);
  const [stagingCds, setStagingCds] = useState<boolean>(false);
  const [stagingResults, setStagingResults] = useState<CdsStagingResult[]>([]);
  const [stagingErrors, setStagingErrors] = useState<Array<{ source: string; error: string }>>([]);
  const [stagingError, setStagingError] = useState<string | null>(null);
  const [stagedSqlPreview, setStagedSqlPreview] = useState<any>(null);
  const [stagedSqlExecution, setStagedSqlExecution] = useState<any>(null);
  const [stagedSqlBusy, setStagedSqlBusy] = useState(false);
  const [replicaCsvBusy, setReplicaCsvBusy] = useState(false);
  const [replicaCsvResult, setReplicaCsvResult] = useState<any>(null);
  const [replicaCsvError, setReplicaCsvError] = useState<string | null>(null);
  const [joinedSourceBusy, setJoinedSourceBusy] = useState(false);
  const [joinedSourceResult, setJoinedSourceResult] = useState<any>(null);
  const [joinedSourceError, setJoinedSourceError] = useState<string | null>(null);
  const sourceStageRequestId = useRef(0);

  // ---------------------------------------------------------------------------
  // Lineage Step Inspection
  // ---------------------------------------------------------------------------
  const [activeLineageStep, setActiveLineageStep] = useState<LineageStep | null>(null);

  // ---------------------------------------------------------------------------
  // Metadata JSON Viewer States
  // ---------------------------------------------------------------------------
  const [jsonSearch, setJsonSearch] = useState<string>("");
  const [copiedNotification, setCopiedNotification] = useState<string | null>(null);

  // ---------------------------------------------------------------------------
  // Data Validation Execution & Results State
  // ---------------------------------------------------------------------------
  const [validating, setValidating] = useState<boolean>(false);
  const validationRequestId = useRef(0);
  const [selectedMeasures, setSelectedMeasures] = useState<string[]>([]);
  const [valTargetMetric, setValTargetMetric] = useState<string>("");
  const [valDimension, setValDimension] = useState<string>("");
  const [valCustomFilter, setValCustomFilter] = useState<string>("");
  const [cdsParametersText, setCdsParametersText] = useState<string>("");
  const [valPrompt, setValPrompt] = useState<string>("");
  const [valFormulaPrompt, setValFormulaPrompt] = useState<string>("");
  const [fieldComparison, setFieldComparison] = useState<FieldComparisonData | null>(null);
  const [loadingComparison, setLoadingComparison] = useState<boolean>(false);
  const [fieldFormulas, setFieldFormulas] = useState<Record<string, string>>({});
  const [customNewFields, setCustomNewFields] = useState<Array<{ name: string; formula: string }>>([]);
  const [activeDataPreviewTab, setActiveDataPreviewTab] = useState<'selected_fields' | 'datasphere' | 's4hana' | 's4source' | 'projections' | 'coverage'>('selected_fields');
  const [validationResult, setValidationResult] = useState<ValidationResponse | null>(null);
  const [validationError, setValidationError] = useState<string | null>(null);
  const [factDataResult, setFactDataResult] = useState<any>(null);
  const [factDataLoading, setFactDataLoading] = useState(false);
  const [factDataError, setFactDataError] = useState<string | null>(null);
  const [factDataReloadKey, setFactDataReloadKey] = useState(0);
  const [valSqlTab, setValSqlTab] = useState<'datasphere' | 's4hana'>('datasphere');
  const [sqlMode, setSqlMode] = useState<'llm' | 'manual'>('llm');
  const [manualSql, setManualSql] = useState('');
  const [manualSqlTarget, setManualSqlTarget] = useState<'datasphere' | 's4hana'>('datasphere');
  const [manualQueryHash, setManualQueryHash] = useState<string | null>(null);
  const [manualApproved, setManualApproved] = useState(false);
  const [breakdownFilter, setBreakdownFilter] = useState<'all' | 'discrepancies' | 'matches'>('all');
  const [breakdownSearch, setBreakdownSearch] = useState<string>("");
  const [validationHistory, setValidationHistory] = useState<ValidationHistoryItem[]>([]);
  const [activeTableMetric, setActiveTableMetric] = useState<string>('all');

  // Available Measures & Dimensions strictly from the selected Analytical Model
  // Available Model Fields strictly from the selected Analytical Model (Measures + Dimensions + Calculated)
  const availableModelFields = useMemo(() => {
    if (!viewDetails) return [];
    return viewDetails.fields || [];
  }, [viewDetails]);

  const validationFactView = useMemo(() => {
    const lineage = viewDetails?.lineage_chain || [];
    // Datasphere often reports the fact view simply as a `view` with the
    // relationship `analytic_model_source` (for example L3_FACT_*), rather
    // than labeling its object type `fact_model` or using an FV_ name.
    const factView = lineage.find((step) =>
      String(step.role || '').toLowerCase() === 'analytic_model_source',
    ) || lineage.find((step) =>
      /^FV[_-]/i.test(step.name) || /fact[_\s-]*model/i.test(step.type),
    ) || lineage.find((step) => /fact\s*view/i.test(step.role)) || lineage.find((step) =>
      /fact|underlying/i.test(`${step.role} ${step.type}`),
    );
    return factView?.name || '';
  }, [viewDetails]);

  const automaticKpiStatistics = useMemo(() => {
    const factRows = factDataResult?.data as Array<Record<string, unknown>> | undefined;
    const cdsRows = replicaCsvResult?.rows as Array<Record<string, unknown>> | undefined;
    if (!factRows?.length || !cdsRows?.length || !viewDetails?.fields?.length) return [];

    const readValues = (rows: Array<Record<string, unknown>>, fieldName: string) => {
      const normalized = fieldName.trim().toLowerCase();
      const key = Object.keys(rows[0] || {}).find((column) => column.trim().toLowerCase() === normalized);
      return key ? rows.map((row) => row[key]) : null;
    };
    const numericValue = (value: unknown): number | null => {
      if (value === null || value === undefined || String(value).trim() === '') return null;
      const parsed = Number(String(value).replace(/,/g, '').trim());
      return Number.isFinite(parsed) ? parsed : null;
    };
    const makeStatistics = (values: unknown[], numeric: boolean) => {
      const present = values.filter((value) => value !== null && value !== undefined && String(value).trim() !== '');
      if (numeric) {
        const numbers = present.map(numericValue).filter((value): value is number => value !== null);
        if (!numbers.length) return null;
        return {
          kind: 'numeric' as const,
          count: numbers.length,
          sum: numbers.reduce((sum, value) => sum + value, 0),
          average: numbers.reduce((sum, value) => sum + value, 0) / numbers.length,
          min: Math.min(...numbers),
          max: Math.max(...numbers),
        };
      }
      return {
        kind: 'text' as const,
        count: present.length,
        distinct: new Set(present.map((value) => String(value).trim())).size,
      };
    };
    const sameNumber = (left: number, right: number) =>
      Math.abs(left - right) <= Math.max(0.000001, Math.max(Math.abs(left), Math.abs(right)) * 0.000001);

    const projectedFields = viewDetails.fields.filter((field: any) =>
      field.selected !== false && field.is_selected !== false && field.projected !== false,
    );
    return projectedFields.flatMap((field: any) => {
      const datasphereValues = readValues(factRows, field.name);
      const cdsValues = readValues(cdsRows, field.name);
      if (!datasphereValues || !cdsValues) return [];
      const declaredType = [field.type, field.data_type, field.dataType, field.edm_type, field.edmType]
        .filter(Boolean).join(' ').toLowerCase();
      const isMeasure = field.category === 'measure' || field.is_measure || field.aggregation || field.measure_type;
      const declaredNumeric = /int|decimal|double|float|numeric|number|real|long|short|byte|uint|bigint/.test(declaredType);
      const declaredText = /string|char|text|date|time|bool|binary|uuid/.test(declaredType);
      const valuesAreNumeric = (values: unknown[]) => {
        const present = values.filter((value) => value !== null && value !== undefined && String(value).trim() !== '');
        return present.length > 0 && present.every((value) => numericValue(value) !== null);
      };
      // Explicit source types decide first, avoiding numeric-looking document
      // identifiers being treated as measures. Infer only when type metadata
      // is absent or inconclusive, and require both systems to agree.
      const numeric = isMeasure || declaredNumeric ||
        (!declaredText && valuesAreNumeric(datasphereValues) && valuesAreNumeric(cdsValues));
      if (!numeric) return [];
      const datasphere = makeStatistics(datasphereValues, numeric);
      const cds = makeStatistics(cdsValues, numeric);
      if (!datasphere || !cds) return [];
      const matched = datasphere.kind === 'numeric' && cds.kind === 'numeric'
        ? datasphere.count === cds.count && sameNumber(datasphere.sum, cds.sum) &&
          sameNumber(datasphere.average, cds.average) && sameNumber(datasphere.min, cds.min) &&
          sameNumber(datasphere.max, cds.max)
        : datasphere.kind === 'text' && cds.kind === 'text' &&
          datasphere.count === cds.count && datasphere.distinct === cds.distinct;
      return [{ name: field.name, type: 'Numeric field', datasphere, cds, matched }];
    });
  }, [factDataResult?.data, replicaCsvResult?.rows, viewDetails?.fields]);

  useEffect(() => {
    if (activeTab !== 'validation' || !replicaCsvResult || !validationFactView) return;
    let cancelled = false;
    setFactDataLoading(true);
    setFactDataError(null);
    setFactDataResult(null);
    fetch('/api/analytics', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        view_name: validationFactView,
        view_names: [validationFactView],
        space_id: viewDetails?.space_id || spaceId,
        user_prompt: `Retrieve all rows and fields from ${validationFactView}`,
        raw_view_preview: true,
        selected_fields: viewDetails?.fields?.map((field) => field.name) || [],
      }),
    })
      .then(async (response) => {
        const body = await response.json().catch(() => null);
        if (!response.ok) throw new Error(typeof body?.detail === 'string' ? body.detail : `HTTP ${response.status}: Datasphere fact data retrieval failed.`);
        if (body?.execution_info?.error) throw new Error(body.execution_info.error);
        if (body?.execution_info?.view_name === 'None') {
          throw new Error(body?.query_blueprint?.explanation || body?.kpi_summary?.direct_answer || 'Datasphere fact data retrieval failed.');
        }
        return body;
      })
      .then((body) => { if (!cancelled) setFactDataResult(body); })
      .catch((error: any) => { if (!cancelled) setFactDataError(error.message || 'Could not retrieve Datasphere fact data.'); })
      .finally(() => { if (!cancelled) setFactDataLoading(false); });
    return () => { cancelled = true; };
  }, [activeTab, replicaCsvResult, validationFactView, viewDetails?.space_id, viewDetails?.fields, spaceId, factDataReloadKey]);

  useEffect(() => {
    if (!workflowRunning && replicaCsvResult && !factDataLoading && (factDataResult || factDataError || !validationFactView)) {
      setWorkflowComplete(true);
    }
  }, [workflowRunning, replicaCsvResult, factDataLoading, factDataResult, factDataError, validationFactView]);

  const toggleModelField = (fieldName: string) => {
    setSelectedFields(prev =>
      prev.includes(fieldName)
        ? prev.filter(f => f !== fieldName)
        : [...prev, fieldName]
    );
  };

  const selectAllModelFields = () => {
    const allF = (viewDetails?.fields || []).map(f => f.name);
    setSelectedFields(allF);
  };

  const clearModelFields = () => {
    setSelectedFields([]);
  };

  // Backward-compatibility helpers
  const availableMeasures = useMemo(() => {
    if (!viewDetails) return [];
    return viewDetails.fields?.filter(f => f.category === 'measure') || [];
  }, [viewDetails]);

  const availableDimensions = useMemo(() => {
    if (!viewDetails) return [];
    return viewDetails.fields?.filter(f => f.category === 'dimension') || [];
  }, [viewDetails]);

  const toggleMeasure = toggleModelField;
  const selectAllModelMeasures = selectAllModelFields;
  const clearModelMeasures = clearModelFields;

  // ---------------------------------------------------------------------------
  // Initial Load: Space Info & Discovered Views Catalog
  // ---------------------------------------------------------------------------
  useEffect(() => {
    fetch('/api/info')
      .then((res) => res.json())
      .then((data) => {
        if (data.space_id) {
          setSpaceId(data.space_id);
          fetchSpaceViews(data.space_id);
        } else {
          setDetailsError('Datasphere space is not configured. Set DATASPHERE_SPACE_ID in the backend environment.');
        }
      })
      .catch((err) => {
        console.warn('Could not fetch server info:', err);
        setDetailsError('Could not read Datasphere server configuration.');
      });
  }, []);

  const fetchSpaceViews = async (targetSpace: string, forceRefresh = false) => {
    setLoadingViews(true);
    try {
      const res = await fetch(`/api/views?space_id=${encodeURIComponent(targetSpace)}${forceRefresh ? '&refresh=true' : ''}`);
      const data = await res.json();
      const views: string[] = data.views || [];
      setDiscoveredViews(views);
      if (data.source) setSpaceSource(data.source);
      if (data.space_id) setSpaceId(data.space_id);

    } catch (err) {
      console.error('Failed to load space views catalog:', err);
    } finally {
      setLoadingViews(false);
    }
  };

  const fetchCdsSourceRows = async (
    details: ViewDetails,
    sourceNames: string[],
  ): Promise<any[]> => {
    const uniqueNames = Array.from(new Set(sourceNames.filter(Boolean)));
    if (!uniqueNames.length) return [];
    const requestId = ++sourceStageRequestId.current;
    setStagingCds(true);
    setStagingError(null);
    setStagingResults([]);
    setStagingErrors([]);
    setStagedSqlPreview(null);
    setStagedSqlExecution(null);
    setReplicaCsvResult(null);
    setReplicaCsvError(null);
    setJoinedSourceResult(null);
    setJoinedSourceError(null);
    const selectedSources = (details.sources || [])
      .filter((source) => source.matched_cds_view && uniqueNames.includes(source.matched_cds_view));
    try {
      const response = await fetch('/api/cds-staging', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          view_name: details.view_name, space_id: details.space_id || spaceId,
          selected_sources: selectedSources.map((source) => ({ cds_view: source.matched_cds_view, key_fields: [] })),
          force_refresh: true,
        }),
      });
      const body = await response.json().catch(() => null);
      if (!response.ok) throw new Error(typeof body?.detail === 'string' ? body.detail : `HTTP ${response.status}: ADT source read failed.`);
      if (sourceStageRequestId.current !== requestId) return [];
      const staged = body?.staged_sources || [];
      setStagingResults(staged);
      setStagingErrors(body?.failed_sources || []);
      if (!staged.length || body?.status === 'FAILED' || body?.status === 'PARTIAL_FAILURE') {
        setStagingError('At least one CDS source could not be read completely from ADT; the replica was not built.');
        return [];
      }
      const stagedNames = new Set(staged.map((item: any) => String(item.source || '').toLowerCase()));
      if (uniqueNames.some((name) => !stagedNames.has(name.toLowerCase()))) {
        setStagingError('Not all resolved CDS sources were staged, so the joined replica was not built.');
        return [];
      }
      return staged;
    } catch (error: any) {
      if (sourceStageRequestId.current === requestId) setStagingError(error.message || 'Failed to retrieve CDS source data and build the replica.');
      return [];
    } finally {
      if (sourceStageRequestId.current === requestId) setStagingCds(false);
    }
  };

  // ---------------------------------------------------------------------------
  // Select a View & Fetch Dynamic Lineage, Metadata JSON, Sources, and Fields
  // ---------------------------------------------------------------------------
  const handleSelectView = async (viewName: string, targetSpace?: string, forceRefresh = false) => {
    setWorkflowStep(0);
    setWorkflowRunning(true);
    setWorkflowComplete(false);
    setActiveTab('lineage');
    setActiveLineageStep(null);
    sourceStageRequestId.current += 1;
    validationRequestId.current += 1;
    setValidating(false);
    setValidationResult(null);
    setValidationError(null);
    setFactDataResult(null);
    setFactDataError(null);
    setSelectedViewName(viewName);
    setStagingCds(false);
    setStagingResults([]);
    setStagingErrors([]);
    setStagingError(null);
    setSelectedCdsForStaging([]);
    setStagedSqlPreview(null);
    setStagedSqlExecution(null);
    setReplicaCsvResult(null);
    setReplicaCsvError(null);
    setLoadingDetails(true);
    setDetailsError(null);
    const space = targetSpace || spaceId;

    try {
      const res = await fetch(`/api/views/${encodeURIComponent(viewName)}/details?space_id=${encodeURIComponent(space)}${forceRefresh ? '&force_refresh=true' : ''}`);
      if (!res.ok) {
        throw new Error(`HTTP ${res.status}: Failed to load details for ${viewName}`);
      }
      const data: ViewDetails = await res.json();
      setViewDetails(data);
      setFieldComparison(null);
      setFieldFormulas(getCalculatedFieldFormulas(data));
      setCustomNewFields([]);

      // Initialize selected fields with all available fields
      const allFieldNames = data.fields?.map(f => f.name) || [];
      setSelectedFields(allFieldNames);

      // Initialize all measures for this analytical model by default
      const modelMeasures = (data.measures && data.measures.length > 0)
        ? data.measures.map((m: any) => m.name)
        : (data.fields?.filter(f => f.category === 'measure').map(f => f.name) || []);
      setSelectedMeasures(modelMeasures);
      if (modelMeasures.length > 0) {
        setValTargetMetric(modelMeasures[0]);
      }

      // Use the complete key declared by the Datasphere entity metadata or upstream source keys.
      const declaredKeys = (data.fields || []).filter((field: any) => field.is_key).map((field: any) => field.name);
      const sourceKeys = data.sources?.flatMap((s: any) => s.key_fields || []).filter(Boolean) || [];
      const fallbackKeys = declaredKeys.length > 0 ? declaredKeys : sourceKeys;
      setValDimension(fallbackKeys.join(', '));

      // Resolve model sources and metadata-driven formula suggestions.
      const primarySource = (data.sources || []).find((source) => Boolean(source.matched_cds_view));
      const resolvedCds = primarySource?.matched_cds_view || '';
      const resolvedSourceNames = Array.from(new Set(
        (data.sources || [])
          .map((source) => source.matched_cds_view)
          .filter((source): source is string => Boolean(source)),
      ));
      setSelectedSourceCds(resolvedCds || null);
      if (resolvedCds) loadFieldComparison(viewName, resolvedCds, data);

      setSelectedCdsForStaging(resolvedSourceNames);
      setJoinedSourceResult(null);
      setJoinedSourceError(null);
      setStagingError(resolvedSourceNames.length ? '' : 'No CDS sources were resolved for this Datasphere model.');
      if (!resolvedSourceNames.length) throw new Error('No CDS sources were resolved for this Datasphere model.');

      // Model selection is the only user action. Complete each validation
      // stage automatically, joining only when the model has multiple sources.
      setWorkflowStep(1);
      const staged = await fetchCdsSourceRows(data, resolvedSourceNames);
      if (staged.length !== resolvedSourceNames.length) {
        throw new Error('Could not retrieve every resolved CDS source from S/4HANA.');
      }
      setWorkflowStep(3);
      if (resolvedSourceNames.length > 1) {
        const joined = await handleJoinStagedSources(data, viewName, staged);
        if (!joined) throw new Error('Could not join the staged sources using the Datasphere model join conditions.');
      }
      setWorkflowStep(4);
      const replica = await handleCreateReplicaCsv(staged, data, viewName, true);
      if (!replica) throw new Error('Could not create the final CDS projection from the staged source data.');
      setWorkflowStep(5);
      setActiveTab('validation');

      // Default active lineage step to first node
      if (data.lineage_chain && data.lineage_chain.length > 0) {
        setActiveLineageStep(data.lineage_chain[0]);
      }
    } catch (err: any) {
      console.error('Error fetching view details:', err);
      setDetailsError(err.message || 'Failed to complete the automatic model validation workflow.');
      setWorkflowComplete(false);
    } finally {
      setWorkflowRunning(false);
      setLoadingDetails(false);
    }
  };

  // ---------------------------------------------------------------------------
  // Toggle Field Selection
  // ---------------------------------------------------------------------------
  
  // ---------------------------------------------------------------------------
  // Load Dynamic S/4HANA CDS vs Datasphere View Fields Comparison
  // ---------------------------------------------------------------------------
  const loadFieldComparison = async (
    viewName: string,
    cdsView?: string,
    details: ViewDetails | null = viewDetails,
  ) => {
    if (!viewName) return;
    setLoadingComparison(true);
    try {
      const cds = cdsView || selectedSourceCds || '';
      const targetSpace = details?.space_id || spaceId;
      const url = `/api/views/${encodeURIComponent(viewName)}/cds-fields-comparison?space_id=${encodeURIComponent(targetSpace)}${cds ? `&source_cds_view=${encodeURIComponent(cds)}` : ''}`;
      const res = await fetch(url);
      if (res.ok) {
        const responseData: FieldComparisonData = await res.json();
        const formulas = getCalculatedFieldFormulas(details);
        const data: FieldComparisonData = {
          ...responseData,
          new_fields: responseData.new_fields.map((field) => {
            const formula = formulas[field.name]
              || Object.entries(formulas).find(
                ([name]) => name.toLowerCase() === field.name.toLowerCase(),
              )?.[1];
            return {
              ...field,
              suggested_formula: formula || field.suggested_formula || '',
            };
          }),
        };
        setFieldComparison(data);
        if (data.source_cds_view && !selectedSourceCds) {
          setSelectedSourceCds(data.source_cds_view);
        }
        setFieldFormulas(prev => {
          const next = { ...prev };
          data.new_fields?.forEach(nf => {
            if (next[nf.name] === undefined && nf.suggested_formula) {
              next[nf.name] = nf.suggested_formula;
            }
          });
          return next;
        });
      }
    } catch (err) {
      console.warn('Field comparison load error:', err);
    } finally {
      setLoadingComparison(false);
    }
  };

  const toggleField = (fieldName: string) => {
    setSelectedFields(prev =>
      prev.includes(fieldName) ? prev.filter(f => f !== fieldName) : [...prev, fieldName]
    );
  };

  const selectAllFields = () => {
    if (viewDetails?.fields) {
      setSelectedFields(viewDetails.fields.map(f => f.name));
    }
  };

  const selectAllMeasures = () => {
    if (viewDetails?.fields) {
      const measures = viewDetails.fields.filter(f => f.category === 'measure').map(f => f.name);
      setSelectedFields(prev => Array.from(new Set([...prev, ...measures])));
    }
  };

  const clearSelectedFields = () => {
    setSelectedFields([]);
  };

  const handleJoinStagedSources = async (details: ViewDetails, viewName: string, stagedRows: any[]) => {
    const sourceNames = Array.from(new Set((details.sources || []).map((source) => source.matched_cds_view).filter((source): source is string => Boolean(source))));
    if (!sourceNames.length || sourceNames.some((name) => !stagedRows.some((item) => String(item.source || '').toLowerCase() === name.toLowerCase()))) return;
    setJoinedSourceBusy(true);
    setJoinedSourceError(null);
    setJoinedSourceResult(null);
    try {
      const response = await fetch('/api/cds-staging/joined-csv', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          space_id: details.space_id || spaceId,
          view_name: viewName,
          source_names: sourceNames,
          projection_fields: (details.fields || []).filter((field) => field.name),
        }),
      });
      const body = await response.json().catch(() => null);
      if (!response.ok) throw new Error(typeof body?.detail === 'string' ? body.detail : `HTTP ${response.status}: Could not join staged CDS sources.`);
      setJoinedSourceResult(body);
      return body;
    } catch (error: any) {
      setJoinedSourceError(error.message || 'Could not join the staged CDS source datasets.');
      return null;
    } finally {
      setJoinedSourceBusy(false);
    }
  };

  const handleCreateReplicaCsv = async (
    stagedOverride?: any[],
    detailsOverride: ViewDetails | null = viewDetails,
    viewNameOverride: string | null = selectedViewName,
    automatic = false,
  ) => {
    const stagedRows = stagedOverride || stagingResults;
    const details = detailsOverride;
    const stagedSourceNames = Array.from(new Set((details?.sources || []).map((source) => source.matched_cds_view).filter((source): source is string => Boolean(source))));
    if (!stagedSourceNames.length || !details
      || stagedSourceNames.some((name) => !stagedRows.some((item) => item.source.toLowerCase() === name.toLowerCase()))) return;
    setReplicaCsvBusy(true);
    setReplicaCsvError(null);
    setReplicaCsvResult(null);
    const formulas: Record<string, string> = {
      ...getCalculatedFieldFormulas(details),
      ...(automatic ? {} : fieldFormulas),
    };
    (automatic ? [] : fieldComparison?.new_fields || []).forEach((field) => {
      if (field.suggested_formula && !formulas[field.name]) formulas[field.name] = field.suggested_formula;
    });
    // Datasphere exposes measure base members with the _M_base suffix. If
    // that member omits its own expression, use its matching measure formula
    // from this model's metadata.
    (details.fields || []).forEach((field: any) => {
      const name = String(field.name || '');
      const baseName = name.replace(/_M_base$/i, '');
      if (baseName !== name && !formulas[name] && formulas[baseName]) formulas[name] = formulas[baseName];
    });
    (automatic ? [] : customNewFields).forEach((field) => {
      if (field.name.trim() && field.formula.trim()) formulas[field.name.trim()] = field.formula.trim();
    });
    const replicaProjection = Array.from(new Map(
      (details.fields || []).map((field: any) => field.name)
        .filter(Boolean)
        .map((name: string) => [name.toLowerCase(), name]),
    ).values());
    try {
      const response = await fetch('/api/cds-staging/replica-csv', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          space_id: details.space_id || spaceId,
          view_name: viewNameOverride,
          source_name: selectedSourceCds,
          source_names: stagedSourceNames,
          // The selected model's final projection is authoritative; the
          // backend resolves its Datasphere joins and calculations from metadata.
          selected_fields: replicaProjection,
          calculated_fields: formulas,
        }),
      });
      const body = await response.json().catch(() => null);
      if (!response.ok) throw new Error(typeof body?.detail === 'string' ? body.detail : `HTTP ${response.status}: CSV creation failed.`);
      let finalRows = Array.isArray(body?.preview) ? body.preview : [];
      try {
        if (body?.download_url) {
          const csvResponse = await fetch(body.download_url);
          if (csvResponse.ok) {
            const csvRows = parseCsvRows(await csvResponse.text());
            if (csvRows.length === Number(body.row_count)) finalRows = csvRows;
          }
        }
      } catch (error) {
        console.warn('Could not load the complete final CDS CSV for prompt validation:', error);
      }
      const result = {
        ...body,
        rows: finalRows,
        rows_complete: finalRows.length === Number(body?.row_count),
      };
      setReplicaCsvResult(result);
      return result;
    } catch (error: any) {
      setReplicaCsvError(error.message || 'Could not create the CDS replica CSV.');
      return null;
    } finally {
      setReplicaCsvBusy(false);
    }
  };

  const handlePreviewStagedSql = async () => {
    if (!selectedViewName || !viewDetails || stagedSqlBusy) return;
    setStagedSqlBusy(true);
    setStagedSqlExecution(null);
    try {
      const keyFields = valDimension.split(',').map((item) => item.trim()).filter(Boolean);
      const response = await fetch('/api/cds-staging/sql-preview', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          view_name: selectedViewName, space_id: viewDetails.space_id || spaceId,
          selected_fields: selectedFields, key_fields: keyFields,
        }),
      });
      const body = await response.json().catch(() => null);
      if (!response.ok) throw new Error(typeof body?.detail === 'string' ? body.detail : JSON.stringify(body?.detail || body));
      setStagedSqlPreview(body);
    } catch (error: any) {
      setStagedSqlPreview({ status: 'FAILED', message: error.message || 'Could not generate local reconstruction SQL.' });
    } finally { setStagedSqlBusy(false); }
  };

  const handleExecuteStagedSql = async () => {
    if (!stagedSqlPreview?.sql || !stagedSqlPreview?.approved_query_hash || stagedSqlBusy) return;
    setStagedSqlBusy(true);
    try {
      const response = await fetch('/api/cds-staging/sql-execute', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql: stagedSqlPreview.sql, approved_query_hash: stagedSqlPreview.approved_query_hash,
          space_id: viewDetails?.space_id || spaceId, approval_context: stagedSqlPreview.approval_context || {} }),
      });
      const body = await response.json().catch(() => null);
      if (!response.ok) throw new Error(typeof body?.detail === 'string' ? body.detail : JSON.stringify(body?.detail || body));
      setStagedSqlExecution(body);
    } catch (error: any) {
      setStagedSqlExecution({ status: 'FAILED', error: error.message || 'Local SQL execution failed.' });
    } finally { setStagedSqlBusy(false); }
  };

  // ---------------------------------------------------------------------------
  // Execute Live Data Validation (Datasphere vs S/4HANA CDS)
  // ---------------------------------------------------------------------------
  const handleExecuteValidation = async (approved: boolean = false, queryHash?: string) => {
    if (!selectedViewName || validating) return;
    if (sqlMode === 'llm' && !valPrompt.trim()) {
      setValidationError('Enter a validation prompt to generate the Datasphere fact query.');
      return;
    }
    if (!replicaCsvResult) {
      setValidationError('Create and review the final CDS replica CSV before starting validation.');
      return;
    }
    const requestId = ++validationRequestId.current;
    setValidating(true);
    setValidationError(null);

    const combinedFormulas = {
      ...getCalculatedFieldFormulas(viewDetails),
      ...fieldFormulas,
    };
    customNewFields.forEach(cf => {
      if (cf.name.trim() && cf.formula.trim()) {
        combinedFormulas[cf.name.trim()] = cf.formula.trim();
      }
    });

    const requestedActiveFields = (selectedFields && selectedFields.length > 0)
      ? selectedFields
      : (viewDetails?.fields?.map(f => f.name) || ['Product', 'GrossWeight', 'MaterialVolume', 'weight']);
    const replicaColumns = new Set((replicaCsvResult?.columns || []).map((name: string) => name.toLowerCase()));
    const activeFields = requestedActiveFields.filter((name) => replicaColumns.has(String(name).toLowerCase()));
    if (!activeFields.length) {
      setValidationError('No selected Datasphere fields are available in the final CDS projection. Create the projection with at least one shared field before calculating statistics.');
      setValidating(false);
      return;
    }

    const declaredKeys = (viewDetails?.fields || []).filter((f: any) => f.is_key).map((f: any) => f.name);
    const sourceKeys = viewDetails?.sources?.flatMap((s: any) => s.key_fields || []).filter(Boolean) || [];
    const requestedKeys = valDimension
      ? valDimension.split(',').map((field) => field.trim()).filter(Boolean)
      : (declaredKeys.length > 0 ? declaredKeys : sourceKeys);
    const activeKeys = requestedKeys.filter((name) => replicaColumns.has(String(name).toLowerCase()));

    let cdsParameters: Record<string, unknown> = {};
    if (cdsParametersText.trim()) {
      try {
        const parsed = JSON.parse(cdsParametersText);
        if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
          throw new Error('Expected a JSON object.');
        }
        cdsParameters = parsed as Record<string, unknown>;
      } catch (error: any) {
        setValidationError(`Invalid CDS parameter JSON: ${error.message || 'expected a JSON object'}`);
        setValidating(false);
        return;
      }
    }

    const payload = {
      view_name: selectedViewName,
      user_prompt: valPrompt.trim(),
      space_id: spaceId,
      source_cds_view: selectedSourceCds || undefined,
      cds_parameters: cdsParameters,
      formula_prompt: valFormulaPrompt || undefined,
      calculated_field_formulas: combinedFormulas,
      selected_metrics: activeFields,
      selected_fields: activeFields,
      reconciliation_dimension: activeKeys.length > 0 ? activeKeys.join(', ') : undefined,
      key_fields: activeKeys,
      custom_filters: valCustomFilter || undefined,
      human_approved: approved,
      approved_query_hash: queryHash,
      sql_mode: sqlMode,
      // LLM mode generates the paired Datasphere and S/4HANA reconciliation plan.
      // Keep prompt-only execution out of the normal validation flow so both
      // systems are planned against the same selected fields and key grain.
      prompt_only: false,
      manual_sql: sqlMode === 'manual' ? manualSql : undefined,
      manual_sql_target: sqlMode === 'manual' ? manualSqlTarget : undefined,
    };

    try {
      const response = await fetch('/api/validate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });

      if (!response.ok) {
        const errorBody = await response.json().catch(() => null);
        const detail = errorBody?.detail;
        throw new Error(typeof detail === 'string' ? detail : `HTTP ${response.status}: ${response.statusText}`);
      }

      let valData: ValidationResponse = await response.json();
      if (valData.approval_required && !approved) {
        const generatedQueryHash = valData.execution_plan?.query_hash || valData.reconstruction?.query_hash;
        if (!generatedQueryHash) throw new Error('The generated query is awaiting approval, but no query hash was returned.');
        const executionResponse = await fetch('/api/validate', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ ...payload, human_approved: true, approved_query_hash: generatedQueryHash }),
        });
        if (!executionResponse.ok) {
          const errorBody = await executionResponse.json().catch(() => null);
          const detail = errorBody?.detail;
          throw new Error(typeof detail === 'string' ? detail : `HTTP ${executionResponse.status}: ${executionResponse.statusText}`);
        }
        valData = await executionResponse.json();
      }
      valData.key_fields = activeKeys;
      const exportId = replicaCsvResult.export_id || String(replicaCsvResult.download_url || '').match(/\/exports\/([a-f0-9]{32})(?:\?|$)/i)?.[1];
      if (sqlMode === 'llm') {
        if (!exportId) throw new Error('The final CDS CSV export ID is missing. Recreate the final CDS replica CSV, then run validation again.');
        const cdsResponse = await fetch('/api/cds-staging/csv-prompt-query', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ export_id: exportId, user_prompt: valPrompt.trim() }),
        });
        const cdsQuery = await cdsResponse.json().catch(() => null);
        if (!cdsResponse.ok) {
          throw new Error(typeof cdsQuery?.detail === 'string' ? cdsQuery.detail : 'Could not query the final CDS replica CSV.');
        }
        const readTotal = (rows: Array<Record<string, unknown>>): number => {
          const first = rows[0] || {};
          const key = Object.keys(first).find((name) => name.toLowerCase() === 'total_count');
          return key ? Number(first[key] || 0) : rows.length;
        };
        const datasphereRows = (valData.datasphere?.data || []) as Array<Record<string, unknown>>;
        const datasphereTotal = readTotal(datasphereRows);
        const cdsTotal = Number(cdsQuery.total_count ?? readTotal(cdsQuery.data || []));
        const delta = Math.abs(datasphereTotal - cdsTotal);
        const isMatch = delta === 0;
        valData = {
          ...valData,
          status: isMatch ? 'MATCH' : 'MISMATCH',
          target_metric: 'TOTAL_COUNT',
          target_metrics: ['TOTAL_COUNT'],
          datasphere: {
            ...valData.datasphere,
            total_kpi: datasphereTotal,
            formatted_kpi: String(datasphereTotal),
          },
          s4hana: {
            ...valData.s4hana,
            sql: cdsQuery.sql,
            data: cdsQuery.data || [],
            columns: cdsQuery.columns || [],
            record_count: cdsQuery.record_count || 0,
            total_rows: cdsQuery.record_count || 0,
            total_kpi: cdsTotal,
            formatted_kpi: String(cdsTotal),
            execution_engine: cdsQuery.execution_engine,
            query_status: cdsQuery.record_count ? 'SUCCEEDED_WITH_ROWS' : 'SUCCEEDED_ZERO_ROWS',
            error: undefined,
          },
          comparison: {
            ...valData.comparison,
            target_metric: 'TOTAL_COUNT',
            delta,
            variance_pct: cdsTotal ? (delta / Math.abs(cdsTotal)) * 100 : (delta ? 100 : 0),
            is_match: isMatch,
            summary_text: `Matching rows: Datasphere ${datasphereTotal}; final CDS CSV ${cdsTotal}. ${isMatch ? 'Counts match.' : 'Counts differ.'}`,
          },
          metrics_comparison: [{
            metric_name: 'TOTAL_COUNT', aggregation: 'COUNT',
            datasphere_total: datasphereTotal, formatted_datasphere: String(datasphereTotal),
            s4hana_total: cdsTotal, formatted_s4hana: String(cdsTotal),
            delta, variance_pct: cdsTotal ? (delta / Math.abs(cdsTotal)) * 100 : (delta ? 100 : 0),
            is_match: isMatch, status: isMatch ? 'MATCH' : 'MISMATCH',
          }],
        };
      }
      if (requestId !== validationRequestId.current) return;
      setValidationResult(valData);
      if (sqlMode === 'manual') {
        setManualQueryHash(valData.execution_plan?.query_hash || null);
        setManualApproved(false);
      }

      // Save to validation history
      const historyItem: ValidationHistoryItem = {
        id: `val_${Date.now()}`,
        timestamp: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' }),
        view_name: selectedViewName,
        metric: valData.target_metrics ? valData.target_metrics.join(', ') : valData.target_metric,
        dimension: valData.reconciliation_dimension,
        status: valData.status,
        delta: valData.comparison.delta,
        response: valData
      };

      setValidationHistory(prev => [historyItem, ...prev.slice(0, 19)]);
    } catch (err: any) {
      console.error('Validation execution error:', err);
      if (requestId === validationRequestId.current) {
        setValidationError(err.message || 'Failed to complete data validation against S/4HANA CDS');
      }
    } finally {
      if (requestId === validationRequestId.current) {
        setValidating(false);
      }
    }
  };

  // ---------------------------------------------------------------------------
  // Clipboard Copy Helper
  // ---------------------------------------------------------------------------
  const copyToClipboard = (text: string, label: string) => {
    navigator.clipboard.writeText(text);
    setCopiedNotification(label);
    setTimeout(() => setCopiedNotification(null), 2500);
  };

  // ---------------------------------------------------------------------------
  // Export Grain Breakdown Table to CSV
  // ---------------------------------------------------------------------------
  const handleExportBreakdownCSV = () => {
    if (!validationResult || !validationResult.grain_breakdown) return;
    const rows = validationResult.grain_breakdown;
    const header = [
      'Grain Key',
      'Datasphere KPI',
      'S4HANA KPI',
      'Delta',
      'Variance %',
      'Datasphere Count',
      'S4HANA Count',
      'Status'
    ];
    const csvContent = [
      header.join(','),
      ...rows.map(r => [
        `"${r.grain_key}"`,
        r.datasphere_kpi,
        r.s4hana_kpi,
        r.delta,
        r.variance_pct,
        r.datasphere_count,
        r.s4hana_count,
        r.status
      ].join(','))
    ].join('\n');

    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.setAttribute('download', `${validationResult.view_name}_reconciliation_${Date.now()}.csv`);
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  // ---------------------------------------------------------------------------
  // Download Full Metadata JSON
  // ---------------------------------------------------------------------------
  const handleDownloadMetadataJson = () => {
    if (!viewDetails?.metadata_json) return;
    const str = JSON.stringify(viewDetails.metadata_json, null, 2);
    const blob = new Blob([str], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.setAttribute('download', `${viewDetails.view_name}_metadata.json`);
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  // ---------------------------------------------------------------------------
  // Filtered Discovered Views
  // ---------------------------------------------------------------------------
  const filteredDiscoveredViews = useMemo(() => {
    if (!viewSearch.trim()) return discoveredViews;
    const term = viewSearch.toLowerCase();
    return discoveredViews.filter(v => v.toLowerCase().includes(term));
  }, [discoveredViews, viewSearch]);

  // ---------------------------------------------------------------------------
  // Filtered View Fields
  // ---------------------------------------------------------------------------
  const filteredFields = useMemo(() => {
    if (!viewDetails?.fields) return [];
    let list = viewDetails.fields;
    if (fieldFilterTab !== 'all') {
      list = list.filter(f => f.category === fieldFilterTab.slice(0, -1));
    }
    if (fieldSearch.trim()) {
      const term = fieldSearch.toLowerCase();
      list = list.filter(f => f.name.toLowerCase().includes(term) || (f.label && f.label.toLowerCase().includes(term)));
    }
    return list;
  }, [viewDetails?.fields, fieldFilterTab, fieldSearch]);

  // ---------------------------------------------------------------------------
  // Filtered Grain Breakdown Rows
  // ---------------------------------------------------------------------------
  const filteredBreakdownRows = useMemo(() => {
    if (!validationResult?.grain_breakdown) return [];
    let list = validationResult.grain_breakdown;
    if (breakdownFilter === 'discrepancies') {
      list = list.filter(r => r.status !== 'MATCH');
    } else if (breakdownFilter === 'matches') {
      list = list.filter(r => r.status === 'MATCH');
    }
    if (breakdownSearch.trim()) {
      const term = breakdownSearch.toLowerCase();
      list = list.filter(r => r.grain_key.toLowerCase().includes(term));
    }
    return list;
  }, [validationResult?.grain_breakdown, breakdownFilter, breakdownSearch]);

  const keyedDataComparison = useMemo(() => {
    const datasphereRows = validationResult?.datasphere.data || [];
    const cdsRows = validationResult?.s4hana.data || [];
    const declaredKeys = (viewDetails?.fields || []).filter((field: any) => field.is_key).map((field: any) => field.name);
    const sourceKeys = viewDetails?.sources?.flatMap((source: any) => source.key_fields || []).filter(Boolean) || [];
    const keyFields = validationResult?.key_fields?.length
      ? validationResult.key_fields
      : valDimension.split(',').map((field) => field.trim()).filter(Boolean).length
        ? valDimension.split(',').map((field) => field.trim()).filter(Boolean)
        : declaredKeys.length ? declaredKeys : sourceKeys;
    const sourceFieldMap = new Map(
      (fieldComparison?.matched_source_fields || []).map((field) => [field.name.toLowerCase(), field.source_col]),
    );
    if (!keyFields.length) return { keyFields: [], fieldPairs: [], rows: [], error: 'No Datasphere key fields were selected or reported.' };
    if (!datasphereRows.length || !cdsRows.length) return { keyFields, fieldPairs: [], rows: [], error: 'Both Datasphere and S/4HANA results are required for a key-based merge.' };

    const findColumn = (row: Record<string, any>, name: string) =>
      Object.keys(row).find((column) => column.toLowerCase() === name.toLowerCase());
    const keyPairs = keyFields.map((field) => {
      const datasphereColumn = findColumn(datasphereRows[0], field);
      const mappedCdsName = sourceFieldMap.get(field.toLowerCase()) || field;
      const cdsColumn = findColumn(cdsRows[0], mappedCdsName);
      return { field, datasphereColumn, cdsColumn };
    });
    const missingKey = keyPairs.find((key) => !key.datasphereColumn || !key.cdsColumn);
    if (missingKey) {
      return {
        keyFields,
        fieldPairs: [],
        rows: [],
        error: `Key field “${missingKey.field}” is not present in both result sets, so records cannot be aligned safely.`,
      };
    }

    const fieldPairs = Object.keys(datasphereRows[0]).flatMap((datasphereField) => {
      if (keyPairs.some((key) => key.datasphereColumn?.toLowerCase() === datasphereField.toLowerCase())) return [];
      const mappedCdsName = sourceFieldMap.get(datasphereField.toLowerCase()) || datasphereField;
      const cdsField = findColumn(cdsRows[0], mappedCdsName);
      return cdsField ? [{ datasphereField, cdsField }] : [];
    });
    if (!fieldPairs.length) {
      return { keyFields, fieldPairs, rows: [], error: 'No comparable data fields are present in both result sets.' };
    }
    const buildKey = (row: Record<string, any>, side: 'datasphere' | 'cds') => {
      const values = keyPairs.map((key) => row[(side === 'datasphere' ? key.datasphereColumn : key.cdsColumn)!]);
      if (values.some((value) => value === undefined || value === null || String(value).trim() === '')) return null;
      return JSON.stringify(values.map((value) => String(value).trim()));
    };
    const datasphereGroups = new Map<string, Record<string, any>[]>();
    const cdsGroups = new Map<string, Record<string, any>[]>();
    const invalidDatasphereRows: Record<string, any>[] = [];
    const invalidCdsRows: Record<string, any>[] = [];
    datasphereRows.forEach((row) => {
      const key = buildKey(row, 'datasphere');
      if (!key) { invalidDatasphereRows.push(row); return; }
      const group = datasphereGroups.get(key) || [];
      group.push(row);
      datasphereGroups.set(key, group);
    });
    cdsRows.forEach((row) => {
      const key = buildKey(row, 'cds');
      if (!key) { invalidCdsRows.push(row); return; }
      const group = cdsGroups.get(key) || [];
      group.push(row);
      cdsGroups.set(key, group);
    });

    const allKeys = new Set([...datasphereGroups.keys(), ...cdsGroups.keys()]);
    const rows: Array<{
      key: string; keyValues: unknown[]; datasphereRecords: Array<Record<string, any>>;
      cdsRecords: Array<Record<string, any>>; datasphere: Record<string, any> | null;
      cds: Record<string, any> | null; datasphereCount: number; cdsCount: number; status: string;
    }> = Array.from(allKeys, (key) => {
      const dsRecords = datasphereGroups.get(key) || [];
      const cdsRecords = cdsGroups.get(key) || [];
      const duplicateKey = dsRecords.length > 1 || cdsRecords.length > 1;
      const hasBoth = dsRecords.length > 0 && cdsRecords.length > 0;
      const valuesMatch = hasBoth && !duplicateKey && fieldPairs.every(({ datasphereField, cdsField }) =>
        (() => {
          const normalize = (value: unknown) => {
            if (value == null) return '';
            const text = String(value).trim();
            if (!text) return '';
            const numeric = Number(text);
            if (Number.isFinite(numeric) && !(typeof value === 'string' && /^0\d+$/.test(text))) return `number:${numeric}`;
            return `text:${text.toLowerCase()}`;
          };
          return normalize(dsRecords[0][datasphereField]) === normalize(cdsRecords[0][cdsField]);
        })(),
      );
      return {
        key,
        keyValues: keyPairs.map((pair) => dsRecords[0]?.[pair.datasphereColumn!] ?? cdsRecords[0]?.[pair.cdsColumn!]),
        datasphereRecords: dsRecords,
        cdsRecords,
        datasphere: dsRecords[0] || null,
        cds: cdsRecords[0] || null,
        datasphereCount: dsRecords.length,
        cdsCount: cdsRecords.length,
        status: duplicateKey ? 'DUPLICATE KEY' : !dsRecords.length ? 'CDS ONLY' : !cdsRecords.length ? 'DATASPHERE ONLY' : valuesMatch ? 'MATCH' : 'DIFFERENT',
      };
    });
    invalidDatasphereRows.forEach((row, index) => rows.push({
      key: `datasphere-invalid-${index}`, keyValues: keyPairs.map((pair) => row[pair.datasphereColumn!]),
      datasphereRecords: [row], cdsRecords: [], datasphere: row, cds: null, datasphereCount: 1, cdsCount: 0, status: 'MISSING KEY',
    }));
    invalidCdsRows.forEach((row, index) => rows.push({
      key: `cds-invalid-${index}`, keyValues: keyPairs.map((pair) => row[pair.cdsColumn!]),
      datasphereRecords: [], cdsRecords: [row], datasphere: null, cds: row, datasphereCount: 0, cdsCount: 1, status: 'MISSING KEY',
    }));
    return { keyFields, fieldPairs, rows, error: null };
  }, [validationResult, valDimension, viewDetails?.fields, viewDetails?.sources, fieldComparison?.matched_source_fields]);

  // Format values nicely (handles numbers, text keys like Product codes, etc.)
  const formatNum = (val: any) => {
    if (val === undefined || val === null || val === '') return '-';
    if (typeof val === 'string' && (val.startsWith('000') || isNaN(Number(val)))) {
      return val.trim();
    }
    const num = Number(val);
    if (!isNaN(num)) {
      return num.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    }
    return String(val).trim();
  };

  return (
    <div className="flex h-screen w-screen overflow-hidden bg-[#070b14] text-slate-100 font-sans select-none">
      
      {/* ===================================================================== */}
      {/* LEFT SIDEBAR: DATASPHERE VIEWS CATALOG */}
      {/* ===================================================================== */}
      <aside className={`${sidebarCollapsed ? 'w-[72px]' : 'w-64 xl:w-72'} border-r border-slate-800/80 bg-[#0c1222] flex flex-col shrink-0 z-30 transition-[width] duration-200`}>
        
        {/* Brand Header */}
        <div className={`${sidebarCollapsed ? 'px-2 py-2 flex-col gap-2' : 'px-3 py-1'} border-b border-slate-200 bg-white flex items-center justify-between`}>
          <div className="flex min-w-0 items-center gap-2">
            <img
              src="/yash-logo-new.jpg"
              alt="Yash Technologies — More than what you think"
              className={`brand-logo block object-contain object-left ${sidebarCollapsed ? 'brand-logo-collapsed' : ''}`}
            />
            {!sidebarCollapsed && <span className="w-32 text-[13px] font-semibold leading-tight tracking-tight text-slate-800">Data Reconciliation Agent</span>}
          </div>
          <div className={`flex ${sidebarCollapsed ? 'flex-row' : 'flex-col'} items-center gap-1`}>
            <button
              onClick={() => fetchSpaceViews(spaceId, true)}
              className="p-1.5 rounded-lg text-slate-400 hover:text-slate-700 hover:bg-slate-100 transition-colors cursor-pointer"
              title="Refresh Views Catalog"
              aria-label="Refresh views catalog"
            >
              <RefreshCw className={`w-3.5 h-3.5 ${loadingViews ? 'animate-spin text-blue-400' : ''}`} />
            </button>
            <button
              onClick={() => setSidebarCollapsed((collapsed) => !collapsed)}
              className="p-1.5 rounded-lg text-slate-400 hover:text-slate-700 hover:bg-slate-100 transition-colors cursor-pointer"
              title={sidebarCollapsed ? 'Open analytical models' : 'Close analytical models'}
              aria-label={sidebarCollapsed ? 'Open analytical models sidebar' : 'Close analytical models sidebar'}
            >
              {sidebarCollapsed ? <ChevronRight className="w-4 h-4" /> : <ChevronLeft className="w-4 h-4" />}
            </button>
          </div>
        </div>

        {!sidebarCollapsed && <>
        {/* Space Indicator */}
        <div className="px-4 py-2.5 bg-slate-900/60 border-b border-slate-800/60 flex items-center justify-between text-xs">
          <div className="flex items-center gap-2">
            <Database className="w-3.5 h-3.5 text-blue-400" />
            <span className="text-slate-400">Space:</span>
            <span className="font-mono font-bold text-white bg-blue-950/60 px-2 py-0.5 rounded border border-blue-500/30 text-[11px]">
              {spaceId || 'Loading...'}
            </span>
          </div>
          <div className="flex items-center gap-1.5">
            <span className="text-[10px] font-mono text-purple-400 bg-purple-950/60 px-2 py-0.5 rounded border border-purple-500/30">
              {discoveredViews.length} Models
            </span>
          </div>
        </div>

        {/* Search Views Input */}
        <div className="p-3 border-b border-slate-800/60">
          <div className="relative">
            <Search className="w-3.5 h-3.5 text-slate-400 absolute left-3 top-2.5" />
            <input
              type="text"
              value={viewSearch}
              onChange={(e) => setViewSearch(e.target.value)}
              className="w-full bg-slate-950/80 border border-slate-800 rounded-lg pl-8 pr-3 py-1.5 text-xs text-slate-200 placeholder:text-slate-500 focus:outline-none focus:border-purple-500 font-mono transition-colors"
            />
            {viewSearch && (
              <button
                onClick={() => setViewSearch("")}
                className="absolute right-2.5 top-2.5 text-slate-500 hover:text-slate-300 cursor-pointer"
              >
                <X className="w-3 h-3" />
              </button>
            )}
          </div>
        </div>

        {/* Views List */}
        <div className="flex-1 overflow-y-auto p-2 space-y-1 scrollbar-thin">
          {loadingViews ? (
            <div className="p-6 text-center space-y-3">
              <RefreshCw className="w-6 h-6 animate-spin text-purple-500 mx-auto" />
              <div className="text-xs text-slate-400">Discovering Analytical Models in {spaceId}...</div>
            </div>
          ) : filteredDiscoveredViews.length === 0 ? (
            <div className="p-6 text-center space-y-2 text-slate-500 text-xs">
              <AlertCircle className="w-5 h-5 mx-auto text-slate-600" />
              <div>No Analytical Models found matching "{viewSearch}"</div>
            </div>
          ) : (
            filteredDiscoveredViews.map((viewName) => {
              const isSelected = selectedViewName === viewName;
              const isAM = viewName.startsWith('AM_');
              const isFV = viewName.startsWith('FV_');
              const isGV = viewName.startsWith('GV_') || viewName.startsWith('CV_');
              const badgeType = isAM ? 'Analytic Model' : isFV ? 'Fact Model' : isGV ? 'Graphical View' : 'View';
              const badgeColor = isAM
                ? 'bg-purple-950/60 text-purple-300 border-purple-500/40'
                : isFV
                ? 'bg-amber-950/60 text-amber-300 border-amber-500/40'
                : 'bg-blue-950/60 text-blue-300 border-blue-500/40';

              return (
                <div
                  key={viewName}
                  onClick={() => handleSelectView(viewName)}
                  className={`group relative p-2 rounded-xl border text-xs cursor-pointer transition-all flex flex-col gap-1 ${
                    isSelected
                      ? 'bg-blue-600/15 border-blue-500/60 text-white shadow-lg shadow-blue-500/10 ring-1 ring-blue-500/30'
                      : 'bg-slate-900/40 border-slate-800/80 text-slate-300 hover:bg-slate-800/60 hover:text-white hover:border-slate-700'
                  }`}
                >
                  <div className="flex items-center justify-between gap-2">
                    <div className="flex items-center gap-2 truncate flex-1 min-w-0">
                      <TableIcon className={`w-3.5 h-3.5 shrink-0 ${isSelected ? 'text-blue-400' : 'text-slate-500 group-hover:text-slate-400'}`} />
                      <span className="font-mono font-semibold truncate text-xs" title={viewName}>
                        {viewName}
                      </span>
                    </div>
                    {isSelected && (
                      <span className="w-2 h-2 rounded-full bg-blue-400 shrink-0 shadow-sm shadow-blue-400"></span>
                    )}
                  </div>

                  <div className="flex items-center justify-between text-[10px] font-mono">
                    <span className={`px-1.5 py-0.2 rounded border text-[9px] font-semibold ${badgeColor}`}>
                      {badgeType}
                    </span>
                  </div>
                </div>
              );
            })
          )}
        </div>

        {/* Sidebar Footer Security Compliance */}
        <div className="p-3 border-t border-slate-800/80 bg-slate-950/80 space-y-1 shrink-0">
          <div className="flex items-center gap-2 text-[11px] font-medium text-emerald-400">
            <ShieldCheck className="w-3.5 h-3.5 shrink-0" />
            <span>Zero Data Leakage Architecture</span>
          </div>
          <div className="flex items-center gap-2 text-[10px] text-slate-400 font-mono">
            <Lock className="w-3 h-3 shrink-0 text-blue-400" />
            <span>Direct HANA Cloud & S/4HANA ADT</span>
          </div>
        </div>
        </>}

      </aside>

      {/* ===================================================================== */}
      {/* MAIN WORKBENCH: SELECTED VIEW DETAILS & DATA VALIDATION */}
      {/* ===================================================================== */}
      <main className="min-w-0 flex-1 flex flex-col h-full overflow-hidden bg-[#0a0f1d] text-slate-100 relative">
        
        {/* Top Notification Toast */}
        {copiedNotification && (
          <div className="absolute top-4 right-6 z-50 bg-emerald-500/90 text-slate-950 px-3.5 py-1.5 rounded-lg text-xs font-bold shadow-xl flex items-center gap-2 animate-in fade-in slide-in-from-top-2 duration-150">
            <CheckCheck className="w-3.5 h-3.5" />
            <span>{copiedNotification} copied to clipboard!</span>
          </div>
        )}

        {/* Selected View Hero Banner */}
        {selectedViewName ? (
          <header className="border-b border-slate-800/90 bg-[#0d1424] px-6 py-4 shrink-0 shadow-md">
            <div className="flex flex-wrap items-center justify-between gap-4">
              
              {/* View Title & Badges */}
              <div className="flex items-center gap-3 min-w-0">
                <div className="w-10 h-10 rounded-xl bg-blue-600/20 border border-blue-500/40 flex items-center justify-center text-blue-400 shrink-0">
                  <TableIcon className="w-5 h-5" />
                </div>
                <div>
                  <div className="flex items-center gap-2.5">
                    <h2 title={selectedViewName} className="min-w-0 max-w-[55vw] truncate text-base font-semibold text-white tracking-tight">{selectedViewName}</h2>
                    <button
                      type="button"
                      onClick={() => void handleSelectView(selectedViewName, spaceId, true)}
                      disabled={loadingDetails}
                      title="Refresh model columns and lineage"
                      aria-label="Refresh model columns and lineage"
                      className="inline-flex h-8 w-8 shrink-0 items-center justify-center rounded-lg border border-slate-700 bg-slate-800 text-slate-300 transition hover:border-blue-400 hover:text-blue-300 disabled:cursor-wait disabled:opacity-60"
                    >
                      <RefreshCw className={`h-4 w-4 ${loadingDetails ? 'animate-spin' : ''}`} />
                    </button>
                    <button
                      onClick={() => copyToClipboard(selectedViewName, 'View name')}
                      className="text-slate-400 hover:text-white p-1 rounded hover:bg-slate-800 transition-colors"
                      title="Copy View Name"
                    >
                      <Copy className="w-3.5 h-3.5" />
                    </button>
                    {viewDetails && (
                      <span className="px-2 py-0.5 rounded-full bg-blue-950 text-blue-300 border border-blue-500/30 text-[10px] font-mono font-semibold">
                        {viewDetails.type.toUpperCase()}
                      </span>
                    )}
                  </div>
                  
                  <div className="flex items-center gap-3 text-xs text-slate-400 mt-0.5 font-mono">
                    <span>Space: <strong className="text-slate-200">{spaceId}</strong></span>
                    <span>•</span>
                    <span>Fields: <strong className="text-slate-200">{viewDetails?.fields?.length || 0}</strong></span>
                    <span>•</span>
                    <span>Measures: <strong className="text-slate-200">{viewDetails?.measures?.length || 0}</strong></span>
                    <span>•</span>
                    <span>Sources: <strong className="text-slate-200">{viewDetails?.sources?.length || 0}</strong></span>
                  </div>
                </div>
              </div>

              {/* Primary Validation Action Button */}
              <div className="flex items-center gap-2">
                <button
                  onClick={() => setActiveTab('validation')}
                  disabled={validating || !replicaCsvResult}
                  className={`px-4 py-2 rounded-xl text-xs font-bold flex items-center gap-2 cursor-pointer shadow-lg transition-all ${
                    validating
                      ? 'bg-slate-100 text-slate-400 border border-slate-200 cursor-not-allowed'
                      : 'bg-gradient-to-r from-emerald-500 to-teal-600 hover:from-emerald-400 hover:to-teal-500 text-slate-950 font-extrabold shadow-emerald-500/20 hover:scale-[1.02]'
                  }`}
                  title={sqlMode === 'manual' ? 'Validate the entered SQL against verified metadata' : 'Validate this view against underlying S/4HANA CDS view'}
                >
                  {validating ? (
                    <>
                      <RefreshCw className="w-3.5 h-3.5 animate-spin" />
                      <span>Reconciling with S/4HANA CDS...</span>
                    </>
                  ) : (
                    <>
                      <ShieldCheck className="w-4 h-4 text-slate-950" />
                      <span>{sqlMode === 'manual' ? 'Validate Manual SQL' : 'Run Data Validation'}</span>
                    </>
                  )}
                </button>
              </div>

            </div>

            {/* Studio Navigation Tabs */}
            <nav aria-label="Workspace sections" className="flex items-center gap-1 mt-4 -mb-1 border-t border-slate-800/60 pt-3 overflow-x-auto">
              <button
                onClick={() => setActiveTab('validation')}
                disabled={!replicaCsvResult}
                title={!replicaCsvResult ? 'Complete the CDS replica workflow before verification' : 'Open data verification'}
                className={`order-3 px-4 py-2 rounded-lg text-xs font-semibold flex items-center gap-2 transition-all cursor-pointer disabled:cursor-not-allowed disabled:opacity-50 ${
                  activeTab === 'validation'
                    ? 'bg-blue-600 text-white shadow-sm shadow-blue-500/30'
                    : 'text-slate-400 hover:text-slate-200 hover:bg-slate-800/60'
                }`}
              >
                <ShieldCheck className="w-3.5 h-3.5" />
                <span>Validation</span>
                {validationResult && (
                  <span className={`w-2 h-2 rounded-full ${validationResult.status === 'VALIDATED' ? 'bg-emerald-400' : 'bg-amber-400'}`}></span>
                )}
              </button>

              <button
                onClick={() => setActiveTab('lineage')}
                className={`order-1 px-4 py-2 rounded-lg text-xs font-semibold flex items-center gap-2 transition-all cursor-pointer ${
                  activeTab === 'lineage'
                    ? 'bg-blue-600 text-white shadow-sm shadow-blue-500/30'
                    : 'text-slate-400 hover:text-slate-200 hover:bg-slate-800/60'
                }`}
              >
                <GitBranch className="w-3.5 h-3.5" />
                <span>Lineage & Upstream Sources</span>
                {viewDetails?.lineage_chain && (
                    <span className="section-count text-[10px] bg-slate-900/60 px-1.5 py-0.2 rounded text-slate-300">
                    {viewDetails.lineage_chain.length}
                  </span>
                )}
              </button>

              <button
                onClick={() => setActiveTab('fields')}
                className={`order-2 px-4 py-2 rounded-lg text-xs font-semibold flex items-center gap-2 transition-all cursor-pointer ${
                  activeTab === 'fields'
                    ? 'bg-blue-600 text-white shadow-sm shadow-blue-500/30'
                    : 'text-slate-400 hover:text-slate-200 hover:bg-slate-800/60'
                }`}
              >
                <SlidersHorizontal className="w-3.5 h-3.5" />
                <span>Field Selector</span>
                <span className="section-count text-[10px] bg-blue-950 px-1.5 py-0.2 rounded text-blue-300 border border-blue-500/30">
                  {selectedFields.length}/{viewDetails?.fields?.length || 0}
                </span>
              </button>

              <button
                onClick={() => setActiveTab('json')}
                className={`ml-auto order-4 shrink-0 px-2 py-2 text-xs font-medium underline underline-offset-4 transition-colors ${
                  activeTab === 'json' ? 'text-blue-800' : 'text-slate-500 hover:text-blue-700'
                }`}
              >
                <FileJson className="w-3.5 h-3.5" />
                <span>Metadata JSON</span>
              </button>
            </nav>

          </header>
        ) : (
          <div className="flex-1 flex items-center justify-center p-8 text-center">
            <div className="max-w-md space-y-3">
              <Database className="w-12 h-12 text-blue-500/40 mx-auto" />
              <h2 className="text-lg font-bold text-white">Select a Datasphere View</h2>
              <p className="text-xs text-slate-400">
                Choose an analytical view or cube from the catalog on the left to inspect its dynamic lineage, metadata JSON, upstream sources, and validate data against SAP S/4HANA CDS views.
              </p>
            </div>
          </div>
        )}

        {/* Tab Content Container */}
        {selectedViewName && (
          <div className="flex-1 overflow-y-auto p-6 scrollbar-thin">
            {loadingDetails ? (
              <div className="p-16 text-center space-y-3">
                <RefreshCw className="w-8 h-8 animate-spin text-blue-500 mx-auto" />
                <div className="text-sm font-semibold text-slate-300">Resolving CSN lineage and probing metadata for {selectedViewName}...</div>
                <div className="text-xs text-slate-500 font-mono">Querying SAP Datasphere Catalog & Dependency Resolver</div>
              </div>
            ) : detailsError ? (
              <div className="p-6 rounded-2xl bg-rose-950/20 border border-rose-500/30 text-rose-300 space-y-2">
                <div className="flex items-center gap-2 font-bold text-sm text-rose-400">
                  <AlertCircle className="w-4 h-4" />
                  <span>Failed to Load View Details</span>
                </div>
                <p className="text-xs">{detailsError}</p>
                <button
                  onClick={() => handleSelectView(selectedViewName)}
                  className="px-3 py-1.5 rounded-lg bg-rose-900/40 hover:bg-rose-900/60 text-xs font-semibold cursor-pointer"
                >
                  Retry Inspection
                </button>
              </div>
            ) : (
              <>
                {/* TAB 1: DATA VALIDATION STUDIO (OUR MAJOR GOAL) */}
                {activeTab === 'validation' && (
                  <div id="workflow-validation" className="space-y-6 max-w-6xl mx-auto">

                    <section className="overflow-hidden rounded-2xl border border-blue-200 bg-gradient-to-br from-white via-blue-50 to-emerald-50 shadow-sm">
                      <div className="border-b border-blue-100 px-5 py-4">
                        <div className="flex flex-wrap items-center justify-between gap-2">
                          <div>
                            <h2 className="text-lg font-bold text-slate-900">Automatic validation report</h2>
                            <p className="mt-1 text-sm text-slate-600">Model selection starts the complete retrieval, projection, and comparison flow.</p>
                          </div>
                          <span className={`rounded-full px-3 py-1 text-xs font-bold ${workflowComplete ? (factDataError ? 'bg-amber-100 text-amber-800' : 'bg-emerald-100 text-emerald-800') : 'bg-blue-100 text-blue-800'}`}>
                            {workflowComplete ? (factDataError || !validationFactView ? 'Completed with fact retrieval issue' : 'Validation complete') : workflowRunning || factDataLoading ? 'In progress' : 'Preparing report'}
                          </span>
                        </div>
                      </div>
                      <div className="grid grid-cols-2 gap-2 p-4 md:grid-cols-3 xl:grid-cols-6">
                        {(selectedCdsForStaging.length > 1 ? ['Model metadata', 'Resolve sources', 'Stage CDS data', 'Join sources', 'Apply calculations', 'Final projection'] : ['Model metadata', 'Resolve source', 'Stage CDS data', 'Project source', 'Apply calculations', 'Final projection']).map((label, index) => {
                          const done = workflowComplete || index < workflowStep;
                          const current = !done && index === workflowStep && (workflowRunning || factDataLoading);
                          return <div key={label} className={`rounded-xl border px-3 py-3 text-xs font-semibold ${done ? 'border-emerald-200 bg-white text-emerald-800' : current ? 'border-blue-200 bg-white text-blue-800' : 'border-slate-200 bg-white/70 text-slate-500'}`}>
                            <div className="mb-1 text-base">{done ? '✓' : current ? '…' : index + 1}</div>{label}
                          </div>;
                        })}
                      </div>
                      <div className="grid gap-3 px-4 pb-4 sm:grid-cols-2 xl:grid-cols-4">
                        <div className="rounded-xl bg-white/90 px-4 py-3"><div className="text-xs text-slate-500">Final CDS rows</div><div className="mt-1 text-lg font-bold text-slate-900">{Number(replicaCsvResult?.row_count || 0).toLocaleString()}</div></div>
                        <div className="rounded-xl bg-white/90 px-4 py-3"><div className="text-xs text-slate-500">Fact model rows</div><div className="mt-1 text-lg font-bold text-slate-900">{factDataLoading ? 'Retrieving…' : Number(factDataResult?.data?.length || 0).toLocaleString()}</div></div>
                        <div className="rounded-xl bg-white/90 px-4 py-3"><div className="text-xs text-slate-500">Numeric KPI fields compared</div><div className="mt-1 text-lg font-bold text-slate-900">{automaticKpiStatistics.length}</div></div>
                        <div className="rounded-xl bg-white/90 px-4 py-3"><div className="text-xs text-slate-500">KPI statistics matched</div><div className="mt-1 text-lg font-bold text-slate-900">{automaticKpiStatistics.filter((item: any) => item.matched).length} / {automaticKpiStatistics.length}</div></div>
                      </div>
                    </section>

                    <div className="hidden">

                    {/* Primary source workflow: model -> fact view -> explicit ADT fetch -> CDS additions */}
                    {(() => {
                      const primaryResult = stagingResults.find((item) => item.source.toLowerCase() === (selectedSourceCds || '').toLowerCase());
                      const factView = viewDetails?.lineage_chain?.find((step) => /fact|underlying/i.test(`${step.role} ${step.type}`) || /^FV[_-]/i.test(step.name));
                      const missingSource = stagingErrors.find((item) => item.source.toLowerCase() === (selectedSourceCds || '').toLowerCase());
                      return (
                        <section className="rounded-2xl border border-blue-200 bg-white shadow-sm overflow-hidden">
                          <div className="px-5 py-4 border-b border-slate-200 flex flex-wrap items-center justify-between gap-3">
                            <div>
                              <p className="text-[11px] font-semibold uppercase tracking-wider text-blue-700">Primary source validation workflow</p>
                              <h3 className="mt-1 text-base font-bold text-slate-900">{selectedViewName || 'Select a Datasphere model'}</h3>
                            </div>
                            <button
                              type="button"
                              onClick={() => setActiveTab('lineage')}
                              disabled={!selectedSourceCds}
                              className="inline-flex items-center gap-2 rounded-lg bg-emerald-600 px-4 py-2.5 text-sm font-semibold text-white shadow-sm hover:bg-emerald-700 disabled:cursor-not-allowed disabled:bg-slate-300"
                            >
                              <GitBranch className="h-4 w-4" />
                              {primaryResult ? 'Review source and reconstruction' : 'Continue to source discovery'}
                            </button>
                          </div>
                          <div className="grid gap-0 divide-y divide-slate-200 md:grid-cols-3 md:divide-x md:divide-y-0">
                            <div className="p-4">
                              <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">1 · Datasphere model</div>
                              <div className="mt-1 font-mono text-sm font-semibold text-slate-900">{selectedViewName || 'No model selected'}</div>
                            </div>
                            <div className="p-4">
                              <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">2 · Underlying fact view</div>
                              <div className="mt-1 font-mono text-sm font-semibold text-slate-900">{factView?.name || selectedViewName || 'Not identified in lineage'}</div>
                              <div className="mt-1 text-xs text-slate-500">Validation compares this fact view to the primary CDS source.</div>
                            </div>
                            <div className="p-4">
                              <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">3 · Primary S/4HANA CDS source</div>
                              <div className="mt-1 font-mono text-sm font-semibold text-emerald-700">{selectedSourceCds || 'No primary CDS source resolved'}</div>
                              <div className="mt-1 text-xs text-slate-500">Only the selected primary source is fetched. Preview is limited to 1,000 rows.</div>
                            </div>
                          </div>
                          {missingSource && <div className="mx-4 mb-4 rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">{missingSource.error}</div>}
                          {primaryResult && (
                            <div className="mx-4 mb-4 rounded-lg border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-800">
                              ADT returned {primaryResult.preview.length.toLocaleString()} preview rows of {primaryResult.total_rows.toLocaleString()} total rows for <span className="font-mono font-semibold">{primaryResult.source}</span>.
                            </div>
                          )}
                          {primaryResult && (
                            <div className="border-t border-slate-200 bg-slate-50 px-5 py-4">
                              <h4 className="text-sm font-semibold text-slate-900">Datasphere fields to add at CDS side</h4>
                              {loadingComparison ? <p className="mt-2 text-sm text-slate-500">Comparing model fields with the primary CDS columns…</p> :
                                fieldComparison?.new_fields?.length ? (
                                  <div className="mt-3 grid gap-3 md:grid-cols-2">
                                    {fieldComparison.new_fields.map((field) => (
                                      <div key={field.name} className="rounded-xl border border-purple-200 bg-white p-3">
                                        <div className="flex flex-wrap items-center justify-between gap-2">
                                          <span className="font-mono text-sm font-semibold text-slate-900">{field.name}</span>
                                          <span className="text-[11px] text-slate-500">{field.category}{field.type ? ` · ${field.type}` : ''}</span>
                                        </div>
                                        <pre className="mt-2 overflow-x-auto whitespace-pre-wrap rounded-lg bg-slate-900 p-3 text-xs text-purple-100">{field.suggested_formula || 'Calculation not available from Datasphere metadata'}</pre>
                                        {!!field.referenced_columns?.length && <p className="mt-2 text-[11px] text-slate-500">Inputs: <span className="font-mono">{field.referenced_columns.join(', ')}</span></p>}
                                        {field.source_object && <p className="mt-1 text-[11px] text-slate-500">Defined in {field.source_object}</p>}
                                      </div>
                                    ))}
                                  </div>
                                ) : <p className="mt-2 text-sm text-slate-500">No additional calculated fields were identified for the selected primary source.</p>}
                            </div>
                          )}
                        </section>
                      );
                    })()}
                    
                    {/* Validation Configuration Card */}
                    <div className="p-5 rounded-2xl bg-[#0e1629] border border-slate-800 shadow-xl space-y-4">
                      <div className="flex flex-wrap items-center justify-between gap-2 border-b border-slate-800/80 pb-3">
                        <div className="flex items-center gap-2">
                          <ShieldCheck className="w-4 h-4 text-emerald-400" />
                          <h3 className="font-bold text-sm text-white">S/4HANA CDS Data Validation Configuration</h3>
                        </div>
                        <div className="text-[11px] font-mono text-slate-400">
                          Target Datasphere: <span className="text-white font-bold">{selectedViewName}</span>
                        </div>
                      </div>

                      {/* Unified Analytical Model Fields to Validate (No bifurcation into measures/dimensions) */}
                      <div className="space-y-2">
                        <div className="flex flex-wrap items-center justify-between gap-2 border-b border-slate-800/80 pb-2">
                          <div className="flex items-center gap-2">
                            <TableIcon className="w-4 h-4 text-purple-400" />
                            <label className="text-[11px] font-bold text-slate-200 uppercase tracking-wider font-mono">
                              Analytical Model Fields ({availableModelFields.length})
                            </label>
                            <span className="text-[10px] bg-purple-950/80 text-purple-300 border border-purple-500/40 px-2 py-0.5 rounded font-mono">
                              Selected: {selectedFields.length} / {availableModelFields.length}
                            </span>
                          </div>

                          <div className="flex items-center gap-2">
                            <button
                              type="button"
                              onClick={selectAllModelFields}
                              className="text-[11px] font-semibold text-purple-400 hover:text-purple-300 hover:underline cursor-pointer font-mono"
                            >
                              Select All Fields
                            </button>
                            <span className="text-slate-600">•</span>
                            <button
                              type="button"
                              onClick={clearModelFields}
                              className="text-[11px] font-semibold text-slate-400 hover:text-slate-200 hover:underline cursor-pointer font-mono"
                            >
                              Clear
                            </button>
                          </div>
                        </div>

                        {/* All Field Badges Grid */}
                        <div className="p-3 bg-slate-950/80 border border-slate-800 rounded-xl flex flex-wrap gap-2">
                          {availableModelFields.length === 0 ? (
                            <div className="text-xs text-slate-500 italic py-1 font-mono">
                              Loading analytical model fields...
                            </div>
                          ) : (
                            availableModelFields.map((f: any) => {
                              const isChecked = selectedFields.includes(f.name);
                              return (
                                <button
                                  key={f.name}
                                  type="button"
                                  onClick={() => toggleModelField(f.name)}
                                  className={`px-3 py-1.5 rounded-lg text-xs font-mono font-medium flex items-center gap-2 cursor-pointer transition-all border ${
                                    isChecked
                                      ? 'bg-purple-600/25 border-purple-500 text-purple-100 shadow-sm shadow-purple-500/20 ring-1 ring-purple-500/40'
                                      : 'bg-slate-900 border-slate-700/80 text-slate-400 hover:text-slate-200 hover:border-slate-600'
                                  }`}
                                >
                                  <div className={`w-3.5 h-3.5 rounded flex items-center justify-center border text-[9px] ${
                                    isChecked ? 'bg-purple-500 border-purple-400 text-white font-bold' : 'border-slate-600'
                                  }`}>
                                    {isChecked && '✓'}
                                  </div>
                                  <span className="font-semibold text-white">{f.name}</span>
                                  <span className="text-[9px] bg-slate-950 px-1.5 py-0.5 rounded text-slate-400 uppercase font-mono">
                                    {f.category || (f.aggregation ? 'Measure' : 'Dimension')}
                                  </span>
                                </button>
                              );
                            })
                          )}
                        </div>
                      </div>

                      {/* Raw S/4HANA CDS Source View Input */}
                      <div className="space-y-1.5">
                        <div className="flex items-center justify-between">
                          <label className="text-[11px] font-bold text-slate-300 uppercase tracking-wider flex items-center gap-1.5 font-mono">
                            <GitBranch className="w-3.5 h-3.5 text-emerald-400" />
                            <span>S/4HANA CDS Source Override (Optional)</span>
                          </label>
                          <div className="flex items-center gap-2">
                            {loadingComparison && (
                              <span className="text-[10px] text-cyan-400 flex items-center gap-1 animate-pulse font-mono">
                                <RefreshCw className="w-2.5 h-2.5 animate-spin" />
                                Checking S/4HANA DB...
                              </span>
                            )}
                            <span className="text-[10px] bg-emerald-950/80 text-emerald-400 px-1.5 py-0.5 rounded border border-emerald-500/30 font-mono">
                              ADT Open SQL
                            </span>
                          </div>
                        </div>
                        <div className="flex items-center gap-2">
                          <input
                            type="text"
                            placeholder="Automatically resolved from model lineage; override only if needed"
                            value={selectedSourceCds || ''}
                            onChange={(e) => setSelectedSourceCds(e.target.value)}
                            onBlur={() => {
                              if (selectedViewName && selectedSourceCds) {
                                loadFieldComparison(selectedViewName, selectedSourceCds);
                              }
                            }}
                            className="flex-1 bg-slate-950 border border-slate-700 rounded-xl px-3 py-2 text-xs text-emerald-300 font-mono focus:outline-none focus:border-emerald-500"
                          />
                          <button
                            type="button"
                            onClick={() => {
                              if (selectedViewName && selectedSourceCds) {
                                loadFieldComparison(selectedViewName, selectedSourceCds);
                              }
                            }}
                            title="Compare CDS Source Fields with Datasphere View"
                            className="px-3 py-2 rounded-xl bg-slate-900 border border-slate-700 hover:border-slate-600 text-xs text-slate-300 flex items-center gap-1.5 cursor-pointer font-mono"
                          >
                            <RefreshCw className={`w-3.5 h-3.5 ${loadingComparison ? 'animate-spin' : ''}`} />
                            <span className="hidden sm:inline">Compare</span>
                          </button>
                        </div>
                      </div>

                      {/* Dynamic Field Comparison: Source Fields in Datasphere View */}
                      {fieldComparison && fieldComparison.matched_source_fields && (
                        <div className="p-3.5 rounded-xl bg-slate-950/90 border border-slate-800 space-y-2">
                          <div className="flex flex-wrap items-center justify-between gap-2">
                            <div className="text-[11px] font-bold text-emerald-300 uppercase tracking-wider flex items-center gap-1.5 font-mono">
                              <CheckCircle2 className="w-3.5 h-3.5 text-emerald-400" />
                              <span>Source Fields in Selected Datasphere View ({fieldComparison.matched_source_fields.length})</span>
                            </div>
                            <span className="text-[10px] text-slate-400 font-mono">
                              Mapped from S/4HANA CDS: <strong className="text-emerald-400">{fieldComparison.source_cds_view}</strong> ({fieldComparison.raw_cds_columns_count} raw cols in DB)
                            </span>
                          </div>
                          
                          <div className="flex flex-wrap gap-2 pt-1">
                            {fieldComparison.matched_source_fields.length === 0 ? (
                              <span className="text-xs text-slate-500 italic">No direct column matches found with raw source view.</span>
                            ) : (
                              fieldComparison.matched_source_fields.map((mf) => (
                                <div
                                  key={mf.name}
                                  className="px-2.5 py-1 rounded-lg bg-emerald-950/40 border border-emerald-500/30 text-emerald-200 text-xs font-mono flex items-center gap-1.5"
                                >
                                  <span className="font-semibold text-white">{mf.name}</span>
                                  <span className="text-[10px] text-emerald-400 opacity-80">(S/4HANA: {mf.source_col})</span>
                                  <span className="text-[9px] bg-slate-900 px-1 py-0.5 rounded text-slate-400 uppercase">{mf.category}</span>
                                </div>
                              ))
                            )}
                          </div>
                        </div>
                      )}

                      {/* Dynamic New Fields (Not Present in the Source) -- Text Box for Calculation */}
                      <div className="p-3.5 rounded-xl bg-purple-950/20 border border-purple-500/30 space-y-3">
                        <div className="flex flex-wrap items-center justify-between gap-2 border-b border-purple-500/20 pb-2">
                          <div className="text-[11px] font-bold text-purple-200 uppercase tracking-wider flex items-center gap-1.5 font-mono">
                            <Sparkles className="w-3.5 h-3.5 text-amber-400" />
                            <span>Model Fields Not Directly Present in the Source</span>
                          </div>
                          <span className="text-[10px] text-purple-300 font-mono">
                            {fieldComparison?.new_fields?.filter((field) => field.suggested_formula).length || 0} model formulas extracted
                          </span>
                        </div>

                        {(!fieldComparison?.new_fields || fieldComparison.new_fields.length === 0) && customNewFields.length === 0 ? (
                          <div className="text-xs text-slate-400 italic py-1 font-mono">
                            All selected model fields map directly to the S/4HANA source.
                          </div>
                        ) : (
                          <div className="space-y-2.5">
                            {fieldComparison?.new_fields?.map((nf) => (
                              <div
                                key={nf.name}
                                className="p-2.5 rounded-xl bg-slate-950 border border-purple-500/40 flex flex-col md:flex-row md:items-center gap-2.5"
                              >
                                <div className="flex items-center gap-2 min-w-[260px]">
                                  <span className="font-mono text-xs font-bold text-purple-200">{nf.name}</span>
                                  <span className={`text-[10px] px-1.5 py-0.5 rounded border font-mono ${
                                    nf.suggested_formula
                                      ? 'bg-purple-950/80 text-purple-300 border-purple-500/30'
                                      : 'bg-slate-900 text-slate-400 border-slate-700'
                                  }`}>
                                    {nf.suggested_formula
                                      ? (nf.source_object ? `calculated in ${nf.source_object}` : 'calculated in model')
                                      : 'no direct source mapping'}
                                  </span>
                                </div>
                                <div className="flex-1 min-w-0 space-y-1">
                                  {nf.suggested_formula ? (
                                    <>
                                      <code className="block break-words rounded-lg bg-slate-900 px-3 py-2 text-xs text-purple-200">
                                        {nf.suggested_formula}
                                      </code>
                                      {nf.referenced_columns && nf.referenced_columns.length > 0 && (
                                        <div className="text-[10px] text-purple-400/80 font-mono flex items-center gap-1">
                                          <span>Inputs:</span>
                                          <span className="text-purple-300 font-semibold">{nf.referenced_columns.join(', ')}</span>
                                        </div>
                                      )}
                                    </>
                                  ) : (
                                    <span className="text-xs text-slate-400">
                                      No formula was declared in the retrieved model metadata. The field will be included in the architecture sent for reconstruction; it is not treated as a user-entered formula.
                                    </span>
                                  )}
                                </div>
                              </div>
                            ))}

                            {/* User Custom Added Calculated Fields */}
                            {customNewFields.map((cf, idx) => (
                              <div
                                key={idx}
                                className="p-2.5 rounded-xl bg-slate-950 border border-purple-500/40 flex flex-col md:flex-row md:items-center gap-2.5"
                              >
                                <div className="flex items-center gap-2 min-w-[260px]">
                                  <input
                                    type="text"
                                    placeholder="New field name..."
                                    value={cf.name}
                                    onChange={(e) => {
                                      const updated = [...customNewFields];
                                      updated[idx].name = e.target.value;
                                      setCustomNewFields(updated);
                                    }}
                                    className="bg-slate-900 border border-purple-500/50 rounded-lg px-2.5 py-1 text-xs text-purple-200 font-mono focus:outline-none focus:border-purple-400 w-36"
                                  />
                                  <span className="text-[10px] px-1.5 py-0.5 rounded bg-amber-950/80 text-amber-300 border border-amber-500/30 font-mono">
                                    (not present in the source)
                                  </span>
                                </div>
                                <div className="flex-1 flex items-center gap-2">
                                  <span className="text-slate-500 font-mono text-xs hidden md:inline">--</span>
                                  <input
                                    type="text"
                                    placeholder={`Calculation formula for ${cf.name || 'new field'}...`}
                                    value={cf.formula}
                                    onChange={(e) => {
                                      const updated = [...customNewFields];
                                      updated[idx].formula = e.target.value;
                                      setCustomNewFields(updated);
                                    }}
                                    className="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-1.5 text-xs text-purple-200 font-mono focus:outline-none focus:border-purple-400 placeholder:text-slate-600"
                                  />
                                  <button
                                    type="button"
                                    onClick={() => setCustomNewFields(prev => prev.filter((_, i) => i !== idx))}
                                    className="p-1.5 rounded-lg text-slate-400 hover:text-rose-400 hover:bg-rose-950/30 transition-colors"
                                    title="Remove field"
                                  >
                                    <X className="w-3.5 h-3.5" />
                                  </button>
                                </div>
                              </div>
                            ))}
                          </div>
                        )}

                        <div className="flex items-center justify-between pt-1">
                          <button
                            type="button"
                            onClick={() => setCustomNewFields(prev => [...prev, { name: '', formula: '' }])}
                            className="text-xs text-purple-400 hover:text-purple-300 flex items-center gap-1.5 cursor-pointer font-semibold"
                          >
                            <Plus className="w-3.5 h-3.5" />
                            <span>Add Extra Calculated Field</span>
                          </button>
                        </div>
                      </div>

                      <div className="space-y-1.5 pt-1">
                        <label className="text-[11px] font-bold text-slate-300 uppercase tracking-wider font-mono">
                          CDS Parameters (Optional JSON)
                        </label>
                        <input
                          type="text"
                          value={cdsParametersText}
                          onChange={(e) => setCdsParametersText(e.target.value)}
                          placeholder={'{"P_FiscalYear": "2025"} or {"ZI_Sales": {"P_Year": 2025}}'}
                          className="w-full bg-slate-950 border border-slate-700/80 rounded-xl px-3 py-2 text-xs text-emerald-200 font-mono placeholder:text-slate-500 focus:outline-none focus:border-emerald-500"
                        />
                        <p className="text-[10px] text-slate-500">
                          Only declared CDS parameters are accepted. Values are included in the approval hash; missing required values are not guessed.
                        </p>
                      </div>

                      {/* Calculated Fields / Formula Prompt Text Box */}
                      <div className="space-y-1.5 pt-1">
                        <div className="flex flex-wrap items-center justify-between gap-1">
                          <label className="text-[11px] font-bold text-slate-300 uppercase tracking-wider flex items-center gap-1.5">
                            <FileCode2 className="w-3.5 h-3.5 text-purple-400" />
                            <span>Calculated Fields & Formula Prompt (CDS Side)</span>
                          </label>
                          <span className="text-[10px] text-slate-400 font-mono">
                            e.g. Warning: OrderProcessingDays BETWEEN 35 AND 59; Critical: OrderProcessingDays &gt;= 60
                          </span>
                        </div>
                        <textarea
                          rows={2}
                          placeholder="Provide custom formula or prompt to create calculated fields at the CDS side (e.g. Warning: OrderProcessingDays BETWEEN 35 AND 59; Critical: OrderProcessingDays >= 60)..."
                          value={valFormulaPrompt}
                          onChange={(e) => setValFormulaPrompt(e.target.value)}
                          className="w-full bg-slate-950 border border-slate-700/80 rounded-xl px-3 py-2 text-xs text-slate-200 font-mono placeholder:text-slate-500 focus:outline-none focus:border-purple-500 resize-none leading-relaxed"
                        />
                      </div>

                      {/* SQL input mode and execution controls */}
                      <div className="space-y-1.5 pt-1">
                        <label className="text-[11px] font-bold text-slate-300 uppercase tracking-wider flex items-center gap-1.5">
                          <Terminal className="w-3.5 h-3.5 text-blue-400" />
                          <span>SQL Input Mode</span>
                        </label>
                        <select
                          value={sqlMode}
                          onChange={(e) => {
                            setSqlMode(e.target.value as 'llm' | 'manual');
                            setManualQueryHash(null);
                            setManualApproved(false);
                          }}
                          className="bg-slate-950 border border-slate-700/80 rounded-lg px-3 py-2 text-xs text-slate-200"
                        >
                          <option value="llm">LLM Generated SQL</option>
                          <option value="manual">Manual SQL</option>
                        </select>

                        {replicaCsvResult && (
                          <details className="w-full rounded-xl border border-blue-500/30 bg-slate-950/50 p-3 group">
                            <summary className="flex cursor-pointer list-none items-center justify-between gap-3 text-xs font-semibold text-slate-200">
                              <span>Final CDS CSV · {replicaCsvResult.row_count.toLocaleString()} rows · {replicaCsvResult.columns.length} fields</span>
                              <span className="text-blue-400 group-open:hidden">Show data</span>
                              <span className="hidden text-blue-400 group-open:inline">Hide data</span>
                            </summary>
                            <div className="mt-3 max-h-72 overflow-auto rounded-lg border border-slate-800">
                              <table className="min-w-full text-left text-[11px] font-mono">
                                <thead className="sticky top-0 bg-slate-900 text-slate-300"><tr>{replicaCsvResult.columns.map((column: string) => <th key={column} className="whitespace-nowrap px-3 py-2">{column}</th>)}</tr></thead>
                                <tbody>{replicaCsvResult.preview.map((row: Record<string, unknown>, index: number) => <tr key={index} className="border-t border-slate-800 text-slate-300">{replicaCsvResult.columns.map((column: string) => <td key={column} className="max-w-xs truncate px-3 py-2" title={String(row[column] ?? '')}>{row[column] == null ? '' : String(row[column])}</td>)}</tr>)}</tbody>
                              </table>
                            </div>
                            <p className="mt-2 text-[10px] text-slate-500">Generated from the staged CDS source. The full CSV remains available for validation.</p>
                          </details>
                        )}

                        {validationResult && (() => {
                          const factData = ((validationResult as any).datasphere?.data || []) as Array<Record<string, unknown>>;
                          const factColumns = factData.length ? Object.keys(factData[0]) : [];
                          return (
                            <details className="w-full rounded-xl border border-emerald-500/30 bg-slate-950/50 p-3 group">
                              <summary className="flex cursor-pointer list-none items-center justify-between gap-3 text-xs font-semibold text-slate-200">
                                <span>Datasphere fact query result · {factData.length.toLocaleString()} rows</span>
                                <span className="text-emerald-400 group-open:hidden">Show data</span>
                                <span className="hidden text-emerald-400 group-open:inline">Hide data</span>
                              </summary>
                              {factData.length > 0 ? (
                                <div className="mt-3 max-h-72 overflow-auto rounded-lg border border-slate-800">
                                  <table className="min-w-full text-left text-[11px] font-mono">
                                    <thead className="sticky top-0 bg-slate-900 text-slate-300"><tr>{factColumns.map((column) => <th key={column} className="whitespace-nowrap px-3 py-2">{column}</th>)}</tr></thead>
                                    <tbody>{factData.map((row, index) => <tr key={index} className="border-t border-slate-800 text-slate-300">{factColumns.map((column) => <td key={column} className="max-w-xs truncate px-3 py-2" title={String(row[column] ?? '')}>{row[column] == null ? '' : String(row[column])}</td>)}</tr>)}</tbody>
                                  </table>
                                </div>
                              ) : <p className="mt-3 text-[11px] text-slate-500">No fact rows were returned. Review query status and results below.</p>}
                            </details>
                          );
                        })()}

                        {sqlMode === 'llm' ? (
                          <div className="flex flex-wrap items-center gap-3">
                            <div className="flex-1 min-w-[280px]">
                              <input
                                type="text"
                                placeholder="Describe what to validate, e.g. compare 2024 delivery KPIs by plant..."
                                value={valPrompt}
                                onChange={(e) => setValPrompt(e.target.value)}
                                className="w-full bg-slate-950 border border-slate-700/80 rounded-xl px-3 py-2 text-xs text-slate-200 font-mono placeholder:text-slate-500 focus:outline-none focus:border-blue-500"
                              />
                            </div>
                            <button
                              onClick={() => handleExecuteValidation()}
                              disabled={validating}
                              className={`px-5 py-2 rounded-xl text-xs font-bold flex items-center gap-2 cursor-pointer shadow-md transition-all ${
                                validating
                                  ? 'bg-emerald-950/60 text-emerald-400 border border-emerald-500/40 cursor-not-allowed'
                                  : 'bg-emerald-500 hover:bg-emerald-400 text-slate-950 font-extrabold shadow-emerald-500/20'
                              }`}
                            >
                              {validating ? (
                                <>
                                  <RefreshCw className="w-3.5 h-3.5 animate-spin" />
                                  <span>Generating SQL & Validating...</span>
                                </>
                              ) : (
                                <>
                                  <Zap className="w-3.5 h-3.5" />
                                  <span>Fetch & Validate Data</span>
                                </>
                              )}
                            </button>
                          </div>
                        ) : (
                          <div className="space-y-2">
                            <div className="flex flex-wrap items-center gap-3">
                              <label className="text-xs text-slate-300" htmlFor="manual-sql-target">Execution target</label>
                              <select
                                id="manual-sql-target"
                                value={manualSqlTarget}
                                onChange={(e) => {
                                  setManualSqlTarget(e.target.value as 'datasphere' | 's4hana');
                                  setManualQueryHash(null);
                                  setManualApproved(false);
                                  setValidationResult(null);
                                }}
                                className="bg-slate-950 border border-slate-700/80 rounded-lg px-3 py-2 text-xs text-slate-200"
                              >
                                <option value="datasphere">Datasphere / SAP HANA Cloud SQL</option>
                                <option value="s4hana">S/4HANA / ABAP Open SQL (ADT)</option>
                              </select>
                            </div>
                            <textarea
                              aria-label={`Manual SQL for ${manualSqlTarget === 'datasphere' ? 'Datasphere / SAP HANA Cloud' : 'S/4HANA / Eclipse ADT'}`}
                              value={manualSql}
                              onChange={(e) => {
                                setManualSql(e.target.value);
                                setManualQueryHash(null);
                                setManualApproved(false);
                                setValidationResult(null);
                                setValidationError(null);
                              }}
                              rows={8}
                              spellCheck={false}
                              placeholder={manualSqlTarget === 'datasphere'
                                ? 'Enter a read-only SAP HANA SELECT against the verified fact-model runtime object.'
                                : 'Enter ABAP Open SQL using verified CDS entities and entity~field references.'}
                              className="w-full resize-y bg-slate-950 border border-slate-700/80 rounded-xl px-3 py-2 text-xs text-slate-200 font-mono placeholder:text-slate-500 focus:outline-none focus:border-blue-500"
                            />
                            <div className="flex flex-wrap items-center gap-3">
                              <button
                                onClick={() => handleExecuteValidation()}
                                disabled={validating || !manualSql.trim()}
                                className="px-4 py-2 rounded-lg bg-blue-600 hover:bg-blue-500 disabled:opacity-50 text-xs font-bold text-white"
                              >
                                {validating ? 'Validating…' : 'Validate SQL'}
                              </button>
                              {manualQueryHash && validationResult?.approval_required && (
                                <label className="flex items-center gap-2 text-xs text-amber-200">
                                  <input
                                    type="checkbox"
                                    checked={manualApproved}
                                    onChange={(e) => setManualApproved(e.target.checked)}
                                  />
                                  I approve this exact query and execution target.
                                </label>
                              )}
                              <button
                                onClick={() => handleExecuteValidation(true, manualQueryHash || undefined)}
                                disabled={validating || !manualQueryHash || !manualApproved}
                                className="px-4 py-2 rounded-lg bg-emerald-600 hover:bg-emerald-500 disabled:opacity-50 text-xs font-bold text-white"
                              >
                                Execute SQL
                              </button>
                              <button
                                onClick={() => copyToClipboard(manualSql, 'Manual SQL')}
                                disabled={!manualSql}
                                className="px-3 py-2 rounded-lg border border-slate-700 text-xs text-slate-300 disabled:opacity-50"
                              >
                                Copy SQL
                              </button>
                            </div>
                            {manualQueryHash && (
                              <p className="text-[11px] text-emerald-300">
                                Query validated for {manualSqlTarget === 'datasphere' ? 'Datasphere / SAP HANA Cloud' : 'S/4HANA / Eclipse ADT'}.
                                Execution remains blocked until you approve the exact SQL above.
                              </p>
                            )}
                          </div>
                        )}
                      </div>

                    </div>

                    </div>

                    <section className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm space-y-4">
                      <h3 className="text-base font-semibold text-slate-900">Validate {selectedViewName}</h3>

                      <details className="group rounded-xl border border-blue-200 bg-blue-50/40 p-3">
                        <summary className="flex cursor-pointer list-none items-center justify-between gap-3 text-sm font-semibold text-slate-800">
                          <span>Final CDS CSV · {replicaCsvResult ? `${replicaCsvResult.row_count.toLocaleString()} rows · ${replicaCsvResult.columns.length} fields` : 'not created yet'}</span>
                          <span className="text-blue-700 group-open:hidden">Show data</span>
                          <span className="hidden text-blue-700 group-open:inline">Hide data</span>
                        </summary>
                        {replicaCsvResult ? (
                          <div className="mt-3 max-h-72 overflow-auto rounded-lg border border-slate-200 bg-white">
                            <table className="min-w-full text-left text-[11px] font-mono">
                              <thead className="sticky top-0 bg-slate-100 text-slate-700"><tr>{replicaCsvResult.columns.map((column: string) => <th key={column} className="whitespace-nowrap px-3 py-2">{column}</th>)}</tr></thead>
                              <tbody>{replicaCsvResult.preview.map((row: Record<string, unknown>, index: number) => <tr key={index} className="border-t border-slate-100 text-slate-700">{replicaCsvResult.columns.map((column: string) => <td key={column} className="max-w-xs truncate px-3 py-2" title={String(row[column] ?? '')}>{row[column] == null ? '' : String(row[column])}</td>)}</tr>)}</tbody>
                            </table>
                          </div>
                        ) : <p className="mt-3 text-xs text-slate-500">Create the final CSV in step 8 to make it available here.</p>}
                      </details>

                      <details className="group rounded-xl border border-emerald-200 bg-emerald-50/40 p-3">
                        <summary className="flex cursor-pointer list-none items-center justify-between gap-3 text-sm font-semibold text-slate-800">
                          <span>Datasphere fact view {validationFactView && `· ${validationFactView}`} · {factDataLoading ? 'retrieving…' : factDataError ? 'retrieval failed' : `${factDataResult?.data?.length || 0} rows retrieved`}</span>
                          <span className="text-emerald-700 group-open:hidden">Show data</span>
                          <span className="hidden text-emerald-700 group-open:inline">Hide data</span>
                        </summary>
                        {factDataLoading ? (
                          <p className="mt-3 text-xs text-slate-500">Retrieving the selected fact view directly from Datasphere. The result is kept in memory and is not staged.</p>
                        ) : factDataError ? (
                          <div className="mt-3 flex items-center justify-between gap-3 text-xs text-rose-700">
                            <span>{factDataError}</span>
                            <button type="button" onClick={() => setFactDataReloadKey((key) => key + 1)} className="shrink-0 font-semibold text-blue-700 hover:underline">Retry</button>
                          </div>
                        ) : factDataResult?.data?.length ? (
                          <div className="mt-3 max-h-72 overflow-auto rounded-lg border border-slate-200 bg-white">
                            <table className="min-w-full text-left text-[11px] font-mono">
                              <thead className="sticky top-0 bg-slate-100 text-slate-700"><tr>{Object.keys(factDataResult.data[0]).map((column: string) => <th key={column} className="whitespace-nowrap px-3 py-2">{column}</th>)}</tr></thead>
                              <tbody>{factDataResult.data.map((row: Record<string, unknown>, index: number) => <tr key={index} className="border-t border-slate-100 text-slate-700">{Object.keys(factDataResult.data[0]).map((column: string) => <td key={column} className="max-w-xs truncate px-3 py-2" title={String(row[column] ?? '')}>{String(row[column] ?? '')}</td>)}</tr>)}</tbody>
                            </table>
                          </div>
                        ) : <div className="mt-3 space-y-2 text-xs text-slate-500">
                          <p>No fact rows were returned from Datasphere.</p>
                          {factDataResult && <details className="rounded-md border border-slate-200 bg-white p-2">
                            <summary className="cursor-pointer font-semibold">Inspect fact query target</summary>
                            <div className="mt-2 space-y-1">
                              <div>Requested fact view: <code>{validationFactView || 'unresolved'}</code></div>
                              <div>Executed view: <code>{factDataResult.execution_info?.view_name || 'not reported'}</code></div>
                              <pre className="max-h-32 overflow-auto whitespace-pre-wrap break-all rounded bg-slate-50 p-2">{factDataResult.query_blueprint?.sql_query || 'SQL was not returned.'}</pre>
                            </div>
                          </details>}
                        </div>}
                        {factDataResult?.query_blueprint?.unavailable_on_fact_view?.length > 0 && (
                          <p className="mt-2 text-xs text-slate-600">The fact view returned its available technical input fields. Calculated model fields are returned by the model query during validation.</p>
                        )}
                      </details>

                      <section className="space-y-4 rounded-xl border border-blue-200 bg-gradient-to-br from-white via-blue-50 to-indigo-50 p-4 shadow-sm">
                        <div className="flex flex-wrap items-start justify-between gap-3">
                          <div>
                            <h3 className="text-base font-bold text-slate-900">Automatic KPI statistics</h3>
                            <p className="mt-1 text-xs text-slate-600">Computed as soon as the selected fact model and final CDS projection are retrieved. No prompt or row matching is needed.</p>
                          </div>
                          <span className="rounded-full bg-blue-100 px-3 py-1 text-xs font-semibold text-blue-800">{automaticKpiStatistics.length} KPI columns</span>
                        </div>
                        {factDataLoading ? <p className="rounded-lg bg-white/80 px-3 py-3 text-sm text-slate-600">Retrieving fact-model data to calculate statistics…</p> : automaticKpiStatistics.length ? <div className="overflow-x-auto rounded-lg border border-slate-200 bg-white">
                          <table className="min-w-full text-left text-xs">
                            <thead className="bg-slate-100 text-slate-600"><tr><th className="px-3 py-3">KPI column</th><th className="px-3 py-3">Data type</th><th className="px-3 py-3">Datasphere statistics</th><th className="px-3 py-3">CDS statistics</th><th className="px-3 py-3">Matching</th></tr></thead>
                            <tbody>{automaticKpiStatistics.map((item: any) => {
                              const format = (value: number) => Number(value).toLocaleString(undefined, { maximumFractionDigits: 4 });
                              const summarize = (stats: any) => stats.kind === 'numeric'
                                ? `Sum ${format(stats.sum)} · Avg ${format(stats.average)} · Min ${format(stats.min)} · Max ${format(stats.max)} · Count ${format(stats.count)}`
                                : `Count ${format(stats.count)} · Distinct ${format(stats.distinct)}`;
                              return <tr key={item.name} className="border-t border-slate-100">
                                <td className="whitespace-nowrap px-3 py-3 font-mono font-semibold text-slate-900">{item.name}</td>
                                <td className="whitespace-nowrap px-3 py-3 text-slate-600">{item.type}</td>
                                <td className="min-w-64 px-3 py-3 text-slate-700">{summarize(item.datasphere)}</td>
                                <td className="min-w-64 px-3 py-3 text-slate-700">{summarize(item.cds)}</td>
                                <td className="px-3 py-3"><span className={`inline-flex rounded-full px-2.5 py-1 font-bold ${item.matched ? 'bg-emerald-100 text-emerald-800' : 'bg-rose-100 text-rose-800'}`}>{item.matched ? 'Matched' : 'Not matched'}</span></td>
                              </tr>;
                            })}</tbody>
                          </table>
                        </div> : <p className="rounded-lg bg-white/80 px-3 py-3 text-sm text-slate-600">{factDataError ? `Fact-model retrieval failed: ${factDataError}` : 'No KPI columns are available in both retrieved datasets yet.'}</p>}
                      </section>
                      <label className="block space-y-2">
                        <span className="text-sm font-semibold text-slate-800">Optional follow-up prompt</span>
                        <textarea
                          value={valPrompt}
                          onChange={(event) => {
                            setValPrompt(event.target.value);
                            setValidationResult(null);
                            setValidationError(null);
                          }}
                          placeholder="Describe what to validate, e.g. compare 2024 delivery KPIs by plant..."
                          rows={3}
                          className="w-full resize-y rounded-xl border border-slate-300 bg-white px-3 py-2.5 text-sm text-slate-900 placeholder:text-slate-400 focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-100"
                        />
                      </label>
                      <div className="flex justify-end">
                        <button
                          type="button"
                          onClick={() => handleExecuteValidation()}
                          disabled={validating || factDataLoading || !factDataResult || !replicaCsvResult || !valPrompt.trim()}
                          className="inline-flex items-center gap-2 rounded-lg bg-blue-600 px-5 py-2.5 text-sm font-semibold text-white hover:bg-blue-700 disabled:cursor-not-allowed disabled:opacity-50"
                        >
                          {validating ? <><RefreshCw className="h-4 w-4 animate-spin" />Submitting…</> : 'Submit'}
                        </button>
                      </div>
                    </section>

                    {/* Validation Error Banner */}
                    {validationError && (
                      <div className="p-4 rounded-xl bg-rose-950/20 border border-rose-500/40 text-rose-300 text-xs flex items-start gap-2.5">
                        <AlertCircle className="w-4 h-4 text-rose-400 shrink-0 mt-0.5" />
                        <div>
                          <div className="font-bold text-rose-400">Validation Notice</div>
                          <div>{validationError}</div>
                        </div>
                      </div>
                    )}

                    {/* Validation Results Workbench */}
                    {validationResult ? (
                      <div className="space-y-6 animate-in fade-in duration-200">
                        
                        {/* Executive Status Card */}
                        {(() => {
                          const isMatch = validationResult.status === 'MATCH' || validationResult.status === 'VALIDATED';
                          const isDiscrepancy = validationResult.status === 'MISMATCH' || validationResult.status === 'DISCREPANCY_DETECTED';
                          const isIncomplete = validationResult.status === 'INDETERMINATE' || validationResult.status === 'INCOMPLETE';
                          const isNotRun = validationResult.status === 'NOT_RUN';
                          const bannerColor = isMatch
                            ? 'bg-emerald-950/25 border-emerald-500/40 text-emerald-200 shadow-emerald-500/10'
                            : isDiscrepancy
                            ? 'bg-amber-50 border-amber-300 text-amber-950 shadow-amber-200/50'
                            : isNotRun
                            ? 'bg-blue-950/30 border-blue-500/50 text-blue-200 shadow-blue-500/10'
                            : isIncomplete
                            ? 'bg-slate-900/70 border-slate-600 text-slate-200 shadow-slate-500/10'
                            : 'bg-rose-950/25 border-rose-500/40 text-rose-200 shadow-rose-500/10';

                          return (
                            <div className={`p-6 rounded-2xl border shadow-xl ${bannerColor} space-y-4`}>
                              <div className="flex flex-wrap items-center justify-between gap-3">
                                <div className="flex items-center gap-3">
                                  {isMatch ? (
                                    <div className="w-10 h-10 rounded-xl bg-emerald-500 text-slate-950 flex items-center justify-center font-bold">
                                      <CheckCircle2 className="w-6 h-6" />
                                    </div>
                                  ) : isDiscrepancy ? (
                                    <div className="w-10 h-10 rounded-xl bg-amber-500 text-slate-950 flex items-center justify-center font-bold">
                                      <AlertTriangle className="w-6 h-6" />
                                    </div>
                                  ) : isNotRun ? (
                                    <div className="w-10 h-10 rounded-xl bg-blue-500 text-slate-950 flex items-center justify-center font-bold">
                                      <ShieldCheck className="w-6 h-6" />
                                    </div>
                                  ) : isIncomplete ? (
                                    <div className="w-10 h-10 rounded-xl bg-slate-500 text-white flex items-center justify-center font-bold">
                                      <AlertCircle className="w-6 h-6" />
                                    </div>
                                  ) : (
                                    <div className="w-10 h-10 rounded-xl bg-rose-500 text-white flex items-center justify-center font-bold">
                                      <AlertCircle className="w-6 h-6" />
                                    </div>
                                  )}
                                  <div>
                                    <div className="text-base font-extrabold tracking-tight">
                                      {isMatch ? 'DATA VALIDATED • 100% MATCH' : isDiscrepancy ? 'DISCREPANCY DETECTED' : isNotRun
                                        ? (validationResult.execution_plan?.mode === 'FACT_QUERY_ONLY' && validationResult.s4hana.query_status === 'BLOCKED'
                                          ? 'FACT QUERY READY • S/4HANA RECONSTRUCTION BLOCKED'
                                          : 'QUERY PLAN READY • AWAITING APPROVAL')
                                        : isIncomplete ? 'VALIDATION INDETERMINATE • NO MATCH CLAIMED' : 'VALIDATION RUN COMPLETED WITH NOTICES'}
                                    </div>
                                    <div className="text-xs opacity-90 mt-0.5 font-mono">
                                      Datasphere Model: <span className="font-bold">{validationResult.view_name}</span> ↔ S/4HANA Source: <span className="font-bold">{validationResult.source_cds_view}</span>
                                    </div>
                                  </div>
                                </div>

                                <div className="text-right font-mono">
                                  <div className="text-xs uppercase tracking-wider opacity-75">Delta (Variance)</div>
                                  <div className="text-xl font-bold">
                                    {formatNum(validationResult.comparison.delta)} ({typeof validationResult.comparison.variance_pct === 'number' ? `${validationResult.comparison.variance_pct.toFixed(2)}%` : 'not computed'})
                                  </div>
                                </div>
                              </div>

                              <p className={`text-xs leading-relaxed pt-2 border-t ${isDiscrepancy ? 'border-amber-200' : 'border-white/10 opacity-95'}`}>
                                {validationResult.comparison.summary_text}
                              </p>

                              {isNotRun && (
                                <div className="pt-3 border-t border-blue-500/30 flex items-center justify-between">
                                  <span className="text-xs text-blue-300">
                                    {validationResult.execution_plan?.mode === 'DUAL_QUERY_ONLY'
                                      ? 'Approve the read-only Datasphere fact query and the verified S/4HANA CDS query. The comparison grain is unresolved, so no reconciliation match will be claimed.'
                                      : validationResult.execution_plan?.mode === 'FACT_QUERY_ONLY'
                                      ? 'Approve the read-only query on the verified Datasphere fact model. S/4HANA reconciliation remains blocked.'
                                      : 'Human approval required before running live database queries on SAP Datasphere and SAP S/4HANA.'}
                                  </span>
                                  <button
                                    onClick={() => handleExecuteValidation(true, validationResult.execution_plan?.query_hash || validationResult.reconstruction?.query_hash)}
                                    disabled={validating}
                                    className="px-4 py-2 bg-blue-600 hover:bg-blue-500 text-white text-xs font-bold rounded-xl shadow-lg transition-all flex items-center gap-2 cursor-pointer disabled:opacity-50"
                                  >
                                    <ShieldCheck className="w-4 h-4" />
                                    {validationResult.execution_plan?.mode === 'DUAL_QUERY_ONLY'
                                      ? 'Approve & Run Both Queries'
                                      : validationResult.execution_plan?.mode === 'FACT_QUERY_ONLY'
                                      ? 'Approve & Run Fact Query'
                                      : 'Approve & Execute Reconciliation'}
                                  </button>
                                </div>
                              )}
                            </div>
                          );
                        })()}

                        {validationResult.reconstruction && (
                          <div className="p-3 rounded-xl bg-[#0e1629] border border-slate-800 space-y-2.5">
                            <div className="flex flex-wrap items-center justify-between gap-2">
                              <div>
                                <h3 className="font-bold text-[13px] text-white">Reconstructed Model Architecture</h3>
                                <p className="text-[11px] text-slate-400 mt-0.5">
                                  Metadata status: {validationResult.architecture_metadata?.completeness || 'unknown'} ·
                                  {' '}{validationResult.architecture_metadata?.resolved_artifacts?.length || 0} resolved artifacts ·
                                  {' '}{validationResult.architecture_metadata?.s4hana_sources?.length || 0} S/4HANA sources
                                </p>
                              </div>
                              <span className={`px-2.5 py-1 rounded-lg border text-[10px] font-bold font-mono ${
                                validationResult.reconstruction.status === 'EXECUTED'
                                  ? 'bg-emerald-950/40 border-emerald-500/40 text-emerald-300'
                                  : 'bg-amber-950/40 border-amber-500/40 text-amber-300'
                              }`}>
                                {validationResult.reconstruction.status}
                              </span>
                            </div>
                            {validationResult.reconstruction.logic_summary && (
                              <p className="text-[11px] leading-snug text-slate-300">
                                {validationResult.reconstruction.logic_summary}
                              </p>
                            )}
                            {validationResult.reconstruction.message && (
                              <p className="text-[11px] leading-snug text-amber-300">
                                {validationResult.reconstruction.message}
                              </p>
                            )}
                            {validationResult.reconstruction.llm_provider && (
                              <p className="text-xs text-slate-400">
                                Query planner: {validationResult.reconstruction.llm_provider}
                                {validationResult.reconstruction.llm_model ? ` (${validationResult.reconstruction.llm_model})` : ''}
                              </p>
                            )}
                            {!!validationResult.architecture_metadata?.gaps?.length && (
                              <ul className="list-disc pl-5 space-y-1 text-xs text-amber-300">
                                {validationResult.architecture_metadata.gaps.map((gap: string, index: number) => (
                                  <li key={`${index}-${gap}`}>{gap}</li>
                                ))}
                              </ul>
                            )}
                            {!!validationResult.reconstruction.assumptions?.length && (
                              <ul className="list-disc pl-5 space-y-1 text-xs text-amber-300">
                                {validationResult.reconstruction.assumptions.map((item, index) => (
                                  <li key={`${index}-${item}`}>{item}</li>
                                ))}
                              </ul>
                            )}
                            {!!validationResult.reconstruction.generation_diagnostics?.length && (
                              <div className="rounded-lg border border-amber-500/30 bg-amber-950/20 p-3 text-xs text-amber-200">
                                <p className="font-semibold">LLM provider status</p>
                                <ul className="mt-1 list-disc space-y-1 pl-5">
                                  {validationResult.reconstruction.generation_diagnostics.map((item, index) => (
                                    <li key={`${item.provider}-${index}`}>
                                      {item.provider}{item.model ? ` (${item.model})` : ''}
                                      {item.status_code ? ` HTTP ${item.status_code}` : ''}: {item.message}
                                    </li>
                                  ))}
                                </ul>
                                <p className="mt-2">A deterministic metadata fallback was used where it could represent the verified plan.</p>
                              </div>
                            )}
                            {!!validationResult.reconstruction.unsupported_transformations?.length && (
                              <div className="rounded-lg border border-rose-500/30 bg-rose-950/20 p-3 text-xs text-rose-200">
                                <p className="font-semibold">Untranslated Datasphere transformations</p>
                                <ul className="mt-1 list-disc space-y-1 pl-5">
                                  {validationResult.reconstruction.unsupported_transformations.map((item, index) => (
                                    <li key={`${index}-${item}`}>{item}</li>
                                  ))}
                                </ul>
                              </div>
                            )}
                            <section className="w-full rounded-lg border border-blue-200 bg-slate-50 p-3">
                              <div className="flex items-center justify-between gap-3 text-[13px] font-semibold text-blue-800">
                                <span>Reconstructed Datasphere and S/4HANA SQL</span>
                                <span className="text-xs font-normal text-slate-500">Validation query details</span>
                              </div>
                              <div className="mt-2.5 grid w-full grid-cols-1 gap-2.5">
                                <div className="min-w-0">
                                  <h4 className="mb-1 text-[10px] font-semibold uppercase tracking-wide text-slate-600">Datasphere query</h4>
                                  <pre className="max-h-40 overflow-auto rounded-md border border-slate-200 bg-white p-2.5 font-mono text-xs leading-snug text-slate-900 whitespace-pre-wrap break-words" style={{ backgroundColor: '#ffffff', color: '#0f172a' }}>
                                  {validationResult.reconstruction.datasphere_sql || 'Datasphere SQL was not generated.'}
                                  </pre>
                                </div>
                                <div className="min-w-0">
                                  <h4 className="mb-1 text-[10px] font-semibold uppercase tracking-wide text-slate-600">S/4HANA query</h4>
                                  <pre className="max-h-40 overflow-auto rounded-md border border-slate-200 bg-white p-2.5 font-mono text-xs leading-snug text-slate-900 whitespace-pre-wrap break-words" style={{ backgroundColor: '#ffffff', color: '#0f172a' }}>
                                  {validationResult.reconstruction.s4hana_sql || 'S/4HANA SQL was not generated.'}
                                  </pre>
                                </div>
                              </div>
                            </section>
                            {validationResult.architecture_metadata?.dependency_tree && (
                              <details>
                                <summary className="cursor-pointer text-xs font-semibold text-slate-300 hover:text-white">
                                  Inspect full resolved dependency metadata
                                </summary>
                                <pre className="max-h-96 overflow-auto rounded-xl bg-slate-950 border border-slate-800 p-3 mt-3 text-[10px] leading-relaxed text-slate-300 whitespace-pre-wrap">
                                  {JSON.stringify(validationResult.architecture_metadata.dependency_tree, null, 2)}
                                </pre>
                              </details>
                            )}
                          </div>
                        )}

                        {/* KPI Comparison Metrics Grid (Multi-Measure Cards) */}
                        <div className="space-y-3">
                          <div className="flex flex-wrap items-center justify-between gap-2 px-1">
                            <div className="flex items-center gap-2">
                              <Zap className="w-3.5 h-3.5 text-amber-400" />
                              <span className="text-xs font-bold uppercase tracking-wider text-slate-200">
                                Aggregate Measure Comparison ({validationResult.metrics_comparison?.length || 0})
                              </span>
                            </div>
                            <span className="text-[11px] font-mono text-slate-400">
                              Verified aggregate semantics; row-level grain status is independent
                            </span>
                          </div>

                          {validationResult.metrics_comparison && validationResult.metrics_comparison.length > 0 ? (
                            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                              {validationResult.metrics_comparison.map((mc) => {
                                const isM = mc.status
                                  ? mc.status === 'MATCH'
                                  : mc.is_match;
                                const comparisonStatus = mc.status || (isM ? 'MATCH' : 'MISMATCH');
                                const isComparable = comparisonStatus === 'MATCH' || comparisonStatus === 'MISMATCH';
                                return (
                                  <div
                                    key={mc.metric_name}
                                    className={`p-4 rounded-xl border shadow-md space-y-2.5 transition-all ${
                                      isM
                                        ? 'bg-[#0e1629] border-emerald-500/30 hover:border-emerald-500/50'
                                        : isComparable
                                          ? 'bg-[#0e1629] border-amber-500/40 hover:border-amber-500/60'
                                          : 'bg-[#0e1629] border-slate-700/70'
                                    }`}
                                  >
                                    <div className="flex items-center justify-between gap-2 border-b border-slate-800/80 pb-2">
                                      <div className="flex items-center gap-1.5 truncate">
                                        <Zap className="w-3.5 h-3.5 text-amber-400 shrink-0" />
                                        <span className="font-bold text-xs text-white font-mono truncate" title={mc.metric_name}>
                                          {mc.metric_name}
                                        </span>
                                        <span className="text-[10px] text-slate-400 font-mono">({mc.aggregation})</span>
                                      </div>
                                      <span className={`text-[10px] font-mono font-bold px-2 py-0.5 rounded-full border shrink-0 ${
                                        comparisonStatus === 'MATCH'
                                          ? 'bg-emerald-950 text-emerald-300 border-emerald-500/30'
                                          : comparisonStatus === 'MISMATCH'
                                            ? 'bg-amber-950 text-amber-300 border-amber-500/40'
                                            : 'bg-slate-900 text-slate-300 border-slate-700'
                                      }`}>
                                        {comparisonStatus === 'MATCH'
                                          ? '✓ MATCH'
                                          : comparisonStatus === 'MISMATCH'
                                            ? `Δ ${formatNum(mc.delta)}`
                                            : comparisonStatus.replace('_', ' ')}
                                      </span>
                                    </div>

                                    <div className="grid grid-cols-2 gap-2 text-xs font-mono">
                                      <div className="p-2 rounded-lg bg-slate-950/60 border border-slate-800/60 space-y-0.5">
                                        <div className="text-[10px] text-blue-400 uppercase">Datasphere KPI</div>
                                        <div className="font-bold text-blue-300 truncate" title={mc.formatted_datasphere}>
                                          {mc.formatted_datasphere || formatNum(mc.datasphere_total)}
                                        </div>
                                      </div>
                                      <div className="p-2 rounded-lg bg-slate-950/60 border border-slate-800/60 space-y-0.5">
                                        <div className="text-[10px] text-emerald-400 uppercase">S/4HANA CDS KPI</div>
                                        <div className="font-bold text-emerald-300 truncate" title={mc.formatted_s4hana}>
                                          {mc.formatted_s4hana || formatNum(mc.s4hana_total)}
                                        </div>
                                      </div>
                                    </div>

                                    <div className="flex items-center justify-between text-[11px] font-mono pt-1 text-slate-400 border-t border-slate-800/50">
                                      <span>Delta: <strong className={isM ? 'text-slate-300' : 'text-amber-300'}>{formatNum(mc.delta)}</strong></span>
                                      <span>Relative difference: <strong className={isM ? 'text-emerald-400' : 'text-amber-400'}>{typeof mc.relative_difference_pct === 'number' ? `${mc.relative_difference_pct.toFixed(2)}%` : '-'}</strong></span>
                                    </div>
                                    <div className="text-[10px] font-mono text-slate-500 border-t border-slate-800/50 pt-1">
                                      Completeness: {mc.completeness || 'UNKNOWN'}
                                      {mc.group_status && mc.group_status !== 'NOT_APPLICABLE' ? ` · groups: ${mc.group_status}` : ''}
                                      {mc.reason ? ` · ${mc.reason}` : ''}
                                    </div>
                                  </div>
                                );
                              })}
                            </div>
                          ) : (
                            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                              <div className="p-4 rounded-xl bg-[#0e1629] border border-slate-800 shadow-sm space-y-1">
                                <div className="text-[10px] font-mono text-slate-400 uppercase tracking-wider">Datasphere Total KPI</div>
                                <div className="text-lg font-bold text-blue-400 font-mono">
                                  {validationResult.datasphere.formatted_kpi || formatNum(validationResult.datasphere.total_kpi)}
                                </div>
                              </div>
                              <div className="p-4 rounded-xl bg-[#0e1629] border border-slate-800 shadow-sm space-y-1">
                                <div className="text-[10px] font-mono text-slate-400 uppercase tracking-wider">S/4HANA CDS Total KPI</div>
                                <div className="text-lg font-bold text-emerald-400 font-mono">
                                  {validationResult.s4hana.formatted_kpi || formatNum(validationResult.s4hana.total_kpi)}
                                </div>
                              </div>
                              <div className="p-4 rounded-xl bg-[#0e1629] border border-slate-800 shadow-sm space-y-1">
                                <div className="text-[10px] font-mono text-slate-400 uppercase tracking-wider">Exact Discrepancy</div>
                                <div className="text-lg font-bold text-amber-400 font-mono">
                                  {formatNum(validationResult.comparison.delta)}
                                </div>
                              </div>
                              <div className="p-4 rounded-xl bg-[#0e1629] border border-slate-800 shadow-sm space-y-1">
                                <div className="text-[10px] font-mono text-slate-400 uppercase tracking-wider">Reconciled Grain</div>
                                <div className="text-base font-bold text-purple-400 font-mono">
                                  {validationResult.reconciliation_dimension}
                                </div>
                              </div>
                            </div>
                          )}

                          {/* Macro Context Bar */}
                          <div className="p-3 rounded-xl bg-slate-950/70 border border-slate-800/80 flex flex-wrap items-center justify-between gap-3 text-xs font-mono text-slate-400">
                            <div className="flex items-center gap-3">
                              <span>Reconciled Grain: <strong className="text-purple-300">{validationResult.reconciliation_dimension}</strong></span>
                              <span>•</span>
                              <span>Records: <strong className="text-slate-200">
                                DS {validationResult.datasphere.query_status === 'SUCCEEDED_ZERO_ROWS'
                                  ? '0 (query succeeded)'
                                  : validationResult.datasphere.query_status?.startsWith('SUCCEEDED')
                                    ? validationResult.datasphere.record_count
                                    : validationResult.datasphere.query_status === 'EXECUTION_FAILED'
                                      ? 'Failed'
                                      : validationResult.datasphere.query_status === 'BLOCKED'
                                        ? 'Blocked'
                                        : 'Not executed'}
                                {' / '}S4 {validationResult.s4hana.query_status === 'SUCCEEDED_ZERO_ROWS'
                                  ? '0 (query succeeded)'
                                  : validationResult.s4hana.query_status?.startsWith('SUCCEEDED')
                                    ? validationResult.s4hana.record_count
                                    : validationResult.s4hana.query_status === 'EXECUTION_FAILED'
                                      ? 'Failed'
                                      : validationResult.s4hana.query_status === 'PARTIAL_FAILURE'
                                        ? 'Partial failure'
                                        : validationResult.s4hana.query_status === 'BLOCKED'
                                          ? 'Blocked'
                                          : 'Not executed'}
                              </strong></span>
                            </div>
                            <div className="flex items-center gap-3">
                              <span>Latencies: <strong className="text-blue-300">DS {validationResult.datasphere.latency_seconds.toFixed(2)}s</strong> / <strong className="text-emerald-300">S4 {validationResult.s4hana.latency_seconds.toFixed(2)}s</strong></span>
                              <span>•</span>
                              <span className="text-emerald-400 flex items-center gap-1">
                                <ShieldCheck className="w-3.5 h-3.5" />
                                In-Memory Zero Leakage
                              </span>
                            </div>
                          </div>
                        </div>

                        {/* Dual-Query Inspection Workbench */}
                        <div className="p-5 rounded-2xl bg-[#0e1629] border border-slate-800 shadow-xl space-y-3">
                          <div className="flex flex-wrap items-center justify-between gap-2 border-b border-slate-800/80 pb-3">
                            <div className="flex items-center gap-2">
                              <Terminal className="w-4 h-4 text-blue-400" />
                              <h4 className="font-bold text-xs text-white uppercase tracking-wider">Live SQL Query Execution Comparison</h4>
                            </div>

                            <div className="flex flex-wrap items-center gap-2">
                              <label className="text-[11px] text-slate-400" htmlFor="comparison-sql-mode">SQL mode</label>
                              <select
                                id="comparison-sql-mode"
                                value={sqlMode}
                                onChange={(e) => {
                                  setSqlMode(e.target.value as 'llm' | 'manual');
                                  setManualQueryHash(null);
                                  setManualApproved(false);
                                }}
                                className="bg-slate-950 border border-slate-700 rounded-lg px-2 py-1 text-[11px] text-slate-200"
                              >
                                <option value="llm">LLM Generated SQL</option>
                                <option value="manual">Manual SQL</option>
                              </select>
                            <div className="flex items-center gap-1 bg-slate-950 p-1 rounded-lg border border-slate-800">
                              <button
                                onClick={() => setValSqlTab('datasphere')}
                                className={`px-2.5 py-1 rounded text-[11px] font-mono font-semibold transition-colors ${
                                  valSqlTab === 'datasphere' ? 'bg-blue-600 text-white' : 'text-slate-400 hover:text-white'
                                }`}
                              >
                                SAP HANA Cloud SQL (Explorer)
                              </button>
                              <button
                                onClick={() => setValSqlTab('s4hana')}
                                className={`px-2.5 py-1 rounded text-[11px] font-mono font-semibold transition-colors ${
                                  valSqlTab === 's4hana' ? 'bg-emerald-600 text-white' : 'text-slate-400 hover:text-white'
                                }`}
                              >
                                S/4HANA Open SQL (Eclipse ADT)
                              </button>
                            </div>
                            </div>
                          </div>

                          {valSqlTab === 'datasphere' ? (
                            <div className="space-y-2">
                              <div className="flex items-center justify-between text-xs text-slate-400 font-mono">
                                <span>Engine: <strong className="text-blue-300">{validationResult.datasphere.execution_engine}</strong></span>
                                <button
                                  onClick={() => copyToClipboard(validationResult.datasphere.sql, 'Datasphere SQL')}
                                  className="text-xs text-blue-400 hover:underline flex items-center gap-1 cursor-pointer"
                                >
                                  <Copy className="w-3 h-3" />
                                  <span>Copy HANA SQL</span>
                                </button>
                              </div>
                              <pre className="p-3.5 rounded-xl bg-slate-950 border border-slate-800/80 text-xs font-mono text-blue-300 overflow-x-auto whitespace-pre-wrap leading-relaxed">
                                {validationResult.datasphere.sql}
                              </pre>
                              {validationResult.datasphere.error && (
                                <div className="p-2.5 rounded-lg bg-rose-950/30 border border-rose-500/30 text-rose-300 text-xs font-mono">
                                  HANA Cloud Notice: {validationResult.datasphere.error}
                                </div>
                              )}
                            </div>
                          ) : (
                            <div className="space-y-2">
                              <div className="flex items-center justify-between text-xs text-slate-400 font-mono">
                                <span>Engine: <strong className="text-emerald-300">{validationResult.s4hana.execution_engine}</strong>
                                  {validationResult.s4hana.query_status && <span className="text-slate-500"> · {validationResult.s4hana.query_status}</span>}
                                  {validationResult.s4hana.metadata_status && <span className="text-slate-500"> · metadata {validationResult.s4hana.metadata_status}</span>}
                                </span>
                                <button
                                  onClick={() => copyToClipboard(
                                    (validationResult.s4hana.queries?.length ?? 0) > 1
                                      ? JSON.stringify(validationResult.s4hana.queries, null, 2)
                                      : validationResult.s4hana.sql,
                                    'S/4HANA SQL',
                                  )}
                                  className="text-xs text-emerald-400 hover:underline flex items-center gap-1 cursor-pointer"
                                >
                                  <Copy className="w-3 h-3" />
                                  <span>Copy ABAP Open SQL</span>
                                </button>
                              </div>
                              <pre className="p-3.5 rounded-xl bg-slate-950 border border-slate-800/80 text-xs font-mono text-emerald-300 overflow-x-auto whitespace-pre-wrap leading-relaxed">
                                {(validationResult.s4hana.queries?.length ?? 0) > 1
                                  ? (validationResult.s4hana.queries ?? []).map((query: any) => (
                                    `-- ${query.source}\n${query.sql}`
                                  )).join('\n\n')
                                  : validationResult.s4hana.sql}
                              </pre>
                              {validationResult.s4hana.query_status === 'BLOCKED' && validationResult.s4hana.sql && (
                                <div className="p-2.5 rounded-lg bg-amber-950/30 border border-amber-500/30 text-amber-200 text-xs font-mono">
                                  Reconstruction draft only: transformation coverage is incomplete. This SQL is not executable or semantically verified.
                                </div>
                              )}
                              {validationResult.s4hana.error && (
                                <div className="p-2.5 rounded-lg bg-rose-950/30 border border-rose-500/30 text-rose-300 text-xs font-mono">
                                  S/4HANA ADT Notice: {validationResult.s4hana.error}
                                </div>
                              )}
                            </div>
                          )}
                        </div>

                        {/* Live Fetched Data Preview & Field Projections */}
                        <div className="p-5 rounded-2xl bg-[#0e1629] border border-slate-800 shadow-xl space-y-4">
                          <div className="flex flex-wrap items-center justify-between gap-3 border-b border-slate-800/80 pb-3">
                            <div className="flex items-center gap-2">
                              <Database className="w-4 h-4 text-cyan-400" />
                              <h4 className="font-bold text-xs text-white uppercase tracking-wider">
                                Live Fetched Data Preview & Field Projections
                              </h4>
                            </div>

                            <div className="flex items-center gap-1 bg-slate-950 p-1 rounded-lg border border-slate-800 text-[11px] font-mono">
                              <button
                                type="button"
                                onClick={() => setActiveDataPreviewTab('selected_fields')}
                                className={`px-2.5 py-1 rounded font-semibold cursor-pointer transition-colors ${
                                  activeDataPreviewTab === 'selected_fields'
                                    ? 'bg-purple-600 text-white shadow-sm'
                                    : 'text-slate-400 hover:text-slate-200'
                                }`}
                              >
                                Reconciliation Results
                              </button>
                              <button
                                type="button"
                                onClick={() => setActiveDataPreviewTab('datasphere')}
                                className={`px-2.5 py-1 rounded font-semibold cursor-pointer transition-colors ${
                                  activeDataPreviewTab === 'datasphere'
                                    ? 'bg-blue-600 text-white shadow-sm'
                                    : 'text-slate-400 hover:text-slate-200'
                                }`}
                              >
                                Datasphere Fetched Rows ({validationResult.datasphere.data?.length || 0})
                              </button>
                              <button
                                type="button"
                                onClick={() => setActiveDataPreviewTab('s4hana')}
                                className={`px-2.5 py-1 rounded font-semibold cursor-pointer transition-colors ${
                                  activeDataPreviewTab === 's4hana'
                                    ? 'bg-emerald-600 text-white shadow-sm'
                                    : 'text-slate-400 hover:text-slate-200'
                                }`}
                              >
                                S/4HANA CDS Calculated Rows ({validationResult.s4hana.data?.length || 0})
                              </button>
                            </div>
                          </div>

                          {/* Tab 0: Selected Fields Parity (Field Name Up -> Datasphere & S/4HANA Down -> Values) */}
                          {activeDataPreviewTab === 'selected_fields' && (
                            <div className="space-y-3">
                              <div className="flex items-center justify-between text-xs font-mono text-slate-400">
                                <span>Key-based reconciliation: <strong className="text-slate-900">Records merged by the selected composite key</strong></span>
                                <span className="rounded-full border border-slate-200 bg-white px-2.5 py-1 text-[11px] font-medium text-slate-600">
                                  {keyedDataComparison.keyFields.length} keys · {keyedDataComparison.rows.length} merged rows
                                </span>
                              </div>

                              <div className="overflow-x-auto rounded-xl border border-slate-200 bg-white shadow-sm">
                                <table className="w-full border-collapse text-left text-sm">
                                  <thead className="border-b border-slate-200 bg-slate-100 text-slate-700">
                                    {validationResult.grain_breakdown?.length ? (
                                      <>
                                    <tr>
                                      <th rowSpan={2} className="border-r border-slate-200 p-3 align-bottom text-left text-[10px] font-semibold uppercase tracking-wider text-slate-600">
                                        Reconciliation Grain ({validationResult.reconciliation_dimension})
                                      </th>
                                      {((validationResult.target_metrics && validationResult.target_metrics.length > 0)
                                        ? validationResult.target_metrics
                                        : [validationResult.target_metric]
                                      ).map((f) => (
                                        <th key={f} colSpan={2} className="border-r border-slate-200 bg-white p-2.5 text-center text-xs font-bold text-slate-800">
                                          {f}
                                        </th>
                                      ))}
                                      <th rowSpan={2} className="border-l border-slate-200 p-3 text-right align-bottom text-[10px] font-semibold uppercase tracking-wider text-slate-600">
                                        Delta
                                      </th>
                                      <th rowSpan={2} className="border-l border-slate-200 p-3 text-right align-bottom text-[10px] font-semibold uppercase tracking-wider text-slate-600">
                                        Variance %
                                      </th>
                                      <th rowSpan={2} className="border-l border-slate-200 p-3 text-center align-bottom text-[10px] font-semibold uppercase tracking-wider text-slate-600">
                                        Status
                                      </th>
                                    </tr>
                                    <tr className="border-b border-slate-200 text-[10px] uppercase tracking-wider">
                                      {((validationResult.target_metrics && validationResult.target_metrics.length > 0)
                                        ? validationResult.target_metrics
                                        : [validationResult.target_metric]
                                      ).map((f) => (
                                        <React.Fragment key={f}>
                                          <th className="border-r border-slate-200 bg-blue-50 p-2 text-right font-semibold text-blue-800">
                                            Datasphere
                                          </th>
                                          <th className="border-r border-slate-200 bg-emerald-50 p-2 text-right font-semibold text-emerald-800">
                                            S/4HANA CDS
                                          </th>
                                        </React.Fragment>
                                      ))}
                                    </tr>
                                      </>
                                    ) : keyedDataComparison.error ? (
                                      <tr>
                                        <th colSpan={Math.max(1, keyedDataComparison.keyFields.length + keyedDataComparison.fieldPairs.length * 2 + 1)} className="px-4 py-3 text-left text-[11px] font-semibold uppercase tracking-wide text-amber-800">
                                          Key-based merge unavailable · {keyedDataComparison.error}
                                        </th>
                                      </tr>
                                    ) : (
                                      <>
                                        <tr>
                                          {keyedDataComparison.keyFields.map((field) => <th key={field} rowSpan={2} className="border-r border-slate-200 px-3 py-3 text-left text-[11px] font-semibold uppercase text-slate-600">Key · {field}</th>)}
                                          {keyedDataComparison.fieldPairs.map((pair) => <th key={pair.datasphereField} colSpan={2} className="border-r border-slate-200 bg-white px-3 py-2.5 text-center font-mono text-xs font-semibold text-slate-800">{pair.datasphereField}</th>)}
                                          <th rowSpan={2} className="px-3 py-3 text-center text-[11px] font-semibold uppercase text-slate-600">Merge status</th>
                                        </tr>
                                        <tr className="border-t border-slate-200 text-[10px] font-semibold uppercase">
                                          {keyedDataComparison.fieldPairs.map((pair) => <React.Fragment key={pair.datasphereField}>
                                            <th className="border-r border-slate-200 bg-blue-50 px-3 py-2 text-right text-blue-800">Datasphere</th>
                                            <th className="border-r border-slate-200 bg-emerald-50 px-3 py-2 text-right text-emerald-800">S/4HANA CDS</th>
                                          </React.Fragment>)}
                                        </tr>
                                      </>
                                    )}
                                  </thead>
                                  <tbody className="divide-y divide-slate-200">
                                    {(!validationResult.grain_breakdown || validationResult.grain_breakdown.length === 0) ? (
                                      keyedDataComparison.error ? (
                                        <tr>
                                          <td colSpan={Math.max(1, keyedDataComparison.keyFields.length + keyedDataComparison.fieldPairs.length * 2 + 1)} className="bg-amber-50 px-4 py-8 text-center text-sm text-amber-900">
                                            {keyedDataComparison.error} Include the complete Datasphere and CDS key fields in both results, then rerun validation.
                                          </td>
                                        </tr>
                                      )
                                      : keyedDataComparison.rows.map((row) => {
                                        const statusClass = row.status === 'MATCH'
                                          ? 'border-emerald-200 bg-emerald-50 text-emerald-800'
                                          : row.status === 'DIFFERENT'
                                            ? 'border-amber-200 bg-amber-50 text-amber-900'
                                            : 'border-rose-200 bg-rose-50 text-rose-800';
                                        return (
                                          <tr key={row.key} className="odd:bg-white even:bg-slate-50/60">
                                            {row.keyValues.map((value, index) => <td key={`${row.key}-key-${index}`} className="whitespace-nowrap border-r border-slate-200 px-3 py-2.5 font-mono text-xs text-slate-700">{value == null ? '—' : String(value)}</td>)}
                                            {keyedDataComparison.fieldPairs.map(({ datasphereField, cdsField }) => (
                                              <React.Fragment key={`${row.key}-${datasphereField}`}>
                                                <td className="max-w-56 border-r border-slate-200 px-3 py-2.5 text-right font-mono text-xs text-blue-900">
                                                  {!row.datasphere ? '—' : row.datasphereRecords.map((record: Record<string, any>) => String(record[datasphereField] ?? '—')).join(' · ')}
                                                </td>
                                                <td className="max-w-56 border-r border-slate-200 px-3 py-2.5 text-right font-mono text-xs text-emerald-900">
                                                  {!row.cds ? '—' : row.cdsRecords.map((record: Record<string, any>) => String(record[cdsField] ?? '—')).join(' · ')}
                                                </td>
                                              </React.Fragment>
                                            ))}
                                            <td className="px-3 py-2 text-center">
                                              <span className={`inline-flex rounded-full border px-2 py-1 text-[10px] font-semibold ${statusClass}`}>{row.status}</span>
                                            </td>
                                          </tr>
                                        );
                                      })
                                    ) : (
                                      validationResult.grain_breakdown.map((r) => {
                                        const selFields = (validationResult.target_metrics && validationResult.target_metrics.length > 0)
                                          ? validationResult.target_metrics
                                          : [validationResult.target_metric];
                                        const pField = selFields[0];
                                        const pMetric = (r.metrics && r.metrics[pField])
                                          ? r.metrics[pField]
                                          : { datasphere_kpi: r.datasphere_kpi, s4hana_kpi: r.s4hana_kpi, delta: r.delta, variance_pct: r.variance_pct };
                                        const isRowMatch = r.status === 'MATCH';

                                        return (
                                          <tr key={r.grain_key} className="hover:bg-slate-800/40 transition-colors">
                                            <td className="p-3 font-semibold text-white border-r border-slate-800 font-mono">
                                              {r.grain_key}
                                            </td>
                                            {selFields.map((f) => {
                                              const mData = (r.metrics && r.metrics[f])
                                                ? r.metrics[f]
                                                : { datasphere_kpi: r.datasphere_kpi, s4hana_kpi: r.s4hana_kpi };
                                              return (
                                                <React.Fragment key={f}>
                                                  <td className="p-2.5 text-right font-mono text-blue-300 font-bold bg-blue-950/10 border-r border-slate-800/60">
                                                    {formatNum(mData.datasphere_kpi)}
                                                  </td>
                                                  <td className="p-2.5 text-right font-mono text-emerald-300 font-bold bg-emerald-950/10 border-r border-slate-800">
                                                    {formatNum(mData.s4hana_kpi)}
                                                  </td>
                                                </React.Fragment>
                                              );
                                            })}
                                            <td className={`p-3 text-right font-bold font-mono border-l border-slate-800/60 ${isRowMatch ? 'text-slate-400' : 'text-amber-400'}`}>
                                              {formatNum(r.delta)}
                                            </td>
                                            <td className={`p-3 text-right font-mono border-l border-slate-800/60 ${isRowMatch ? 'text-slate-400' : 'text-amber-400'}`}>
                                              {typeof r.variance_pct === 'number' ? `${r.variance_pct.toFixed(2)}%` : '-'}
                                            </td>
                                            <td className="p-3 text-center border-l border-slate-800/60">
                                              <span
                                                className={`px-2 py-0.5 rounded text-[10px] font-bold ${
                                                  isRowMatch
                                                    ? 'bg-emerald-950/80 text-emerald-400 border border-emerald-500/30'
                                                    : 'bg-amber-950/80 text-amber-300 border border-amber-500/30'
                                                }`}
                                              >
                                                {isRowMatch ? 'MATCH' : 'DISCREPANCY'}
                                              </span>
                                            </td>
                                          </tr>
                                        );
                                      })
                                    )}
                                  </tbody>
                                </table>
                              </div>
                            </div>
                          )}

                          {/* Tab 1: Datasphere Fetched Rows */}
                          {activeDataPreviewTab === 'datasphere' && (
                            <div className="space-y-2">
                              <div className="flex items-center justify-between text-xs font-mono text-slate-400">
                                <span>Source: <strong className="text-blue-300">{validationResult.datasphere.view}</strong> ({validationResult.datasphere.execution_engine})</span>
                                <span className="text-[11px] bg-blue-950/80 text-blue-300 border border-blue-500/30 px-2 py-0.5 rounded">
                                  {validationResult.datasphere.query_status?.startsWith('SUCCEEDED')
                                    ? `${validationResult.datasphere.data?.length ?? 0} returned rows`
                                    : 'Rows unavailable'}
                                  {' • '}{validationResult.datasphere.query_status || 'status unavailable'}
                                  {validationResult.datasphere.truncated ? ' • TRUNCATED' : ''}
                                  {' • '}{validationResult.datasphere.latency_seconds.toFixed(2)}s
                                </span>
                              </div>
                              {(!validationResult.datasphere.data || validationResult.datasphere.data.length === 0) ? (
                                <div className="p-6 text-center text-xs text-slate-500 italic bg-slate-950 rounded-xl border border-slate-800">
                                  {validationResult.datasphere.query_status === 'SUCCEEDED_ZERO_ROWS'
                                    ? 'Query succeeded and returned zero rows.'
                                    : validationResult.datasphere.query_status?.startsWith('SUCCEEDED')
                                      ? 'Query succeeded; no preview rows are available.'
                                      : `No result rows are available because the query is ${validationResult.datasphere.query_status || 'not executed'}.`}
                                  {validationResult.datasphere.columns?.length
                                    ? ` Columns: ${validationResult.datasphere.columns.map(column => column.name).filter(Boolean).join(', ')}.`
                                    : ''}
                                </div>
                              ) : (
                                <div className="overflow-x-auto rounded-xl border border-slate-800/80 max-h-72">
                                  <table className="w-full text-left text-xs font-mono">
                                    <thead className="bg-slate-950 text-slate-300 uppercase text-[10px] tracking-wider border-b border-slate-800 sticky top-0">
                                      <tr>
                                        {Object.keys(validationResult.datasphere.data[0]).map((col) => (
                                          <th key={col} className="p-2.5 whitespace-nowrap">{col}</th>
                                        ))}
                                      </tr>
                                    </thead>
                                    <tbody className="divide-y divide-slate-800/60 bg-slate-900/40">
                                      {validationResult.datasphere.data.map((row, idx) => (
                                        <tr key={idx} className="hover:bg-slate-800/40 transition-colors">
                                          {Object.keys(validationResult.datasphere.data![0]).map((col) => (
                                            <td key={col} className="p-2.5 whitespace-nowrap text-slate-200">
                                              {typeof row[col] === 'number' ? formatNum(row[col]) : String(row[col] ?? '')}
                                            </td>
                                          ))}
                                        </tr>
                                      ))}
                                    </tbody>
                                  </table>
                                </div>
                              )}
                            </div>
                          )}

                          {/* Tab 2: S/4HANA CDS Calculated Rows */}
                          {activeDataPreviewTab === 's4hana' && (
                            <div className="space-y-2">
                              <div className="flex items-center justify-between text-xs font-mono text-slate-400">
                                <span>Source: <strong className="text-emerald-300">{validationResult.s4hana.cds_view}</strong> ({validationResult.s4hana.execution_engine})</span>
                                <span className="text-[11px] bg-emerald-950/80 text-emerald-300 border border-emerald-500/30 px-2 py-0.5 rounded">
                                  {validationResult.s4hana.query_status?.startsWith('SUCCEEDED')
                                    ? `${validationResult.s4hana.data?.length ?? 0} returned rows`
                                    : validationResult.s4hana.query_status === 'PARTIAL_FAILURE'
                                      ? `${validationResult.s4hana.data?.length ?? 0} partial rows`
                                    : 'Rows unavailable'}
                                  {validationResult.s4hana.total_rows !== undefined && validationResult.s4hana.total_rows !== null
                                    ? ` of ${validationResult.s4hana.total_rows} total` : ''}
                                  {' • '}{validationResult.s4hana.query_status || 'status unavailable'}
                                  {validationResult.s4hana.truncated ? ' • TRUNCATED' : ''}
                                  {' • '}{validationResult.s4hana.latency_seconds.toFixed(2)}s
                                </span>
                              </div>
                              {(!validationResult.s4hana.data || validationResult.s4hana.data.length === 0) ? (
                                <div className="p-6 text-center text-xs text-slate-500 italic bg-slate-950 rounded-xl border border-slate-800">
                                  {validationResult.s4hana.query_status === 'SUCCEEDED_ZERO_ROWS'
                                    ? 'Query succeeded and returned zero rows.'
                                    : validationResult.s4hana.query_status?.startsWith('SUCCEEDED')
                                      ? 'Query succeeded; no preview rows are available.'
                                      : `No result rows are available because the query is ${validationResult.s4hana.query_status || 'not executed'}.`}
                                  {validationResult.s4hana.columns?.length
                                    ? ` Columns: ${validationResult.s4hana.columns.map(column => column.name).filter(Boolean).join(', ')}.`
                                    : ''}
                                </div>
                              ) : (
                                <div className="overflow-x-auto rounded-xl border border-slate-800/80 max-h-72">
                                  <table className="w-full text-left text-xs font-mono">
                                    <thead className="bg-slate-950 text-slate-300 uppercase text-[10px] tracking-wider border-b border-slate-800 sticky top-0">
                                      <tr>
                                        {Object.keys(validationResult.s4hana.data[0]).map((col) => (
                                          <th key={col} className="p-2.5 whitespace-nowrap">{col}</th>
                                        ))}
                                      </tr>
                                    </thead>
                                    <tbody className="divide-y divide-slate-800/60 bg-slate-900/40">
                                      {validationResult.s4hana.data.map((row, idx) => (
                                        <tr key={idx} className="hover:bg-slate-800/40 transition-colors">
                                          {Object.keys(validationResult.s4hana.data![0]).map((col) => (
                                            <td key={col} className="p-2.5 whitespace-nowrap text-slate-200">
                                              {typeof row[col] === 'number' ? formatNum(row[col]) : String(row[col] ?? '')}
                                            </td>
                                          ))}
                                        </tr>
                                      ))}
                                    </tbody>
                                  </table>
                                </div>
                              )}
                            </div>
                          )}

                          {activeDataPreviewTab === 's4source' && (() => {
                            const source = stagingResults.find((item) => item.source.toLowerCase() === (selectedSourceCds || '').toLowerCase());
                            const sourceError = stagingErrors.find((item) => item.source.toLowerCase() === (selectedSourceCds || '').toLowerCase());
                            return (
                              <div className="space-y-3">
                                <div className="flex flex-wrap items-center justify-between gap-2 text-xs">
                                  <span className="text-slate-500">Configured S/4HANA ADT · CDS entity <strong className="text-emerald-700">{selectedSourceCds || 'No CDS source selected'}</strong></span>
                                  {source && <span className="rounded border border-emerald-200 bg-emerald-50 px-2 py-1 text-emerald-700">Showing {source.preview.length.toLocaleString()} of {source.total_rows.toLocaleString()} ADT rows</span>}
                                </div>
                                {stagingCds && !source && <p className="rounded-lg border border-blue-200 bg-blue-50 px-3 py-2 text-xs text-blue-700">Retrieving source rows from the configured S/4HANA ADT connection…</p>}
                                {sourceError && <p className="rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-xs text-rose-700">ADT source retrieval failed: {sourceError.error}</p>}
                                {!source && !sourceError && !stagingCds && <p className="text-xs text-slate-500">No direct ADT source extract is available yet. Select the Datasphere view to retrieve its matched CDS source.</p>}
                                {source && source.preview.length > 0 && (
                                  <div className="max-h-[28rem] overflow-auto rounded-xl border border-slate-200 bg-white">
                                    <table className="w-full border-collapse text-left text-xs font-mono">
                                      <thead className="sticky top-0 bg-slate-100 text-slate-600"><tr>{source.columns.map((column) => <th key={column.name} className="whitespace-nowrap border-b border-slate-200 px-3 py-2">{column.name}</th>)}</tr></thead>
                                      <tbody>{source.preview.map((row, rowIndex) => <tr key={rowIndex} className="odd:bg-white even:bg-slate-50">{source.columns.map((column) => <td key={column.name} className="max-w-xs truncate border-b border-slate-100 px-3 py-2 text-slate-700" title={String(row[column.name] ?? '')}>{row[column.name] == null ? '—' : String(row[column.name])}</td>)}</tr>)}</tbody>
                                    </table>
                                  </div>
                                )}
                              </div>
                            );
                          })()}

                          {/* Tab 3: Field Projections & Formulas */}
                          {activeDataPreviewTab === 'projections' && validationResult.field_projections && (
                            <div className="space-y-4">
                              <div className="grid grid-cols-1 md:grid-cols-3 gap-3 text-xs font-mono">
                                <div className="p-3 bg-slate-950 rounded-xl border border-slate-800 space-y-1">
                                  <span className="text-slate-400 text-[10px] uppercase font-bold">Raw CDS Fields Total</span>
                                  <div className="text-emerald-400 font-bold text-sm">
                                    {validationResult.field_projections.raw_cds_fields_count} Columns in {validationResult.field_projections.raw_cds_view}
                                  </div>
                                </div>
                                <div className="p-3 bg-slate-950 rounded-xl border border-slate-800 space-y-1">
                                  <span className="text-slate-400 text-[10px] uppercase font-bold">Projected & Matched</span>
                                  <div className="text-blue-400 font-bold text-sm">
                                    {validationResult.field_projections.projected_fields?.length || 0} Filtered In for Final View
                                  </div>
                                </div>
                                <div className="p-3 bg-slate-950 rounded-xl border border-slate-800 space-y-1">
                                  <span className="text-slate-400 text-[10px] uppercase font-bold">Calculated Measures</span>
                                  <div className="text-purple-400 font-bold text-sm">
                                    {Object.keys(validationResult.field_projections.calculated_fields || {}).length} Formulas Generated
                                  </div>
                                </div>
                              </div>

                              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                                {/* Projected Fields */}
                                <div className="p-3.5 bg-slate-950 rounded-xl border border-slate-800 space-y-2">
                                  <div className="text-xs font-bold text-emerald-300 flex items-center gap-1.5">
                                    <CheckCircle2 className="w-3.5 h-3.5 text-emerald-400" />
                                    <span>Projected Attributes (Filtered from Raw CDS Source)</span>
                                  </div>
                                  <div className="flex flex-wrap gap-1.5 max-h-40 overflow-y-auto">
                                    {validationResult.field_projections.projected_fields?.map((f) => (
                                      <span key={f} className="text-[11px] font-mono px-2 py-0.5 rounded bg-emerald-950/60 border border-emerald-500/30 text-emerald-200">
                                        {f}
                                      </span>
                                    ))}
                                  </div>
                                </div>

                                {/* Calculated Fields Formulas */}
                                <div className="p-3.5 bg-slate-950 rounded-xl border border-slate-800 space-y-2">
                                  <div className="text-xs font-bold text-purple-300 flex items-center gap-1.5">
                                    <FileCode2 className="w-3.5 h-3.5 text-purple-400" />
                                    <span>Calculated Fields & Dynamic Prompt Rules</span>
                                  </div>
                                  <div className="space-y-1.5 max-h-40 overflow-y-auto">
                                    {Object.entries(validationResult.field_projections.calculated_fields || {}).map(([metric, formula]) => (
                                      <div key={metric} className="p-2 rounded bg-purple-950/40 border border-purple-500/30 text-xs font-mono">
                                        <div className="font-bold text-purple-200">{metric}</div>
                                        <div className="text-[11px] text-slate-300 mt-0.5">{formula}</div>
                                      </div>
                                    ))}
                                  </div>
                                </div>
                              </div>

                              {/* Filtered Out Raw Columns */}
                              {validationResult.field_projections.filtered_out_fields && validationResult.field_projections.filtered_out_fields.length > 0 && (
                                <div className="p-3 bg-slate-950/60 rounded-xl border border-slate-800/80 text-xs font-mono space-y-1.5">
                                  <div className="text-slate-400 text-[11px] font-bold">
                                    Filtered Out Raw CDS Columns ({validationResult.field_projections.filtered_out_count} total pruned):
                                  </div>
                                  <div className="flex flex-wrap gap-1 opacity-70">
                                    {validationResult.field_projections.filtered_out_fields.map((f) => (
                                      <span key={f} className="text-[10px] px-1.5 py-0.5 rounded bg-slate-900 text-slate-400 border border-slate-800">
                                        {f}
                                      </span>
                                    ))}
                                  </div>
                                </div>
                              )}
                            </div>
                          )}
                        </div>

                        {activeDataPreviewTab === 'coverage' && (
                          <div className="p-4 rounded-xl bg-slate-950/70 border border-slate-800 space-y-3">
                            <div className="flex items-center justify-between text-xs font-mono">
                              <span className="text-slate-300 font-bold">Semantic equivalence coverage</span>
                              <span className={
                                (validationResult.reconstruction?.transformation_coverage?.overall_status || 'SEMANTIC_EQUIVALENCE_UNVERIFIED') === 'VERIFIED'
                                  ? 'text-emerald-300' : 'text-amber-300'
                              }>
                                {validationResult.reconstruction?.transformation_coverage?.overall_status || 'SEMANTIC_EQUIVALENCE_UNVERIFIED'}
                              </span>
                            </div>
                            {(validationResult.reconstruction?.transformation_coverage?.items || [])
                              .filter((item) => item.status !== 'VERIFIED')
                              .map((item, index) => (
                                <div
                                  key={item.transformation_id || `${item.transformation}-${index}`}
                                  className="p-3 rounded-lg bg-amber-950/20 border border-amber-500/30 space-y-1.5 text-xs"
                                >
                                  <div className="flex flex-wrap items-center gap-2 font-mono">
                                    <span className="font-bold text-amber-200">{item.transformation}</span>
                                    <span className="text-amber-300">{item.state || 'TRANSLATION_PENDING'}</span>
                                    {item.transformation_id && <span className="text-slate-400">{item.transformation_id}</span>}
                                  </div>
                                  {item.datasphere_expression !== undefined && (
                                    <code className="block break-words text-slate-200">
                                      Datasphere: {typeof item.datasphere_expression === 'string'
                                        ? item.datasphere_expression
                                        : JSON.stringify(item.datasphere_expression)}
                                    </code>
                                  )}
                                  <div className="text-slate-400">
                                    Source: {item.source_object || item.owner || 'not identified'}
                                    {item.source_fields?.length ? ` · fields: ${item.source_fields.join(', ')}` : ''}
                                    {` · metadata: ${item.metadata_status || 'not confirmed'}`}
                                    {item.metadata_source ? ` (${item.metadata_source})` : ''}
                                  </div>
                                  <div className="text-amber-100">{item.diagnostic || 'An equivalent ABAP Open SQL expression has not been verified.'}</div>
                                  <div className="text-slate-400">
                                    Resolve the source mapping or translation, then regenerate the plan. This query cannot be approved for S/4HANA execution while this item remains unresolved.
                                  </div>
                                </div>
                              ))}
                            <pre className="p-3 rounded-lg bg-black/40 border border-slate-800 text-[10px] font-mono text-amber-100 overflow-auto max-h-80 whitespace-pre-wrap">
                              {JSON.stringify(
                                validationResult.reconstruction?.transformation_coverage || {
                                  diagnostic: 'Coverage verification was not completed; no semantic-equivalence claim is made.',
                                  transformations: validationResult.architecture_metadata?.transformation_ir?.unsupported_constructs || [],
                                },
                                null,
                                2,
                              )}
                            </pre>
                          </div>
                        )}

                        {/* Grain Breakdown Reconciliation Table */}
                        {validationResult.grain_breakdown && validationResult.grain_breakdown.length > 0 && (
                          <div className="p-5 rounded-2xl bg-[#0e1629] border border-slate-800 shadow-xl space-y-4">
                            <div className="flex flex-wrap items-center justify-between gap-3 border-b border-slate-800/80 pb-3">
                              <div className="flex items-center gap-2">
                                <TableIcon className="w-4 h-4 text-purple-400" />
                                <h4 className="font-bold text-xs text-white uppercase tracking-wider">
                                  Grain-by-Grain Reconciliation Breakdown ({validationResult.reconciliation_dimension})
                                </h4>
                              </div>

                              <div className="flex flex-wrap items-center gap-2">
                                {/* Interactive Measure Selector Tab for Table */}
                                {validationResult.target_metrics && validationResult.target_metrics.length > 1 && (
                                  <div className="flex items-center gap-1 bg-slate-950 p-1 rounded-lg border border-slate-800 text-[11px] font-mono">
                                    <span className="text-slate-400 px-1 text-[10px] uppercase font-bold">Measure:</span>
                                    {validationResult.target_metrics.map((m) => {
                                      const isCurr = (activeTableMetric === m || (activeTableMetric === 'all' && m === validationResult.target_metrics?.[0]));
                                      return (
                                        <button
                                          key={m}
                                          type="button"
                                          onClick={() => setActiveTableMetric(m)}
                                          className={`px-2 py-0.5 rounded font-semibold cursor-pointer transition-colors ${
                                            isCurr
                                              ? 'bg-purple-600 text-white shadow-sm'
                                              : 'text-slate-400 hover:text-slate-200 hover:bg-slate-800/60'
                                          }`}
                                        >
                                          {m}
                                        </button>
                                      );
                                    })}
                                  </div>
                                )}

                                <div className="relative">
                                  <Search className="w-3 h-3 text-slate-400 absolute left-2.5 top-2" />
                                  <input
                                    type="text"
                                    placeholder="Filter grain..."
                                    value={breakdownSearch}
                                    onChange={(e) => setBreakdownSearch(e.target.value)}
                                    className="bg-slate-950 border border-slate-800 rounded-lg pl-7 pr-2.5 py-1 text-xs text-slate-200 font-mono focus:outline-none focus:border-blue-500"
                                  />
                                </div>

                                <div className="flex items-center gap-1 bg-slate-950 p-1 rounded-lg border border-slate-800 text-[11px] font-mono">
                                  <button
                                    onClick={() => setBreakdownFilter('all')}
                                    className={`px-2 py-0.5 rounded ${breakdownFilter === 'all' ? 'bg-blue-600 text-white font-bold' : 'text-slate-400 hover:text-white'}`}
                                  >
                                    All ({validationResult.grain_breakdown.length})
                                  </button>
                                  <button
                                    onClick={() => setBreakdownFilter('discrepancies')}
                                    className={`px-2 py-0.5 rounded ${breakdownFilter === 'discrepancies' ? 'bg-amber-600 text-white font-bold' : 'text-slate-400 hover:text-white'}`}
                                  >
                                    Discrepancies ({validationResult.grain_breakdown.filter(r => r.status !== 'MATCH').length})
                                  </button>
                                  <button
                                    onClick={() => setBreakdownFilter('matches')}
                                    className={`px-2 py-0.5 rounded ${breakdownFilter === 'matches' ? 'bg-emerald-600 text-white font-bold' : 'text-slate-400 hover:text-white'}`}
                                  >
                                    Matches ({validationResult.grain_breakdown.filter(r => r.status === 'MATCH').length})
                                  </button>
                                </div>

                                <button
                                  onClick={handleExportBreakdownCSV}
                                  className="px-2.5 py-1 rounded-lg bg-slate-800 hover:bg-slate-700 text-xs text-slate-200 font-semibold flex items-center gap-1.5 cursor-pointer transition-colors"
                                  title="Export reconciliation table to CSV"
                                >
                                  <Download className="w-3.5 h-3.5" />
                                  <span>Export CSV</span>
                                </button>
                              </div>
                            </div>

                            {/* 2-Tier Header Table: Field Name Up -> Datasphere & S/4HANA Down -> Values (Only for Selected Fields) */}
                            {(() => {
                              const selFieldsToDisplay = (
                                validationResult.target_metrics && validationResult.target_metrics.length > 0
                              )
                                ? (activeTableMetric !== 'all' && validationResult.target_metrics.includes(activeTableMetric))
                                  ? [activeTableMetric]
                                  : validationResult.target_metrics
                                : [validationResult.target_metric];

                              return (
                                <div className="overflow-x-auto rounded-xl border border-slate-800/80">
                                  <table className="w-full text-left text-xs font-mono border-collapse">
                                    <thead className="bg-slate-950 text-slate-300 border-b border-slate-800">
                                      {/* Tier 1 Header: Field Name Up */}
                                      <tr>
                                        <th rowSpan={2} className="p-3 font-semibold text-slate-400 uppercase text-[10px] tracking-wider border-r border-slate-800 align-bottom">
                                          {validationResult.reconciliation_dimension}
                                        </th>
                                        {selFieldsToDisplay.map((field) => (
                                          <th
                                            key={field}
                                            colSpan={2}
                                            className="p-2.5 text-center font-bold text-white text-xs border-r border-slate-800 bg-slate-900/90"
                                          >
                                            <div className="flex items-center justify-center gap-1.5 font-mono text-purple-300">
                                              <Zap className="w-3.5 h-3.5 text-amber-400" />
                                              <span className="font-extrabold uppercase">{field}</span>
                                            </div>
                                          </th>
                                        ))}
                                        <th rowSpan={2} className="p-3 text-right uppercase text-[10px] text-slate-400 tracking-wider align-bottom border-l border-slate-800">
                                          Delta
                                        </th>
                                        <th rowSpan={2} className="p-3 text-right uppercase text-[10px] text-slate-400 tracking-wider align-bottom border-l border-slate-800">
                                          Variance %
                                        </th>
                                        <th rowSpan={2} className="p-3 text-center uppercase text-[10px] text-slate-400 tracking-wider align-bottom border-l border-slate-800">
                                          DS / S4 Count
                                        </th>
                                        <th rowSpan={2} className="p-3 text-center uppercase text-[10px] text-slate-400 tracking-wider align-bottom border-l border-slate-800">
                                          Status
                                        </th>
                                      </tr>
                                      {/* Tier 2 Header: Datasphere & S/4HANA CDS Down */}
                                      <tr className="bg-slate-950 text-[10px] uppercase tracking-wider border-b border-slate-800">
                                        {selFieldsToDisplay.map((field) => (
                                          <React.Fragment key={field}>
                                            <th className="p-2 text-right text-blue-300 bg-blue-950/25 border-r border-slate-800/60 font-mono">
                                              Datasphere
                                            </th>
                                            <th className="p-2 text-right text-emerald-300 bg-emerald-950/25 border-r border-slate-800 font-mono">
                                              S/4HANA CDS
                                            </th>
                                          </React.Fragment>
                                        ))}
                                      </tr>
                                    </thead>
                                    <tbody className="divide-y divide-slate-800/60 bg-slate-900/30">
                                      {filteredBreakdownRows.length === 0 ? (
                                        <tr>
                                          <td colSpan={(selFieldsToDisplay.length * 2) + 5} className="p-6 text-center text-slate-500 italic">
                                            No grain rows match current filter.
                                          </td>
                                        </tr>
                                      ) : (
                                        filteredBreakdownRows.map((r) => {
                                          const pField = selFieldsToDisplay[0];
                                          const pData = (r.metrics && r.metrics[pField])
                                            ? r.metrics[pField]
                                            : {
                                                datasphere_kpi: r.datasphere_kpi,
                                                s4hana_kpi: r.s4hana_kpi,
                                                delta: r.delta,
                                                variance_pct: r.variance_pct
                                              };
                                          const isRowMatch = pData.delta === 0;

                                          return (
                                            <tr key={r.grain_key} className="hover:bg-slate-800/30 transition-colors">
                                              <td className="p-3 font-semibold text-white border-r border-slate-800 font-mono">
                                                {r.grain_key}
                                              </td>
                                              {/* Values: Datasphere Value & S/4HANA CDS Value for each selected field */}
                                              {selFieldsToDisplay.map((field) => {
                                                const mData = (r.metrics && r.metrics[field])
                                                  ? r.metrics[field]
                                                  : {
                                                      datasphere_kpi: r.datasphere_kpi,
                                                      s4hana_kpi: r.s4hana_kpi
                                                    };
                                                return (
                                                  <React.Fragment key={field}>
                                                    <td className="p-2.5 text-right font-mono text-blue-300 font-bold bg-blue-950/10 border-r border-slate-800/60">
                                                      {formatNum(mData.datasphere_kpi)}
                                                    </td>
                                                    <td className="p-2.5 text-right font-mono text-emerald-300 font-bold bg-emerald-950/10 border-r border-slate-800">
                                                      {formatNum(mData.s4hana_kpi)}
                                                    </td>
                                                  </React.Fragment>
                                                );
                                              })}
                                              <td className={`p-3 text-right font-bold font-mono border-l border-slate-800/60 ${isRowMatch ? 'text-slate-400' : 'text-amber-400'}`}>
                                                {formatNum(pData.delta)}
                                              </td>
                                              <td className={`p-3 text-right font-mono border-l border-slate-800/60 ${isRowMatch ? 'text-slate-400' : 'text-amber-400'}`}>
                                                {pData.variance_pct.toFixed(2)}%
                                              </td>
                                              <td className="p-3 text-center text-slate-400 border-l border-slate-800/60">
                                                {r.datasphere_count} / {r.s4hana_count}
                                              </td>
                                              <td className="p-3 text-center border-l border-slate-800/60">
                                                <span
                                                  className={`px-2 py-0.5 rounded text-[10px] font-bold ${
                                                    isRowMatch
                                                      ? 'bg-emerald-950/80 text-emerald-400 border border-emerald-500/30'
                                                      : 'bg-amber-950/80 text-amber-300 border border-amber-500/30'
                                                  }`}
                                                >
                                                  {isRowMatch ? 'MATCH' : 'DISCREPANCY'}
                                                </span>
                                              </td>
                                            </tr>
                                          );
                                        })
                                      )}
                                    </tbody>
                                  </table>
                                </div>
                              );
                            })()}
                          </div>
                        )}

                        {/* Root-Cause Discrepancy Triage */}
                        {validationResult.triage && (
                          <div className="p-5 rounded-2xl bg-amber-950/15 border border-amber-500/30 text-amber-200 shadow-lg space-y-3">
                            <div className="flex items-center gap-2 font-bold text-xs uppercase tracking-wider text-amber-400">
                              <AlertTriangle className="w-4 h-4" />
                              <span>Automated Reconciliation Triage & Root Cause Analysis</span>
                            </div>
                            <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-xs font-mono">
                              <div>
                                <span className="text-slate-400 block">Diagnosed Root Cause:</span>
                                <strong className="text-white">{validationResult.triage.root_cause}</strong>
                              </div>
                              <div>
                                <span className="text-slate-400 block">Cardinality Status:</span>
                                <strong className="text-white">{validationResult.triage.cardinality_status}</strong>
                              </div>
                              <div>
                                <span className="text-slate-400 block">Orphan Keys Detected:</span>
                                <strong className="text-white">{validationResult.triage.orphan_keys_count}</strong>
                              </div>
                              <div>
                                <span className="text-slate-400 block">Recommended Remediation Action:</span>
                                <strong className="text-amber-300">{validationResult.triage.action_recommended}</strong>
                              </div>
                            </div>
                          </div>
                        )}

                      </div>
                    ) : (
                      <div className="p-12 text-center rounded-2xl border border-dashed border-slate-800 bg-slate-950/20 space-y-3">
                        <ShieldCheck className="w-10 h-10 text-emerald-500/40 mx-auto" />
                        <h4 className="font-bold text-sm text-white">Validation Ready for {selectedViewName}</h4>
                        <p className="text-xs text-slate-400 max-w-lg mx-auto">
                          Click "Run Data Validation" or "Execute Validation" above to test mathematical parity and row-level grain alignment directly between SAP HANA Cloud and S/4HANA Eclipse ADT with zero data leakage.
                        </p>
                      </div>
                    )}

                  </div>
                )}

                {/* TAB 2: LINEAGE & UPSTREAM SOURCES */}
                {activeTab === 'lineage' && viewDetails && (
                  <div className="space-y-5 max-w-6xl mx-auto">
                    <div className="rounded-xl border border-slate-200 bg-white px-4 py-3 shadow-sm">
                      <div className="mb-2 flex items-center justify-between gap-3">
                        <div className="text-sm font-semibold text-slate-800">CDS replication workflow</div>
                        <div className="text-xs text-slate-500">Step {workflowStep + 1} of 6</div>
                      </div>
                      <div className="h-1.5 overflow-hidden rounded-full bg-slate-100">
                        <div className="h-full rounded-full bg-blue-600 transition-all" style={{ width: `${((workflowStep + 1) / 6) * 100}%` }} />
                      </div>
                      <div className="mt-2 text-xs text-slate-500">{(selectedCdsForStaging.length > 1 ? ['Review lineage', 'Resolve model sources', 'Stage all CDS sources', 'Join staged source datasets', 'Apply calculated fields', 'Create final model projection'] : ['Review lineage', 'Resolve model sources', 'Stage CDS source', 'Project single source', 'Apply calculated fields', 'Create final model projection'])[workflowStep]}</div>
                      <div className="mt-3 grid grid-cols-2 gap-2 md:grid-cols-6">
                        {(selectedCdsForStaging.length > 1 ? ['Model metadata', 'Resolve sources', 'Stage CDS data', 'Join and project', 'Apply calculations', 'Final projection'] : ['Model metadata', 'Resolve source', 'Stage CDS data', 'Project source', 'Apply calculations', 'Final projection']).map((label, index) => (
                          <div key={label} className={`rounded-lg border px-2 py-2 text-left text-[11px] font-semibold transition-colors ${workflowComplete || index < workflowStep ? 'border-emerald-200 bg-emerald-50 text-emerald-800' : index === workflowStep ? 'border-blue-300 bg-blue-50 text-blue-800' : 'border-slate-200 bg-slate-50 text-slate-400'}`}>
                            <span className="mr-1 font-mono">{workflowComplete || index < workflowStep ? '✓' : index === workflowStep && workflowRunning ? '…' : index + 1}</span>{label}
                          </div>
                        ))}
                      </div>
                      <div className={`rounded-lg px-3 py-2 text-xs font-medium ${workflowComplete ? 'bg-emerald-50 text-emerald-800' : workflowRunning ? 'bg-blue-50 text-blue-800' : 'bg-rose-50 text-rose-800'}`}>
                        {workflowComplete ? 'All workflow steps completed. The automatic validation report is ready below.' : workflowRunning ? 'Running the complete metadata, source retrieval, projection, and validation workflow…' : 'The workflow stopped before completion. See the error details above.'}
                      </div>
                    </div>
                    
                    {/* Visual Lineage Pipeline */}
                    {workflowStep === 0 && <div id="workflow-lineage" className="p-6 rounded-2xl bg-[#0e1629] border border-slate-800 shadow-xl space-y-4">
                      <div className="flex items-center justify-between border-b border-slate-800/80 pb-3">
                        <div className="flex items-center gap-2">
                          <GitBranch className="w-4 h-4 text-blue-400" />
                          <h3 className="font-bold text-sm text-white">Datasphere Lineage Analysis</h3>
                        </div>
                        <span className="text-[11px] font-mono text-slate-400">
                          {viewDetails.lineage_chain?.length || 0} stages in chain
                        </span>
                      </div>

                      {viewDetails.lineage_analysis?.status !== 'available' && (
                        <p className="text-xs text-amber-300">{viewDetails.lineage_analysis?.message || 'Lineage is not exposed in the current tenant response.'}</p>
                      )}
                      {/* Interactive Step Chain Cards */}
                      <div className="grid grid-cols-1 md:grid-cols-5 gap-3 pt-2">
                        {viewDetails.lineage_chain?.map((step, idx) => {
                          const isLast = idx === (viewDetails.lineage_chain?.length || 0) - 1;
                          const isSelectedStep = activeLineageStep?.step === step.step;

                          return (
                            <div
                              key={step.step}
                              onClick={() => setActiveLineageStep(step)}
                              className={`p-3.5 rounded-xl border text-xs cursor-pointer transition-all flex flex-col justify-between gap-3 relative ${
                                isSelectedStep
                                  ? 'bg-blue-600/20 border-blue-500 text-white shadow-lg shadow-blue-500/20 ring-1 ring-blue-500/50'
                                  : 'bg-slate-950/70 border-slate-800/80 text-slate-300 hover:bg-slate-800/50 hover:border-slate-700'
                              }`}
                            >
                              <div className="flex items-center justify-between">
                                <span className="w-5 h-5 rounded-full bg-blue-950 text-blue-400 border border-blue-500/40 text-[10px] font-mono font-bold flex items-center justify-center">
                                  {step.step}
                                </span>
                                <span className="text-[9px] font-mono font-bold uppercase px-1.5 py-0.5 rounded bg-slate-900 border border-slate-800 text-slate-400">
                                  {step.space}
                                </span>
                              </div>

                              <div className="space-y-1">
                                <div className="font-mono font-bold text-white text-xs truncate" title={step.name}>
                                  {step.name}
                                </div>
                                <div className="text-[10px] text-slate-400 leading-snug line-clamp-2" title={step.role}>
                                  {step.role}
                                </div>
                              </div>

                              <div className="text-[10px] font-mono text-blue-400 flex items-center justify-between pt-1 border-t border-slate-800/60">
                                <span>{step.type.replace('_', ' ').toUpperCase()}</span>
                                {!isLast && <ArrowRight className="w-3 h-3 text-slate-600" />}
                              </div>
                            </div>
                          );
                        })}
                      </div>

                      {/* Active Step Inspector Card */}
                      {activeLineageStep && (
                        <div className="p-4 rounded-xl bg-slate-950/80 border border-blue-500/30 text-xs font-mono space-y-2 mt-4">
                          <div className="flex items-center justify-between text-blue-400 font-bold border-b border-slate-800 pb-2">
                            <span>Lineage Node Details: Step {activeLineageStep.step} - {activeLineageStep.name}</span>
                            <span className="text-slate-400">{activeLineageStep.space}</span>
                          </div>
                          <div className="grid grid-cols-1 md:grid-cols-3 gap-3 text-slate-300">
                            <div>
                              <span className="text-slate-500 block">Object Type:</span>
                              <strong>{activeLineageStep.type}</strong>
                            </div>
                            <div>
                              <span className="text-slate-500 block">Architecture Role:</span>
                              <strong>{activeLineageStep.role}</strong>
                            </div>
                            <div>
                              <span className="text-slate-500 block">Matched S/4HANA CDS:</span>
                              <strong className="text-emerald-400">{activeLineageStep.matched_cds_view || 'Not reported by Datasphere'}</strong>
                            </div>
                          </div>
                        </div>
                      )}
                    </div>}

                    {workflowStep === 0 && <div className="text-right text-xs text-slate-500">Model metadata is processed automatically after selection.</div>}

                    {/* Upstream Sources Section */}
                    {workflowStep > 0 && <div id="workflow-sources" className="p-6 rounded-2xl bg-[#0e1629] border border-slate-800 shadow-xl space-y-4">
                      {workflowStep === 1 && <>
                      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-slate-800/80 pb-3">
                        <div className="flex items-center gap-2">
                          <Boxes className="w-4 h-4 text-emerald-400" />
                          <h3 className="font-bold text-sm text-white">CDS Source Datasets for {selectedViewName}</h3>
                        </div>
                        <div className="flex items-center gap-3">
                          <span className="text-[11px] font-mono text-slate-400">
                            {new Set((viewDetails.sources || []).map((source) => source.matched_cds_view).filter(Boolean)).size} resolved sources
                          </span>
                        </div>
                      </div>

                      <p className="text-[11px] text-slate-400">
                        Retrieve every CDS source resolved from this model’s Datasphere lineage. Each source is fetched from S/4HANA ADT and shown below.
                      </p>
                      {stagingCds && (
                          <div className="flex items-center gap-2 rounded-lg border border-blue-200 bg-blue-50 px-3 py-2 text-xs font-medium text-blue-900">
                          <RefreshCw className="h-3.5 w-3.5 animate-spin" />
                          ADT is returning the source rows. The preview will show up to 1,000 rows when this read completes.
                        </div>
                      )}
                      {(viewDetails.lineage_analysis?.status !== 'available' || !viewDetails.sources?.length) && (
                        <p className="text-xs text-amber-300">
                          {viewDetails.lineage_analysis?.message || 'No verified RAD_HDLF replicated source matched the dependency names.'}
                        </p>
                      )}
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-4 pt-1">
                        {viewDetails.sources?.map((source) => {
                          const isSourceSelected = selectedSourceCds === source.matched_cds_view;

                          return (
                            <div
                              key={source.name}
                              className={`p-4 rounded-xl border space-y-3 transition-all ${
                                isSourceSelected
                                  ? 'bg-emerald-950/20 border-emerald-500/50 shadow-md shadow-emerald-500/10'
                                  : 'bg-slate-950/60 border-slate-800/80'
                              }`}
                            >
                              <div className="flex items-center justify-between">
                                <div className="flex items-center gap-2">
                                  <Database className="w-4 h-4 text-emerald-400" />
                                  <span className="font-mono font-bold text-white text-xs">{source.name}</span>
                                </div>
                                <span className="px-2 py-0.5 rounded text-[10px] font-mono font-semibold bg-blue-950 text-blue-300 border border-blue-500/30">
                                  {source.replication_space || source.space || 'Space not reported'}
                                </span>
                              </div>

                              <div className="space-y-1 text-xs font-mono">
                                <div className="text-slate-400">
                                  Datasphere object type: <strong className="text-emerald-300">{source.type}</strong>
                                </div>
                                <div className="text-slate-400">
                                  Lineage Path: <span className="text-slate-300">{source.lineage_path}</span>
                                </div>
                                <div className="text-slate-400">
                                  Key Fields: <span className="text-slate-300">{source.key_fields?.join(', ') || 'Not reported'}</span>
                                </div>
                              </div>

                              <div className="pt-2 border-t border-slate-800/60 flex items-center justify-between text-xs">
                                <span className="text-[10px] text-emerald-400 flex items-center gap-1 font-semibold">
                                  {source.replicated_from_adt ? <CheckCircle2 className="w-3 h-3" /> : null}
                                  {source.replicated_from_adt ? 'Replication reported' : 'Source reported by Datasphere'}
                                </span>
                                {source.matched_cds_view && (
                                  <div className="flex flex-wrap justify-end gap-2">
                                    <button
                            onClick={() => {
                              const cdsName = source.matched_cds_view || null;
                              setSelectedSourceCds(cdsName);
                              setSelectedCdsForStaging(Array.from(new Set(
                                (viewDetails.sources || [])
                                  .map((item) => item.matched_cds_view)
                                  .filter((item): item is string => Boolean(item)),
                              )));
                              setStagingResults([]);
                              setStagingErrors([]);
                              setStagingError(null);
                              setStagedSqlPreview(null);
                              setStagedSqlExecution(null);
                              setReplicaCsvResult(null);
                              setReplicaCsvError(null);
                              if (selectedViewName && cdsName) loadFieldComparison(selectedViewName, cdsName, viewDetails);
                            }}
                                      className={`px-2.5 py-1 rounded text-[11px] font-bold cursor-pointer transition-colors ${
                                        isSourceSelected
                                          ? 'bg-emerald-500 text-slate-950'
                                          : 'bg-slate-800 text-slate-300 hover:bg-slate-700'
                                      }`}
                                    >
                                      {isSourceSelected ? 'Primary Source' : 'Set as Primary'}
                                    </button>
                                  </div>
                                )}
                              </div>
                            </div>
                          );
                        })}
                      </div>
                      <div className="flex justify-between border-t border-slate-200 pt-4">
                        <button type="button" disabled={stagingCds || !(viewDetails.sources || []).some((source) => source.matched_cds_view)} onClick={() => {
                          const sourceNames = Array.from(new Set((viewDetails.sources || []).map((source) => source.matched_cds_view).filter((source): source is string => Boolean(source))));
                          setSelectedCdsForStaging(sourceNames);
                          setWorkflowStep(2);
                        }} className="inline-flex items-center gap-2 rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:cursor-not-allowed disabled:bg-slate-300">
                          <CheckCircle2 className="h-4 w-4" />
                          Sources are resolved automatically from model metadata.
                        </button>
                      </div>
                      </>}

                      {workflowStep === 2 && <>
                      <div className="mb-3 border-b border-slate-200 pb-3">
                        <h3 className="text-base font-semibold text-slate-900">Staged CDS source datasets</h3>
                        <p className="mt-1 text-xs text-slate-500">All resolved sources are listed below. Expand a preview to inspect each source’s retrieved rows.</p>
                      </div>
                      {stagingCds && <div className="rounded-lg border border-blue-200 bg-blue-50 px-3 py-2 text-sm text-blue-800">Retrieving all resolved source datasets from S/4HANA ADT…</div>}
                      {stagingError && (
                        <div className="rounded-lg border border-amber-500/30 bg-amber-950/20 px-3 py-2 text-xs text-amber-200">
                          {stagingError}
                        </div>
                      )}
                      {stagingErrors.map((item) => (
                        <div key={item.source} className="rounded-lg border border-rose-500/30 bg-rose-950/20 px-3 py-2 text-xs text-rose-200">
                          {item.source}: {item.error}
                        </div>
                      ))}
                      {stagingResults.map((result) => (
                        <div key={result.source} className="rounded-xl border border-emerald-500/30 bg-slate-950/60 p-4 space-y-3">
                          <div className="flex flex-wrap items-center justify-between gap-2">
                            <div className="flex items-center gap-2 text-xs font-bold text-emerald-300">
                              <CheckCircle2 className="h-4 w-4" />
                              {result.source} staged completely
                            </div>
                            <span className="text-[11px] font-mono text-slate-400">
                              {result.row_count} rows · {result.batch_count} batches · local table {result.table_name}
                            </span>
                            <a
                              href={`/api/cds-staging/source-csv?space_id=${encodeURIComponent(spaceId)}&source_name=${encodeURIComponent(result.source)}`}
                              className="rounded-lg border border-blue-200 bg-blue-50 px-3 py-1.5 text-xs font-semibold text-blue-800 hover:bg-blue-100"
                            >
                              Download staged source CSV
                            </a>
                          </div>
                          <div className="text-[10px] font-mono text-slate-400">
                            {result.cache_status || 'STAGED'} · extracted/staged {result.staged_at ? new Date(result.staged_at * 1000).toLocaleString() : 'time unavailable'} · cache age {Math.floor((result.cache_age_seconds || 0) / 3600)}h · expires {result.expires_at ? new Date(result.expires_at * 1000).toLocaleString() : 'time unavailable'} · request duration {result.extraction_duration_seconds ?? '—'}s
                          </div>
                          <div className="overflow-x-auto rounded-lg border border-slate-800">
                            <details className="group">
                              <summary className="mb-2 flex cursor-pointer list-none items-center justify-between rounded-lg border border-slate-800 bg-slate-900/60 px-3 py-2 text-xs text-slate-300 hover:bg-slate-900">
                                <span>Retrieved data preview · {result.preview.length.toLocaleString()} rows</span>
                                <span className="text-blue-400 group-open:hidden">Show data</span>
                                <span className="hidden text-blue-400 group-open:inline">Hide data</span>
                              </summary>
                              <table className="min-w-full text-[11px] font-mono">
                              <thead className="bg-slate-900 text-slate-300">
                                <tr>{result.columns.map((column) => <th key={column.name} className="px-3 py-2 text-left whitespace-nowrap">{column.name}</th>)}</tr>
                              </thead>
                              <tbody>
                                {result.preview.map((row, rowIndex) => (
                                  <tr key={`${result.source}-${rowIndex}`} className="border-t border-slate-800 text-slate-300">
                                    {result.columns.map((column) => (
                                      <td key={column.name} className="px-3 py-2 whitespace-nowrap">
                                        {row[column.name] === null || row[column.name] === undefined ? '' : String(row[column.name])}
                                      </td>
                                    ))}
                                  </tr>
                                ))}
                                {result.preview.length === 0 && (
                                  <tr><td colSpan={Math.max(result.columns.length, 1)} className="px-3 py-3 text-center text-slate-500">The CDS returned no rows; an empty complete table was staged.</td></tr>
                                )}
                              </tbody>
                              </table>
                            </details>
                          </div>
                        </div>
                      ))}
                      <div className="flex justify-between border-t border-slate-200 pt-4">
                        <div className="flex flex-wrap justify-end gap-2">
                          <span className="text-xs text-slate-500">CDS sources are retrieved automatically after model selection.</span>
                        </div>
                      </div>
                      </>}
                      {workflowStep === 3 && <div className="space-y-4 rounded-xl border border-blue-200 bg-white p-4">
                        <div>
                          <h3 className="text-base font-semibold text-slate-900">Datasphere final view projection</h3>
                          <p className="mt-1 text-xs text-slate-600">The staged CDS sources are joined using Datasphere metadata, then restricted to the fields exposed by the selected final view.</p>
                        </div>
                        {joinedSourceBusy && <div className="rounded-lg border border-blue-200 bg-blue-50 px-3 py-2 text-sm text-blue-800">Joining staged CDS sources using the Datasphere join condition…</div>}
                        {joinedSourceError && <div className="rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-800">{joinedSourceError}</div>}
                        {joinedSourceResult && <>
                          <div className="flex flex-wrap items-center justify-between gap-3 rounded-lg border border-emerald-200 bg-emerald-50 px-3 py-3">
                            <div className="text-sm font-semibold text-emerald-900">Final view data · {joinedSourceResult.row_count} rows · {joinedSourceResult.columns?.length || 0} projected fields</div>
                            {joinedSourceResult.download_url && <a href={joinedSourceResult.download_url} className="rounded-lg bg-blue-600 px-3 py-2 text-xs font-semibold text-white hover:bg-blue-700">Download projected dataset</a>}
                          </div>
                          {!!joinedSourceResult.pending_calculated_fields?.length && <div className="rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-900">Calculated fields to be created in the next step: {joinedSourceResult.pending_calculated_fields.join(', ')}</div>}
                          {!!joinedSourceResult.missing_calculation_metadata?.length && <div className="rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-900">No formula is attached directly to these output members; the calculation step will resolve matching base-measure formulas from the model metadata: {joinedSourceResult.missing_calculation_metadata.join(', ')}</div>}
                          {!!joinedSourceResult.field_mappings?.length && <details className="rounded-lg border border-slate-200 bg-white" open>
                            <summary className="cursor-pointer px-3 py-2 text-xs font-semibold text-slate-800">Final field mapping across all staged sources · {joinedSourceResult.field_mappings.length} fields</summary>
                            <div className="max-h-56 overflow-auto border-t border-slate-100">
                              <table className="min-w-full text-left text-xs">
                                <thead className="sticky top-0 bg-slate-50 text-slate-600"><tr><th className="px-3 py-2">Final view field</th><th className="px-3 py-2">Found in</th><th className="px-3 py-2">Source field / handling</th></tr></thead>
                                <tbody>{joinedSourceResult.field_mappings.map((mapping: { output_field: string; source: string; source_field: string; kind: string }) => <tr key={mapping.output_field} className="border-t border-slate-100"><td className="px-3 py-1.5 font-mono text-slate-800">{mapping.output_field}</td><td className="px-3 py-1.5 text-slate-700">{mapping.source}</td><td className="px-3 py-1.5 text-slate-700">{mapping.kind === 'calculated' ? 'Calculated from model metadata' : mapping.source_field}</td></tr>)}</tbody>
                              </table>
                            </div>
                          </details>}
                          {(joinedSourceResult.join_conditions || []).map((condition: string, index: number) => <div key={`${condition}-${index}`} className="rounded-md bg-slate-50 px-3 py-2 font-mono text-xs text-slate-700">{condition}</div>)}
                          {joinedSourceResult.preview?.length > 0 && <div className="max-h-[420px] overflow-auto rounded-lg border border-slate-200">
                            <table className="min-w-full border-collapse text-left font-mono text-xs">
                              <thead className="sticky top-0 bg-slate-100 text-slate-700"><tr>{joinedSourceResult.columns.map((column: string) => <th key={column} className="whitespace-nowrap border-b border-slate-200 px-3 py-2">{column}</th>)}</tr></thead>
                              <tbody>{joinedSourceResult.preview.map((row: Record<string, unknown>, rowIndex: number) => <tr key={rowIndex} className="odd:bg-white even:bg-slate-50">{joinedSourceResult.columns.map((column: string) => <td key={column} className="whitespace-nowrap border-b border-slate-100 px-3 py-2 text-slate-700">{String(row[column] ?? '')}</td>)}</tr>)}</tbody>
                            </table>
                          </div>}
                        </>}
                        <div className="flex justify-between border-t border-slate-200 pt-4">
                          <button type="button" onClick={() => setWorkflowStep(2)} className="rounded-lg border border-slate-300 bg-white px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-50">Back to staged sources</button>
                          <div className="flex gap-2">
                            <span className="text-xs text-slate-500">{selectedCdsForStaging.length > 1 ? 'Sources are joined using Datasphere metadata automatically.' : 'The single source is projected directly.'}</span>
                          </div>
                        </div>
                      </div>}

                      {workflowStep === 4 && selectedSourceCds && stagingResults.some((item) => item.source.toLowerCase() === selectedSourceCds.toLowerCase()) && <>
                      <div className="rounded-xl border border-amber-200 bg-amber-50 p-4 space-y-3">
                        <div>
                          <h4 className="text-sm font-bold text-amber-950">Calculated measures from Datasphere metadata</h4>
                          <p className="mt-1 text-xs text-amber-800">Review the source expressions and aggregations resolved for this model. A measure base member uses its matching formula when its own metadata entry has no expression.</p>
                        </div>
                        <div className="grid gap-2 md:grid-cols-2">
                          {(viewDetails.fields || []).filter((field: any) => field.category === 'measure').map((field: any) => {
                            const formulas = getCalculatedFieldFormulas(viewDetails);
                            const name = String(field.name || '');
                            const baseName = name.replace(/_M_base$/i, '');
                            const formula = field.calculation || field.expression || formulas[name] || formulas[baseName] || '';
                            return <div key={name} className="rounded-lg border border-amber-200 bg-white p-3">
                              <div className="flex items-center justify-between gap-2"><strong className="font-mono text-xs text-slate-900">{name}</strong><span className="text-[10px] font-semibold text-amber-800">{field.aggregation || 'MEASURE'}</span></div>
                              <div className="mt-2 break-all font-mono text-[11px] text-slate-700">{formula || 'No calculation expression is present in the resolved metadata.'}</div>
                              {baseName !== name && formula && <div className="mt-1 text-[10px] text-emerald-700">Formula resolved from metadata measure: {baseName}</div>}
                            </div>;
                          })}
                        </div>
                      </div>
                      {selectedSourceCds && stagingResults.some((item) => item.source.toLowerCase() === selectedSourceCds.toLowerCase()) && (
                        <div className="rounded-xl border border-purple-300 bg-purple-50 p-4 space-y-3">
                          <div className="flex flex-wrap items-center justify-between gap-2 border-b border-purple-200 pb-2">
                            <h4 className="text-xs font-bold uppercase tracking-wide text-purple-900">Final fields not mapped to a staged source column</h4>
                            <span className="text-[11px] text-purple-700">{fieldComparison?.new_fields?.filter((field) => !joinedSourceResult?.field_mappings?.some((mapping: { output_field: string; kind: string }) => mapping.output_field.toLowerCase() === field.name.toLowerCase() && mapping.kind === 'source')).length || 0} fields still need a metadata calculation</span>
                          </div>
                          {loadingComparison ? (
                            <p className="text-xs text-purple-700">Checking final view fields against all staged source columns…</p>
                          ) : fieldComparison?.new_fields?.filter((field) => !joinedSourceResult?.field_mappings?.some((mapping: { output_field: string; kind: string }) => mapping.output_field.toLowerCase() === field.name.toLowerCase() && mapping.kind === 'source')).length ? (
                            <div className="space-y-2">
                              {fieldComparison.new_fields.filter((field) => !joinedSourceResult?.field_mappings?.some((mapping: { output_field: string; kind: string }) => mapping.output_field.toLowerCase() === field.name.toLowerCase() && mapping.kind === 'source')).map((field) => (
                                <div key={field.name} className="rounded-lg border border-purple-200 bg-white p-3">
                                  <div className="mb-1 flex flex-wrap items-center gap-2">
                                    <strong className="font-mono text-xs text-purple-900">{field.name}</strong>
                                    <span className="text-[10px] text-slate-500">{field.source_object ? `Defined in ${field.source_object}` : 'Datasphere calculated field'}</span>
                                  </div>
                                  <textarea
                                    value={fieldFormulas[field.name] ?? field.suggested_formula ?? ''}
                                    onChange={(event) => setFieldFormulas((previous) => ({ ...previous, [field.name]: event.target.value }))}
                                    placeholder="Enter the Datasphere calculation for this CDS column"
                                    rows={2}
                                    className="block w-full resize-y rounded border border-blue-200 bg-white px-2 py-1.5 font-mono text-xs text-black focus:border-blue-500 focus:outline-none"
                                  />
                                  {(field.referenced_columns?.length || 0) > 0 && <div className="mt-1 text-[10px] text-slate-500">Inputs: {field.referenced_columns?.join(', ')}</div>}
                                </div>
                              ))}
                            </div>
                          ) : (
                            <p className="text-xs text-purple-800">Every non-calculated final field was found in the staged source set; calculated measures are listed above from the Datasphere metadata.</p>
                          )}
                        </div>
                      )}
                      <div className="flex justify-between border-t border-slate-200 pt-4">
                        <span className="text-xs text-slate-500">Metadata calculations and final projection are applied automatically.</span>
                      </div>
                      </>}
                      {workflowStep === 5 && <>
                      <div id="workflow-replica" className="rounded-xl border border-blue-200 bg-white p-4 space-y-4">
                        <div>
                          <h3 className="text-sm font-bold text-black">Create CDS replica data</h3>
                          <p className="mt-1 text-xs text-slate-700">{selectedCdsForStaging.length > 1 ? 'Join every staged source using the Datasphere model join conditions, then apply calculations and filters and project the final fields.' : 'Use the single staged CDS source directly, apply its calculations and filters, and project the final model fields.'}</p>
                        </div>
                        {replicaCsvError && <div className="rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-xs text-rose-800">{replicaCsvError}</div>}
                        <div className="flex flex-wrap items-center justify-between gap-3">
                          <span className="text-xs text-slate-700">Sources: <strong className="font-mono text-black">{selectedCdsForStaging.join(', ')}</strong></span>
                          {replicaCsvBusy && <span className="text-xs font-semibold text-blue-700">{selectedCdsForStaging.length > 1 ? 'Joining sources and building final projection…' : 'Adding calculated fields to the source and building final projection…'}</span>}
                        </div>
                        <div className="flex justify-end border-t border-slate-200 pt-3">
                        </div>
                        {replicaCsvBusy && <p className="text-xs font-medium text-blue-800">{selectedCdsForStaging.length > 1 ? 'Applying the Datasphere join, calculations, filters, and final projection to all staged sources…' : 'Applying calculations, filters, and the final projection directly to the staged source…'}</p>}
                        {replicaCsvResult && (
                          <div className="space-y-3 rounded-lg border border-blue-200 bg-blue-50/50 p-3">
                            <div className="flex flex-wrap items-center justify-between gap-2">
                              <div className="text-sm font-semibold text-black">Final CDS replica CSV created · {replicaCsvResult.row_count.toLocaleString()} rows · {replicaCsvResult.columns.length} columns</div>
                              {!!replicaCsvResult.omitted_output_fields?.length && <div className="mt-2 rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-900">Omitted {replicaCsvResult.omitted_output_fields.length} final-view field(s) that could not be built from the available CDS source data: {replicaCsvResult.omitted_output_fields.map((item: { field: string }) => item.field).join(', ')}. Statistics will use the available projected fields.</div>}
                              <a href={replicaCsvResult.download_url} download={replicaCsvResult.filename} className="rounded-lg bg-blue-600 px-3 py-1.5 text-xs font-semibold text-white hover:bg-blue-700">Download final CSV</a>
                            </div>
                            {replicaCsvResult.applied_metadata_filter && (
                              <p className="text-[11px] text-slate-700">Applied CDS and Datasphere model filters: <code className="break-all text-slate-900">{replicaCsvResult.applied_metadata_filter}</code></p>
                            )}
                            <details className="rounded-lg border border-blue-200 bg-white" open>
                              <summary className="cursor-pointer px-3 py-2 text-xs font-bold text-blue-900">SQL execution and calculated-column logic</summary>
                              <div className="space-y-3 border-t border-blue-100 p-3">
                                <p className="text-[11px] text-slate-700">Runs locally in the backend with SQLite against the complete staged ADT CDS source. It appends the reviewed Datasphere formulas to the source fields to write the CSV; it does not create or activate a CDS artifact in S/4HANA.</p>
                                <pre className="sql-code-block max-h-56 overflow-auto whitespace-pre-wrap break-words rounded-md bg-slate-950 p-3 text-[11px]">{replicaCsvResult.replica_sql || 'SQL was not returned by the server.'}</pre>
                                <div className="space-y-2">
                                  {(replicaCsvResult.calculated_field_logic || []).map((item: { name: string; formula: string; sql_expression: string }) => (
                                    <div key={item.name} className="rounded-md border border-slate-200 bg-slate-50 p-2 text-[11px]">
                                      <div className="font-bold text-black">{item.name}</div>
                                      <div className="mt-1 text-slate-600">Datasphere formula: <code className="break-all text-slate-900">{item.formula}</code></div>
                                      <div className="mt-1 text-slate-600">Evaluated as: <code className="break-all text-blue-800">{item.sql_expression}</code></div>
                                    </div>
                                  ))}
                                </div>
                              </div>
                            </details>
                            <div className="max-h-80 overflow-auto rounded-lg border border-blue-100 bg-white">
                              <table className="min-w-full border-collapse text-left text-[11px] font-mono">
                                <thead className="sticky top-0 bg-blue-50 text-black"><tr>{replicaCsvResult.columns.map((column: string) => <th key={column} className="whitespace-nowrap border-b border-blue-100 px-2 py-2">{column}</th>)}</tr></thead>
                                <tbody>{replicaCsvResult.preview.map((row: Record<string, unknown>, index: number) => <tr key={index} className="odd:bg-white even:bg-slate-50">{replicaCsvResult.columns.map((column: string) => <td key={column} className="max-w-xs truncate border-b border-slate-100 px-2 py-1.5 text-black" title={String(row[column] ?? '')}>{row[column] == null ? '' : String(row[column])}</td>)}</tr>)}</tbody>
                              </table>
                            </div>
                            <p className="text-[11px] text-slate-700">The downloadable CSV contains all {replicaCsvResult.row_count.toLocaleString()} rows; this table shows the first 1,000.</p>
                          </div>
                        )}
                      </div>
                      {replicaCsvResult && <div className="flex justify-between border-t border-slate-200 pt-4">
                      </div>}
                      </>}
                    </div>
                    }

                  </div>
                )}

                {/* TAB 3: FIELD SELECTOR */}
                {activeTab === 'fields' && viewDetails && (
                  <div className="space-y-6 max-w-6xl mx-auto">
                    
                    {/* Field Controls Header */}
                    <div className="p-5 rounded-2xl bg-[#0e1629] border border-slate-800 shadow-xl space-y-4">
                      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-slate-800/80 pb-3">
                        <div className="flex items-center gap-2">
                          <SlidersHorizontal className="w-4 h-4 text-blue-400" />
                          <h3 className="font-bold text-sm text-white">Select View Fields for Validation & Reconcile</h3>
                        </div>

                        <div className="flex items-center gap-2">
                          <button
                            onClick={selectAllMeasures}
                            className="px-2.5 py-1 rounded-lg bg-blue-950/60 hover:bg-blue-900/60 text-blue-300 border border-blue-500/30 text-xs font-semibold cursor-pointer transition-colors"
                          >
                            Select All Measures
                          </button>
                          <button
                            onClick={selectAllFields}
                            className="px-2.5 py-1 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-200 text-xs font-semibold cursor-pointer transition-colors"
                          >
                            Select All
                          </button>
                          <button
                            onClick={clearSelectedFields}
                            className="px-2.5 py-1 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-400 text-xs font-semibold cursor-pointer transition-colors"
                          >
                            Clear
                          </button>
                        </div>
                      </div>

                      <div className="space-y-1.5">
                        <label className="text-[11px] font-bold text-slate-300 uppercase tracking-wider font-mono">
                          Row key fields (comma separated)
                        </label>
                        <input
                          type="text"
                          placeholder="Enter the Datasphere fields that uniquely identify each row"
                          value={valDimension}
                          onChange={(e) => setValDimension(e.target.value)}
                          className="w-full bg-slate-950 border border-slate-800 rounded-xl px-3 py-2 text-xs text-slate-200 font-mono placeholder:text-slate-500 focus:outline-none focus:border-blue-500"
                        />
                        <p className="text-[10px] text-slate-500">Use every key field when the view grain is composite. Fields must be present in the Datasphere view and the raw CDS source.</p>
                      </div>

                      <div className="flex flex-wrap items-center justify-between gap-3">
                        <div className="relative flex-1 min-w-[260px]">
                          <Search className="w-3.5 h-3.5 text-slate-400 absolute left-3 top-2.5" />
                          <input
                            type="text"
                            placeholder="Filter columns by name or label..."
                            value={fieldSearch}
                            onChange={(e) => setFieldSearch(e.target.value)}
                            className="w-full bg-slate-950 border border-slate-800 rounded-xl pl-8 pr-3 py-1.5 text-xs text-slate-200 font-mono placeholder:text-slate-500 focus:outline-none focus:border-blue-500"
                          />
                        </div>

                        <div className="flex items-center gap-1 bg-slate-950 p-1 rounded-xl border border-slate-800 text-xs font-mono">
                          <button
                            onClick={() => setFieldFilterTab('all')}
                            className={`px-3 py-1 rounded-lg ${fieldFilterTab === 'all' ? 'bg-blue-600 text-white font-bold' : 'text-slate-400 hover:text-white'}`}
                          >
                            All ({viewDetails.fields?.length || 0})
                          </button>
                          <button
                            onClick={() => setFieldFilterTab('measures')}
                            className={`px-3 py-1 rounded-lg ${fieldFilterTab === 'measures' ? 'bg-amber-600 text-white font-bold' : 'text-slate-400 hover:text-white'}`}
                          >
                            Measures ({viewDetails.measures?.length || 0})
                          </button>
                          <button
                            onClick={() => setFieldFilterTab('dimensions')}
                            className={`px-3 py-1 rounded-lg ${fieldFilterTab === 'dimensions' ? 'bg-purple-600 text-white font-bold' : 'text-slate-400 hover:text-white'}`}
                          >
                            Dimensions ({viewDetails.dimensions?.length || 0})
                          </button>
                        </div>
                      </div>
                    </div>

                    {/* Fields Grid */}
                    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
                      {filteredFields.map((field) => {
                        const isSelected = selectedFields.includes(field.name);
                        const isMeasure = field.category === 'measure';

                        return (
                          <div
                            key={field.name}
                            onClick={() => toggleField(field.name)}
                            className={`p-3.5 rounded-xl border text-xs cursor-pointer transition-all flex flex-col justify-between gap-2.5 ${
                              isSelected
                                ? 'bg-blue-600/15 border-blue-500/60 text-white shadow-sm ring-1 ring-blue-500/20'
                                : 'bg-[#0e1629]/70 border-slate-800/80 text-slate-400 hover:bg-slate-800/40 hover:text-slate-200'
                            }`}
                          >
                            <div className="flex items-start justify-between gap-2">
                              <div className="flex items-center gap-2 truncate flex-1 min-w-0">
                                <div
                                  className={`w-4 h-4 rounded border flex items-center justify-center shrink-0 transition-colors ${
                                    isSelected ? 'bg-blue-600 border-blue-500 text-white' : 'border-slate-700 bg-slate-950'
                                  }`}
                                >
                                  {isSelected && <Check className="w-3 h-3" />}
                                </div>
                                <span className="font-mono font-bold truncate text-white" title={field.name}>
                                  {field.name}
                                </span>
                              </div>

                              <span
                                className={`px-1.5 py-0.5 rounded text-[9px] font-mono font-bold uppercase shrink-0 ${
                                  isMeasure
                                    ? 'bg-amber-950/80 text-amber-300 border border-amber-500/40'
                                    : 'bg-purple-950/80 text-purple-300 border border-purple-500/40'
                                }`}
                              >
                                {isMeasure ? (field.aggregation || 'MEASURE') : (field.is_key ? 'KEY DIM' : 'DIMENSION')}
                              </span>
                            </div>

                            <div className="flex items-center justify-between text-[10px] font-mono text-slate-500 pt-1 border-t border-slate-800/60">
                              <span>{field.type || 'cds.String'}</span>
                              
                              {/* Quick validation shortcut buttons */}
                              {isMeasure ? (
                                <button
                                  onClick={(e) => {
                                    e.stopPropagation();
                                    setValTargetMetric(field.name);
                                    setActiveTab('validation');
                                  }}
                                  className="text-amber-400 hover:underline font-semibold cursor-pointer"
                                  title="Validate using this metric"
                                >
                                  Set as Metric
                                </button>
                              ) : (
                                <button
                                  onClick={(e) => {
                                    e.stopPropagation();
                                    setValDimension(field.name);
                                    setActiveTab('validation');
                                  }}
                                  className="text-blue-400 hover:underline font-semibold cursor-pointer"
                                  title="Reconcile on this grain dimension"
                                >
                                  Set as Grain
                                </button>
                              )}
                            </div>
                          </div>
                        );
                      })}
                    </div>

                  </div>
                )}

                {/* TAB 4: METADATA JSON VIEWER */}
                {activeTab === 'json' && viewDetails && (
                  <div className="space-y-4 max-w-6xl mx-auto">
                    <div className="p-4 rounded-2xl bg-[#0e1629] border border-slate-800 shadow-xl flex flex-wrap items-center justify-between gap-3">
                      <div className="flex items-center gap-2">
                        <FileJson className="w-4 h-4 text-blue-400" />
                        <h3 className="font-bold text-sm text-white">Full Datasphere Metadata JSON</h3>
                      </div>

                      <div className="flex items-center gap-2">
                        <button
                          onClick={() => copyToClipboard(JSON.stringify(viewDetails.metadata_json, null, 2), 'Metadata JSON')}
                          className="px-3 py-1.5 rounded-lg bg-slate-800 hover:bg-slate-700 text-xs text-slate-200 font-semibold flex items-center gap-1.5 cursor-pointer transition-colors"
                        >
                          <Copy className="w-3.5 h-3.5" />
                          <span>Copy JSON</span>
                        </button>
                        <button
                          onClick={handleDownloadMetadataJson}
                          className="px-3 py-1.5 rounded-lg bg-blue-600 hover:bg-blue-500 text-xs text-white font-bold flex items-center gap-1.5 cursor-pointer transition-colors shadow-sm"
                        >
                          <Download className="w-3.5 h-3.5" />
                          <span>Download JSON</span>
                        </button>
                      </div>
                    </div>

                    <pre className="p-6 rounded-2xl bg-slate-950 border border-slate-800 text-xs font-mono text-blue-300 overflow-x-auto leading-relaxed max-h-[70vh] shadow-2xl">
                      {JSON.stringify(viewDetails.metadata_json, null, 2)}
                    </pre>
                  </div>
                )}
              </>
            )}
          </div>
        )}

      </main>

    </div>
  );
}
