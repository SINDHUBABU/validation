# Metadata-driven Datasphere resolver and reconciliation safety update

## Configuration

Keep your existing `backend/.env` file and credentials. Do not replace it with `.env.example`.
Add or verify these settings in `backend/.env`:

```dotenv
DATASPHERE_CLI_COMMAND=datasphere
DATASPHERE_MODELING_SPACE=RADH_S3X
DATASPHERE_REPLICATION_SPACE=RAD_HDLF
DATASPHERE_CLI_ACCEPT=application/vnd.sap.datasphere.object.content+json
```

The space IDs are environment configuration, not Python constants. Change them in `.env` if your tenant uses different spaces. The CLI command can be an absolute path to the installed executable/wrapper if it is not on the backend process PATH.

## What changed

- Windows `.cmd` wrappers are invoked through the Windows shell path, avoiding the prior `WinError 2` behavior for npm-installed CLI shims.
- Object definitions use a supported Datasphere content media type instead of `application/json`.
- Object reads are scoped to the requested space, not only the constructor's initial space.
- Cached inventories no longer synthesize analytical models, views, fake columns, default measures, or guessed source names.
- Model and replication spaces are configured with environment variables.
- Source matching is exact and case-insensitive; prefix or business-domain guesses are not accepted as proof of lineage.
- Reconciliation planning requires live ADT source metadata and explicit field/aggregation metadata. The planner builds a dynamic ABAP Open SQL query for the discovered Datasphere graph and runs a deterministic transformation-coverage check before approval or ADT execution.
- The coverage report records the Datasphere path/expression and corresponding S/4HANA expression for projections, mappings, filters, joins, calculations, constants, parameters, aggregations, grouping, null handling, and supported DISTINCT semantics. Any unavailable or unsupported transformation blocks the S/4HANA query and marks semantic equivalence `SEMANTIC_EQUIVALENCE_UNVERIFIED`; terminal-source-only projections are not treated as an equivalent reconstruction.
- Group-by reference arrays are checked one expression at a time. Coverage labels known translations that are absent from SQL as `SUPPORTED_NOT_COMPILED`, missing definitions as `METADATA_UNAVAILABLE`, uncertain translations as `UNASSESSED`, and known unsupported constructs as `TRANSLATION_UNSUPPORTED`. It recognizes literal-argument `LEFT`/`SUBSTRING` translations and only simplifies a calculated CASE filter when it equals the defined true-branch value.
- Existing CDS entities are queried through Eclipse ADT Data Preview when their verified metadata supports the reconstruction. Dynamic SQL is limited to the supported ABAP Open SQL query endpoint. The workflow does not create, change, or activate CDS repository objects.
- Generated Open SQL is line-wrapped before coverage checks and approval hashing, preserving quoted literals while respecting the ADT handler's 255-character ABAP source-line limit.
- Aggregate comparisons use selected-model aggregation metadata and actual complete query results. Unsupported/non-additive aggregates, empty results, failed queries, and truncated results remain explicitly indeterminate; an aggregate comparison does not imply a verified row-level grain.
- Datasphere execution failures return `INDETERMINATE`; S/4HANA rows are never copied to fabricate Datasphere results.
- Execution requires an explicit `human_approved=True` argument; the default is `False`.
- Explicit CDS staging is available separately from reconciliation: users select lineage-matched CDS sources, and the backend uses the existing ADT Data Preview client with verified keyset pagination to stage complete extracts in `backend/cds_staging.db`. The local SQLite snapshot is atomically replaced only after key uniqueness and SAP row-count checks pass; unsupported or incomplete extracts are reported as failed and are not published.

## Run from CMD

```cmd
cd /d "C:\New folder (1) - Copy\backend"
python -m unittest discover -s tests -v
python main.py
```

If you extracted this archive to another directory, use that path instead. Restart the backend after changing `.env`.

## Important implementation boundary

Semantic equivalence is declared only when deterministic coverage maps the discovered transformations to expressions in the generated ABAP Open SQL. Expression coverage is conservative: syntactically different but equivalent expressions remain unverified. Missing source metadata, missing parameter bindings, unsupported constructs, or transformations that cannot be retrieved block S/4HANA execution and are reported with their lineage path; they are never inferred from field-name similarity or row counts.

Verification using mocks or unit tests proves planner and safety behavior only. It does not establish that a live SAP tenant accepts the generated SQL or that live Datasphere and S/4HANA results are equivalent. Live SAP verification requires explicitly approved queries and the configured tenant connections.

The LLM should be passed only object definitions, schemas, lineage, and transformation metadata—not business data rows. Reconciliation SQL remains read-only and requires human approval before execution. CDS staging is a separate, explicit read-only action; it does not approve or execute a reconciliation query.
