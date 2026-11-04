# Metadata-driven reconciliation

The validation endpoint resolves the selected analytical model or view and its
upstream views, remote tables, and local tables with the SAP Datasphere CLI.
Install the CLI before starting the backend:

```cmd
npm install --global @sap/datasphere-cli
```

The backend process must be able to find `datasphere` on its `PATH`, or
`DATASPHERE_CLI_COMMAND` must point to the executable in `backend/.env`.
When the OAuth client ID, client secret, token URL, and Datasphere API URL are
configured in `backend/.env`, the Python backend supplies client-credentials
authentication to each CLI call through a short-lived temporary options file;
no interactive CLI login is needed. If those credentials are not configured,
the CLI can use a previously authenticated profile. The resolver uses the Data
Builder object list/read commands exposed by the CLI for the target tenant and
space. If neither authentication path is available, artifact reads remain
incomplete; the app will not infer missing joins, filters, or source mappings.

Reconciliation also requires SAP ADT credentials, a configured LLM provider,
and direct SAP HANA Cloud access. The LLM receives model/CDS definitions and
schemas—not transaction rows—and returns paired Datasphere HANA and S/4HANA
Open SQL `SELECT` statements. Queries are checked for read-only behavior and
lineage-approved sources before execution. The app compares the resulting rows
using the selected composite key fields.

Validation is reported as incomplete, not successful, when the dependency
graph, source mapping, query plan, or complete execution results cannot be
verified. OData-only execution is not treated as equivalent because it cannot
guarantee that the generated user filters and grain are applied identically.

The RAD_HDLF local-table inventory is stored in `backend/datasphere_cache.db`
and refreshed from the CLI before a model's details or reconciliation are
resolved. If a refresh fails, the resolver still reads the dependency graph and
uses the last saved inventory, clearly marking that inventory as stale. Source
matching checks dependency names and explicit remote/source-object references
in artifact metadata against RAD_HDLF local-table names, ignoring case and an
optional namespace qualifier. Prefix stripping and partial-name matches are
intentionally not used for this verification.

Calculated-field expressions exposed by analytical-model and business-layer
metadata are shown as read-only model logic and are included automatically in
the reconciliation request. A field missing from the raw CDS schema is not
automatically assumed to be calculated, and users are not asked to re-enter
formulas that are already present in model metadata.
