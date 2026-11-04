# SAP Datasphere & S/4HANA Validation Agent

A metadata-driven validation and reconciliation engine that inspects SAP Datasphere analytical models, recursively traverses upstream dependency graphs (views, shared local tables, remote tables), resolves underlying SAP S/4HANA CDS entities via SAP ADT, compiles transformation logic, and performs independent query execution with an explicit human-approval gate.

---

## 1. Prerequisites (Install on New System)

Before starting, ensure the following are installed and added to your system `PATH`:

1. **Python 3.10+ (64-bit)**
   - Verify in `cmd`:
     ```cmd
     python --version
     ```
2. **Node.js (v18 or v20 LTS) & npm**
   - Verify in `cmd`:
     ```cmd
     node --version
     npm --version
     ```
3. **SAP Datasphere CLI**
   - Install globally via npm:
     ```cmd
     npm install --global @sap/datasphere-cli
     ```
   - Verify installation:
     ```cmd
     datasphere --version
     ```

---

## 2. Setup & Execution Instructions (via Windows CMD)

Open **two separate Command Prompt (`cmd.exe`) windows**: one for the Python Backend and one for the React Frontend.

---

### Terminal 1: Backend Setup & Launch

#### Step 1: Navigate to the project backend
```cmd
cd /d "C:\path\to\your\project\backend"
```
*(Replace `C:\path\to\your\project` with your actual extracted directory path)*

#### Step 2: Create and activate a Python virtual environment
```cmd
python -m venv venv
call venv\Scripts\activate
```
*(Your prompt should now be prefixed with `(venv)`)*

#### Step 3: Upgrade pip and install required dependencies
```cmd
python -m pip install --upgrade pip
pip install -r requirements.txt
```

#### Step 4: Configure environment variables
Create your local `.env` file from the provided template:
```cmd
copy .env.example .env
```
Open `.env` in Notepad or your preferred editor:
```cmd
notepad .env
```
Fill in the parameters for your SAP environment:
- **Datasphere OAuth & API**:
  - `DATASPHERE_API_URL`: Base URL for Datasphere Relational API.
  - `DATASPHERE_TOKEN_URL`: BTP OAuth token URL (`/oauth/token`).
  - `OAUTH_CLIENT_ID` / `OAUTH_CLIENT_SECRET`: BTP Client Credentials.
  - `DATASPHERE_SPACE_ID`: Primary modeling space (e.g., `RADH_S3X`).
  - `DATASPHERE_MODELING_SPACE`: Modeling space (e.g., `RADH_S3X`).
  - `DATASPHERE_REPLICATION_SPACE`: Replication space if applicable (e.g., `RAD_HDLF`).
- **HANA Direct SQL**:
  - `HANA_HOST`: SAP HANA Cloud endpoint host.
  - `HANA_PORT`: Usually `443`.
  - `HANA_USER` / `HANA_PASSWORD`: HANA database credentials.
- **SAP S/4HANA ADT**:
  - `SAP_BASE_URL`: Base HTTPS URL of your S/4HANA system (e.g., `https://hostname:44301`).
  - `SAP_USERNAME` / `SAP_PASSWORD`: ABAP user credentials with ADT authorization.
- **LLM Provider** *(Optional - the system contains an automatic deterministic compiler fallback if keys are omitted)*:
  - `LLM_PROVIDER`: `anthropic`, `gemini`, or `openai` to prefer a configured provider; defaults to `auto`.
  - `ANTHROPIC_API_KEY` / `GEMINI_API_KEY` / `OPENAI_API_KEY`
  - `ANTHROPIC_MODEL` / `GEMINI_MODEL` / `OPENAI_MODEL`: provider model identifiers. Authentication failures identify the specific API-key variable to update; unavailable models identify the corresponding model variable.

#### Step 5: Run Automated Tests (Verification)
Verify that all 38 dependency-resolution and validation test suites pass:
```cmd
python -m unittest discover -s tests -v
```
*(You should see `Ran 38 tests ... OK`)*

#### Step 6: Start the Backend Server
```cmd
python main.py
```
- The backend will start on **`http://127.0.0.1:9050`**.
- Keep this CMD window open.

---

### Terminal 2: Frontend Setup & Launch

#### Step 1: Open a second CMD window and navigate to `frontend`
```cmd
cd /d "C:\path\to\your\project\frontend"
```

#### Step 2: Install Node modules
```cmd
npm install
```

#### Step 3: Start the Frontend Vite dev server
```cmd
npm run dev
```
- The frontend will start on **`http://localhost:3050`**.
- Keep this CMD window open.

---

## 3. Using the Validation Agent

1. Open your web browser and navigate to:
   ```text
   http://localhost:3050
   ```
2. Navigate to the **Model Reconciliation / Validation** tab.
3. Select or enter:
   - **Datasphere Space**: (e.g., `RADH_S3X`)
   - **Analytical Model**: (e.g., `AM_Customer_Receivables_KPI` or `AM_DeliveryKPI`)
4. Click **Resolve & Reconstruct Logic**:
   - The agent recursively traces all intermediate views and boundary objects (e.g. `RAD_S3X.ZI_CUSTOMERRECEIVABLESKPI`).
   - Resolves the underlying SAP S/4HANA CDS entity and verifies column schemas via SAP ADT.
   - Reconstructs calculated fields, filters, and output grain.
5. In **Lineage & Upstream Sources**, select one or more lineage-matched CDS views with **Select to Stage**, then click **Stage selected CDS**.
   - The backend uses the existing read-only S/4HANA ADT data-preview client and keyset pagination to extract every row and column.
   - This is a raw CDS staging extract, not OData and not execution of the reconstructed S/4HANA comparison query.
   - Complete snapshots are stored as local SQLite tables in `backend/cds_staging.db`. An extraction that cannot verify its keys and row counts is not published.
6. Continue to the validation workflow when ready. Its existing approval and query execution steps remain separate from staging.

### Staged SQL reconstruction

The source staging API keeps a complete source snapshot in the existing SQLite staging database for at least 24 hours. Compatible, unexpired snapshots are reused; schema fingerprint changes or the **Force refresh** option trigger a new extraction. In the Lineage & Upstream Sources panel, use **Generate local SQL preview** after staging the complete source set. The planner receives Datasphere lineage metadata and staged table schemas, emits SQLite reconstruction SQL, and blocks execution when it reports unsupported transformations or when SQLite cannot compile the query against those tables. Review the generated SQL and use **Approve and execute this exact SQL** to run it. The approval hash is tied to the query and current snapshot IDs, so refreshing a source invalidates the approval.

This local reconstruction flow is separate from the existing Datasphere HANA execution and ADT source extraction paths. It returns reconstructed rows and execution status; it does not yet claim semantic equivalence or replace the existing Datasphere-versus-ADT comparison workflow.

---

## 4. Production Build (Optional)

To build a production bundle of the frontend:
```cmd
cd /d "C:\path\to\your\project\frontend"
npm run build
npm run preview
```

---

## 5. Troubleshooting & Quick Fixes

| Issue | Cause | Solution in CMD |
|---|---|---|
| `'python' is not recognized` | Python is not added to Windows PATH | Re-run Python installer and check **"Add Python to PATH"**, or specify the full path to `python.exe`. |
| `'datasphere' is not recognized` | Global npm packages are not in PATH | Run `npm install --global @sap/datasphere-cli`, or set `DATASPHERE_CLI_COMMAND="C:\Users\<user>\AppData\Roaming\npm\datasphere.cmd"` in `backend/.env`. |
| `Port 9050 is already in use` | Another instance of the backend is running | Run `netstat -ano \| findstr :9050` and terminate the PID with `taskkill /PID <PID> /F`. |
| `Port 3050 is already in use` | Another instance of frontend is running | Run `netstat -ano \| findstr :3050` and terminate the PID with `taskkill /PID <PID> /F`. |
| `ADT query returns HTTP 401/403` | Invalid SAP S/4HANA credentials or missing ADT role | Check `SAP_USERNAME` and `SAP_PASSWORD` in `backend/.env`. User requires `SAP_BC_DWB_ABAPBASICS` or equivalent ADT authorizations. |
| `HANA connection failed` | Missing or incorrect HANA Cloud host/credentials | Check `HANA_HOST`, `HANA_USER`, and `HANA_PASSWORD` in `backend/.env`. Ensure `HANA_ENCRYPT=True`. |
