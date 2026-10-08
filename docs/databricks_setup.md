# Databricks Setup Guide — Pakistan Economy Data Warehouse

This guide takes a beginner from an empty account to a verified Databricks environment for the Pakistan Economy Data Warehouse. Follow the numbered steps in order. Do not skip a checkpoint even if the screen looks familiar.

> **Important product update (checked 2026-10-09):** Databricks retired the legacy **Community Edition** in 2025 and replaced it with **Databricks Free Edition**. New Free Edition workspaces use quota-limited serverless compute; they do not let you create or configure a classic single-node cluster. New workspaces may also block legacy DBFS root/FileStore access. The 15 GB single-node cluster, two-hour auto-termination, DBR selector, DBFS root, and `spark_catalog` workflow in this course describe a **legacy Community Edition lab workspace**. This guide supports both screens:
>
> - **Legacy course workspace:** follow the DBFS, Hive Metastore, and single-node cluster instructions exactly.
> - **New Free Edition account:** follow the registration and Git steps, use serverless compute, and run the capability check before continuing. If DBFS is blocked, ask the instructor whether to use the current Unity Catalog/default-storage variant; do not pretend the DBFS steps succeeded.

Official references: [Free Edition signup](https://docs.databricks.com/aws/en/getting-started/free-edition), [Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations), and [DBFS/Unity Catalog guidance](https://docs.databricks.com/aws/en/dbfs/unity-catalog).

## Before you start

Have these items ready:

- a personal email address you can verify;
- a GitHub account with access to the project repository;
- the repository HTTPS URL, for example `https://github.com/<owner>/pakistan-economy-data-warehouse.git`;
- the project checked out on your computer, including `data/raw/full/` and `data/raw/incremental/batch_001/`;
- a password manager or other safe place to hold short-lived tokens;
- permission from the instructor to use the legacy DBFS design if your screen is the new Free Edition UI.

Never place a GitHub token, Databricks token, password, or `.databrickscfg` file in the repository.

## Step 1 — Register and verify the workspace

### 1.1 Create the correct no-cost account

1. Open the official [Databricks Free Edition signup page](https://login.databricks.com/).
2. Choose **Sign up with Google**, **Sign up with Microsoft**, or email/one-time-password, depending on what the page offers.
3. Complete email verification.
4. Wait while Databricks creates one Free Edition workspace.
5. Bookmark the workspace URL after it opens. It resembles `https://dbc-<identifier>.cloud.databricks.com`.

Do **not** choose a page labelled **14-day free trial**, **Start free trial**, or one that asks you to configure an AWS/Azure/GCP account, payment method, subscription, VPC, or cloud storage bucket. Those are full-platform trial/onboarding paths, not the no-cost student workspace described here.

```text
Correct path                                  Stop: wrong path for this guide
┌──────────────────────────────┐              ┌──────────────────────────────┐
│ Databricks Free Edition      │              │ 14-day trial / cloud setup   │
│ Personal learning use       │              │ AWS / Azure / GCP account    │
│ No cloud account setup      │              │ Billing/subscription prompts │
│ Create workspace automatically│             │ Deployment configuration     │
└──────────────────────────────┘              └──────────────────────────────┘
```

Free Edition is subject to usage quotas/fair-use limits, but it is not the 14-day cloud trial. Databricks can pause compute after quotas are reached; your workspace data/settings are normally retained until access resumes.

### 1.2 Record which workspace type you received

Look at the left navigation after the workspace opens:

```text
Legacy Community Edition-style UI             Current Free Edition UI
┌───────────────────────────┐                  ┌───────────────────────────┐
│ Workspace                 │                  │ Workspace                 │
│ Repos                     │                  │ Catalog                   │
│ Compute / Clusters        │                  │ SQL Editor                │
│ Data                      │                  │ Jobs & Pipelines          │
└───────────────────────────┘                  │ Serverless in notebooks   │
                                               └───────────────────────────┘
```

Write one of these in your course notes:

```text
WORKSPACE_PROFILE=LEGACY_CE_DBFS
```

or:

```text
WORKSPACE_PROFILE=CURRENT_FREE_SERVERLESS
```

Do not infer the profile from the marketing name alone. Use the capability test next.

### 1.3 Run the mandatory capability check

Create a temporary notebook:

1. Select **Workspace** in the sidebar.
2. Open your user folder.
3. Select **Create > Notebook** (or **New > Notebook**).
4. Name it `capability_check` and choose **Python**.
5. Attach the available compute. In current Free Edition, select the default **Serverless** option. In a legacy workspace, attach any running cluster or return after Step 3 if none exists yet.
6. Run this cell:

```python
from pprint import pprint

print("Spark version:", spark.version)
print("Current catalog:", spark.sql("SELECT current_catalog()").first()[0])

results = {}

try:
    results["dbfs_root_list"] = [x.path for x in dbutils.fs.ls("dbfs:/")[:5]]
    results["dbfs_root_available"] = True
except Exception as exc:
    results["dbfs_root_available"] = False
    results["dbfs_error"] = f"{type(exc).__name__}: {exc}"[:500]

try:
    results["filestore_write"] = dbutils.fs.mkdirs(
        "dbfs:/FileStore/pakistan_economy/_capability_probe"
    )
except Exception as exc:
    results["filestore_write"] = False
    results["filestore_error"] = f"{type(exc).__name__}: {exc}"[:500]

pprint(results)
```

Interpret the result:

- If `dbfs_root_available` and `filestore_write` are both `True`, this guide's DBFS path is usable.
- If either is `False`, stop before Step 4. Your new Free Edition workspace has disabled a legacy feature required by the course design. Show the printed error to the instructor and request either the course's legacy workspace or approval to use Unity Catalog/default storage.
- Do not try to bypass a disabled DBFS root by writing to the driver's `/tmp` directory. Driver-local files disappear with compute.

**Checkpoint 1:** You can sign in again, have bookmarked the workspace URL, know your workspace profile, and have recorded whether DBFS/FileStore is writable.

## Step 2 — Connect GitHub to Databricks Git folders/Repos

Databricks renamed **Repos** to **Git folders**. The old and new labels refer to the same development workflow.

### 2.1 Create a GitHub classic personal access token

GitHub recommends fine-grained tokens for new integrations, and Databricks recommends its GitHub App where available. This course uses a classic PAT because it works with the legacy Repos screen.

1. Sign in to GitHub.
2. Navigate to **Profile picture > Settings > Developer settings**.
3. Select **Personal access tokens > Tokens (classic)**.
4. Select **Generate new token > Generate new token (classic)**.
5. Enter a descriptive note such as `databricks-pakistan-economy-course`.
6. Choose the shortest practical expiration, such as 30 or 60 days. Do not choose **No expiration** for coursework.
7. Select the `repo` scope. This grants repository read/write access required for cloning private repositories and pushing commits.
8. Select `workflow` only if you must edit files under `.github/workflows/`; it is not required for ordinary notebooks/documentation.
9. Select **Generate token**.
10. Copy the token immediately into a password manager. GitHub shows it only once.

```text
GitHub
Settings
└── Developer settings
    └── Personal access tokens
        └── Tokens (classic)
            └── Generate new token (classic)
                ├── Expiration: 30–60 days
                ├── [x] repo
                └── [ ] workflow  (only if required)
```

Security rules:

- The classic `repo` scope can reach every private repository that your GitHub identity can reach. Revoke the token after the course or if exposed.
- Never paste the token into a notebook, terminal command, screenshot, chat, or Git-tracked file.
- If the repository belongs to an organization using SAML SSO, use GitHub's **Configure SSO** action to authorize the token for that organization.

GitHub's official steps are documented in [Managing personal access tokens](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens).

### 2.2 Store the Git credential in Databricks

Use the navigation path available in your workspace:

```text
Current UI:
User icon (top right) > Settings > Linked accounts > Add Git credential

Legacy UI:
User icon > User Settings > Git integration
```

Then:

1. Choose **GitHub** as the provider.
2. Choose **Personal access token** if Databricks offers GitHub App and PAT options.
3. Enter your GitHub username or GitHub account email.
4. Paste the GitHub PAT into the token field.
5. Select **Save**.
6. Close Settings. Do not leave the token visible on screen.

If the UI offers **Link Git account** using the Databricks GitHub App, that is the safer current option. Use it if your instructor does not specifically require a classic PAT.

### 2.3 Clone the project repository

Use the path that matches your UI:

```text
Legacy UI: Workspace > Repos > Add Repo

Current UI: Workspace > Users > <your email> > Create > Git folder
```

1. Copy the repository's **HTTPS** clone URL from GitHub. Do not use the SSH URL.
2. Paste it into **Git repository URL**.
3. Choose **GitHub** as provider if asked.
4. Leave the suggested folder name or enter `pakistan-economy-data-warehouse`.
5. Select **Create Repo** or **Create Git folder**.
6. Expand the new folder and verify that `docs/` and the project's other tracked folders are visible.

```text
/Workspace/Users/<your-email>/pakistan-economy-data-warehouse/
├── docs/
├── notebooks/
├── src/
├── sql/
└── ...
```

Git stores code and small configuration files. Do not commit source PDFs/XLSX/CSV extracts if the repository policy excludes them; raw data belongs in the staging storage created in Step 4.

### 2.4 Create/switch a branch and synchronize it

1. Open the Git folder/Repo.
2. Select the current branch name near the top of the page.
3. Select **Create branch**.
4. Create a branch such as `feature/phase2-databricks-setup` from the repository's current default branch.
5. Before each work session, open the Git dialog and select **Pull** to fetch remote changes.
6. Edit a harmless documentation line, save it, then reopen the Git dialog.
7. Review the changed-file list. Make sure no token, raw data, notebook output, or unrelated file is included.
8. Enter a precise message such as `docs: verify Databricks Git integration`.
9. Select **Commit & Push** (or **Commit**, followed by **Push**).
10. Open GitHub in the browser and confirm that the commit appears on your branch.

If **Pull** reports a conflict, do not click random overwrite options. Copy your uncommitted text somewhere safe, inspect the conflict, and ask the instructor or resolve it in a local Git checkout.

**Checkpoint 2:** The repository is visible in Databricks, you are on a feature branch, and one test commit appears in GitHub.

## Step 3 — Configure compute and Python libraries

First use the branch matching your workspace profile.

### 3.1 Legacy Community Edition: create the single-node cluster

Skip this subsection if you have current serverless-only Free Edition.

1. Select **Compute** in the sidebar. Older screens may label it **Clusters**.
2. Select **Create compute** or **Create Cluster**.
3. Set **Cluster name** to `pakistan-economy-course`.
4. Select **Single node** if the option is visible. Community Edition may enforce it automatically.
5. Select the newest available LTS runtime that your course code supports:
   - first choice: **Databricks Runtime 15.4 LTS** (Spark 3.5 family);
   - fallback: **Databricks Runtime 14.3 LTS** (Spark 3.5 family).
6. Do not choose an ML/GPU runtime; this project needs standard Python, Spark, and Delta only.
7. Leave the Community Edition node type/default worker configuration unchanged. You cannot scale this environment like a paid workspace.
8. Set **Terminate after** to `120 minutes` if editable. Some Community Edition workspaces enforce two-hour idle termination automatically.
9. Select **Create compute**, then wait until the state becomes **Running**.

```text
Compute > Create compute
┌──────────────────────────────────────────────┐
│ Name: pakistan-economy-course               │
│ Mode: Single node                           │
│ Runtime: 15.4 LTS (or 14.3 LTS)             │
│ Auto termination: 120 minutes               │
│ Workers: fixed by Community Edition         │
└──────────────────────────────────────────────┘
```

The approximate memory available to a legacy single-node Community Edition cluster is limited (commonly described by courses as about 15 GB). Treat that as a planning estimate, not a guaranteed service specification. Process PDFs one page at a time, avoid `collect()` on large DataFrames, and do not cache every layer.

### 3.2 Current Free Edition: attach serverless compute

Current Free Edition is serverless-only. You will not see a usable **Create cluster** form and cannot select DBR 14.3/15.4 or a node type.

1. Open a notebook.
2. Use the compute selector at the top of the notebook.
3. Select the default **Serverless** environment.
4. Wait until the notebook reports that it is connected.
5. Run `print(spark.version)` to record the Spark version chosen by the platform.

Do not spend time looking for a hidden DBR selector; it does not exist in current Free Edition. Serverless quotas and restricted outbound internet also mean some package installations may be unavailable.

### 3.3 Install the required Python packages

The project uses:

| Package | Purpose | Important note |
|---|---|---|
| `openpyxl` | Read PBS `.xlsx` workbooks and inspect sheets/cells/formulas | Use read-only mode for large workbooks. |
| `pdfplumber` | Extract text/tables from text-based PBS/OGRA PDFs | It does not perform OCR on image-only scans. |
| `pypdf` | Inspect PDF metadata/pages and perform basic text extraction | Useful fallback, not an OCR engine. |
| `Pillow` | Image handling for OCR preparation | Keep page resolution modest on limited memory. |
| `pytesseract` | Python wrapper around the Tesseract OCR executable | Installing the wrapper does **not** install the Tesseract system executable. |

#### Legacy cluster-scoped installation

1. Navigate to **Compute > pakistan-economy-course**.
2. Open the **Libraries** tab.
3. Select **Install new**.
4. Select **PyPI**.
5. Enter and install each package name separately: `openpyxl`, `pdfplumber`, `pypdf`, `Pillow`, and `pytesseract`.
6. Wait for every library to show **Installed**.
7. Restart the cluster if Databricks requests it.

If the **Libraries** tab is missing, use the notebook-scoped method below.

#### Notebook-scoped installation (works for serverless and legacy notebooks)

Put this in the first executable cell of the notebook:

```python
%pip install openpyxl pdfplumber pypdf Pillow pytesseract
```

Then restart the Python process if the notebook requests it:

```python
dbutils.library.restartPython()
```

On ephemeral/serverless compute, `%pip` dependencies may need to be restored in a new session. Keep the installation cell at the top of the verification notebook and record tested versions after the first successful run:

```python
from importlib.metadata import version

for package in ["openpyxl", "pdfplumber", "pypdf", "Pillow", "pytesseract"]:
    print(package, version(package))
```

#### OCR limitation you must test

Run:

```python
import shutil
import pytesseract

print("Tesseract executable:", shutil.which("tesseract"))
try:
    print("Tesseract version:", pytesseract.get_tesseract_version())
except Exception as exc:
    print("OCR unavailable on this compute:", type(exc).__name__, str(exc)[:300])
```

If the executable is missing, `pytesseract` cannot OCR the scanned OGRA PDF. Do not attempt an unapproved system installation on Community/Free Edition. Use one of these instructor-approved options:

1. OCR the PDF on your computer with Tesseract, retain the original PDF, and upload the extracted text/CSV alongside it with lineage metadata.
2. Manually transcribe the small assessed sample with a second-person check and mark the extraction method.
3. Use another OCR service only if course policy permits uploading the public document and credentials can be handled safely.

### 3.4 Understand what survives termination

| Location/object | Survives compute termination? | Use |
|---|---:|---|
| GitHub / Databricks Git folder | Yes | Notebooks, Python modules, SQL, documentation |
| DBFS root/FileStore in a compatible legacy workspace | Yes | Course raw/Delta storage |
| Hive Metastore/Delta table data on DBFS | Yes | Bronze, Silver, Gold, Ops tables |
| Driver paths such as `/tmp`, `/local_disk0`, `file:/tmp` | No | Temporary extraction only |
| Python variables, cached DataFrames, installed session state | No | Recreated each session |

Before leaving the workspace, write useful outputs to Delta/DBFS, commit code to Git, and allow the cluster to terminate. A cluster restart is a normal event, not a recovery disaster.

**Checkpoint 3:** Compute runs a Python cell; package imports succeed; you know whether OCR is available; and no required data is stored only on the driver.

## Step 4 — Initialize the DBFS directory hierarchy

Continue only if Step 1 confirmed that `dbfs:/FileStore` is writable. The path is the course's legacy storage profile:

```text
dbfs:/FileStore/pakistan_economy/
├── staging/
│   ├── full/
│   │   ├── sbp_remittances/
│   │   ├── sbp_fdi/
│   │   ├── sbp_fx/
│   │   ├── sbp_exports/
│   │   ├── sbp_imports/
│   │   ├── pbs_spi/
│   │   ├── pbs_cpi/
│   │   └── ogra_fuel/
│   └── incremental/
│       └── batch_001/
│           └── <the same eight source folders>
├── bronze/
├── silver/
├── gold/
├── quarantine/
├── checkpoints/
└── ops/
```

`ops/` is an additional operational location for audit tables and manifests. Code remains in Git, not DBFS.

### 4.1 Create the directories

Create a Python notebook named `01_initialize_storage` and run:

```python
PROJECT_ROOT = "dbfs:/FileStore/pakistan_economy"

SOURCE_FOLDERS = [
    "sbp_remittances",
    "sbp_fdi",
    "sbp_fx",
    "sbp_exports",
    "sbp_imports",
    "pbs_spi",
    "pbs_cpi",
    "ogra_fuel",
]

required_paths = [
    f"{PROJECT_ROOT}/bronze",
    f"{PROJECT_ROOT}/silver",
    f"{PROJECT_ROOT}/gold",
    f"{PROJECT_ROOT}/quarantine",
    f"{PROJECT_ROOT}/checkpoints",
    f"{PROJECT_ROOT}/ops",
]

for source_folder in SOURCE_FOLDERS:
    required_paths.append(f"{PROJECT_ROOT}/staging/full/{source_folder}")
    required_paths.append(
        f"{PROJECT_ROOT}/staging/incremental/batch_001/{source_folder}"
    )

creation_results = []
for path in required_paths:
    created = dbutils.fs.mkdirs(path)
    creation_results.append((path, bool(created)))

display(spark.createDataFrame(creation_results, ["path", "mkdirs_returned_true"]))
```

The code is idempotent: running it again keeps existing directories and files.

### 4.2 Verify the hierarchy

Run:

```python
def list_paths(path: str) -> None:
    print(f"\n{path}")
    for item in dbutils.fs.ls(path):
        print("  ", item.path)

list_paths("dbfs:/FileStore/pakistan_economy/")
list_paths("dbfs:/FileStore/pakistan_economy/staging/full/")
list_paths("dbfs:/FileStore/pakistan_economy/staging/incremental/batch_001/")
```

Expected top-level output includes `staging`, `bronze`, `silver`, `gold`, `quarantine`, `checkpoints`, and `ops`.

### 4.3 Upload raw files using the UI

The exact upload label varies by legacy workspace. Try these navigation paths in order:

```text
Data > Add data > Upload files > DBFS

or

Data > Create table > Upload file

or

Catalog/Data > Add data > Upload files
```

For each source folder:

1. Select files from local `data/raw/full/<source_folder>/`.
2. Set the destination to `dbfs:/FileStore/pakistan_economy/staging/full/<source_folder>/` if the UI permits a destination.
3. Upload and wait for success before closing the page.
4. Repeat for `data/raw/incremental/batch_001/<source_folder>/`, targeting `dbfs:/FileStore/pakistan_economy/staging/incremental/batch_001/<source_folder>/`.
5. Return to the notebook and verify with `dbutils.fs.ls()`.

Some legacy upload screens always place files under `dbfs:/FileStore/tables/`. If so, copy each file to its final path and verify it before removing or ignoring the temporary upload:

```python
source = "dbfs:/FileStore/tables/dataset.csv"
target = (
    "dbfs:/FileStore/pakistan_economy/"
    "staging/full/sbp_remittances/dataset.csv"
)

assert dbutils.fs.cp(source, target), "DBFS copy failed"
display(dbutils.fs.ls("dbfs:/FileStore/pakistan_economy/staging/full/sbp_remittances"))
```

Do not overwrite an existing raw file casually. If a publisher reused a filename with different bytes, keep both immutable snapshots under distinct batch/hash-aware paths.

If there is no DBFS destination and every UI path points only to Unity Catalog tables/Volumes, your workspace is not compatible with this legacy step. Return to the instructor rather than uploading to an unrelated location.

### 4.4 Upload raw files with the Databricks CLI (recommended for folders)

The command-line upload is repeatable and preserves the source-folder organization better than a browser upload.

#### Install the current CLI on your computer

On Windows PowerShell/Command Prompt:

```powershell
winget search databricks
winget install Databricks.DatabricksCLI
databricks -v
```

The version should be `0.205.0` or newer. On macOS/Linux, follow the official [Databricks CLI installation guide](https://docs.databricks.com/aws/en/dev-tools/cli/install).

#### Authenticate — do not confuse the two tokens

- The **GitHub PAT** from Step 2 authenticates Databricks to GitHub.
- A **Databricks OAuth login or Databricks PAT** authenticates your computer's CLI to the Databricks workspace.

Try the safer browser-based OAuth flow first:

```powershell
databricks auth login --host "https://<your-workspace-host>" --profile pakistan-economy
databricks auth profiles
databricks current-user me --profile pakistan-economy
```

Use only the origin part of the workspace URL—no trailing notebook path. Example: `https://dbc-a1b2c3d4.cloud.databricks.com`.

If the legacy workspace does not support OAuth:

1. In Databricks, navigate to **User icon > Settings > Developer > Access tokens > Manage > Generate new token**. On older screens use **User Settings > Access tokens**.
2. Give it a short lifetime and copy it once.
3. Run:

```powershell
databricks configure --host "https://<your-workspace-host>" --profile pakistan-economy
```

4. Paste the **Databricks** token only at the hidden token prompt.

If the Access Tokens menu is absent, PATs are disabled. Use OAuth; do not paste the GitHub token as a substitute.

#### Upload from the repository root

Open PowerShell in the local repository root—the directory that contains `data/`. Run:

```powershell
$sourceFolders = @(
  "sbp_remittances", "sbp_fdi", "sbp_fx", "sbp_exports",
  "sbp_imports", "pbs_spi", "pbs_cpi", "ogra_fuel"
)

foreach ($sourceFolder in $sourceFolders) {
  databricks fs cp `
    "data/raw/full/$sourceFolder" `
    "dbfs:/FileStore/pakistan_economy/staging/full/$sourceFolder" `
    --recursive `
    --profile pakistan-economy

  databricks fs cp `
    "data/raw/incremental/batch_001/$sourceFolder" `
    "dbfs:/FileStore/pakistan_economy/staging/incremental/batch_001/$sourceFolder" `
    --recursive `
    --profile pakistan-economy
}
```

Do not add `--overwrite` to a raw-data upload. An existing file should cause you to stop and compare hashes rather than destroy an immutable source snapshot.

Verify representative destinations:

```powershell
databricks fs ls "dbfs:/FileStore/pakistan_economy/staging/full" --profile pakistan-economy
databricks fs ls "dbfs:/FileStore/pakistan_economy/staging/full/sbp_remittances" --profile pakistan-economy
databricks fs ls "dbfs:/FileStore/pakistan_economy/staging/incremental/batch_001/pbs_spi" --profile pakistan-economy
```

macOS/Linux uses the same commands, for example:

```bash
databricks fs cp \
  "data/raw/full/sbp_remittances" \
  "dbfs:/FileStore/pakistan_economy/staging/full/sbp_remittances" \
  --recursive --profile pakistan-economy
```

Repeat the command for the eight source folders and for `data/raw/incremental/batch_001`. The CLI requires the `dbfs:/` scheme for DBFS paths. See the official [`fs` command reference](https://docs.databricks.com/aws/en/dev-tools/cli/reference/fs-commands).

### 4.5 Upload one file using the DBFS REST API (fallback)

Use this only when the CLI filesystem command is unavailable and the workspace still exposes the DBFS API. The configured CLI is safer for a beginner because it does not require placing a token in a command.

If you deliberately use a short-lived Databricks PAT in a temporary environment variable, a multipart upload looks like this:

```powershell
$env:DATABRICKS_HOST = "https://<your-workspace-host>"
$env:DATABRICKS_TOKEN = Read-Host "Paste short-lived Databricks token"

curl.exe --request POST `
  --url "$env:DATABRICKS_HOST/api/2.0/dbfs/put" `
  --header "Authorization: Bearer $env:DATABRICKS_TOKEN" `
  --form "path=/FileStore/pakistan_economy/staging/full/sbp_remittances/dataset.csv" `
  --form "overwrite=false" `
  --form "contents=@data/raw/full/sbp_remittances/dataset.csv"

Remove-Item Env:DATABRICKS_TOKEN
```

The API `path` uses `/FileStore/...`, not `dbfs:/FileStore/...`. Verify the result with `databricks fs ls` or `dbutils.fs.ls()`. Never put the token in a script file or shell history.

### 4.6 Verify file counts and hashes

At minimum, compare local and remote file counts for each source. Locally in PowerShell:

```powershell
Get-ChildItem -LiteralPath "data/raw/full" -File -Recurse |
  Select-Object FullName, Length,
    @{Name="SHA256";Expression={(Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash}}
```

In a Databricks notebook:

```python
def files_recursive(root: str):
    found = []
    pending = [root]
    while pending:
        current = pending.pop()
        for entry in dbutils.fs.ls(current):
            if entry.isDir():
                pending.append(entry.path)
            else:
                found.append((entry.path, entry.size))
    return found

full_files = files_recursive("dbfs:/FileStore/pakistan_economy/staging/full")
incremental_files = files_recursive(
    "dbfs:/FileStore/pakistan_economy/staging/incremental/batch_001"
)

print("Full-load files:", len(full_files))
print("Incremental files:", len(incremental_files))
display(spark.createDataFrame(full_files + incremental_files, ["path", "size_bytes"]))
```

The production acquisition process must compute SHA-256 while registering each file in the manifest. This setup check confirms visibility and size; it is not a replacement for manifest hashing.

**Checkpoint 4:** All required directories exist, representative full and incremental files are visible at the intended DBFS paths, and local raw files were not committed to Git by accident.

## Step 5 — Initialize the Hive Metastore databases

This section is for the legacy DBFS/`spark_catalog` profile. The SQL uses explicit database locations so managed Delta table data stays under the course project root.

### 5.1 Create the databases

Create a notebook named `02_initialize_metastore`, choose **SQL**, attach the legacy cluster, and run:

```sql
CREATE DATABASE IF NOT EXISTS lakehouse_ops
LOCATION 'dbfs:/FileStore/pakistan_economy/ops/_tables';

CREATE DATABASE IF NOT EXISTS lakehouse_bronze
LOCATION 'dbfs:/FileStore/pakistan_economy/bronze/_tables';

CREATE DATABASE IF NOT EXISTS lakehouse_silver
LOCATION 'dbfs:/FileStore/pakistan_economy/silver/_tables';

CREATE DATABASE IF NOT EXISTS lakehouse_gold
LOCATION 'dbfs:/FileStore/pakistan_economy/gold/_tables';
```

`DATABASE` and `SCHEMA` are synonyms in this legacy metastore context. Keep Quarantine files under `dbfs:/FileStore/pakistan_economy/quarantine/`; the later pipeline can register its quarantine Delta table in `lakehouse_ops`.

### 5.2 Verify `spark_catalog`

Run:

```sql
SELECT current_catalog();

SHOW DATABASES IN spark_catalog;

DESCRIBE DATABASE EXTENDED lakehouse_ops;
DESCRIBE DATABASE EXTENDED lakehouse_bronze;
DESCRIBE DATABASE EXTENDED lakehouse_silver;
DESCRIBE DATABASE EXTENDED lakehouse_gold;
```

Expected database names:

```text
lakehouse_ops
lakehouse_bronze
lakehouse_silver
lakehouse_gold
```

Each `Location` must point to the corresponding `dbfs:/FileStore/pakistan_economy/.../_tables` path. If a database already exists with a different location, stop and show the `DESCRIBE DATABASE EXTENDED` output to the instructor. Do not drop a database that might contain another student's tables.

### 5.3 Keep the SQL in source control

Place the four `CREATE DATABASE` statements in a tracked file such as `sql/create_catalog_objects.sql`. Commit the SQL file, not the generated database contents.

**Checkpoint 5:** All four databases appear in `spark_catalog` and each has the intended project-specific DBFS location.
