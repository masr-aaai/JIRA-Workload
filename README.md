# streamlit_worklogs_transfer — eco2ve TimeSheet Generator

Short description
- Streamlit app that generates an eco2ve TimeSheet Excel file from monthly worklog XLSX files.
- The app reads `Employees.xlsx` (maps Resource No. → Personal Number), uses `eco2ve_TimeSheet.xlsx` as template and processes all worklog .xlsx files inside a monthly folder (e.g. `2025-10`).

Prerequisites
- Windows, Python 3.8 (or compatible)
- Dependencies listed in `requirements.txt` (streamlit, pandas, openpyxl, requests, ...)

Expected folder structure (example)
- base_folder/
  - Employees.xlsx
  - eco2ve_TimeSheet.xlsx
  - 2025-10/
    - alice_worklog.xlsx
    - bob_worklog.xlsx

Important file/column names
- Worklog files must contain columns: `Resource No.`, `Date`, `Start Time`, `End Time`, `Text/Description`
- `Employees.xlsx` must contain: `Resource No.` and `Personal Number`

Install dependencies (recommended: use centralized venv)
1. Change to the project folder:
   cd "C:\Users\masr\OneDrive - VINCI Energies\Dokumente\GitHub\JIRA-Workload"

2. Install into the central venv (example path):
   C:\Users\masr\VENVs\.venv38\Scripts\python.exe -m pip install --upgrade pip
   C:\Users\masr\VENVs\.venv38\Scripts\python.exe -m pip install -r requirements.txt

Or activate the venv in PowerShell and install:
   & C:\Users\masr\VENVs\.venv38\Scripts\Activate.ps1
   pip install -r requirements.txt

Run the Streamlit app
- Preferred (use full venv python to avoid OneDrive launcher issues):
  C:\Users\masr\VENVs\.venv38\Scripts\python.exe -m streamlit run "streamlit_worklogs_transfer.py"

- Or after activation:
  & C:\Users\masr\VENVs\.venv38\Scripts\Activate.ps1
  streamlit run "streamlit_worklogs_transfer.py"

How to use the app
1. Enter the full path to the base folder (contains `Employees.xlsx`, the template and the monthly folder).
2. Click "Generate TimeSheet".
3. If successful, the generated file `<YYYY-MM>-eco2veTimeSheet.xlsx` will be saved in the monthly folder and a download button appears.

Troubleshooting
- If `pip` or `python` resolves to a OneDrive path or shows a "launcher" error, always call the desired venv python.exe by full path (see examples above).
- If required columns are missing, the app will report which columns/files are problematic.
- If monthly folder not found, ensure folder name matches `YYYY-MM` for the intended month.

Security & notes
- The app reads and writes only local files; no credentials are persisted.
- Adjust column name constants in `streamlit_worklogs_transfer.py` if your spreadsheets
