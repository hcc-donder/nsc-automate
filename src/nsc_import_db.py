"""
Import NSC StudentTracker return files into the data warehouse (CCDW_STG on the
SQL Server named in config.yml), then rebuild the NSC tables.

Run after a batch of files comes back from NSC (get_nsc_files.py calls it once
at the end of a download). Every step is idempotent, so it is safe to re-run.

Steps:
    1. Move return files from the receive folder to the NSC data folder
       (cfg nsc.local.file_path). If a file with the same name is already
       there and differs, the older copy goes to <data folder>/_superseded/
       with its NSC processing timestamp appended.
    2. Load every DETLRPT and ANALYSISRDY file in the data folder that is not
       yet in CCDW_STG.nsc_staging.nsc_file_manifest (matched on name + hash).
       Student names, dates of birth, and "Your Unique Identifier" (SSN on PA
       requests that send one) are dropped before loading.
    3. Check detail-report students against the control report (CNTLRPT).
    4. Log each file to CCDW.etl.etl_nsc_import_log and nsc_log.csv.
    5. EXEC CCDW.etl.usp_build_nsc (all NSC intermediate tables and marts).

Usage:
    python nsc_import_db.py                 # normal batch run (steps 1-5)
    python nsc_import_db.py --no-move       # initial load from the data folder only
    python nsc_import_db.py --no-build      # load without rebuilding
    python nsc_import_db.py --build-only    # rebuild without loading
    python nsc_import_db.py --dry-run       # show what would happen
    python nsc_import_db.py --ipeds [PATH]  # reload ipeds_staging.ipeds_hd and ipeds_cip from
                                            # the import_ipeds combined DuckDB, then exit
    python nsc_import_db.py ENTRY FN DT     # legacy per-file call from rename_nsc_files.py;
                                            # runs a normal batch run

Design: dw_mssql/docs/NSC_DESIGN.md
"""

import argparse
import csv
import hashlib
import io
import json
import re
import shutil
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Final, List, Optional, Tuple

import pyodbc
import yaml

from support_scripts import find_root, merge_dicts

####
# %% Constants
####
CURRENT_DATETIME: Final[datetime] = datetime.now()

ROOT_PATH: Final[Path] = find_root("_IERG_SHARED_ROOT_DIR_", Path(__file__).resolve().parent)
NSC_PATH: Final[Path] = Path(ROOT_PATH / Path("nsc"))
DATA_PATH: Final[Path] = Path(ROOT_PATH / Path("Data"))
CONFIG_FILE: Final[Path] = Path(DATA_PATH / "config.yml")
NSC_CONFIG_FILE: Final[Path] = Path(NSC_PATH / "nscconfig.yml")

# Return file names after renaming, e.g. 20242_2024FA_DETLRPT_SE_hcc_enrolled.csv
# or 20242-20257_2024FA-2025SU_DETLRPT_SE_hcc_forestry_grads.csv
RETURN_FILE_PATTERN: Final[re.Pattern] = re.compile(
    r"^(?P<termidx>\d+(?:-\d+)?)_"
    r"(?P<termid>\d{4}[A-Za-z]{2}(?:-\d{4}[A-Za-z]{2})?)_"
    r"(?P<nsctype>AGGRRPT|ANALYSISRDY|CNTLRPT|DETLRPT)_"
    r"(?P<nscmode>SE|PA)_"
    r"(?P<desc>.+)\.(?P<ext>csv|htm|html)$",
    re.IGNORECASE,
)

# Detail report header (normalized) -> nsc_staging.nsc_detail column.
# None = deliberately dropped (PII not needed in the warehouse).
DETAIL_COLUMNS: Final[Dict[str, Optional[str]]] = {
    "your_unique_identifier": None,
    "first_name": None,
    "middle_initial": None,
    "last_name": None,
    "name_suffix": None,
    "date_of_birth": None,
    "requester_return_field": "requester_return_field",
    "requestor_return_field": "requester_return_field",
    "record_found_y_n": "record_found",
    "search_date": "search_date",
    "college_code_branch": "college_code_branch",
    "college_name": "college_name",
    "college_state": "college_state",
    "2_year_4_year": "college_level",
    "public_private": "college_control",
    "enrollment_begin": "enrollment_begin",
    "enrollment_end": "enrollment_end",
    "enrollment_status": "enrollment_status",
    "class_level": "class_level",
    "enrollment_major_1": "enrollment_major_1",
    "enrollment_cip_1": "enrollment_cip_1",
    "enrollment_major_2": "enrollment_major_2",
    "enrollment_cip_2": "enrollment_cip_2",
    "graduated": "graduated",
    "graduation_date": "graduation_date",
    "degree_title": "degree_title",
    "degree_major_1": "degree_major_1",
    "degree_cip_1": "degree_cip_1",
    "degree_major_2": "degree_major_2",
    "degree_cip_2": "degree_cip_2",
    "degree_major_3": "degree_major_3",
    "degree_cip_3": "degree_cip_3",
    "degree_major_4": "degree_major_4",
    "degree_cip_4": "degree_cip_4",
    "college_sequence": "college_sequence",
}
DETAIL_TABLE_COLUMNS: Final[List[str]] = list(dict.fromkeys(c for c in DETAIL_COLUMNS.values() if c))

# Analysis-ready columns never stored (normalized header prefixes)
ANALYSIS_READY_DROP: Final[Tuple[str, ...]] = (
    "your_unique_identifier", "first_name", "middle_initial", "last_name",
    "name_suffix", "date_of_birth",
)

REQUEST_TYPE: Final[Dict[str, str]] = {"SE": "SUBSEQUENT_ENROLLMENT", "PA": "PRIOR_ATTENDANCE"}

BATCH_SIZE: Final[int] = 5000


def normalize_header(name: str) -> str:
    """'2-year / 4-year' -> '2_year_4_year', 'Record Found Y/N' -> 'record_found_y_n'."""
    return re.sub(r"[^a-z0-9]+", "_", name.strip().lower()).strip("_")


def load_cfg() -> Dict:
    """Merge Data/config.yml with nsc/nscconfig.yml, the same way get_nsc_files.py does."""
    cfg: Dict = {}
    for cfg_file in (CONFIG_FILE, NSC_CONFIG_FILE):
        try:
            with open(cfg_file, "r", encoding="utf-8") as file:
                cfg = merge_dicts(cfg, yaml.load(file, Loader=yaml.FullLoader) or {})
        except FileNotFoundError:
            pass
    return cfg


def connect(cfg: Dict, server: Optional[str]) -> pyodbc.Connection:
    sql = cfg.get("sql", {})
    conn_str = (
        f"DRIVER={{{sql.get('driver', 'ODBC Driver 17 for SQL Server')}}};"
        f"SERVER={server or sql['server']};DATABASE=CCDW_STG;"
        "Trusted_Connection=yes;Encrypt=yes;TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str, autocommit=False)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def read_text(path: Path) -> str:
    raw = path.read_bytes()
    for encoding in ("utf-8-sig", "cp1252"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("latin-1")


def processed_datetime_lookup(log_file: Path) -> Dict[str, datetime]:
    """
    Map local file name (lower case) -> NSC processing datetime, from the NSC
    download log. The original NSC file name carries it as MMDDYYYYHHMMSS.
    """
    lookup: Dict[str, datetime] = {}
    if not log_file.exists():
        return lookup
    with open(log_file, newline="", encoding="utf-8", errors="replace") as f:
        for row in csv.DictReader(f):
            m = re.search(r"_(\d{14})_", row.get("nsc_file_name") or "")
            local = Path(row.get("local_file_name") or "").name
            if not m or not local:
                continue
            try:
                lookup[local.lower()] = datetime.strptime(m.group(1), "%m%d%Y%H%M%S")
            except ValueError:
                continue
    return lookup


@dataclass
class ReturnFile:
    path: Path
    termidx: str
    termid: str
    nsctype: str
    nscmode: str
    desc: str

    @classmethod
    def parse(cls, path: Path) -> Optional["ReturnFile"]:
        m = RETURN_FILE_PATTERN.match(path.name)
        if not m:
            return None
        return cls(path, m["termidx"], m["termid"].upper(), m["nsctype"].upper(),
                   m["nscmode"].upper(), m["desc"])

    @property
    def canonical_name(self) -> str:
        """Upper-case term id, as the rename patterns now produce (older files were lower case)."""
        return f"{self.termidx}_{self.termid}_{self.nsctype}_{self.nscmode}_{self.desc}{self.path.suffix.lower()}"

    @property
    def group_key(self) -> str:
        """Identifies 'the same report' across re-runs of the same request."""
        return f"{self.termidx}_{self.termid}_{self.nscmode}_{self.desc}".lower()


class Logger:
    """Appends rows to nsc_log.csv in the format get_nsc_files.py uses."""

    def __init__(self, log_file: Path, dry_run: bool):
        self.log_file = log_file
        self.dry_run = dry_run
        self.now = CURRENT_DATETIME.strftime("%Y-%m-%d %H:%M:%S")

    def write(self, nsc_name: str, local_name: str, file_dt: Optional[datetime], status: str) -> None:
        print(f"{status}: {local_name}")
        if self.dry_run:
            return
        new_file = not self.log_file.exists()
        with open(self.log_file, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if new_file:
                writer.writerow(["nsc_file_name", "local_file_name", "file_date_time", "status", "date_time"])
            writer.writerow([nsc_name, local_name,
                             file_dt.strftime("%Y-%m-%d %H:%M:%S") if file_dt else "", status, self.now])


####
# %% Step 1: move received files into the data folder
####
def move_received_files(receive_path: Path, data_path: Path, processed: Dict[str, datetime],
                        log: Logger, dry_run: bool) -> int:
    superseded_path = data_path / "_superseded"
    moved = 0
    for path in sorted(receive_path.iterdir()):
        if not path.is_file():
            continue
        rf = ReturnFile.parse(path)
        if rf is None:
            continue
        target = data_path / rf.canonical_name
        file_dt = processed.get(path.name.lower()) or datetime.fromtimestamp(path.stat().st_mtime)
        if target.exists():
            if sha256(target) == sha256(path):
                if not dry_run:
                    path.unlink()
                log.write(path.name, str(target), file_dt, "Duplicate removed")
                continue
            old_dt = processed.get(target.name.lower()) or datetime.fromtimestamp(target.stat().st_mtime)
            old_name = f"{target.stem}__{old_dt:%Y%m%d_%H%M%S}{target.suffix}"
            if not dry_run:
                superseded_path.mkdir(exist_ok=True)
                shutil.move(str(target), str(superseded_path / old_name))
            log.write(target.name, str(superseded_path / old_name), old_dt, "Superseded")
        if not dry_run:
            shutil.move(str(path), str(target))
        processed[target.name.lower()] = file_dt
        log.write(path.name, str(target), file_dt, "Moved")
        moved += 1
    return moved


####
# %% Step 2: load files
####
def control_total(data_path: Path, rf: ReturnFile) -> Optional[int]:
    """'TOTAL STUDENTS IN YOUR REQUEST FILE: n' from the matching control report."""
    for ext in (".htm", ".html"):
        cntl = data_path / f"{rf.termidx}_{rf.termid}_CNTLRPT_{rf.nscmode}_{rf.desc}{ext}"
        if cntl.exists():
            text = re.sub(r"<[^>]+>|&nbsp;", " ", read_text(cntl))
            m = re.search(r"TOTAL\s+STUDENTS\s+IN\s+YOUR\s+REQUEST\s+FILE\s*:?\s*([\d,]+)", text, re.I)
            if m:
                return int(m.group(1).replace(",", ""))
    return None


def read_detail(path: Path) -> Tuple[List[Tuple], List[str]]:
    reader = csv.reader(io.StringIO(read_text(path)))
    header = next(reader)
    norm = [normalize_header(h) for h in header]
    unmapped = [h for h, n in zip(header, norm) if n and n not in DETAIL_COLUMNS]
    positions: Dict[str, int] = {}
    for i, n in enumerate(norm):
        col = DETAIL_COLUMNS.get(n)
        if col and col not in positions:
            positions[col] = i
    rows = []
    for row_number, row in enumerate(reader, start=1):
        if not any(v.strip() for v in row):
            continue
        values = []
        for col in DETAIL_TABLE_COLUMNS:
            i = positions.get(col)
            v = row[i].strip() if i is not None and i < len(row) else ""
            values.append(v or None)
        rows.append((row_number, *values))
    return rows, unmapped


def read_analysis_ready(path: Path) -> List[Tuple]:
    reader = csv.reader(io.StringIO(read_text(path)))
    header = [h.strip() for h in next(reader)]
    norm = [normalize_header(h) for h in header]
    return_col = next((i for i, n in enumerate(norm) if n in ("requester_return_field", "requestor_return_field")), None)
    search_col = next((i for i, n in enumerate(norm) if n == "search_date"), None)
    rows = []
    for row_number, row in enumerate(reader, start=1):
        if not any(v.strip() for v in row):
            continue
        record = {h: v.strip() for h, n, v in zip(header, norm, row) if n and not n.startswith(ANALYSIS_READY_DROP)}
        return_field = row[return_col].strip() if return_col is not None and return_col < len(row) else ""
        search_date = row[search_col].strip() if search_col is not None and search_col < len(row) else ""
        rows.append((row_number, return_field or None, search_date or None, json.dumps(record)))
    return rows


def load_files(conn: pyodbc.Connection, data_path: Path, processed: Dict[str, datetime],
               log: Logger, dry_run: bool) -> int:
    cur = conn.cursor()
    cur.execute("SELECT LOWER(file_name), file_sha256 FROM nsc_staging.nsc_file_manifest")
    loaded = {(name, h) for name, h in cur.fetchall()}

    files = [rf for p in sorted(data_path.iterdir()) if p.is_file()
             and (rf := ReturnFile.parse(p)) and rf.nsctype in ("DETLRPT", "ANALYSISRDY")]
    return_field_pos = 1 + DETAIL_TABLE_COLUMNS.index("requester_return_field")
    count = 0
    for rf in files:
        file_hash = sha256(rf.path)
        if (rf.path.name.lower(), file_hash) in loaded:
            continue
        file_dt = processed.get(rf.path.name.lower()) or datetime.fromtimestamp(rf.path.stat().st_mtime)

        if rf.nsctype == "DETLRPT":
            rows, unmapped = read_detail(rf.path)
            expected = control_total(data_path, rf)
            students = len({r[return_field_pos] for r in rows})
            if expected is not None and expected != students:
                print(f"WARNING: {rf.path.name}: {students} students in detail, "
                      f"control report says {expected} in request file")
        else:
            rows, unmapped, expected = read_analysis_ready(rf.path), [], None

        if dry_run:
            log.write(rf.path.name, str(rf.path), file_dt, f"Would import {len(rows)} rows")
            count += 1
            continue

        try:
            cur.execute(
                """
                SET NOCOUNT ON;
                INSERT INTO nsc_staging.nsc_file_manifest
                    (file_name, report_type, inquiry_type, request_term_index, request_term_id,
                     request_description, nsc_processed_datetime, file_sha256, row_count,
                     control_total, unmapped_columns)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                SELECT CAST(SCOPE_IDENTITY() AS int);
                """,
                rf.path.name, rf.nsctype, rf.nscmode, rf.termidx, rf.termid, rf.desc,
                file_dt.replace(microsecond=0), file_hash, len(rows), expected,
                ", ".join(unmapped)[:1000] or None,
            )
            file_id = cur.fetchone()[0]

            if rf.nsctype == "DETLRPT":
                sql = (f"INSERT INTO nsc_staging.nsc_detail (file_id, row_number, {', '.join(DETAIL_TABLE_COLUMNS)}) "
                       f"VALUES ({', '.join(['?'] * (len(DETAIL_TABLE_COLUMNS) + 2))})")
                cur.fast_executemany = True
            else:
                sql = ("INSERT INTO nsc_staging.nsc_analysis_ready "
                       "(file_id, row_number, requester_return_field, search_date, row_json) VALUES (?, ?, ?, ?, ?)")
                cur.fast_executemany = False   # nvarchar(max) JSON: row-wise is slower but safe
            for start in range(0, len(rows), BATCH_SIZE):
                cur.executemany(sql, [(file_id, *r) for r in rows[start:start + BATCH_SIZE]])
            cur.fast_executemany = False

            # Older loads of the same report are no longer the current version
            cur.execute(
                """
                UPDATE m SET is_current_version = 0, superseded_by_file_id = ?
                FROM nsc_staging.nsc_file_manifest m
                WHERE m.file_id <> ? AND m.report_type = ? AND m.is_current_version = 1
                  AND LOWER(CONCAT(m.request_term_index, '_', m.request_term_id, '_',
                                   m.inquiry_type, '_', m.request_description)) = ?
                """,
                file_id, file_id, rf.nsctype, rf.group_key,
            )
            cur.execute(
                "INSERT INTO CCDW.etl.etl_nsc_import_log (file_name, file_date, request_type, row_count) "
                "VALUES (?, ?, ?, ?)",
                rf.path.name, file_dt.date(), REQUEST_TYPE[rf.nscmode], len(rows),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            log.write(rf.path.name, str(rf.path), file_dt, "Import failed")
            raise
        log.write(rf.path.name, str(rf.path), file_dt, "Imported")
        count += 1
    return count


####
# %% IPEDS HD
####
def load_ipeds(conn: pyodbc.Connection, duckdb_path: Path, dry_run: bool) -> None:
    """Replace ipeds_staging.ipeds_hd and ipeds_cip from the import_ipeds combined DuckDB."""
    import duckdb  # only needed for this option

    con = duckdb.connect(str(duckdb_path), read_only=True)
    rows = con.sql(
        """
        WITH labels AS (
            SELECT lower(varname) AS varname, codevalue, valuelabel,
                   row_number() OVER (PARTITION BY lower(varname), codevalue
                                      ORDER BY last_collection_year DESC) AS rn
            FROM combine_valuesets WHERE base_group = 'hd{y}'
        ), lbl AS (SELECT * FROM labels WHERE rn = 1),
        hd AS (
            SELECT *,
                CASE WHEN c21basic IS NOT NULL THEN 'c21basic' WHEN c18basic IS NOT NULL THEN 'c18basic'
                     WHEN c15basic IS NOT NULL THEN 'c15basic' WHEN ccbasic IS NOT NULL THEN 'ccbasic' END AS cv,
                coalesce(c21basic, c18basic, c15basic, ccbasic) AS cb
            FROM hd
        )
        SELECT hd.unitid, hd.collection_year, hd.instnm, hd.city, hd.stabbr, trim(hd.opeid),
               hd.sector, s.valuelabel, hd.iclevel, l.valuelabel, hd.control, c.valuelabel,
               hd.hloffer, h.valuelabel, hd.locale, lo.valuelabel, hd.cv, hd.cb, cb.valuelabel,
               CAST(hd.closedat AS VARCHAR), hd.newid
        FROM hd
        LEFT JOIN lbl s  ON s.varname = 'sector'  AND s.codevalue  = CAST(hd.sector AS VARCHAR)
        LEFT JOIN lbl l  ON l.varname = 'iclevel' AND l.codevalue  = CAST(hd.iclevel AS VARCHAR)
        LEFT JOIN lbl c  ON c.varname = 'control' AND c.codevalue  = CAST(hd.control AS VARCHAR)
        LEFT JOIN lbl h  ON h.varname = 'hloffer' AND h.codevalue  = CAST(hd.hloffer AS VARCHAR)
        LEFT JOIN lbl lo ON lo.varname = 'locale' AND lo.codevalue = CAST(hd.locale AS VARCHAR)
        LEFT JOIN lbl cb ON cb.varname = hd.cv    AND cb.codevalue = CAST(hd.cb AS VARCHAR)
        """
    ).fetchall()
    # CIP codes and titles from the Completions value sets; latest title per code
    cip_rows = con.sql(
        """
        SELECT codevalue, valuelabel, last_collection_year
        FROM (
            SELECT codevalue, valuelabel, last_collection_year,
                   row_number() OVER (PARTITION BY codevalue ORDER BY last_collection_year DESC) AS rn
            FROM combine_valuesets
            WHERE base_group LIKE 'c%_a' AND lower(varname) = 'cipcode'
              AND regexp_matches(codevalue, '^\\d{2}(\\.\\d{2}(\\d{2})?)?$') AND codevalue <> '99'
        ) WHERE rn = 1
        """
    ).fetchall()
    con.close()
    # Some NCES labels carry trailing CR/LF
    rows = [tuple(v.strip() if isinstance(v, str) else v for v in r) for r in rows]
    cip_rows = [tuple(v.strip() if isinstance(v, str) else v for v in r) for r in cip_rows]
    print(f"IPEDS HD rows read: {len(rows)}; CIP codes read: {len(cip_rows)}")
    if dry_run:
        return
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE ipeds_staging.ipeds_cip")
    cur.fast_executemany = True
    cur.executemany("INSERT INTO ipeds_staging.ipeds_cip (cip_code, cip_title, last_collection_year) "
                    "VALUES (?, ?, ?)", cip_rows)
    cur.execute("TRUNCATE TABLE ipeds_staging.ipeds_hd")
    sql = ("INSERT INTO ipeds_staging.ipeds_hd (unitid, collection_year, instnm, city, stabbr, opeid, "
           "sector, sector_label, iclevel, iclevel_label, control, control_label, hloffer, hloffer_label, "
           "locale, locale_label, carnegie_version, carnegie_basic, carnegie_label, closedat, newid) "
           f"VALUES ({', '.join(['?'] * 21)})")
    for start in range(0, len(rows), BATCH_SIZE):
        cur.executemany(sql, rows[start:start + BATCH_SIZE])
    conn.commit()
    print("Loaded ipeds_staging.ipeds_hd and ipeds_staging.ipeds_cip")


def run_build(conn: pyodbc.Connection) -> None:
    print("Running CCDW.etl.usp_build_nsc ...")
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("EXEC CCDW.etl.usp_build_nsc")
    while cur.nextset():   # drain result sets so errors surface
        pass
    print("Build complete")


####
# %% Main
####
def main() -> int:
    parser = argparse.ArgumentParser(description="Import NSC return files into the warehouse.")
    parser.add_argument("legacy", nargs="*", help="ENTRY FN DT from the per-file import hook (ignored)")
    parser.add_argument("--no-move", action="store_true", help="do not move files from the receive folder")
    parser.add_argument("--no-build", action="store_true", help="do not run CCDW.etl.usp_build_nsc")
    parser.add_argument("--build-only", action="store_true", help="only run CCDW.etl.usp_build_nsc")
    parser.add_argument("--dry-run", action="store_true", help="report what would happen; change nothing")
    parser.add_argument("--ipeds", nargs="?", const="", metavar="DUCKDB",
                        help="reload IPEDS HD from the combined DuckDB (default: cfg ipeds.combined_duckdb)")
    parser.add_argument("--server", help="override cfg sql.server (e.g. localhost\\SQLEXPRESS)")
    parser.add_argument("--data-path", help="override cfg nsc.local.file_path")
    parser.add_argument("--receive-path", help="override cfg nsc.local.receive_path")
    parser.add_argument("--log-file", help="override cfg nsc.local.log_file")
    args = parser.parse_args()

    cfg = load_cfg()
    conn = connect(cfg, args.server)

    if args.ipeds is not None:
        duckdb_path = args.ipeds or cfg.get("ipeds", {}).get("combined_duckdb")
        if not duckdb_path:
            print("No IPEDS DuckDB path given and cfg ipeds.combined_duckdb is not set")
            return 1
        load_ipeds(conn, Path(duckdb_path), args.dry_run)
        return 0

    if args.build_only:
        run_build(conn)
        return 0

    data_path = Path(args.data_path or cfg["nsc"]["local"]["file_path"])
    receive_path = Path(args.receive_path or cfg["nsc"]["local"]["receive_path"])
    log = Logger(Path(args.log_file or cfg["nsc"]["local"]["log_file"]), args.dry_run)
    processed = processed_datetime_lookup(log.log_file)

    if not args.no_move:
        moved = move_received_files(receive_path, data_path, processed, log, args.dry_run)
        print(f"Files moved to {data_path}: {moved}")

    imported = load_files(conn, data_path, processed, log, args.dry_run)
    print(f"Files imported: {imported}")

    if imported and not args.no_build and not args.dry_run:
        run_build(conn)
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
