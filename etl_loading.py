# requirements: psycopg2-binary>=2.9
import csv
import os
import glob
import psycopg2
from psycopg2 import sql
import tempfile

# ---- CONFIG ----
PG_DSN = "postgresql://postgres:password@localhost:5432/DMproject"
CSV_DIR = "testdata/staging"
DEFAULT_SCHEMA = "rdl"
TRUNCATE_BEFORE_LOAD = True
STRIP_SUFFIX = "_staging"
FORCE_TARGET_SCHEMA = None
# ----------------


LOAD_ORDER = [
    "countries",
    "drivers",
    "driver_nationality",
    "constructors",
    "constructor_nationality",
    "circuits",
    "seasons",
    "races",
    "constructors_standings",
    "constructors_results",
    "status",
    "drivers_standings",
    "lap_times",
    "pit_stops",
    "qualifying",
    "race_results",
    "sprint_results",
    "sessions",
    "weather",
    "race_lineup",
    "speed",
    "stints"
]

def parse_table_from_filename(path):
    base = os.path.basename(path)
    stem = os.path.splitext(base)[0]
    parts = stem.split(".")
    if len(parts) == 1:
        schema, table = DEFAULT_SCHEMA, parts[0]
    elif len(parts) == 2:
        schema, table = parts[0], parts[1]
    else:
        raise ValueError(f"Unexpected filename format: {base}")
    if STRIP_SUFFIX and table.endswith(STRIP_SUFFIX):
        table = table[: -len(STRIP_SUFFIX)]
    if FORCE_TARGET_SCHEMA:
        schema = FORCE_TARGET_SCHEMA
    return schema, table

def table_exists(conn, schema, table):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema=%s AND table_name=%s
            LIMIT 1
        """, (schema, table))
        return cur.fetchone() is not None

def get_table_columns(conn, schema, table):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema=%s AND table_name=%s
            ORDER BY ordinal_position
        """, (schema, table))
        return [r[0] for r in cur.fetchall()]

def maybe_truncate(conn, schema, table):
    if not TRUNCATE_BEFORE_LOAD:
        return
    with conn.cursor() as cur:
        ident = sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(table))
        cur.execute(sql.SQL("TRUNCATE {} RESTART IDENTITY CASCADE;").format(ident))
        print(f"  - truncated {schema}.{table}")

def normalize_nulls(row):
    return ["" if val == r"\N" else val for val in row]

def load_csv_into_table(conn, schema, table, csv_path):
    # Step 1: Read + normalize CSV into memory
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        header = [h.strip() for h in header]
        normalized_rows = [normalize_nulls(row) for row in reader]

    # Step 2: Sanity checks
    table_cols = set(get_table_columns(conn, schema, table))
    csv_cols = set(header)
    missing = table_cols - csv_cols
    extra = csv_cols - table_cols
    if missing:
        print(f"[WARN] {schema}.{table}: missing in CSV: {sorted(missing)}")
    if extra:
        print(f"[WARN] {schema}.{table}: extra in CSV (ensure table has these): {sorted(extra)}")

    # Step 3: Write normalized CSV to temporary file
    with tempfile.NamedTemporaryFile("w+", newline="", encoding="utf-8") as tmp:
        writer = csv.writer(tmp)
        writer.writerow(header)
        writer.writerows(normalized_rows)
        tmp.flush()
        tmp.seek(0)

        # Step 4: COPY with NULL '' (empty string becomes SQL NULL)
        opts = ["FORMAT csv", "HEADER true", "NULL ''"]
        with conn.cursor() as cur:
            copy_sql = sql.SQL("COPY {}.{} ({}) FROM STDIN WITH (" + ", ".join(opts) + ")").format(
                sql.Identifier(schema),
                sql.Identifier(table),
                sql.SQL(", ").join(map(sql.Identifier, header))
            )
            cur.copy_expert(copy_sql, tmp)

def sort_key(path):
    _, table = parse_table_from_filename(path)
    try:
        return LOAD_ORDER.index(table)
    except ValueError:
        return len(LOAD_ORDER)  # unknown tables go last

def main():
    csvs = sorted(glob.glob(os.path.join(CSV_DIR, "*.csv")), key=sort_key)
    if not csvs:
        raise SystemExit(f"No CSVs found in {CSV_DIR}")

    with psycopg2.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("SET CONSTRAINTS ALL DEFERRED;")

        for path in csvs:
            schema, table = parse_table_from_filename(path)
            print(f"Loading {path} -> {schema}.{table}")
            if not table_exists(conn, schema, table):
                raise SystemExit(f"[ERROR] Target table {schema}.{table} does not exist.")
            maybe_truncate(conn, schema, table)
            try:
                load_csv_into_table(conn, schema, table, path)
            except Exception as e:
                raise SystemExit(f"[ERROR] Failed loading {path} into {schema}.{table}: {e}")

if __name__ == "__main__":
    main()

