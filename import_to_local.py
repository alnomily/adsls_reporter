import json
import psycopg2
import os
import glob
from datetime import datetime
from psycopg2.extras import execute_values

# Local PostgreSQL connection
LOCAL_DB = {
    "host": "localhost",
    "port": 5432,
    "database": "postgres",
    "user": "postgres",
    "password": "alnomily_2024"
}

# Import settings
# If EXPORT_DIR is None, the latest `supabase_export_*` folder will be used.
EXPORT_DIR = None

# When True, TRUNCATE all tables (CASCADE) before importing.
# This avoids duplicate key errors and, importantly, avoids broken foreign-keys
# when identity columns were previously auto-generated.
RESET_BEFORE_IMPORT = True

# Batch size for PostgreSQL inserts
INSERT_BATCH_SIZE = 2000


def _pick_export_dir() -> str:
    if EXPORT_DIR:
        return EXPORT_DIR
    export_dirs = glob.glob("supabase_export_20260203_065855")
    if not export_dirs:
        return ""
    export_dirs.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return export_dirs[0]


def _get_pk_columns(cur, table_name: str):
    cur.execute(
        """
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = %s::regclass
          AND i.indisprimary
        ORDER BY array_position(i.indkey, a.attnum)
        """,
        (f"public.{table_name}",),
    )
    return [r[0] for r in cur.fetchall()]


def _get_identity_columns(cur, table_name: str):
    cur.execute(
        """
        SELECT column_name, identity_generation
        FROM information_schema.columns
        WHERE table_schema='public'
          AND table_name=%s
          AND is_identity='YES'
        """,
        (table_name,),
    )
    return {name: gen for (name, gen) in cur.fetchall()}


def _ensure_enum_values(cur):
    # Fix: chats_networks has network_type_enum but data contains "owner"
    try:
        cur.execute("ALTER TYPE network_type_enum ADD VALUE IF NOT EXISTS 'owner'")
    except Exception:
        # Older postgres versions may not support IF NOT EXISTS
        try:
            cur.execute("ALTER TYPE network_type_enum ADD VALUE 'owner'")
        except Exception:
            pass

    # Fix: chats_networks data may contain "partner"
    try:
        cur.execute("ALTER TYPE network_type_enum ADD VALUE IF NOT EXISTS 'partner'")
    except Exception:
        try:
            cur.execute("ALTER TYPE network_type_enum ADD VALUE 'partner'")
        except Exception:
            pass

    # Fix: chats_networks has permissions_enum but data contains "owner"
    try:
        cur.execute("ALTER TYPE permissions_enum ADD VALUE IF NOT EXISTS 'owner'")
    except Exception:
        try:
            cur.execute("ALTER TYPE permissions_enum ADD VALUE 'owner'")
        except Exception:
            pass

    # Fix: chats_networks data may contain these permission values
    for v in ("full", "read", "read_write"):
        try:
            cur.execute(f"ALTER TYPE permissions_enum ADD VALUE IF NOT EXISTS '{v}'")
        except Exception:
            try:
                cur.execute(f"ALTER TYPE permissions_enum ADD VALUE '{v}'")
            except Exception:
                pass

def import_json_to_postgres():
    # Connect to local PostgreSQL
    conn = psycopg2.connect(**LOCAL_DB)
    cur = conn.cursor()

    export_dir = _pick_export_dir()
    if export_dir:
        json_files = glob.glob(os.path.join(export_dir, "*.json"))
    else:
        # Fallback: current directory
        json_files = glob.glob("*.json")

    if export_dir:
        print(f"Using export directory: {export_dir}")
    
    print(f"Found {len(json_files)} JSON files to import")

    if not json_files:
        print("No JSON files found. Make sure you have a supabase_export_* folder with .json files.")
        cur.close()
        conn.close()
        return

    # Sort tables for parent-first import (customize as needed)
    # IMPORTANT: some tables reference others via FKs.
    table_order = [
        'accounts_plans',
        'networks',
        'users_accounts',
        'temp_users',
        'chats_users',
        'chats_networks',
        'pending_requests',
        'account_data',
        'logs',
        'app_received_notifications',
        'temp_messages',
        'temp_offers',
        'temp_profiles',
        'temp_rewards_requests',
        'temp_user_cards',
        'network_partners',
        'payments',
        'adsl_plans',
        'nigga',
    ]

    def _table_name_from_path(p: str) -> str:
        return os.path.basename(p).replace('.json', '')

    json_files_sorted = sorted(
        json_files,
        key=lambda f: table_order.index(_table_name_from_path(f)) if _table_name_from_path(f) in table_order else 999,
    )

    # Optional reset to avoid duplicates and broken FK mappings
    if RESET_BEFORE_IMPORT:
        print("\nRESET_BEFORE_IMPORT=True -> truncating tables (CASCADE) ...")
        # Truncate in reverse dependency order (best-effort). CASCADE handles dependencies.
        truncate_tables = [
            _table_name_from_path(f) for f in json_files_sorted
        ]
        # Deduplicate
        seen = set()
        truncate_tables_unique = []
        for t in truncate_tables:
            if t not in seen:
                seen.add(t)
                truncate_tables_unique.append(t)
        # Execute
        try:
            cur.execute("SELECT 1")
            _ensure_enum_values(cur)
            conn.commit()
        except Exception:
            conn.rollback()
        try:
            cur.execute(
                "TRUNCATE " + ", ".join([f"public.{t}" for t in truncate_tables_unique]) + " RESTART IDENTITY CASCADE"
            )
            conn.commit()
            print("  ✅ Truncated tables")
        except Exception as e:
            conn.rollback()
            print(f"  ⚠️  Could not truncate tables: {e}")

    # Ensure required enum values exist before importing
    try:
        _ensure_enum_values(cur)
        conn.commit()
    except Exception:
        conn.rollback()
    for json_file in json_files_sorted:
        table_name = os.path.basename(json_file).replace('.json', '')
        print(f"\nImporting {table_name}...")
        try:
            # Read JSON file with utf-8
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if not data:
                print(f"  ⚠️  No data in {table_name}")
                continue

            # Discover PK columns and identity columns from DB
            pk_cols = _get_pk_columns(cur, table_name)
            identity_cols = _get_identity_columns(cur, table_name)

            # Get column names from first row
            columns = list(data[0].keys())

            # Keep identity values (so FKs match) by using OVERRIDING SYSTEM VALUE
            use_override_system_value = any(gen == 'ALWAYS' for gen in identity_cols.values())

            # If table has identity columns but the JSON doesn't contain them, fine.
            # If table has identity columns and JSON contains them, we keep them.
            # (This prevents broken FK mappings like networks.id and users_accounts.network_id)

            # Prepare values
            values = []
            for row in data:
                row_values = []
                for col in columns:
                    value = row.get(col)
                    if isinstance(value, dict):
                        value = json.dumps(value, ensure_ascii=False)
                    row_values.append(value)
                values.append(row_values)

            columns_str = ', '.join([f'"{col}"' for col in columns])

            conflict_clause = ""
            if pk_cols:
                pk_str = ", ".join([f'"{c}"' for c in pk_cols])
                conflict_clause = f" ON CONFLICT ({pk_str}) DO NOTHING"
            else:
                conflict_clause = " ON CONFLICT DO NOTHING"

            override_clause = " OVERRIDING SYSTEM VALUE" if use_override_system_value else ""
            query = f'INSERT INTO public.{table_name} ({columns_str}){override_clause} VALUES %s{conflict_clause}'

            # Execute in batches
            total = len(values)
            imported = 0
            for start in range(0, total, INSERT_BATCH_SIZE):
                batch = values[start:start + INSERT_BATCH_SIZE]
                execute_values(cur, query, batch)
                imported += len(batch)
            conn.commit()
            print(f"  ✅ Imported {len(data)} rows into {table_name}")
        except Exception as e:
            print(f"  ❌ Error importing {table_name}: {str(e)}")
            conn.rollback()
            continue
    
    # Commit all changes
    try:
        conn.commit()
    except Exception:
        conn.rollback()
    cur.close()
    conn.close()
    
    print("\n" + "="*50)
    print("✅ Import completed!")
    print("="*50)

if __name__ == "__main__":
    import_json_to_postgres()