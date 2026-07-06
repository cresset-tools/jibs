#!/usr/bin/env python3
"""
Generate a complete .jibs import configuration from a Magento schema dump.

Uses schema-relations.py logic to discover relations (explicit FK + inferred),
then categorizes tables and adds anonymization rules.

Usage:
    python3 scripts/generate-magento-jibs.py your-schema.sql > shop.jibs
"""

import re
import sys
import argparse
from collections import defaultdict
from dataclasses import dataclass, field

# Re-uses parsing logic from schema-relations.py (standalone, no import needed)


@dataclass
class Column:
    name: str
    col_type: str
    nullable: bool


@dataclass
class Table:
    name: str
    columns: dict[str, Column] = field(default_factory=dict)
    primary_key: list[str] = field(default_factory=list)


@dataclass
class ForeignKey:
    from_table: str
    from_column: str
    to_table: str
    to_column: str
    explicit: bool


def parse_schema(sql: str) -> tuple[dict[str, Table], list[ForeignKey]]:
    tables: dict[str, Table] = {}
    explicit_fks: list[ForeignKey] = []

    create_pattern = re.compile(r'CREATE TABLE `(\w+)`\s*\((.*?)\n\)\s*', re.DOTALL)

    for match in create_pattern.finditer(sql):
        table_name = match.group(1)
        body = match.group(2)
        table = Table(name=table_name)

        for line in body.split('\n'):
            line = line.strip().rstrip(',')

            col_match = re.match(r'^`(\w+)`\s+(\w[\w() ]*)', line)
            if col_match and not line.startswith(('PRIMARY', 'KEY', 'UNIQUE', 'CONSTRAINT', 'FULLTEXT', 'INDEX')):
                col_name = col_match.group(1)
                col_type = col_match.group(2).strip()
                nullable = 'NOT NULL' not in line
                table.columns[col_name] = Column(col_name, col_type, nullable)

            pk_match = re.match(r'PRIMARY KEY \((.+?)\)', line)
            if pk_match:
                table.primary_key = [c.strip('` ') for c in pk_match.group(1).split(',')]

            fk_match = re.match(
                r'CONSTRAINT .+? FOREIGN KEY \(`(\w+)`\) REFERENCES `(\w+)` \(`(\w+)`\)', line)
            if fk_match:
                explicit_fks.append(ForeignKey(
                    from_table=table_name, from_column=fk_match.group(1),
                    to_table=fk_match.group(2), to_column=fk_match.group(3),
                    explicit=True,
                ))

        tables[table_name] = table

    return tables, explicit_fks


def infer_relations(tables: dict[str, Table], explicit_fks: list[ForeignKey]) -> list[ForeignKey]:
    existing = {(fk.from_table, fk.from_column) for fk in explicit_fks}

    pk_lookup: dict[str, str] = {}
    for table in tables.values():
        if len(table.primary_key) == 1:
            pk_lookup[table.name] = table.primary_key[0]

    pk_col_to_tables: dict[str, list[str]] = defaultdict(list)
    for table_name, pk_col in pk_lookup.items():
        pk_col_to_tables[pk_col].append(table_name)

    inferred: list[ForeignKey] = []

    for table in tables.values():
        for col_name, col in table.columns.items():
            if not col_name.endswith('_id'):
                continue
            if (table.name, col_name) in existing:
                continue
            if col_name in table.primary_key:
                if 'auto_increment' in col.col_type.lower() or col_name == 'entity_id':
                    continue

            candidates = pk_col_to_tables.get(col_name, [])
            candidates = [t for t in candidates if t != table.name]

            if len(candidates) == 1:
                inferred.append(ForeignKey(
                    from_table=table.name, from_column=col_name,
                    to_table=candidates[0], to_column=col_name,
                    explicit=False,
                ))
                continue

            if len(candidates) > 1:
                col_prefix = col_name.removesuffix('_id')
                best = [t for t in candidates if t == col_prefix or t.endswith('_' + col_prefix)]
                if len(best) == 1:
                    inferred.append(ForeignKey(
                        from_table=table.name, from_column=col_name,
                        to_table=best[0], to_column=col_name,
                        explicit=False,
                    ))
                    continue

            col_prefix = col_name.removesuffix('_id')
            if col_prefix in tables:
                target = tables[col_prefix]
                if len(target.primary_key) == 1:
                    inferred.append(ForeignKey(
                        from_table=table.name, from_column=col_name,
                        to_table=col_prefix, to_column=target.primary_key[0],
                        explicit=False,
                    ))
                    continue

            for suffix in ['_entity', '_flat']:
                for prefix in ['catalog_', 'sales_', 'customer_', '']:
                    candidate_table = prefix + col_prefix + suffix
                    if candidate_table in tables:
                        target = tables[candidate_table]
                        if len(target.primary_key) == 1:
                            inferred.append(ForeignKey(
                                from_table=table.name, from_column=col_name,
                                to_table=candidate_table, to_column=target.primary_key[0],
                                explicit=False,
                            ))
                            break
                else:
                    continue
                break

    return inferred


# ─── Corrections for Magento-specific false positives ───

def fix_inferred_relations(fks: list[ForeignKey], tables: dict[str, Table]) -> list[ForeignKey]:
    """Fix known false-positive inferred relations in Magento schemas."""
    fixed = []
    for fk in fks:
        if not fk.explicit:
            # Skip self-references
            if fk.from_table == fk.to_table:
                continue

            # type_id in product tables is a varchar ('simple', 'configurable'), not a FK
            if fk.from_column == 'type_id' and fk.from_table.startswith('catalog_product'):
                continue

            # customer_id should reference customer_entity, not login_as_customer_assistance_allowed
            if fk.from_column == 'customer_id' and fk.to_table == 'login_as_customer_assistance_allowed':
                if 'customer_entity' in tables:
                    fk = ForeignKey(fk.from_table, 'customer_id', 'customer_entity', 'entity_id', False)

            # order_id should reference sales_order, not sales_order_confirm_cancel
            if fk.from_column == 'order_id' and fk.to_table == 'sales_order_confirm_cancel':
                if 'sales_order' in tables:
                    fk = ForeignKey(fk.from_table, 'order_id', 'sales_order', 'entity_id', False)

            # catalogrule_*.rule_id should reference catalogrule, not authorization_rule
            if (fk.from_column == 'rule_id'
                    and fk.from_table.startswith('catalogrule_')
                    and fk.to_table == 'authorization_rule'):
                if 'catalogrule' in tables:
                    fk = ForeignKey(fk.from_table, 'rule_id', 'catalogrule', 'rule_id', False)

        fixed.append(fk)
    return fixed


# ─── Table categorization ───

def categorize_tables(tables: dict[str, Table]) -> tuple[set[str], set[str], set[str]]:
    """
    Returns (ignore_tables, exclude_data_tables, normal_tables).

    ignore_tables: not imported at all (session, cache, temp index tables)
    exclude_data_tables: schema imported, but no data (logs, reports, queues)
    normal_tables: fully imported
    """
    ignore = set()
    exclude_data = set()

    for name in tables:
        # Changelog tables (_cl suffix) - used by Magento indexers, rebuilt automatically
        if name.endswith('_cl'):
            ignore.add(name)
            continue

        # Temporary index tables (_tmp, _idx suffixes)
        if name.endswith('_tmp') or name.endswith('_idx'):
            exclude_data.add(name)
            continue

        # Index replica tables
        if name.endswith('_replica'):
            exclude_data.add(name)
            continue

        # Session and cache
        if name in ('session', 'cache', 'cache_tag'):
            ignore.add(name)
            continue

        # Cron schedule - rebuilt
        if name == 'cron_schedule':
            ignore.add(name)
            continue

        # Queue tables - transient
        if name in ('queue_lock', 'queue_poison_pill', 'queue_message', 'queue_message_status'):
            exclude_data.add(name)
            continue

        # Report/aggregated tables - can be regenerated via reindex
        if (name.startswith('report_') or name.endswith('_aggregated')
                or '_aggregated_' in name
                or name.startswith('reporting_')):
            exclude_data.add(name)
            continue

        # Captcha log
        if name == 'captcha_log':
            exclude_data.add(name)
            continue

        # Customer log and visitor (privacy)
        if name in ('customer_log', 'customer_visitor'):
            exclude_data.add(name)
            continue

        # OAuth nonce and token request log
        if name in ('oauth_nonce', 'oauth_token_request_log', 'password_reset_request_event'):
            exclude_data.add(name)
            continue

        # Sendfriend log
        if name == 'sendfriend_log':
            exclude_data.add(name)
            continue

        # Data exporter tables
        if '_data_exporter_' in name or name.endswith('_data_exporter_cl') or name == 'data_exporter_uuid':
            exclude_data.add(name)
            continue

        # Payment services hash tables
        if name.startswith('payment_services_') and name.endswith('_submitted_hash'):
            exclude_data.add(name)
            continue

        # Import/export temp data
        if name == 'importexport_importdata':
            exclude_data.add(name)
            continue

    normal = set(tables.keys()) - ignore - exclude_data
    return ignore, exclude_data, normal


# ─── Sensitive columns detection ───

SENSITIVE_PATTERNS = {
    'email': ['email', 'customer_email', 'subscriber_email', 'customer_email',
              'template_sender_email'],
    'name': ['firstname', 'lastname', 'first_name', 'last_name',
             'customer_firstname', 'customer_lastname',
             'billing_firstname', 'billing_lastname',
             'shipping_firstname', 'shipping_lastname'],
    'address': ['street', 'billing_street', 'shipping_street'],
    'phone': ['telephone', 'fax', 'billing_telephone', 'billing_fax',
              'shipping_telephone', 'shipping_fax'],
    'postcode': ['postcode', 'billing_postcode', 'shipping_postcode'],
    'ip': ['remote_ip', 'x_forwarded_for', 'remote_ip_long', 'ip'],
    'token': ['password', 'password_hash', 'rp_token', 'session_data',
              'access_token', 'access_token_hash'],
}


def find_sensitive_columns(tables: dict[str, Table], ignore: set[str], exclude_data: set[str]) -> dict[str, dict[str, str]]:
    """
    Returns {table_name: {column_name: anonymization_category}}
    for tables that need anonymization.
    """
    result = {}
    for table_name, table in tables.items():
        if table_name in ignore or table_name in exclude_data:
            continue

        sensitive = {}
        for col_name in table.columns:
            for category, patterns in SENSITIVE_PATTERNS.items():
                if col_name in patterns:
                    sensitive[col_name] = category
                    break

        if sensitive:
            result[table_name] = sensitive

    return result


# ─── Faker assignment ───

def get_faker_target(category: str) -> str:
    """Map a sensitivity category to a faker name or 'null'."""
    return {
        'email': 'fake_emails',
        'name': 'fake_names',
        'address': 'fake_streets',
        'phone': 'fake_phones',
        'postcode': 'fake_postcodes',
        'ip': 'null',
        'token': 'null',
    }[category]


# ─── Output generation ───

def generate_jibs(sql: str) -> str:
    tables, explicit_fks = parse_schema(sql)
    inferred_fks = infer_relations(tables, explicit_fks)
    inferred_fks = fix_inferred_relations(inferred_fks, tables)
    all_fks = explicit_fks + inferred_fks

    ignore, exclude_data, normal = categorize_tables(tables)
    sensitive = find_sensitive_columns(tables, ignore, exclude_data)

    # Deduplicate relations (same from_table.from_column -> to_table.to_column)
    seen_rels = set()
    unique_fks = []
    for fk in all_fks:
        key = (fk.from_table, fk.from_column, fk.to_table, fk.to_column)
        if key not in seen_rels:
            seen_rels.add(key)
            # Skip relations involving ignored tables
            if fk.from_table not in ignore and fk.to_table not in ignore:
                unique_fks.append(fk)

    lines = []
    lines.append("// Magento 2 import configuration")
    lines.append("// Generated from schema dump - review and adjust as needed")
    lines.append("//")
    lines.append(f"// {len(tables)} tables: {len(normal)} imported, "
                 f"{len(exclude_data)} schema-only, {len(ignore)} ignored")
    lines.append(f"// {len(unique_fks)} relations "
                 f"({sum(1 for f in unique_fks if f.explicit)} explicit FK, "
                 f"{sum(1 for f in unique_fks if not f.explicit)} inferred)")
    lines.append("")

    # ── Variables ──
    lines.append("// ─── Variables ───")
    lines.append('var admin_password_hash: string = "$2y$10$placeholder"')
    lines.append('var customer_password_hash: string = "$2y$10$placeholder"')
    lines.append("")

    # ── Fakers ──
    lines.append("// ─── Faker pools for anonymization ───")
    lines.append("")
    lines.append('faker fake_emails [')
    for i in range(1, 21):
        comma = "," if i < 20 else ""
        lines.append(f'    "user{i}@example.test"{comma}')
    lines.append(']')
    lines.append("")

    lines.append('faker fake_names [')
    names = ["Alice", "Bob", "Charlie", "Diana", "Edward", "Fiona",
             "George", "Hannah", "Ivan", "Julia", "Kevin", "Laura",
             "Michael", "Nancy", "Oscar", "Patricia", "Quinn", "Rachel",
             "Steven", "Teresa"]
    for i, name in enumerate(names):
        comma = "," if i < len(names) - 1 else ""
        lines.append(f'    "{name}"{comma}')
    lines.append(']')
    lines.append("")

    lines.append('faker fake_streets [')
    streets = ["123 Test Street", "456 Demo Avenue", "789 Sample Road",
               "101 Example Lane", "202 Mock Boulevard"]
    for i, s in enumerate(streets):
        comma = "," if i < len(streets) - 1 else ""
        lines.append(f'    "{s}"{comma}')
    lines.append(']')
    lines.append("")

    lines.append('faker fake_phones [')
    for i in range(1, 6):
        comma = "," if i < 5 else ""
        lines.append(f'    "+31600000{i:03d}"{comma}')
    lines.append(']')
    lines.append("")

    lines.append('faker fake_postcodes [')
    postcodes = ["1000AA", "2000BB", "3000CC", "4000DD", "5000EE"]
    for i, p in enumerate(postcodes):
        comma = "," if i < len(postcodes) - 1 else ""
        lines.append(f'    "{p}"{comma}')
    lines.append(']')
    lines.append("")

    # ── Relations ──
    lines.append("")
    lines.append("// ─── Table relations ───")
    lines.append("// Relations tell jibs how to traverse between tables for aggregates.")
    lines.append("// inferred = from column naming convention.")

    # Group by target table for readability
    rels_by_target: dict[str, list[ForeignKey]] = defaultdict(list)
    for fk in sorted(unique_fks, key=lambda f: (f.to_table, f.from_table, f.from_column)):
        rels_by_target[fk.to_table].append(fk)

    current_prefix = ""
    for target in sorted(rels_by_target.keys()):
        fks = rels_by_target[target]
        # Add section header when prefix changes
        prefix = target.split('_')[0]
        if prefix != current_prefix:
            current_prefix = prefix
            lines.append("")

        for fk in fks:
            if fk.explicit:
                continue
            if any('index' in table or 'tmp' in table or 'temp' in table or 'idx' in table or 'replica' in table for table in [fk.from_table, fk.to_table]):
                continue
            lines.append(f"relation {fk.from_table}.{fk.from_column} -> {fk.to_table}.{fk.to_column}")

    # ── Ignore tables ──
    lines.append("")
    lines.append("")
    lines.append("// ─── Ignored tables (not imported at all) ───")
    lines.append("// Session, cache, changelog, and cron tables that are rebuilt automatically.")
    for t in sorted(ignore):
        lines.append(f"ignore_table {t}")

    # ── Exclude data tables ──
    lines.append("")
    lines.append("")
    lines.append("// ─── Schema-only tables (structure imported, no data) ───")
    lines.append("// Index temp tables, reports, logs, and aggregated data that gets rebuilt.")
    for t in sorted(exclude_data):
        lines.append(f"exclude_data {t}")

    # ── Anonymize ──
    lines.append("")
    lines.append("")
    lines.append("// ─── Anonymization rules ───")
    lines.append("// Strips PII from customer, order, and admin tables.")
    for table_name in sorted(sensitive.keys()):
        cols = sensitive[table_name]
        lines.append("")
        lines.append(f"anonymize {table_name} {{")
        for col_name in sorted(cols.keys()):
            target = get_faker_target(cols[col_name])
            lines.append(f"    {col_name} -> {target}")
        lines.append("}")

    return '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser(description='Generate Magento .jibs configuration')
    parser.add_argument('schema_file', help='Path to SQL schema dump')
    args = parser.parse_args()

    with open(args.schema_file) as f:
        sql = f.read()

    print(generate_jibs(sql))


if __name__ == '__main__':
    main()
