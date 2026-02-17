#!/usr/bin/env python3
"""
Analyze a MySQL schema dump and output a simplified view of all table relations.

Detects:
1. Explicit foreign keys from CONSTRAINT ... FOREIGN KEY ... REFERENCES
2. Implicit relations by matching column names to primary keys of other tables
   (e.g. `store_id` in table X likely references `store`.`store_id` or similar)

Usage:
    python3 scripts/schema-relations.py magento-default-schema.sql
    python3 scripts/schema-relations.py magento-default-schema.sql --format dot > schema.dot
    python3 scripts/schema-relations.py magento-default-schema.sql --format grouped
"""

import re
import sys
import argparse
from collections import defaultdict
from dataclasses import dataclass, field


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
    unique_keys: list[list[str]] = field(default_factory=list)


@dataclass
class ForeignKey:
    from_table: str
    from_column: str
    to_table: str
    to_column: str
    explicit: bool  # True = CONSTRAINT FK, False = inferred from naming


def parse_schema(sql: str) -> tuple[dict[str, Table], list[ForeignKey]]:
    tables: dict[str, Table] = {}
    explicit_fks: list[ForeignKey] = []

    # Split into CREATE TABLE blocks
    # The closing ) is on its own line (possibly with leading spaces)
    create_pattern = re.compile(
        r'CREATE TABLE `(\w+)`\s*\((.*?)\n\)\s*',
        re.DOTALL
    )

    for match in create_pattern.finditer(sql):
        table_name = match.group(1)
        body = match.group(2)
        table = Table(name=table_name)

        for line in body.split('\n'):
            line = line.strip().rstrip(',')

            # Column definition
            col_match = re.match(r'^`(\w+)`\s+(\w[\w() ]*)', line)
            if col_match and not line.startswith(('PRIMARY', 'KEY', 'UNIQUE', 'CONSTRAINT', 'FULLTEXT', 'INDEX')):
                col_name = col_match.group(1)
                col_type = col_match.group(2).strip()
                nullable = 'NOT NULL' not in line
                table.columns[col_name] = Column(col_name, col_type, nullable)

            # Primary key
            pk_match = re.match(r'PRIMARY KEY \((.+?)\)', line)
            if pk_match:
                table.primary_key = [c.strip('` ') for c in pk_match.group(1).split(',')]

            # Unique keys
            uk_match = re.match(r'UNIQUE KEY .+?\((.+?)\)', line)
            if uk_match:
                table.unique_keys.append([c.strip('` ') for c in uk_match.group(1).split(',')])

            # Foreign keys
            fk_match = re.match(
                r'CONSTRAINT .+? FOREIGN KEY \(`(\w+)`\) REFERENCES `(\w+)` \(`(\w+)`\)',
                line
            )
            if fk_match:
                explicit_fks.append(ForeignKey(
                    from_table=table_name,
                    from_column=fk_match.group(1),
                    to_table=fk_match.group(2),
                    to_column=fk_match.group(3),
                    explicit=True,
                ))

        tables[table_name] = table

    return tables, explicit_fks


def infer_relations(tables: dict[str, Table], explicit_fks: list[ForeignKey]) -> list[ForeignKey]:
    """Infer relations from column naming conventions."""

    # Build lookup: what explicit FKs already exist (from_table, from_column)
    existing = {(fk.from_table, fk.from_column) for fk in explicit_fks}

    # Build lookup: table_name -> primary key columns (single-column PKs only for matching)
    pk_lookup: dict[str, str] = {}
    for table in tables.values():
        if len(table.primary_key) == 1:
            pk_lookup[table.name] = table.primary_key[0]

    # Build reverse mapping: pk_column_name -> list of tables that use it as PK
    pk_col_to_tables: dict[str, list[str]] = defaultdict(list)
    for table_name, pk_col in pk_lookup.items():
        pk_col_to_tables[pk_col].append(table_name)

    # Common Magento naming patterns:
    # - `store_id` references `store.store_id`
    # - `customer_id` references `customer_entity.entity_id`
    # - `product_id` references `catalog_product_entity.entity_id`
    # - `category_id` references `catalog_category_entity.entity_id`
    # - `order_id` references `sales_order.entity_id`
    # - `quote_id` references `quote.entity_id`
    # We handle these through a combination of exact PK match and prefix-based heuristics.

    # Build a lookup: for a column name like `xyz_id`, find candidate target tables
    # Strategy 1: column name matches a PK column in another table exactly
    # Strategy 2: column name `xyz_id` -> look for table where name contains 'xyz' and has single PK

    inferred: list[ForeignKey] = []

    for table in tables.values():
        for col_name, col in table.columns.items():
            if not col_name.endswith('_id'):
                continue
            if (table.name, col_name) in existing:
                continue
            # Skip the table's own PK
            if col_name in table.primary_key:
                # Still check - sometimes a PK is also an FK (1:1 relations)
                # But only if it's not the auto-increment id
                if 'auto_increment' in col.col_type.lower() or col_name == 'entity_id':
                    continue

            # Strategy 1: exact PK column match
            candidates = pk_col_to_tables.get(col_name, [])
            # Filter out self-references that are obvious (same table)
            candidates = [t for t in candidates if t != table.name]

            if len(candidates) == 1:
                inferred.append(ForeignKey(
                    from_table=table.name,
                    from_column=col_name,
                    to_table=candidates[0],
                    to_column=col_name,
                    explicit=False,
                ))
                continue

            if len(candidates) > 1:
                # Pick the best candidate: prefer table whose name is a prefix of the column
                # e.g., store_id -> store (table `store` has PK `store_id`)
                col_prefix = col_name.removesuffix('_id')
                best = [t for t in candidates if t == col_prefix or t.endswith('_' + col_prefix)]
                if len(best) == 1:
                    inferred.append(ForeignKey(
                        from_table=table.name,
                        from_column=col_name,
                        to_table=best[0],
                        to_column=col_name,
                        explicit=False,
                    ))
                    continue

            # Strategy 2: column `xyz_id` -> look for table `xyz` or known entity tables
            col_prefix = col_name.removesuffix('_id')

            # Direct table name match (e.g., store_id -> table `store`)
            if col_prefix in tables:
                target = tables[col_prefix]
                if len(target.primary_key) == 1:
                    inferred.append(ForeignKey(
                        from_table=table.name,
                        from_column=col_name,
                        to_table=col_prefix,
                        to_column=target.primary_key[0],
                        explicit=False,
                    ))
                    continue

            # Magento entity pattern: xyz_id -> some_xyz_entity.entity_id
            for suffix in ['_entity', '_flat']:
                for prefix in ['catalog_', 'sales_', 'customer_', '']:
                    candidate_table = prefix + col_prefix + suffix
                    if candidate_table in tables:
                        target = tables[candidate_table]
                        if len(target.primary_key) == 1:
                            inferred.append(ForeignKey(
                                from_table=table.name,
                                from_column=col_name,
                                to_table=candidate_table,
                                to_column=target.primary_key[0],
                                explicit=False,
                            ))
                            break
                else:
                    continue
                break

    return inferred


def format_grouped(tables: dict[str, Table], all_fks: list[ForeignKey]) -> str:
    """Group tables by their relationships, showing a simplified view."""
    lines = []

    # Build adjacency: for each table, what does it reference and what references it
    refs_from: dict[str, list[ForeignKey]] = defaultdict(list)  # table -> FKs pointing outward
    refs_to: dict[str, list[ForeignKey]] = defaultdict(list)    # table -> FKs pointing inward

    for fk in all_fks:
        refs_from[fk.from_table].append(fk)
        refs_to[fk.to_table].append(fk)

    # Find root tables (referenced by others but don't reference much themselves)
    # Sort by number of incoming references descending
    root_tables = sorted(
        tables.keys(),
        key=lambda t: len(refs_to.get(t, [])),
        reverse=True,
    )

    printed_tables = set()

    for root in root_tables:
        incoming = refs_to.get(root, [])
        if not incoming and root in printed_tables:
            continue

        children = refs_to.get(root, [])
        if not children:
            continue

        if root in printed_tables:
            continue
        printed_tables.add(root)

        pk_str = ', '.join(tables[root].primary_key) if root in tables else '?'
        lines.append(f"\n{'='*70}")
        lines.append(f"  {root}  (PK: {pk_str})")
        incoming_from_others = refs_from.get(root, [])
        if incoming_from_others:
            parent_strs = [f"{fk.to_table}.{fk.to_column}" for fk in incoming_from_others]
            lines.append(f"  parents: {', '.join(parent_strs)}")
        lines.append(f"{'='*70}")

        # Group children by relation type
        explicit_children = [fk for fk in children if fk.explicit]
        inferred_children = [fk for fk in children if not fk.explicit]

        if explicit_children:
            for fk in sorted(explicit_children, key=lambda f: f.from_table):
                lines.append(f"  <- {fk.from_table}.{fk.from_column}  (FK)")

        if inferred_children:
            for fk in sorted(inferred_children, key=lambda f: f.from_table):
                lines.append(f"  <- {fk.from_table}.{fk.from_column}  (inferred)")

    # Show orphan tables (no relations at all)
    orphans = [t for t in sorted(tables.keys())
               if t not in refs_from and t not in refs_to]
    if orphans:
        lines.append(f"\n{'='*70}")
        lines.append("  ORPHAN TABLES (no detected relations)")
        lines.append(f"{'='*70}")
        for t in orphans:
            lines.append(f"  - {t}")

    return '\n'.join(lines)


def format_list(tables: dict[str, Table], all_fks: list[ForeignKey]) -> str:
    """Simple flat list of all relations."""
    lines = []
    lines.append(f"# Schema Relations ({len(tables)} tables, {len(all_fks)} relations)")
    lines.append(f"# {'FK':<10} = explicit FOREIGN KEY constraint")
    lines.append(f"# {'inferred':<10} = inferred from column naming convention")
    lines.append("")

    for fk in sorted(all_fks, key=lambda f: (f.from_table, f.from_column)):
        tag = "FK" if fk.explicit else "inferred"
        lines.append(f"{fk.from_table}.{fk.from_column}  ->  {fk.to_table}.{fk.to_column}  [{tag}]")

    return '\n'.join(lines)


def format_dot(tables: dict[str, Table], all_fks: list[ForeignKey]) -> str:
    """Output Graphviz DOT format for visualization."""
    lines = []
    lines.append('digraph schema {')
    lines.append('  rankdir=LR;')
    lines.append('  node [shape=box, fontname="Helvetica", fontsize=10];')
    lines.append('  edge [fontname="Helvetica", fontsize=8];')
    lines.append('')

    # Only include tables that have relations
    related_tables = set()
    for fk in all_fks:
        related_tables.add(fk.from_table)
        related_tables.add(fk.to_table)

    for t in sorted(related_tables):
        pk = ', '.join(tables[t].primary_key) if t in tables else ''
        label = f"{t}\\n({pk})" if pk else t
        lines.append(f'  "{t}" [label="{label}"];')

    lines.append('')

    for fk in sorted(all_fks, key=lambda f: (f.from_table, f.to_table)):
        style = 'solid' if fk.explicit else 'dashed'
        color = 'black' if fk.explicit else '#888888'
        lines.append(
            f'  "{fk.from_table}" -> "{fk.to_table}" '
            f'[label="{fk.from_column}", style={style}, color="{color}"];'
        )

    lines.append('}')
    return '\n'.join(lines)


def format_summary(tables: dict[str, Table], all_fks: list[ForeignKey]) -> str:
    """High-level summary: tables grouped by prefix with relation counts."""
    lines = []

    # Count relations per table
    rel_count: dict[str, int] = defaultdict(int)
    for fk in all_fks:
        rel_count[fk.from_table] += 1
        rel_count[fk.to_table] += 1

    # Group by prefix (first part before _)
    groups: dict[str, list[str]] = defaultdict(list)
    for t in sorted(tables.keys()):
        # Use first meaningful prefix segment
        parts = t.split('_')
        prefix = parts[0]
        # Merge common Magento prefixes
        if prefix in ('catalog', 'cataloginventory', 'catalogrule', 'catalogsearch'):
            prefix = 'catalog'
        elif prefix in ('sales', 'salesrule'):
            prefix = 'sales'
        elif prefix in ('customer', 'customerbalance'):
            prefix = 'customer'
        groups[prefix].append(t)

    explicit_count = sum(1 for fk in all_fks if fk.explicit)
    inferred_count = sum(1 for fk in all_fks if not fk.explicit)

    lines.append(f"Schema: {len(tables)} tables, {len(all_fks)} relations "
                 f"({explicit_count} explicit FK, {inferred_count} inferred)")
    lines.append("")

    for prefix in sorted(groups.keys()):
        group_tables = groups[prefix]
        group_rels = sum(rel_count.get(t, 0) for t in group_tables)
        lines.append(f"  {prefix}* ({len(group_tables)} tables, {group_rels} relations)")
        for t in group_tables:
            rc = rel_count.get(t, 0)
            marker = f" [{rc} rels]" if rc else ""
            lines.append(f"    {t}{marker}")

    return '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser(description='Analyze MySQL schema relations')
    parser.add_argument('schema_file', help='Path to SQL schema dump')
    parser.add_argument('--format', choices=['list', 'grouped', 'dot', 'summary'],
                        default='grouped', help='Output format (default: grouped)')
    args = parser.parse_args()

    with open(args.schema_file) as f:
        sql = f.read()

    tables, explicit_fks = parse_schema(sql)
    inferred_fks = infer_relations(tables, explicit_fks)
    all_fks = explicit_fks + inferred_fks

    print(f"# Parsed {len(tables)} tables, {len(explicit_fks)} explicit FKs, "
          f"{len(inferred_fks)} inferred relations", file=sys.stderr)

    formatters = {
        'list': format_list,
        'grouped': format_grouped,
        'dot': format_dot,
        'summary': format_summary,
    }

    print(formatters[args.format](tables, all_fks))


if __name__ == '__main__':
    main()
