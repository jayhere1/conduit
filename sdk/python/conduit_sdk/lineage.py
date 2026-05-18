"""
Lineage primitives — Dataset and ColumnSpec.

These are the cross-task interop unit. A Python task declares what it reads
and writes via:

    @task(
        inputs=[Dataset("staging.orders", columns=[ColumnSpec("id"), ...])],
        outputs=[Dataset("analytics.daily_revenue", columns=[...])],
    )

A downstream SQL task whose query reads `FROM staging.orders` resolves
column references against the producer task's declared schema, so the
compiler can stitch column-level lineage across the Python ↔ SQL boundary.

These dataclasses are read by Conduit's compiler at parse time via
tree-sitter; they must remain plain data, no runtime computation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass(frozen=True)
class ColumnSpec:
    """A single column. `dtype` is informational; lineage stitches by name."""
    name: str
    dtype: Optional[str] = None


@dataclass
class Dataset:
    """A schema-qualified named collection of columns."""
    name: str
    columns: list[ColumnSpec] = field(default_factory=list)
