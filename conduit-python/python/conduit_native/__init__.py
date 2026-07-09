"""PyO3 bindings for Conduit pipeline orchestrator.

This module provides Python access to Conduit's core Rust functionality:
- DAG compilation and validation
- Change detection and fingerprinting
- Column-level lineage extraction
- Environment state management

Example:
    >>> from conduit_native import compiler
    >>> plan_json = compiler.compile_dags("/path/to/dag/definitions")
"""

__doc__ = "PyO3 bindings for Conduit pipeline orchestrator"

try:
    from . import conduit_native
    from .conduit_native import compiler, planner, lineage, state
    __all__ = ["compiler", "planner", "lineage", "state"]
except ImportError:
    # Module not yet compiled - provide helpful error
    raise ImportError(
        "conduit_native module not found. "
        "Build with: maturin develop or pip install ."
    )

# Single-sourced from the crate version (see conduit-python/Cargo.toml);
# the native module stamps it via env!("CARGO_PKG_VERSION").
__version__ = getattr(conduit_native, "__version__", "unknown")
