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

__version__ = "0.1.0"
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
