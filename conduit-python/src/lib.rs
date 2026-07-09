//! conduit-python: PyO3 bindings for Conduit pipeline orchestrator
//!
//! Exposes core Rust functionality to Python, enabling:
//! - DAG compilation and validation
//! - Change detection and fingerprinting
//! - Column-level lineage extraction
//! - Environment state management
//!
//! All complex types use JSON for pragmatic interchange in v0.1.

pub mod compiler;
pub mod planner;
pub mod lineage;
pub mod state;

use pyo3::prelude::*;

/// Create the conduit_native module exposed to Python
#[pymodule]
fn conduit_native(py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    // Register submodules
    let compiler_module = compiler::create_module(py)?;
    m.add_submodule(&compiler_module)?;

    let planner_module = planner::create_module(py)?;
    m.add_submodule(&planner_module)?;

    let lineage_module = lineage::create_module(py)?;
    m.add_submodule(&lineage_module)?;

    let state_module = state::create_module(py)?;
    m.add_submodule(&state_module)?;

    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add("__doc__", "PyO3 bindings for Conduit pipeline orchestrator")?;

    Ok(())
}
