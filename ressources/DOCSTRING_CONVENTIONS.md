# NumPy-Style Docstring Conventions

This repository uses **NumPy-style docstrings** for all Python modules, classes, and functions.

## Required Sections

For non-trivial functions, use this order:

1. Summary line
2. Parameters
3. Returns
4. Raises (when relevant)
5. Notes
6. Examples (required for non-trivial public functions)

## Minimal Function Template

```python
def compute_metric(values: list[float]) -> float:
    """Compute a stable metric from input values.

    Parameters
    ----------
    values : list[float]
        Numeric values used by the metric.

    Returns
    -------
    float
        Computed metric value.

    Raises
    ------
    ValueError
        Raised when `values` is empty.

    Notes
    -----
    This function is deterministic for identical input order.
    """
```

## Module Docstring Template

```python
"""Short module title.

One-paragraph explanation of what the module owns.

Notes
-----
Mention contract boundaries or assumptions when relevant.
"""
```

## Style Rules

- Keep language concrete and implementation-aware.
- State whether metrics are computed live or read from precomputed artifacts.
- Document ordering/tie-break rules for replay and sequence logic.
- Document filter semantics (work mode, date range, module scope).
- Avoid placeholder text and vague summaries.
