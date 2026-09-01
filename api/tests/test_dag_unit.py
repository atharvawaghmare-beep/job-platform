"""Pure unit tests for DAG dependency resolution (CLAUDE.md Testing
Expectations: "Unit tests for ... DAG dependency resolution logic"). No
DB/queue needed.
"""
import pytest

from app.dag import DagValidationError, topological_order


def test_simple_chain_order():
    order = topological_order([("A", []), ("B", ["A"]), ("C", ["B"])])
    assert order.index("A") < order.index("B") < order.index("C")


def test_diamond_order():
    order = topological_order([("A", []), ("B", ["A"]), ("C", ["A"]), ("D", ["B", "C"])])
    assert order.index("A") < order.index("B")
    assert order.index("A") < order.index("C")
    assert order.index("B") < order.index("D")
    assert order.index("C") < order.index("D")


def test_independent_jobs_all_included():
    order = topological_order([("A", []), ("B", [])])
    assert set(order) == {"A", "B"}


def test_duplicate_key_rejected():
    with pytest.raises(DagValidationError, match="duplicate"):
        topological_order([("A", []), ("A", [])])


def test_unknown_dependency_rejected():
    with pytest.raises(DagValidationError, match="unknown"):
        topological_order([("A", ["B"])])


def test_self_dependency_rejected():
    with pytest.raises(DagValidationError, match="itself"):
        topological_order([("A", ["A"])])


def test_two_node_cycle_rejected():
    with pytest.raises(DagValidationError, match="cycle"):
        topological_order([("A", ["B"]), ("B", ["A"])])


def test_longer_cycle_rejected():
    with pytest.raises(DagValidationError, match="cycle"):
        topological_order([("A", ["C"]), ("B", ["A"]), ("C", ["B"])])
