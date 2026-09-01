"""Pure DAG validation for workflow submission — no DB, fully unit-testable.

A workflow request identifies its jobs by a client-supplied local `key`
(e.g. "A", "B"), used only to express `depends_on` edges within that one
request. `topological_order` resolves those edges: it either returns a
valid processing order (every key after all the keys it depends on), or
raises `DagValidationError` explaining exactly what's wrong — a duplicate
key, a dependency that doesn't exist in the request, a self-dependency, or
a cycle.
"""


class DagValidationError(ValueError):
    pass


def topological_order(jobs: list[tuple[str, list[str]]]) -> list[str]:
    """`jobs` is a list of (key, depends_on_keys) pairs. Returns the keys in
    an order where every job appears after all of its dependencies (Kahn's
    algorithm) — the order itself isn't currently used for anything beyond
    validation, but it's a natural, cheaply-obtained byproduct of detecting
    cycles this way.
    """
    seen: set[str] = set()
    for key, _ in jobs:
        if key in seen:
            raise DagValidationError(f"duplicate job key: {key!r}")
        seen.add(key)

    depends_on_map = dict(jobs)
    for key, deps in depends_on_map.items():
        for dep in deps:
            if dep == key:
                raise DagValidationError(f"job {key!r} cannot depend on itself")
            if dep not in depends_on_map:
                raise DagValidationError(f"job {key!r} depends on unknown key {dep!r}")

    in_degree = {key: len(deps) for key, deps in depends_on_map.items()}
    dependents: dict[str, list[str]] = {key: [] for key in depends_on_map}
    for key, deps in depends_on_map.items():
        for dep in deps:
            dependents[dep].append(key)

    queue = [key for key, degree in in_degree.items() if degree == 0]
    order: list[str] = []
    while queue:
        key = queue.pop(0)
        order.append(key)
        for dependent in dependents[key]:
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)

    if len(order) != len(depends_on_map):
        remaining = sorted(set(depends_on_map) - set(order))
        raise DagValidationError(f"cycle detected among job keys: {remaining}")

    return order
