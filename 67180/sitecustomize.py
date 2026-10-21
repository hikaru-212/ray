"""Load the selected immutable source snapshot above the shared Core wheel."""
import os

if os.environ.get("PHASE2_SOURCE"):
    import ray

    ray.__path__.insert(0, os.environ["PHASE2_SOURCE"] + "/python/ray")
