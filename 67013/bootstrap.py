"""Import unmodified git-exported Tune sources over a wheel runtime."""
import importlib.abc
import importlib.util
import os
from pathlib import Path
import sys

ROOT = Path(os.environ['PR67013_RUN_DIR']).resolve()
VARIANT = os.environ['PR67013_VARIANT']
assert VARIANT in ('parent', 'head')
SOURCE = ROOT / VARIANT / 'python'
REPO = Path(os.environ['PR67013_RAY_REPO']).resolve()
COMMITS = {
    'parent': 'e00c7f4bed3d92da863230c2dfdc3ea81eec1778',
    'head': '8ffddc460b8b65da3930b2a789399aafe0282568',
}

class TuneSourceFinder(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname != 'ray.tune' and not fullname.startswith('ray.tune.'):
            return None
        source = SOURCE.joinpath(*fullname.split('.'))
        package = source / '__init__.py'
        if package.is_file():
            return importlib.util.spec_from_file_location(fullname, package, submodule_search_locations=[str(source)])
        module = source.with_suffix('.py')
        if module.is_file():
            return importlib.util.spec_from_file_location(fullname, module)
        raise ImportError(f'No snapshot Tune source for {fullname}')

assert not any(k == 'ray.tune' or k.startswith('ray.tune.') for k in sys.modules)
sys.meta_path.insert(0, TuneSourceFinder())
