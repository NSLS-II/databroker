import six
import os
import numpy as np


if six.PY2:
    # http://stackoverflow.com/a/5032238/380231
    def _make_sure_path_exists(path):
        import errno
        try:
            os.makedirs(path)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise
else:
    # technically, this won't work with py3.1, but no one uses that
    def _make_sure_path_exists(path):
        return os.makedirs(path, exist_ok=True)

def sanitize_np(val):
    "Convert any numpy objects into built-in Python types."
    if isinstance(val, (np.generic, np.ndarray)):
        if np.isscalar(val):
            return val.item()
        return val.tolist()
    return val


def apply_to_dict_recursively(d, f):
    for key, val in d.items():
        if hasattr(val, 'items'):
            d[key] = apply_to_dict_recursively(val, f)
        d[key] = f(val)
