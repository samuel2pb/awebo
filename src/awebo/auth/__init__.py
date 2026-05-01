"""Authentication providers.

Auth is a feature package: the abstract :class:`AuthProvider` lives in
``base`` and concrete cloud implementations live as siblings (``aws``).
"""

from awebo.auth.aws import AwsAuthProvider
from awebo.auth.base import AuthProvider, Credentials

__all__ = ["AuthProvider", "AwsAuthProvider", "Credentials"]
