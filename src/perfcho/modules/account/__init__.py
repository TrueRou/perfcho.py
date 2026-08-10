"""Expose canonical account registration operations."""

from perfcho.modules.account.errors import EmailUnavailable, NameUnavailable, RegistrationRejected
from perfcho.modules.account.models import PublicAccountView, RegisterAccount, RegistrationResult
from perfcho.modules.account.ports import AccountRepository
from perfcho.modules.account.services import AccountService

__all__ = (
    "AccountRepository",
    "AccountService",
    "EmailUnavailable",
    "NameUnavailable",
    "PublicAccountView",
    "RegisterAccount",
    "RegistrationRejected",
    "RegistrationResult",
)
