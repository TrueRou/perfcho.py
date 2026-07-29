"""Define protocol-neutral account registration errors."""

from perfcho.modules.common.errors import InputRejected, ResourceConflict


class RegistrationRejected(InputRejected):
    """Reject invalid account registration input."""

    code = "registration_rejected"


class NameUnavailable(ResourceConflict):
    """Indicate that a normalized current account name is already claimed."""

    code = "name_unavailable"


class EmailUnavailable(ResourceConflict):
    """Indicate that a normalized active account email is already claimed."""

    code = "email_unavailable"
