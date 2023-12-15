"""Services for the cronjobs app."""

from __future__ import annotations

from typing import TYPE_CHECKING

from django.utils.timezone import now

if TYPE_CHECKING:
    from .models import PgcronnerJobs


def _populate_created_field(instance: PgcronnerJobs) -> None:
    """Populates the created field of the instance.

    Args:
        instance (PgcronnerJobs): The instance of the PgcronnerJobs model.
    """
    instance.created = now()


def handle_cronjobs_pre_save(instance: PgcronnerJobs) -> None:
    """Does things before the instance is saved."""
    _populate_created_field(instance)

