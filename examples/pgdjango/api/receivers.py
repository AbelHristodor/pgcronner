"""Receivers for the cronjobs app."""

from django.conf import settings
from django.db import transaction
from django.db.models.signals import post_delete, post_save
from django.dispatch import receiver

from .models import PgcronnerJobs


@receiver(sender=PgcronnerJobs, signal=[post_delete, post_save])
def sync_pgcronnerjobs(**kwargs):
    """Syncs the pgcronnerjobs."""
    pgcron = getattr(settings, "PGCRON", None)
    if pgcron:
        transaction.on_commit(lambda: pgcron.sync())

