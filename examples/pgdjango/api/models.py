"""Module for cronjobs models."""
import logging
from typing import Any, Iterable

from django.conf import settings
from django.db import models
from django.utils import timezone

from .services import handle_cronjobs_pre_save

from pgcronner import Job

logger = logging.getLogger(__name__)
pgcron = getattr(settings, "PGCRON", None)

if pgcron is None:
    logger.warn("PGCRON is None. Please set PGCRON in settings.py accordingly to use it.")


class PgcronnerManager(models.Manager):
    """PgCronner manager."""

    def get(self) -> Any:
        """Overrides default get method to add additional functionality."""
        try:
            pgcron.refresh()
        except OSError as e:
            logger.error("Error refreshing cronjobs: %s", e)

        return super(PgcronnerManager, self).get()

    def get_queryset(self) -> Any:
        """Overrides default get_queryset method to add additional functionality."""

        try:
            pgcron.refresh()
        except OSError as e:
            logger.error("Error refreshing cronjobs: %s", e)

        return super(PgcronnerManager, self).get_queryset()

    def create(self, **kwargs: Any) -> Any:
        """Overrides default create method to add additional functionality."""
        if "created" not in kwargs:
            kwargs["created"] = timezone.now()

        try:
            pgcron.add(
                Job(
                    name=kwargs["name"],
                    schedule=kwargs["schedule"],
                    command=kwargs["command"],
                    source=kwargs["source"],
                )
            )
        except OSError as e:
            logger.error("Error creating cronjobs: %s", e)

        pgcron.sync()


class PgcronnerJobs(models.Model):
    """PgCronner jobs model created by pgcronner and adapted to Django."""

    name = models.CharField(unique=True, max_length=255)
    command = models.TextField()
    schedule = models.CharField(max_length=255)
    source = models.TextField(blank=True, null=True)
    created = models.DateTimeField(auto_now=True, blank=True, null=True)

    active = models.BooleanField(default=True)
    last_run = models.DateTimeField(blank=True, null=True)

    objects = PgcronnerManager()

    class Meta:
        """Meta class."""

        db_table = "pgcronner_jobs"
        verbose_name = "PgCronner Job"
        verbose_name_plural = "PgCronner Jobs"

    def save(
        self,
        force_insert: bool = ...,
        force_update: bool = ...,
        using: str | None = ...,
        update_fields: Iterable[str] | None = ...,
    ) -> None:
        """Overrides default save method to add additional functionality."""
        handle_cronjobs_pre_save(self)

        super(PgcronnerJobs, self).save(force_insert, force_update, using, update_fields)

