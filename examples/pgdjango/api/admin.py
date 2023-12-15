"""Admin module for the cronjobs app."""
import logging

from django import forms
from django.conf import settings
from django.contrib import admin
from django.http.request import HttpRequest

from .models import PgcronnerJobs

from pgcronner import Job

logger = logging.getLogger(__name__)


def validate_command(value: str) -> None:
    """Validates the command field.

    Args:
        value (str): The value of the command field.

    Raises:
        forms.ValidationError: If the command is not valid.
    """
    if "CALL" in value:
        if "();" not in value:
            raise forms.ValidationError("CALL must be followed by a function call, e.g. CALL my_function();")


def validate_schedule(value: str) -> None:
    """Validates the schedule field.

    Args:
        value (str): The value of the schedule field.

    Raises:
        forms.ValidationError: If the schedule is not valid.
    """
    if not value:
        raise forms.ValidationError("Schedule cannot be empty")


class PgCronnerJobsForm(forms.ModelForm):
    """Form for the PgCronnerJobs model."""

    command = forms.CharField(widget=forms.Textarea, validators=[validate_command])
    schedule = forms.CharField(validators=[validate_schedule])

    class Meta:
        """Meta class."""

        model = PgcronnerJobs
        fields = "__all__"


@admin.register(PgcronnerJobs)
class PgCronnerJobsAdmin(admin.ModelAdmin):
    """Admin class for the PgCronnerJobs model."""

    form = PgCronnerJobsForm

    list_display = ("name", "command", "schedule", "last_run")
    search_fields = ("name", "command", "schedule")
    list_filter = ("name", "command", "schedule", "active")
    ordering = ("last_run", "name", "command", "schedule")

    def save_model(self, request: HttpRequest, obj: PgcronnerJobs, form: forms.Form, change):
        """Override save_model to add the job using pgcronner.

        Args:
            obj (PgcronnerJobs): Obj to be saved
            form (forms.Form): Form data to be saved
            change (bool): Whether the obj is being changed or not
            request (Request): The request object
        """
        if pgcron := getattr(settings, "PGCRON", None):
            logger.info("PGCRON: %s", pgcron)
            if form.changed_data and change:
                logger.info("Removing job %s from pgcronner", form.initial["name"])
                pgcron.remove(form.initial["name"])

            job = Job(str(obj.name), str(obj.schedule), str(obj.command), str(obj.source))

            logger.info("Adding job %s to pgcronner", obj.name)
            pgcron.add(job)

            logger.info("Syncing pgcronner jobs")
            pgcron.sync()

