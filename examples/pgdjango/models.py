# This is an auto-generated Django model module.
# You'll have to do the following manually to clean this up:
#   * Rearrange models' order
#   * Make sure each model has one field with primary_key=True
#   * Make sure each ForeignKey and OneToOneField has `on_delete` set to the desired behavior
#   * Remove `managed = False` lines if you wish to allow Django to create, modify, and delete the table
# Feel free to rename the models, but don't rename db_table values or field names.
from django.db import models


class PgcronnerJobs(models.Model):
    name = models.CharField(unique=True, max_length=255)
    command = models.TextField()
    schedule = models.CharField(max_length=255)
    source = models.TextField(blank=True, null=True)
    active = models.BooleanField()
    last_run = models.DateTimeField(blank=True, null=True)
    created = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'pgcronner_jobs'
