from django.db import models
from django.conf import settings
from django.utils.timezone import now

from django.utils.functional import LazyObject

class LazyPgCronner(LazyObject):
    def _setup(self):
        self._wrapped = getattr(settings, 'PGCRON', None)


class PgcronnerJobs(models.Model):
    name = models.CharField(unique=True, max_length=255)
    command = models.TextField()
    schedule = models.CharField(max_length=255)
    source = models.TextField(blank=True, null=True)
    created = models.DateTimeField(blank=True, null=True)
    
    class Meta:
        db_table = 'pgcronner_jobs'

    def save(self, *args, **kwargs):
        if not self.created:
            self.created = now()

        cronner = LazyPgCronner()
        cronner.sync()
        super().save(*args, **kwargs)
