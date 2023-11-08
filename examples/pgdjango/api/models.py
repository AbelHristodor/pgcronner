from django.db import models, transaction
from django.db.models import signals
from django.conf import settings
from django.dispatch import receiver
from django.utils.timezone import now

from pgcronner import Job


class PgCronnerJobsManager(models.Manager):
    def create(self, *args, **kwargs):
        # if not kwargs.get("created"):
        #     kwargs["created"] = now()
        #
        # pgcron = getattr(settings, "PGCRON", None)
        # if pgcron:
        #     job = Job(
        #         kwargs["name"], kwargs["schedule"], kwargs["command"], kwargs["source"]
        #     )
        #     print(pgcron.add(job)
        
        super(PgCronnerJobsManager, self).create(*args, **kwargs)


class PgcronnerJobs(models.Model):
    name = models.CharField(unique=True, max_length=255)
    command = models.TextField()
    schedule = models.CharField(max_length=255)
    source = models.TextField(blank=True, null=True)
    created = models.DateTimeField(blank=True, null=True)

    objects = PgCronnerJobsManager()

    class Meta:
        db_table = "pgcronner_jobs"

    def save(self, *args, **kwargs):
        if not self.created:
            self.created = now()
        super(PgcronnerJobs, self).save(*args, **kwargs)


# @receiver(sender=PgcronnerJobs, signal=[signals.post_save, signals.post_delete])
# def sync_pgcron_receiver(sender, **kwargs):
#     transaction.on_commit(sync_pgcron)
#
#
# def sync_pgcron():
#     pgcron = getattr(settings, "PGCRON", None)
#     if pgcron:
#         pgcron.sync()
