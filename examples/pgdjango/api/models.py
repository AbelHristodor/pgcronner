# class PgCronnerJobsManager(models.Manager):
#     def create(self, *args, **kwargs):
#         # if not kwargs.get("created"):
#         #     kwargs["created"] = now()
#         #
#         # pgcron = getattr(settings, "PGCRON", None)
#         # if pgcron:
#         #     job = Job(
#         #         kwargs["name"], kwargs["schedule"], kwargs["command"], kwargs["source"]
#         #     )
#         #     print(pgcron.add(job)

#         super(PgCronnerJobsManager, self).create(*args, **kwargs)
