
from pgcronner import PgCronner, JobBuilder
import logging

FORMAT = '%(levelname)s %(name)s %(asctime)-15s %(filename)s:%(lineno)d %(message)s'
logging.basicConfig(format=FORMAT)
logging.getLogger().setLevel(logging.INFO)

a = {
    "myjob": {
        "name": "testjob",
        "command": "command",
        "schedule": "*/5 * * * *",
        "source": "source",
    }
}

db_uri = "postgresql://postgres:postgres@localhost:5432/postgres"

r = PgCronner(a, db_uri)
print(r.all())

job = JobBuilder("testjob", "*/5 * * * *", "SELECT 1;", "source")
print(r.add(job))
print(r.one("testjob"))
print(r.remove("testjob"))

print(r.all())
