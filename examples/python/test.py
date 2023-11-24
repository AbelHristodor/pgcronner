import logging
from pgcronner import PgCronner, Job

logging.basicConfig(level=logging.DEBUG)

logger = logging.getLogger(__name__)

# Connect to the database
pg = PgCronner('postgres://postgres@localhost:5432/postgres')

# Define a job
job = Job(
    name='test_job',
    schedule='* * * * *',
    command='SELECT 1',
    source=''
)

logger.debug("Job: %s", job)

# Add the job to the database

try:
    add = pg.add(job)
    logger.debug("Add job: %s", add)
except ValueError:
    logger.debug("Job already exists")
    job = Job(
        name='test_job_2',
        schedule='* * * * *',
        command='SELECT 1',
        source=''
    )
    add = pg.add(job)

# Get the job from the database
get = pg.one(job.name)
logger.debug("Get job: %s", get)

# Refres the job from the database
refresh = pg.refresh()
logger.debug("Refresh job: %s", refresh)

# Sync the job with the database
sync = pg.sync()
logger.debug("Sync job: %s", sync)

# Remove the job from the database
remove = pg.remove(job.name)
logger.debug("Remove job: %s", remove)

# Clear the database
clear = pg.clear()
logger.debug("Clear database: %s", clear)


