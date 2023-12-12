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

logger.info("Job: %s", job)

# Add the job to the database

try:
    add = pg.add(job)
    logger.info("Add job: %s", add)
except ValueError:
    logger.info("Job already exists")


# Get the job from the database
get = pg.one(job.name)
logger.info("Get job: %s", get)

# Refres the job from the database
refresh = pg.refresh()
logger.info("Refresh job: %s", refresh)

logger.info("%s", pg.one(job.name))

# Sync the job with the database
sync = pg.sync()
logger.info("Sync job: %s", sync)

# Remove the job from the database
remove = pg.remove(job.name)
logger.info("Remove job: %s", remove)

# Clear the database
clear = pg.clear()
logger.info("Clear database: %s", clear)


