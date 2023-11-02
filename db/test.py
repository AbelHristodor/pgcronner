
from pgcronner import PgCronner, JobBuilder
import logging

FORMAT = '%(levelname)s %(name)s %(asctime)-15s %(filename)s:%(lineno)d %(message)s'
logging.basicConfig(format=FORMAT)
logging.getLogger().setLevel(logging.INFO)

logger = logging.getLogger(__name__)

db_uri = "postgresql://postgres:postgres@localhost:5432/postgres"

r = PgCronner(db_uri)
logger.info(f"Retrieving all cronjobs from DB: {r.all()}")

job = JobBuilder("testjob", "*/5 * * * *", "SELECT 1;", "source")
logger.info(f"Built job: {job.build()}")
try:
    logger.info(f"Adding job: {r.add(job)}")
except ValueError as e:
    logger.warn("Caught exception: {}".format(e))

logger.info(f"Retrieving object from db: {r.one('testjob')}")
logger.info(f"Removing object from db: {r.remove('testjob')}")

try:
    logger.info("Adding job: {}".format(r.add(JobBuilder("testjob2", "*/5 * * * *", "CALL test_func()", "SELECT 1;"))))
except ValueError as e:
    logger.info("Caught exception: {}".format(e))

logger.info(r.sync())

logger.info(r.all())

logger.info("Cleaning up...")
logger.info("Cleaning result: {}".format(r.clear()))

