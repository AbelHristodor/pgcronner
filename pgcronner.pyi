from typing import Optional, Self, List, Dict, Any
import datetime

class Job(object):
    """
    Job object
    
    :param name: job name
    :param schedule: cron schedule
    :param command: command to run
    :param source: source of function if command is a function call e.g. "CALL f();"

    """
    name: str
    schedule: str
    command: str
    source: str
    last_run: Optional[datetime.datetime]
    active: bool

    def __init__(self, name: str, schedule: str, command: str, source: str) -> None: ...

class PgCronner(object):
    """
    PgCronner object

    :param db_uri: database uri, if not set tries to use DATABASE_URL env var
    :param table_name: table name to store jobs in
    """

    def __init__(self, db_uri: str = None, table_name: str = "pgcronner_jobs"): ...

    def __str__(self) -> str: ...

    def __repr__(self) -> str: ...

    def all(self) -> List[Dict[str, str]]:
        """
        Get all jobs in the table

        :return: List of jobs as dicts
        :throws: ValueError
        """

    def one(self, jobname: str) -> Optional[Dict[str, str]]:
        """
        Get one job by name

        :param jobname: job name
        :return: Job instance or None as dict

        :throws: ValueError
        """

    def add(self, job: Job) -> bool:
        """
        Add a job to the table

        :param job: Job instance
        :return: True if successful

        :throws: ValueError
        """

    def remove(self, jobname: str) -> bool:
        """
        Remove a job from the table
        
        :param jobname: job name
        :return: True if successful

        :throws: ValueError
        """
    
    def refresh(self) -> bool:
        """
        Refreshes jobs's last_run field
        
        :return: True if successful

        """

    def clear(self) -> bool:
        """
        Clears all jobs from the table and crontab
        
        :return: True if successful
        :throws: ValueError
        """


    def sync(self) -> bool:
        """
        Syncs jobs from the table to crontab creating functions if necessary
        
        :return: True if successful

        :throws: ValueError
        """

