# pgcronner
A Rust pgcron manager for python



## Defining pgcron jobs


```python
PGCRONNER_JOBS = {
  "job_name": {
    "schedule": "<str>",
    "stored_procedure": "<Optional[str]>"
    "source": "<str>" #"dotted.path.to.sql.file",
  }
}


