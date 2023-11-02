# PGCronner

PGCronner is a simple and fast Python package written in Rust using PyO3 that helps manage PostgreSQL cron jobs. It lets you take control of your cron jobs by allowing you to add, remove, or list them easily. PGCronner uses a custom table to handle all these operations seamlessly.

The tool takes advantage of the PyO3 library—an extension module that enables interoperability between Python and Rust. This allows for an increased performance and memory safety. 

## Installation

First of all, you need to ensure that you have Python installed on your system. If you do not, please follow the official guide to get it up and running. 

The package can be installed via pip:

```bash
pip install pgcronner
```

## Usage

To use the package, you just need to import it into your Python script. 

Here is an example of how to create a new pgcron job:

```python
from pgcronner import PGCronner, JobBuilder

PG_URI = "postgresql://postgres:postgres@localhost:5432/postgres" 
pgcronner = PGCronner(PG_URI)

# Create a Job using the provided JobBuilder
# my_job = JobBuilder("<name>", "<schedule>", "<command>", "<source>")
my_job = JobBuilder("testjob", "*/5 * * * *", "SELECT 1;", "source")

# Pass the Job Object
pgcronner.add(job.build())

# Sync db table with pgcron
# !!! WARNING - DANGER ZONE !!!
# This drops the `cron.job` table and populates it with the jobs in `pgcronner_jobs`
# See below for implementation

pgcronner.sync()
```

Listing all the jobs:

```python
jobs = pgcronner.all()
for job in jobs:
    print(job)
```

Retrieve one job:
```python
job = pgcronner.one("<jobname>")

```

Removing a job:

```python
# The parameter would be the job id you want to delete.
pgcronner.remove(job_id)
```

## Documentation

For more detailed instructions and API details, refer to the full documentation at `https://pgcronner.readthedocs.io/`.

## Contributing

We welcome any kind of contribution - reporting issues, suggesting new features, or even writing code. Please make sure to read the CONTRIBUTING.md file before making a pull request.

## License

This project is licensed under the MIT License.

## Acknowledgments

This Python package has been realized using PyO3 to create a bridge between Rust and Python.

---

Enjoy the power of managing your PostgreSQL cron jobs with simplicity and ease. Happy PGCronning!
