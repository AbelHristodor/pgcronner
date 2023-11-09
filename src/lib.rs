//! # pgcronner
//!
//! `pgcronner` is a Python library that allows you to schedule jobs in PostgreSQL using the `cron` extension.

use crate::job::Job;
use crate::utils::{
    create_stored_procedure, create_table, delete_all_jobs, delete_all_stored_procedures,
    get_stored_procedure_name, schedule_job,
};
use log::{debug, info, warn};
use postgres::{Client, NoTls};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

mod job;
mod utils;

const PREFIX: &str = "pgcronner__";

// TODO: Add active flag to jobs
// TODO: Add last run

fn get_db_connection(uri: &String) -> anyhow::Result<Client> {
    let client = Client::connect(uri, NoTls)?;
    Ok(client)
}

fn row_to_job(row: &postgres::Row) -> anyhow::Result<Job> {
    let job = Job {
        name: row.try_get(1)?,
        schedule: row.try_get(3)?,
        command: row.try_get(2)?,
        source: row.try_get(4)?,
    };
    Ok(job)
}

#[pyclass(module = "pgcronner")]
struct PgCronner {
    db_uri: String,
    client: Client,
    table_name: String,
}

#[pymethods]
impl PgCronner {
    // TODO: add validation
    // TODO: Add tests

    #[setter]
    fn set_db_uri(&mut self, db_uri: String) -> PyResult<()> {
        self.db_uri = db_uri;
        Ok(())
    }

    #[getter]
    fn get_db_uri(&self) -> PyResult<String> {
        Ok(self.db_uri.clone())
    }

    /// Create a new PgCronner instance
    ///
    /// # Arguments
    /// * `db_uri` - The database uri to connect to (optional) (default: DATABASE_URL env variable)
    /// * `table_name` - The name of the table to use (optional) (default: pgcronner)
    ///
    /// # Example
    /// ```
    /// import pgcronner
    ///
    /// pgcronner = pgcronner.PgCronner()
    ///```
    ///
    #[new]
    #[pyo3(text_signature = "(db_uri=None, table_name=None)")]
    fn new(db_uri: Option<String>, table_name: Option<String>) -> PyResult<Self> {
        let uri: String =
            match db_uri {
                Some(uri) => uri,
                None => {
                    warn!("No DB Uri set, trying env variable");
                    match std::env::var("DATABASE_URL") {
                        Ok(uri) => uri,
                        Err(_) => return Err(PyValueError::new_err(
                            "No DB Uri set, please set it or set the DATABASE_URL env variable!",
                        )),
                    }
                }
            };

        let mut client =
            get_db_connection(&uri).or(Err(PyValueError::new_err("Could not connect to DB!")))?;

        let table_name = create_table(&mut client, &table_name.unwrap_or("".to_string()))
            .map_err(|e| PyValueError::new_err(format!("Could not create table: {}", e)))?;

        Ok(PgCronner {
            db_uri: uri,
            client,
            table_name,
        })
    }
}

#[pymethods]
impl PgCronner {
    /// Get all jobs
    ///
    /// # Example
    /// ```
    /// import pgcronner
    ///
    /// pgcronner = pgcronner.PgCronner()
    /// jobs = pgcronner.all()
    /// ```
    ///
    /// # Returns
    /// A list of jobs
    #[pyo3(text_signature = "($self)")]
    fn all(&mut self, _py: Python) -> PyResult<Vec<Py<PyAny>>> {
        let rows = self
            .client
            .query(&format!("SELECT * FROM {}", self.table_name), &[])
            .map_err(|e| PyValueError::new_err(format!("Could not get jobs from DB: {}", e)))?;

        let mut jobs = Vec::new();

        for row in rows {
            let job = row_to_job(&row).map_err(|e| {
                PyValueError::new_err(format!("Could not convert row to job: {}", e))
            })?;
            jobs.push(job.into_py(_py));
        }
        Ok(jobs)
    }

    /// Get a job by name
    ///
    /// # Arguments
    /// * `jobname` - The name of the job to get
    ///
    /// # Example
    /// ```
    /// import pgcronner
    ///
    /// pgcronner = pgcronner.PgCronner()
    /// job = pgcronner.one("myjob")
    /// ```
    ///
    /// # Returns
    /// A job
    #[pyo3(text_signature = "($self, jobname)")]
    fn one(&mut self, jobname: String, _py: Python) -> PyResult<Py<PyAny>> {
        let rows = self
            .client
            .query_opt(
                &format!("SELECT * FROM {} WHERE name = $1", self.table_name),
                &[&jobname],
            )
            .map_err(|e| PyValueError::new_err(format!("Could not get job from DB: {}", e)))?;

        match rows {
            Some(row) => {
                let job = row_to_job(&row).map_err(|e| {
                    PyValueError::new_err(format!("Could not convert row to job: {}", e))
                })?;
                Ok(job.into_py(_py))
            }
            None => Err(PyValueError::new_err(format!("Job {} not found!", jobname))),
        }
    }

    /// Add a job
    ///
    /// # Arguments
    /// * `job` - The job to add
    ///
    /// # Example
    /// ```
    /// import pgcronner
    ///
    /// pgcronner = pgcronner.PgCronner()
    ///
    /// job = pgcronner.Job("myjob", "0 0 * * *", "SELECT * FROM mytable")
    /// pgcronner.add(job)
    /// ```
    ///
    /// # Returns
    /// True if the job was added, false if not
    #[pyo3(text_signature = "($self, job)")]
    fn add(&mut self, job: Job) -> PyResult<bool> {
        job.is_valid()
            .then_some(())
            .ok_or_else(|| PyValueError::new_err(format!("Job is not valid: {}", job)))?;

        let id: i32 = self
            .client
            .query_one(
                &format!("INSERT INTO {} (name, schedule, command, source) VALUES ($1, $2, $3, $4) RETURNING id", self.table_name),
                &[&job.name, &job.schedule, &job.command, &job.source],
            )
            .map_err(|e| PyValueError::new_err(format!("Could not add job to DB: {}", e)))?.get(0);

        match id {
            0 => Err(PyValueError::new_err(format!(
                "Could not add job: {}",
                &job
            ))),
            _ => Ok(true),
        }
    }

    /// Remove a job
    ///
    /// # Arguments
    /// * `jobname` - The name of the job to remove
    ///
    /// # Example
    /// ```
    /// import pgcronner
    ///
    /// pgcronner = pgcronner.PgCronner()
    /// pgcronner.remove("myjob")
    /// ```
    ///
    /// # Returns
    /// True if the job was removed, false if not
    #[pyo3(text_signature = "($self, jobname)")]
    fn remove(&mut self, jobname: String) -> PyResult<bool> {
        info!("Removing job: {}", jobname);
        self.client
            .query(
                &format!("DELETE FROM {} WHERE name=$1", self.table_name),
                &[&jobname],
            )
            .map_err(|e| PyValueError::new_err(format!("Could not remove job from DB: {}", e)))?;

        Ok(true)
    }

    /// Clear all jobs
    ///
    /// # Example
    /// ```
    /// import pgcronner
    ///
    /// pgcronner = pgcronner.PgCronner()
    /// pgcronner.clear()
    /// ```
    ///
    /// # Returns
    /// True if the jobs were cleared, false if not
    #[pyo3(text_signature = "($self)")]
    fn clear(&mut self) -> PyResult<bool> {
        self.client
            .query(&format!("DELETE FROM {}", self.table_name), &[])
            .map_err(|e| PyValueError::new_err(format!("Could not clear jobs from DB: {}", e)))?;

        self.client
            .query(
                &format!("DELETE FROM cron.job WHERE jobname LIKE {}", &PREFIX),
                &[],
            )
            .map_err(|e| {
                PyValueError::new_err(format!("Could not clear jobs from cron.job: {}", e))
            })?;

        Ok(true)
    }

    /// Sync all jobs while dumping all old jobs
    ///
    /// # Example
    /// ```
    /// import pgcronner
    ///
    /// pgcronner = pgcronner.PgCronner()
    /// pgcronner.sync()
    /// ```
    ///
    /// # Returns
    /// True if the jobs were synced, false if not
    #[pyo3(text_signature = "($self)")]
    fn sync(&mut self, _py: Python) -> PyResult<bool> {
        let jobs: Vec<Job> = self
            .client
            .query(&format!("SELECT * FROM {}", self.table_name), &[])
            .map_err(|e| {
                PyValueError::new_err(format!("Could not fetch cronjobs from table: {e}"))
            })?
            .iter()
            .map(|row| {
                row_to_job(&row)
                    .map_err(|e| {
                        PyValueError::new_err(format!("Could not convert row to job: {}", e))
                    })
                    .unwrap_or_else(|_| {
                        warn!("Could not convert row to job, skipping");
                        Job {
                            name: "".to_string(),
                            schedule: "".to_string(),
                            command: "".to_string(),
                            source: "".to_string(),
                        }
                    })
            })
            .collect::<Vec<Job>>();
        debug!("Fetched {} jobs from DB", jobs.len());

        debug!("Dumping all jobs and replacing with jobs in DB");
        delete_all_jobs(&mut self.client)?;
        delete_all_stored_procedures(&mut self.client)?;

        // Schedule cronjobs
        jobs.into_iter().for_each(|job| {
            debug!("Found job: {}", job);
            job.is_valid()
                .then(|| {
                    info!("Job is valid: {}", job);

                    if job.command.contains("CALL") {
                        debug!("Job calls stored procedure and source is not empty.");

                        let name: String = get_stored_procedure_name(&job.command, &job.name);

                        debug!("Creating stored procedure: {}", name);
                        create_stored_procedure(&mut self.client, &name, &job.source)
                    } else {
                        Ok(())
                    }
                })
                .and_then(|_| {
                    debug!("Scheduling job: {}", job);
                    schedule_job(&mut self.client, &job).ok()
                });
        });

        info!("Synced jobs");
        Ok(true)
    }

    /// String representation
    fn __str__(&self) -> PyResult<String> {
        Ok(format!(
            "PgCronner(db_uri={}, table_name={})",
            self.db_uri, self.table_name
        ))
    }

    /// String representation
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!(
            "PgCronner(db_uri={}, table_name={})",
            self.db_uri, self.table_name
        ))
    }
}

/// A Python module implemented in Rust.
#[pymodule]
fn pgcronner(_py: Python, m: &PyModule) -> PyResult<()> {
    pyo3_log::init();
    m.add_class::<PgCronner>()?;
    m.add_class::<Job>()?;
    Ok(())
}

/// Tests

#[cfg(test)]
mod tests {

    use super::*;
    use crate::job::validate_schedule;
    use std::ops::Not;

    #[test]
    #[should_panic]
    fn test_validate_schedule() {
        // Shouldn't panic
        assert_eq!(validate_schedule("* * * * *"), true);
        assert_eq!(validate_schedule("*/5 * * * *"), true);

        // Should panic
        assert_eq!(validate_schedule("* * * *"), false);
    }

    #[test]
    fn test_db_connection() {
        let uri = "postgres://postgres:postgres@localhost:5432/postgres";
        let client = get_db_connection(&uri.to_string()).unwrap();
        assert!(client.is_closed().not());
    }

    #[test]
    fn test_job_is_valid_without_source() {
        let job: Job = Job {
            name: "TEST".to_string(),
            schedule: "*/5 * * * *".to_string(),
            command: "SELECT 1".to_string(),
            source: "".to_string(),
        };
        assert!(job.name.is_empty().not());
        assert!(job.schedule.is_empty().not());
        assert!(job.command.is_empty().not());

        assert_eq!(validate_schedule(&job.schedule), true);
        assert!(job.is_valid());
    }
}
