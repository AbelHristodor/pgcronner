//! # pgcronner
//!
//! `pgcronner` is a Python library that allows you to schedule jobs in PostgreSQL using the `cron` extension.

use crate::job::Job;
use crate::utils::{
    create_stored_procedure, create_table, delete_all_jobs, delete_all_stored_procedures,
    get_last_run, get_stored_procedure_name, schedule_job,
};
use errors::{ConvertError, DbError};
use log::{debug, info, warn};
use postgres::{Client, NoTls};
use pyo3::exceptions::{PyOSError, PyValueError};
use pyo3::prelude::*;

mod errors;
mod job;
mod utils;

const PREFIX: &str = "pgcronner__";

fn get_db_connection(uri: &str) -> anyhow::Result<Client> {
    let client = Client::connect(uri, NoTls)?;
    Ok(client)
}

fn row_to_job(row: &postgres::Row, client: &mut Client) -> Result<Job, ConvertError> {
    let name: String = row.try_get(1).map_err(|e| {
        ConvertError::new(format!(
            "Could not convert row to job, could not get name: {}",
            &e
        ))
    })?;
    let schedule: String = row.try_get(3).map_err(|e| {
        ConvertError::new(format!(
            "Could not convert row to job, could not get schedule: {}",
            e
        ))
    })?;
    let command: String = row.try_get(2).map_err(|e| {
        ConvertError::new(format!(
            "Could not convert row to job, could not get command: {}",
            &e
        ))
    })?;
    let source: String = row.try_get(4).map_err(|e| {
        ConvertError::new(format!(
            "Could not convert row to job, could not get source: {}",
            &e
        ))
    })?;
    let active: bool = row.try_get(5).map_err(|e| {
        ConvertError::new(format!(
            "Could not convert row to job, could not get active: {}",
            &e
        ))
    })?;

    let job = Job {
        name: name.clone(),
        schedule,
        command,
        source,
        active,
        last_run: get_last_run(client, &name),
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
    /// pgcronner = pgcronner.PgCronner(db_uri="postgres://postgres:postgres@localhost:5432/postgres", table_name="pgcronner")
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
                        Err(_) => return Err(PyOSError::new_err(
                            "No DB Uri set, please set it or set the DATABASE_URL env variable!",
                        )),
                    }
                }
            };

        let mut client =
            get_db_connection(&uri).or(Err(PyOSError::new_err("Could not connect to DB!")))?;

        let table_name = create_table(&mut client, &table_name.unwrap_or("".to_string()))?;

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
            .map_err(|e| DbError::new(format!("Could not get jobs from DB: {}", &e)))?;

        let mut jobs = Vec::new();

        for row in rows {
            let job = row_to_job(&row, &mut self.client)?;
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
            .map_err(|e| PyValueError::new_err(format!("Could not get job from DB: {}", &e)))?;

        match rows {
            Some(row) => {
                let job = row_to_job(&row, &mut self.client)?;
                Ok(job.into_py(_py))
            }
            None => Err(DbError::new(format!("Job {} not found!", &jobname)).into()),
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
    /// job = pgcronner.Job("myjob", "0 0 * * *", "SELECT * FROM mytable", ...)
    /// pgcronner.add(job)
    /// ```
    ///
    /// # Returns
    /// True if the job was added, false if not
    #[pyo3(text_signature = "($self, job)")]
    fn add(&mut self, job: Job) -> PyResult<bool> {
        job.is_valid()?;

        match self
            .client
            .query_one(
                &format!("INSERT INTO {} (name, schedule, command, source) VALUES ($1, $2, $3, $4) RETURNING id", self.table_name),
                &[&job.name, &job.schedule, &job.command, &job.source],
            ) {
            Ok(_) => {
                debug!("Added job: {}", job);
                Ok(true)
            }
            Err(e) => Err(DbError::new(format!("Could not add job to DB: {}", &e)).into()),
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

        match self.client.query(
            &format!("DELETE FROM {} WHERE name=$1", self.table_name),
            &[&jobname],
        ) {
            Ok(_) => {
                info!("Removed job: {}", jobname);
                Ok(true)
            }
            Err(e) => Err(DbError::new(format!("Could not remove job from DB: {}", &e)).into()),
        }
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
        let q1 = self
            .client
            .query(&format!("DELETE FROM {}", self.table_name), &[]);

        if q1.is_err() {
            return Err(DbError::new(format!(
                "Could not clear jobs from table: {}",
                &q1.err().unwrap()
            ))
            .into());
        }

        match self.client.query("DELETE FROM cron.job", &[]) {
            Ok(_) => {
                info!("Cleared all jobs");
                Ok(true)
            }
            Err(e) => Err(DbError::new(format!("Could not clear jobs from table: {}", &e)).into()),
        }
    }

    /// Refresh all jobs
    /// This will update the last_run column for all jobs
    /// This is useful if you want to keep track of when a job was last run
    ///
    /// # Example
    /// ```
    /// import pgcronner
    ///
    /// pgcronner = pgcronner.PgCronner()
    /// pgcronner.refresh()
    ///```
    ///
    /// # Returns
    /// True if the jobs were refreshed, false if not
    #[pyo3(text_signature = "($self)")]
    fn refresh(&mut self) -> PyResult<bool> {
        let q = self
            .client
            .query(&format!("SELECT * FROM {}", self.table_name), &[])
            .map_err(|e| DbError::new(format!("Could not fetch cronjobs from table: {}", &e)))?;

        match q.len() {
            0 => Ok(true),
            _ => {
                q.iter().for_each(|job| {
                    let name: String = job.get(1);
                    let last_run: chrono::DateTime<chrono::Utc> =
                        get_last_run(&mut self.client, &name).unwrap_or_default();

                    debug!("Last run for job: {:?}", last_run);

                    match self.client.query(
                        &format!("UPDATE {} SET last_run=$1 WHERE name=$2", self.table_name),
                        &[&last_run, &name],
                    ) {
                        Ok(_) => {}
                        Err(e) => {
                            warn!("Could not update last_run for job: {}", &e);
                        }
                    }

                    debug!("Updated last_run for job: {}", name);
                });
                Ok(true)
            }
        }
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
    fn sync(&mut self, _py: Python) -> PyResult<u32> {
        let jobs: Vec<Job> = self
            .client
            .query(&format!("SELECT * FROM {}", self.table_name), &[])
            .map_err(|e| DbError::new(format!("Could not fetch cronjobs from table: {}", &e)))?
            .iter()
            .map_while(|row| match row_to_job(row, &mut self.client) {
                Ok(job) => Some(job),
                Err(e) => {
                    warn!("Could not convert row to job: {}", e);
                    None
                }
            })
            .filter(|job| job.is_valid().is_ok())
            .collect();

        debug!("Fetched {} jobs from DB", jobs.len());

        debug!("Dumping all jobs and replacing with jobs in DB");
        delete_all_jobs(&mut self.client)?;
        delete_all_stored_procedures(&mut self.client)?;

        // Schedule cronjobs
        let jobs = jobs
            .iter()
            .map_while(|job| match job.uses_stored_procedure() {
                true => {
                    debug!("Creating stored procedure for job: {}", job.name);
                    let stored_procedure = get_stored_procedure_name(&job.command, &job.name);
                    match create_stored_procedure(&mut self.client, &stored_procedure, &job.source)
                    {
                        Ok(_) => {
                            debug!("Created stored procedure for job: {}", job.name);
                            Some(job)
                        }
                        Err(e) => {
                            warn!("Could not create stored procedure for job: {}", e);
                            None
                        }
                    }
                }
                false => Some(job),
            })
            .collect::<Vec<&Job>>();

        let results = jobs
            .iter()
            .map(|job| schedule_job(&mut self.client, job))
            .collect::<Vec<Result<(), DbError>>>();

        Ok(results.into_iter().filter_map(|r| r.ok()).count() as u32)
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
    use crate::job::schedule_is_valid;
    use std::ops::Not;

    #[test]
    #[should_panic]
    fn test_validate_schedule() {
        // Shouldn't panic
        assert!(schedule_is_valid("* * * * *").is_ok());
        assert!(schedule_is_valid("*/5 * * * *").is_ok());

        // Should panic
        assert!(schedule_is_valid("* * * *").is_ok());
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
            active: true,
            last_run: None,
        };
        assert!(job.name.is_empty().not());
        assert!(job.schedule.is_empty().not());
        assert!(job.command.is_empty().not());

        assert!(schedule_is_valid(&job.schedule).is_ok());
        assert!(job.is_valid().is_ok());
    }
}
