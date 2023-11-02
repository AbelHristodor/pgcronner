use crate::utils::{
    create_stored_procedure, create_table, delete_all_jobs, get_stored_procedure_name, schedule_job,
};
use log::{info, warn};
use postgres::{Client, NoTls};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::FromPyObject;
use std::fmt;
use std::ops::Not;

mod utils;

#[derive(Debug, Clone)]
#[pyclass]
pub struct JobBuilder {
    #[pyo3(get, set)]
    name: String,
    #[pyo3(get, set)]
    schedule: String,
    #[pyo3(get, set)]
    command: String,
    #[pyo3(get, set)]
    source: String,
}

#[pymethods]
impl JobBuilder {
    // TODO: Add docstrings, add validation
    // TODO: Add __repr__, __str__, __eq__, __hash__, __dict__, __iter__
    // TODO: Add tests

    #[new]
    fn new(name: String, schedule: String, command: String, source: String) -> Self {
        JobBuilder {
            name,
            schedule,
            command,
            source,
        }
    }

    fn build(&self) -> Job {
        Job {
            name: self.name.clone(),
            schedule: self.schedule.clone(),
            command: self.command.clone(),
            source: self.source.clone(),
        }
    }
}

#[derive(Debug, FromPyObject, Clone)]
#[pyo3(from_item_all)]
pub struct Job {
    pub name: String,     // Name of the job
    pub schedule: String, // cron schedule
    pub command: String,  // E.g. CALL my_command()
    pub source: String,   // SQL source
}

impl fmt::Display for Job {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Job ({}, {}, {}, {})",
            self.name, self.schedule, self.command, self.source
        )
    }
}

impl IntoPy<PyObject> for Job {
    fn into_py(self, py: Python<'_>) -> PyObject {
        let job_dict = PyDict::new(py);
        job_dict.set_item("name", self.name).unwrap();
        job_dict.set_item("schedule", self.schedule).unwrap();
        job_dict.set_item("command", self.command).unwrap();
        job_dict.set_item("source", self.source).unwrap();
        job_dict.into()
    }
}

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
    // TODO: Add docstrings, add validation
    // TODO: Add __repr__, __str__, __eq__, __hash__, __dict__, __iter__
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

    #[new]
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

    fn add(&mut self, job: JobBuilder) -> PyResult<bool> {
        let id: i32 = self
            .client
            .query_one(
                &format!("INSERT INTO {} (name, schedule, command, source) VALUES ($1, $2, $3, $4) RETURNING id", self.table_name),
                &[&job.name, &job.schedule, &job.command, &job.source],
            )
            .map_err(|e| PyValueError::new_err(format!("Could not add job to DB: {}", e)))?.get(0);

        Ok(id > 0)
    }

    fn remove(&mut self, jobname: String) -> PyResult<bool> {
        let id: i32 = self
            .client
            .query_one(
                &format!("DELETE FROM {} WHERE name=$1 RETURNING id", self.table_name),
                &[&jobname],
            )
            .map_err(|e| PyValueError::new_err(format!("Could not remove job from DB: {}", e)))?
            .get(0);

        Ok(id > 0)
    }

    fn clear(&mut self) -> PyResult<bool> {
        self.client
            .query(&format!("DELETE FROM {}", self.table_name), &[])
            .map_err(|e| PyValueError::new_err(format!("Could not clear jobs from DB: {}", e)))?;

        self.client
            .query("DELETE FROM cron.job", &[])
            .map_err(|e| {
                PyValueError::new_err(format!("Could not clear jobs from cron.job: {}", e))
            })?;

        // TODO: Find a way to delete stored procedures too

        Ok(true)
    }

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
                    .unwrap()
            })
            .collect::<Vec<Job>>();

        info!("Dumping all jobs and replacing with jobs in DB");
        delete_all_jobs(&mut self.client).map_err(|e| {
            PyValueError::new_err(format!("Could not delete all jobs from table: {}", e))
        })?;

        // Schedule cronjobs
        jobs.into_iter().for_each(|job| {
            job.source.is_empty().not().then(|| {
                info!("Job calls stored procedure and source is not empty.");

                let name: String = get_stored_procedure_name(&job.command, &job.name).unwrap();
                create_stored_procedure(&mut self.client, &name, &job.source)
                    .map_err(|e| {
                        PyValueError::new_err(format!("Could not create stored procedure: {}", e))
                    })
                    .unwrap();
            });

            schedule_job(&mut self.client, &job)
                .map_err(|e| PyValueError::new_err(format!("Could not schedule job: {}", e)))
                .unwrap();
        });

        Ok(true)
    }
}

/// A Python module implemented in Rust.
#[pymodule]
fn pgcronner(_py: Python, m: &PyModule) -> PyResult<()> {
    pyo3_log::init();
    m.add_class::<PgCronner>()?;
    m.add_class::<JobBuilder>()?;
    Ok(())
}
