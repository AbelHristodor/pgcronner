use log::{info, warn};
use postgres::{Client, NoTls};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::FromPyObject;
use std::fmt;

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
    #[new]
    fn new(name: String, schedule: String, command: String, source: String) -> Self {
        JobBuilder {
            name,
            schedule,
            command,
            source,
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
    fn new(
        jobs_map: Option<&PyDict>,
        db_uri: Option<String>,
        table_name: Option<String>,
    ) -> PyResult<Self> {
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

        info!("DB Uri: {}", uri);
        let mut client =
            get_db_connection(&uri).or(Err(PyValueError::new_err("Could not connect to DB!")))?;

        let table_name: String = match table_name {
            Some(name) => name,
            _ => String::from("pgcronner_cronjobs"),
        };
        let table = format!(
            "
        CREATE IF NOT EXISTS TABLE {table_name} 
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        command TEXT NOT NULL,
        schedule VARCHAR(255) NOT NULL,
        source TEXT,
        created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        "
        );

        client
            .query(&table, &[])
            .map_err(|err| PyValueError::new_err(format!("Could not create init table: {err}")))?;

        // Get all jobs from the HashMap
        jobs_map
            .ok_or(PyValueError::new_err("Job map is mandatory!"))?
            .iter()
            .for_each(|(key, value)| {
                // Convert value to Job
                let job = value
                    .extract::<Job>()
                    .or(Err(PyValueError::new_err("Could not parse job!")))
                    .unwrap();
                info!("Name: {}, Job: {}", key, job);
            });

        // Check DB for jobs
        // Sync

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

    fn add(&mut self, job: JobBuilder) -> PyResult<i64> {
        let row = self
            .client
            .query_one(
                &format!("INSERT INTO {} (name, schedule, command, source) VALUES ($1, $2, $3, $4) RETURNING id", self.table_name),
                &[&job.name, &job.schedule, &job.command, &job.source],
            )
            .map_err(|e| PyValueError::new_err(format!("Could not add job to DB: {}", e)))?;

        // TODO: Schedule cron

        Ok(row.get(0))
    }

    fn remove(&mut self, jobname: String) -> PyResult<bool> {
        let row = self
            .client
            .query_one(
                &format!("DELETE FROM {} WHERE name=$1", self.table_name),
                &[&jobname],
            )
            .map_err(|e| PyValueError::new_err(format!("Could not remove job from DB: {}", e)))?;

        //TODO: Unschedule cron

        Ok(row.get(0))
    }

    fn sync(&mut self, _py: Python) -> PyResult<u32> {
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

        // Schedule cronjobs
        jobs.into_iter().for_each(|job| {
            if job.command.contains("CALL") && !job.source.is_empty() {
                info!(
                    "Job has command that calls a function (stored procedure), source not empty."
                );
                info!("Updating or creating stored procedure for job: {}", job);

                let fname = &job
                    .command
                    .split_whitespace()
                    .nth(1)
                    .unwrap_or(job.name.as_str());

                self.client
                    .query(
                        &format!(
                            "CREATE OR REPLACE PROCEDURE {} AS $$ BEGIN {} END; $$ LANGUAGE plpgsql",
                            fname, job.source
                        ),
                        &[],
                    )
                    .map_err(|e| {
                        PyValueError::new_err(format!("Could not create stored procedure: {}", e))
                    })
                    .unwrap();
            }

            info!("Scheduling job: {}", job);

            self.client
                .query(
                    &format!(
                        "SELECT cron.schedule('{}', '{}', '{}')",
                        job.name, job.schedule, job.command,
                    ),
                    &[],
                )
                .map_err(|e| PyValueError::new_err(format!("Could not schedule job: {}", e)))
                .unwrap();
        });

        Ok(10)
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
