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
    name: Option<String>,
    #[pyo3(get, set)]
    schedule: Option<String>,
    #[pyo3(get, set)]
    command: Option<String>,
    #[pyo3(get, set)]
    source: Option<String>,
}

#[pymethods]
impl JobBuilder {
    #[new]
    fn new(name: String, schedule: String, command: String, source: String) -> Self {
        JobBuilder {
            name: Some(name),
            schedule: Some(schedule),
            command: Some(command),
            source: Some(source),
        }
    }
}

#[derive(Debug, FromPyObject)]
#[pyo3(from_item_all)]
pub struct Job {
    pub name: Option<String>,     // Name of the job
    pub schedule: Option<String>, // cron schedule
    pub command: Option<String>,  // E.g. CALL my_command()
    pub source: Option<String>,   // SQL source
}

impl fmt::Display for Job {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Job ({}, {}, {}, {})",
            self.name.as_ref().unwrap_or(&String::from("")),
            self.schedule.as_ref().unwrap_or(&String::from("")),
            self.command.as_ref().unwrap_or(&String::from("")),
            self.source.as_ref().unwrap_or(&String::from(""))
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

#[pyclass(module = "pgcronner")]
struct PgCronner {
    db_uri: String,
    client: Client,
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
    fn new(jobs_map: Option<&PyDict>, db_uri: Option<String>) -> PyResult<Self> {
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
        let client =
            get_db_connection(&uri).or(Err(PyValueError::new_err("Could not connect to DB!")))?;

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
        })
    }
}

#[pymethods]
impl PgCronner {
    fn all(&mut self, _py: Python) -> PyResult<Vec<Py<PyAny>>> {
        let rows = self
            .client
            .query("SELECT * FROM cron.job", &[])
            .map_err(|e| PyValueError::new_err(format!("Could not get jobs from DB: {}", e)))?;

        let mut jobs = Vec::new();

        for row in rows {
            let job = Job {
                name: row.get(8),
                schedule: row.get(1),
                command: row.get(2),
                source: Some("".to_string()),
            };

            jobs.push(job.into_py(_py));
        }
        Ok(jobs)
    }

    fn one(&mut self, jobname: String, _py: Python) -> PyResult<Py<PyAny>> {
        let rows = self
            .client
            .query_opt("SELECT * FROM cron.job WHERE jobname = $1", &[&jobname])
            .map_err(|e| PyValueError::new_err(format!("Could not get job from DB: {}", e)))?;

        match rows {
            Some(row) => {
                let job = Job {
                    name: row.get(8),
                    schedule: row.get(1),
                    command: row.get(2),
                    source: Some("".to_string()),
                };
                Ok(job.into_py(_py))
            }
            None => Err(PyValueError::new_err(format!("Job {} not found!", jobname))),
        }
    }

    fn add(&mut self, job: JobBuilder) -> PyResult<i64> {
        let row = self
            .client
            .query_one(
                "SELECT cron.schedule($1, $2, $3)",
                &[&job.name, &job.schedule, &job.command],
            )
            .map_err(|e| PyValueError::new_err(format!("Could not add job to DB: {}", e)))?;

        let val: i64 = row.get(0);

        Ok(val)
    }

    fn remove(&mut self, jobname: String) -> PyResult<bool> {
        let row = self
            .client
            .query_one("SELECT cron.unschedule($1)", &[&jobname])
            .map_err(|e| PyValueError::new_err(format!("Could not remove job from DB: {}", e)))?;

        Ok(row.get(0))
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
