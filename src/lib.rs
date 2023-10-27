use anyhow::Result;
use log::error;
use pyo3::prelude::*;
use std::collections::HashMap;
use std::env;
use std::fmt;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub struct Job {
    pub schedule: String, // cron schedule
    pub command: String,  // E.g. CALL my_command()
    pub source: String,   // SQL source
}

impl fmt::Display for Job {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "(Schedule: {}, Command: {}, Source: {})",
            self.schedule, self.command, self.source
        )
    }
}

#[pyclass]
struct PgCronner {}

#[pymethods]
impl PgCronner {
    #[pyo3(signature=(db_uri=None))]
    fn init(&self, db_uri: Option<String>) -> PyResult<bool> {
        if db_uri.is_none() {
            let db_uri = match env::var("DATABASE_URL") {
                Ok(val) => val,
                Err(val) => {
                    error!("Env var: <DATABASE_URL> not set! ");
                }
            };
        }

        Ok(true)
    }
}

pub fn get_sql_source_path(source: &String) -> PathBuf {
    let mut path = String::from("sql/");
    let dots = source.matches(".").count();
    match dots {
        0 => path.push_str(&source),
        _ => path.push_str(source.replacen(".", "/", dots - 1).as_str()),
    }

    PathBuf::from(Path::new(&path))
}

pub fn print_jobs(jobs: &HashMap<String, Job>) -> Result<()> {
    jobs.iter().for_each(|(name, job)| {
        println!(
            "Name: {}, Command: {}, Source: {}, Schedule: {}",
            name, job.command, job.source, job.schedule
        );
    });

    Ok(())
}

/// Formats the sum of two numbers as string.
#[pyfunction]
fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b).to_string())
}

/// A Python module implemented in Rust.
#[pymodule]
fn pgcronner(_py: Python, m: &PyModule) -> PyResult<()> {
    pyo3_log::init();
    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    Ok(())
}
