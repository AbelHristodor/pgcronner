use pyo3::prelude::*;
use std::fmt;

#[derive(Debug, Clone)]
#[pyclass]
pub struct JobBuilder {
    #[pyo3(get, set)]
    pub name: String,
    #[pyo3(get, set)]
    pub schedule: String,
    #[pyo3(get, set)]
    pub command: String,
    #[pyo3(get, set)]
    pub source: String,
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

#[derive(Debug, Clone)]
#[pyclass]
pub struct Job {
    pub name: String,     // Name of the job
    pub schedule: String, // cron schedule
    pub command: String,  // E.g. CALL my_command()
    pub source: String,   // SQL source
}

#[pymethods]
impl Job {
    #[new]
    fn new(name: String, schedule: String, command: String, source: String) -> Self {
        Job {
            name,
            schedule,
            command,
            source,
        }
    }
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
