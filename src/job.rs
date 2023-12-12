//! Job struct
//! A Job is a scheduled SQL command

use crate::get_stored_procedure_name;
use chrono::{DateTime, Utc};
use cron_parser::parse;
use log::debug;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::fmt;

use crate::errors::ValidationError;
use crate::PREFIX;

/// A Job is a scheduled SQL command
///
/// # Arguments
/// * `name` - Name of the job
/// * `schedule` - cron schedule
/// * `command` - E.g. CALL my_command();
/// * `source` - SQL source
///
#[derive(Debug, Clone)]
#[pyclass]
pub struct Job {
    #[pyo3(get, set)]
    pub name: String, // Name of the job
    #[pyo3(get, set)]
    pub schedule: String, // cron schedule
    #[pyo3(get, set)]
    pub command: String, // E.g. CALL my_command()
    #[pyo3(get, set)]
    pub source: String, // SQL source
    pub last_run: Option<DateTime<Utc>>,
    pub active: bool,
}

fn parse_command(command: &str, name: &str) -> String {
    match command.contains("CALL") {
        true => {
            let name = get_stored_procedure_name(command, name);

            if name.starts_with(PREFIX) {
                format!("CALL {}();", name)
            } else {
                format!("CALL {}{}();", PREFIX, name)
            }
        }
        false => command.clone().to_string(),
    }
}

fn format_name(name: &str) -> Result<String, ValidationError> {
    match name.starts_with(PREFIX) {
        true => Ok(name.to_string()),
        false => Ok(format!("{}{}", PREFIX, name)),
    }
}

pub fn schedule_is_valid(schedule: &str) -> Result<(), ValidationError> {
    let now: DateTime<Utc> = Utc::now();
    match parse(schedule, &now) {
        Ok(_) => Ok(()),
        Err(e) => Err(format!("Invalid schedule: {}", e).into()),
    }
}

#[pymethods]
impl Job {
    /// Create a new Job
    ///
    /// # Arguments
    /// * `name` - Name of the job
    /// * `schedule` - cron schedule
    /// * `command` - E.g. CALL my_command()
    /// * `source` - SQL source
    ///
    /// # Example
    /// ```
    /// job = Job("my_job", "0 0 * * *", "CALL my_command();", "SELECT * FROM my_table;")
    /// ```
    ///
    #[new]
    pub fn new(name: String, schedule: String, command: String, source: String) -> Self {
        let name = format_name(&name).unwrap();
        let command = parse_command(&command, &name);

        Self {
            name,
            schedule,
            command,
            source,
            last_run: None,
            active: true,
        }
    }

    pub fn __dict__(&self, _py: Python) -> PyResult<Py<PyAny>> {
        let dict = PyDict::new(_py);

        let last_run = match self.last_run {
            Some(last_run) => last_run.to_string(),
            None => "".to_string(),
        };

        dict.set_item("name", self.name.clone())?;
        dict.set_item("schedule", self.schedule.clone())?;
        dict.set_item("command", self.command.clone())?;
        dict.set_item("source", self.source.clone())?;
        dict.set_item("last_run", last_run)?;
        dict.set_item("active", self.active)?;

        Ok(dict.into())
    }

    pub fn __str__(&self) -> PyResult<String> {
        Ok(format!(
            "Job ({}, {}, {}, {}, {}, {})",
            self.name,
            self.schedule,
            self.command,
            self.source,
            self.active,
            match self.last_run {
                Some(last_run) => last_run.to_string(),
                None => "".to_string(),
            }
        ))
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

impl Job {
    pub fn is_valid(&self) -> Result<(), ValidationError> {
        if self.name.is_empty() {
            return Err("Name is empty".to_string().into());
        }
        if !self.name.starts_with(PREFIX) {
            return Err(format!("Name must start with {}", PREFIX).into());
        }
        if self.schedule.is_empty() {
            return Err("Schedule is empty".to_string().into());
        }
        if self.command.is_empty() {
            return Err("Command is empty".to_string().into());
        }

        schedule_is_valid(&self.schedule)?;

        if self.command.contains("CALL") {
            if self.source.is_empty() {
                return Err("Source is empty".to_string().into());
            }
        }
        debug!("Job is valid");
        return Ok(());
    }

    pub fn uses_stored_procedure(&self) -> bool {
        self.command.contains("CALL")
    }
}
