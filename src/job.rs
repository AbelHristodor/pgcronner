//! Job struct
//! A Job is a scheduled SQL command

use crate::get_stored_procedure_name;
use chrono::{DateTime, Utc};
use cron_parser::parse;
use pyo3::prelude::*;
use std::fmt;

use crate::PREFIX;

pub fn validate_schedule(schedule: &str) -> bool {
    let now: DateTime<Utc> = Utc::now();
    match parse(schedule, &now).or_else(|_| Err(())) {
        Ok(_) => true,
        Err(_) => false,
    }
}

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
}

fn parse_command(command: &str, name: &str) -> String {
    let command = match command.contains("CALL") {
        true => {
            let name = get_stored_procedure_name(&command, &name);

            if name.starts_with(PREFIX) {
                format!("CALL {}();", name)
            } else {
                format!("CALL {}{}();", PREFIX, name)
            }
        }
        false => command.clone().to_string(),
    };

    return command;
}

fn parse_name(name: &str) -> String {
    let name = match name.starts_with(PREFIX) {
        true => name.to_string(),
        false => format!("{}{}", PREFIX, name),
    };

    return name;
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
        let name = parse_name(&name);
        let command = parse_command(&command, &name);

        Self {
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

impl Job {
    pub fn is_valid(&self) -> bool {
        if self.name.is_empty() {
            return false;
        }
        if self.name.starts_with(PREFIX) {
            return false;
        }
        if self.schedule.is_empty() {
            return false;
        }
        if self.command.is_empty() {
            return false;
        }

        if !validate_schedule(&self.schedule) {
            return false;
        }

        if self.command.contains("CALL") {
            if self.source.is_empty() {
                return false;
            }
        }

        return true;
    }
}
