use crate::job::Job;
use log::debug;
use postgres::Client;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use regex::Regex;

pub fn get_stored_procedure_name(command: &str, default: &str) -> String {
    // If string contains CALL, then it's a stored procedure
    // We need to extract the name of the stored procedure
    // Example: CALL my_stored_procedure() -> my_stored_procedure

    let re = Regex::new(r"CALL\s+(\w+)\(\);?").unwrap();

    let res = match re.captures(command) {
        Some(caps) => caps.get(1).unwrap().as_str(),
        None => default,
    };

    return res.to_string();
}

pub fn create_stored_procedure(
    client: &mut Client,
    name: &str,
    source: &str,
) -> anyhow::Result<(), PyErr> {
    let q = client
        .query(
            &format!(
                "CREATE OR REPLACE FUNCTION {}() RETURNS void AS $$
            BEGIN
                {}
            END;
            $$ LANGUAGE plpgsql;",
                name, source
            ),
            &[],
        )
        .map_err(|e| PyValueError::new_err(format!("Could not create stored procedure: {}", e)))?;

    match q.len() {
        0 => Ok(()),
        _ => Err(PyValueError::new_err(format!(
            "Could not create stored procedure: {}",
            name
        ))),
    }
}

pub fn schedule_job(client: &mut Client, job: &Job) -> anyhow::Result<(), PyErr> {
    client
        .query_one(
            &format!(
                "SELECT cron.schedule('{}', '{}', '{}')",
                job.name, job.schedule, job.command,
            ),
            &[],
        )
        .map_err(|e| PyValueError::new_err(format!("Could not schedule job: {}", e)))?;

    debug!("Scheduled job: {}", job);
    Ok(())
}

#[allow(dead_code)]
pub fn unschedule_job(client: &mut Client, name: &str) -> anyhow::Result<()> {
    client
        .query_one(&format!("SELECT cron.unschedule('{}')", name), &[])
        .map_err(|e| PyValueError::new_err(format!("Could not unschedule job: {}", e)))?;

    debug!("Unscheduled job: {}", name);
    Ok(())
}

pub fn delete_all_jobs(client: &mut Client) -> anyhow::Result<(), PyErr> {
    let q = client
        .query("DELETE FROM cron.job ", &[])
        .map_err(|e| PyValueError::new_err(format!("Could not fetch cronjobs from table: {e}")))?;

    match q.len() {
        0 => Ok(()),
        e => Err(PyValueError::new_err(format!(
            "Could not delete all jobs: {}",
            e
        ))),
    }
}

pub fn delete_all_stored_procedures(client: &mut Client) -> anyhow::Result<(), PyErr> {
    debug!("Deleting all stored procedures");
    client
        .query(
            "
            DO $$
            DECLARE
                func_name TEXT;
            BEGIN
                FOR func_name IN
                    SELECT proname
                    FROM pg_catalog.pg_proc
                    WHERE proname LIKE 'pgcronner__%' AND prokind = 'f'
                LOOP
                    EXECUTE 'DROP FUNCTION IF EXISTS ' || func_name || ' CASCADE';
                END LOOP;
            END $$;
        ",
            &[],
        )
        .map_err(|e| {
            PyValueError::new_err(format!("Could not delete all stored procedures: {e}"))
        })?;

    Ok(())
}

pub fn create_table<'a>(client: &mut Client, table_name: &str) -> anyhow::Result<String> {
    let table_name = match table_name.is_empty() {
        true => "pgcronner_jobs".to_string(),
        false => table_name.trim().to_lowercase(),
    };
    let table = format!(
        "
        CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL UNIQUE,
        command TEXT NOT NULL,
        schedule VARCHAR(255) NOT NULL,
        source TEXT,
        created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)",
    );

    client
        .query(&table, &[])
        .map_err(|err| PyValueError::new_err(format!("Could not create init table: {err}")))?;

    Ok(table_name)
}
