use crate::Job;
use log::info;
use postgres::Client;
use pyo3::exceptions::PyValueError;

pub fn get_stored_procedure_name(command: &str, default: &str) -> anyhow::Result<String> {
    command
        .contains("CALL")
        .then(|| {
            command
                .split_whitespace()
                .nth(1)
                .unwrap_or(default)
                .to_string()
        })
        .ok_or(anyhow::anyhow!("Could not get stored procedure name!"))
}

pub fn create_stored_procedure(
    client: &mut Client,
    name: &str,
    source: &str,
) -> anyhow::Result<bool> {
    client
        .query(
            &format!(
                "CREATE OR REPLACE PROCEDURE {} AS $$ BEGIN {} END; $$ LANGUAGE plpgsql",
                name, source
            ),
            &[],
        )
        .map_err(|e| PyValueError::new_err(format!("Could not create stored procedure: {}", e)))?;
    Ok(true)
}

pub fn schedule_job(client: &mut Client, job: &Job) -> anyhow::Result<()> {
    client
        .query_one(
            &format!(
                "SELECT cron.schedule('{}', '{}', '{}')",
                job.name, job.schedule, job.command,
            ),
            &[],
        )
        .map_err(|e| PyValueError::new_err(format!("Could not schedule job: {}", e)))?;

    info!("Scheduled job: {}", job);
    Ok(())
}

pub fn unschedule_job(client: &mut Client, name: &str) -> anyhow::Result<()> {
    client
        .query_one(&format!("SELECT cron.unschedule('{}')", name), &[])
        .map_err(|e| PyValueError::new_err(format!("Could not unschedule job: {}", e)))?;

    info!("Unscheduled job: {}", name);
    Ok(())
}

pub fn delete_all_jobs(client: &mut Client) -> anyhow::Result<()> {
    client
        .query("DELETE FROM cron.job ", &[])
        .map_err(|e| PyValueError::new_err(format!("Could not fetch cronjobs from table: {e}")))?;
    info!("Deleted all jobs");
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
