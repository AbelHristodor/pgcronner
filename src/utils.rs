use crate::errors::DbError;
use crate::job::Job;
use crate::PREFIX;
use chrono::DateTime;
use chrono::Utc;
use log::debug;
use postgres::Client;
use regex::Regex;

const DEFAULT_TABLE_NAME: &str = "pgcronner_jobs";

pub fn get_stored_procedure_name(command: &str, default: &str) -> String {
    // If string contains CALL, then it's a stored procedure
    // We need to extract the name of the stored procedure
    // Example: CALL my_stored_procedure() -> my_stored_procedure

    let re = Regex::new(r"CALL\s+(\w+)\(\);?").unwrap();

    let res = match re.captures(command) {
        Some(caps) => caps.get(1).unwrap().as_str(),
        None => default,
    };

    res.to_string()
}

pub fn create_stored_procedure(
    client: &mut Client,
    name: &str,
    source: &str,
) -> Result<(), DbError> {
    match client.query(
        &format!(
            "CREATE OR REPLACE FUNCTION {}() RETURNS void AS $$
            BEGIN
                {}
            END;
            $$ LANGUAGE plpgsql;",
            name, source
        ),
        &[],
    ) {
        Ok(_) => {
            debug!("Created stored procedure: {}", name);
            Ok(())
        }
        Err(e) => Err(format!("Could not create stored procedure: {}", e).into()),
    }
}

pub fn schedule_job(client: &mut Client, job: &Job) -> Result<(), DbError> {
    match client.query_one(
        &format!(
            "SELECT cron.schedule('{}', '{}', '{}')",
            job.name, job.schedule, job.command,
        ),
        &[],
    ) {
        Ok(_) => {
            debug!("Scheduled job: {}", job);
            Ok(())
        }
        Err(e) => Err(format!("Could not schedule job: {}", e).into()),
    }
}

#[allow(dead_code)]
pub fn unschedule_job(client: &mut Client, name: &str) -> Result<(), DbError> {
    match client.query_one(&format!("SELECT cron.unschedule('{}')", name), &[]) {
        Ok(_) => {
            debug!("Unscheduled job: {}", name);
            Ok(())
        }
        Err(e) => Err(format!("Could not unschedule job: {}", e).into()),
    }
}

pub fn delete_all_jobs(client: &mut Client) -> Result<(), DbError> {
    debug!("Deleting all jobs");
    match client.query_opt(
        &format!("DELETE FROM cron.job WHERE jobname LIKE 'pgcronner%'"),
        &[],
    ) {
        Ok(_) => {
            debug!("Deleted all jobs");
            Ok(())
        }
        Err(e) => Err(format!("Could not fetch cronjobs from table: {e}").into()),
    }
}

pub fn delete_all_stored_procedures(client: &mut Client) -> Result<(), DbError> {
    match client.query(
        "
            DO $$
            DECLARE
                func_name TEXT;
            BEGIN
                FOR func_name IN
                    SELECT proname
                    FROM pg_catalog.pg_proc
                    WHERE proname LIKE 'pgcronner%' AND prokind = 'f'
                LOOP
                    EXECUTE 'DROP FUNCTION IF EXISTS ' || func_name || ' CASCADE';
                END LOOP;
            END $$;
        ",
        &[],
    ) {
        Ok(_) => {
            debug!("Deleted all stored procedures");
            Ok(())
        }
        Err(e) => Err(e.to_string().into()),
    }
}

pub fn create_table<'a>(client: &mut Client, table_name: &str) -> Result<String, DbError> {
    let table_name = match table_name.is_empty() {
        true => DEFAULT_TABLE_NAME.to_string(),
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
        active BOOLEAN NOT NULL DEFAULT TRUE,
        last_run TIMESTAMPTZ,
        created TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP)",
    );

    match client.query(&table, &[]) {
        Ok(_) => Ok(table_name),
        Err(e) => Err(format!("Could not create table: {}", e.to_string()).into()),
    }
}

pub fn get_last_run(client: &mut Client, jobname: &str) -> Option<DateTime<Utc>> {
    let q = client
        .query(
            &format!(
                "
            WITH job AS (
                SELECT jobid FROM cron.job WHERE jobname = '{}'
            )
            SELECT max(start_time) as last_run_time FROM cron.job_run_details WHERE jobid = (SELECT jobid FROM job)",
                jobname
            ),
            &[],
        )
        .map_err(|e| DbError::new(format!("Could not fetch last run time: {e}")))
        .expect("Could not fetch last run time");

    debug!("Last run query: {:?}", q);

    let last_run = q
        .first()
        .expect("Could not get last run time")
        .get::<_, Option<DateTime<Utc>>>("last_run_time");

    last_run
}
