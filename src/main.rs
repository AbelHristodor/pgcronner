use anyhow::Result;
use std::collections::HashMap;

use pgcronnerlib::{print_jobs, Job};

fn main() -> Result<()> {
    let mut my_jobs: HashMap<String, Job> = HashMap::new();

    let job: Job = Job {
        schedule: "0 0 * * *".to_string(),
        command: "CALL my_command".to_string(),
        source: "SELECT * FROM cron.job;".to_string(),
    };

    my_jobs.insert("TestJob".to_string(), job);

    print_jobs(&my_jobs)?;

    Ok(())
}
