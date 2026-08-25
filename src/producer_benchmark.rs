use std::error::Error;
use std::time::Instant;

use crate::client::init::client;

pub struct BenchmarkConfig {
    pub clients: usize,
    pub topics: usize,
    pub partitions_per_topic: u64,
    pub operations_per_partition: usize,
    pub runs: usize,
}

struct RunResult {
    run_no: usize,
    total_operations: usize,
    elapsed_ms: f64,
    throughput: f64,
}

pub async fn run(
    client: &mut client,
    config: BenchmarkConfig,
) -> Result<(), Box<dyn Error>> {

    println!("==============================================");
    println!("Single Client Producer Benchmark");
    println!("==============================================");

    println!("Clients:                 {}", config.clients);
    println!("Topics:                  {}", config.topics);
    println!(
        "Partitions/topic:        {}",
        config.partitions_per_topic
    );
    println!(
        "Operations/partition:    {}",
        config.operations_per_partition
    );
    println!("Runs:                    {}", config.runs);

    let total_partitions =
        config.topics *
        config.partitions_per_topic as usize;

    let total_operations =
        total_partitions *
        config.operations_per_partition;

    println!(
        "Total partitions:        {}",
        total_partitions
    );

    println!(
        "Total operations/run:    {}",
        total_operations
    );

    println!("==============================================");

    let mut results = Vec::new();

    // ======================================================
    // RUNS
    // ======================================================

    for run_no in 1..=config.runs {

        println!();
        println!("==============================================");
        println!("RUN {}", run_no);
        println!("==============================================");

        // ==================================================
        // STEP 1
        // CREATE ALL TOPICS
        //
        // This is NOT part of the benchmark timing.
        // ==================================================

        println!("Creating topics...");

        for topic_id in 0..config.topics {

            let topic_name =
                format!(
                    "benchmark_run_{}_topic_{}",
                    run_no,
                    topic_id
                );

            println!(
                "Creating topic: {}",
                topic_name
            );

            client
                .insert_topic(
                    topic_name.clone(),
                    config.partitions_per_topic,
                )
                .await?;

            println!(
                "Topic creation request completed: {}",
                topic_name
            );
        }

        println!("All topics created.");

        // ==================================================
        // IMPORTANT
        //
        // At this point:
        //
        // Client -> server
        //        -> topic creation
        //        -> server processes it
        //        -> ACK/response
        //        -> insert_topic() returns
        //
        // Therefore benchmark starts only NOW.
        // ==================================================

        println!("Starting benchmark...");

        let start = Instant::now();

        // ==================================================
        // STEP 2
        // SEND DATA
        // ==================================================

        for topic_id in 0..config.topics {

            let topic =
                format!(
                    "benchmark_run_{}_topic_{}",
                    run_no,
                    topic_id
                );

            for partition_id in
                0..config.partitions_per_topic
            {

                // --------------------------------------------------
                // ONE FIXED KEY FOR THIS TOPIC + PARTITION
                //
                // Every value written below uses the SAME key.
                //
                // This allows you to inspect the log and determine
                // whether values arrived in sequence:
                //
                // seq_0
                // seq_1
                // seq_2
                // ...
                // --------------------------------------------------

                let key =
                    format!(
                        "run_{}_topic_{}_partition_{}_key",
                        run_no,
                        topic_id,
                        partition_id
                    );

                for sequence in
                    0..config.operations_per_partition
                {

                    let value =
                        format!(
                            "run_{}_topic_{}_partition_{}_seq_{}",
                            run_no,
                            topic_id,
                            partition_id,
                            sequence
                        );

                    client
                        .send_topic_data(
                            topic.clone(),
                            Some(key.clone()),
                            value,
                        )
                        .await?;
                }
            }
        }

        // ==================================================
        // STEP 3
        // BENCHMARK END
        // ==================================================

        let elapsed =
            start.elapsed();

        let elapsed_seconds =
            elapsed.as_secs_f64();

        let throughput =
            total_operations as f64
                / elapsed_seconds;

        let result =
            RunResult {
                run_no,
                total_operations,
                elapsed_ms:
                    elapsed_seconds * 1000.0,
                throughput,
            };

        // ==================================================
        // PRINT RUN RESULT
        // ==================================================

        println!();
        println!(
            "Run {} completed.",
            run_no
        );

        println!(
            "Total operations: {}",
            result.total_operations
        );

        println!(
            "Time: {:.3} ms",
            result.elapsed_ms
        );

        println!(
            "Throughput: {:.2} ops/sec",
            result.throughput
        );

        results.push(result);

        // ==================================================
        // DO NOT DROP CLIENT
        //
        // The same TCP connection is reused for the next run.
        // ==================================================
    }

    // ======================================================
    // FINAL RESULTS
    // ======================================================

    println!();
    println!("==============================================");
    println!("FINAL RESULTS");
    println!("==============================================");

    for result in &results {

        println!(
            "RUN {} -> {:.2} ops/sec | {:.3} ms",
            result.run_no,
            result.throughput,
            result.elapsed_ms
        );
    }

    let average_throughput =
        results
            .iter()
            .map(|r| r.throughput)
            .sum::<f64>()
            / results.len() as f64;

    let average_time =
        results
            .iter()
            .map(|r| r.elapsed_ms)
            .sum::<f64>()
            / results.len() as f64;

    println!();

    println!(
        "Average time:       {:.3} ms",
        average_time
    );

    println!(
        "Average throughput: {:.2} ops/sec",
        average_throughput
    );

    println!("==============================================");

    Ok(())
}