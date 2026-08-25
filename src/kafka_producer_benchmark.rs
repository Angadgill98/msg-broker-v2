use std::error::Error;
use std::time::Instant;

use crate::client::init::client;

pub struct BenchmarkConfig {
    pub clients: usize,
    pub topics: usize,
    pub partitions_per_topic: u64,
    pub total_records: usize,
    pub record_size: usize,
    pub warmup_records: usize,
    pub runs: usize,
    pub throughput: i64,
}

struct RunResult {
    run_no: usize,
    total_operations: usize,
    successful_operations: usize,
    errors: usize,

    elapsed_ms: f64,

    records_per_sec: f64,
    mb_per_sec: f64,

    avg_latency_ms: f64,
    p50_latency_ms: f64,
    p95_latency_ms: f64,
    p99_latency_ms: f64,
    max_latency_ms: f64,
}

fn percentile(sorted: &[f64], percentile: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }

    let index =
        ((percentile / 100.0) * (sorted.len() - 1) as f64).round() as usize;

    sorted[index]
}

pub async fn run(
    clients: &mut [client],
    config: BenchmarkConfig,
) -> Result<(), Box<dyn Error>> {

    println!("============================================================");
    println!("                 PRODUCER BENCHMARK");
    println!("============================================================");

    println!();
    println!("Configuration");
    println!("------------------------------------------------------------");

    println!("Clients:                         {}", config.clients);
    println!("Topics:                          {}", config.topics);

    println!(
        "Partitions/topic:                {}",
        config.partitions_per_topic
    );

    println!(
        "Total records/client:            {}",
        config.total_records
    );

    println!(
        "Record size:                     {} bytes",
        config.record_size
    );

    println!(
        "Warmup records/client:           {}",
        config.warmup_records
    );

    println!("Runs:                            {}", config.runs);

    println!(
        "Throughput limit:                {}",
        config.throughput
    );

    let total_records =
        config.total_records * config.clients;

    println!(
        "Total records/run:               {}",
        total_records
    );

    println!("============================================================");

    let mut results = Vec::new();

    for run_no in 1..=config.runs {

        println!();
        println!("============================================================");
        println!("RUN {}", run_no);
        println!("============================================================");

        // =====================================================
        // CREATE TOPICS
        //
        // Topic creation is NOT included in benchmark timing.
        // =====================================================

        println!("Creating topics...");

        for topic_id in 0..config.topics {

            let topic_name =
                format!(
                    "benchmark_run_{}_topic_{}",
                    run_no,
                    topic_id
                );

            clients[0]
                .insert_topic(
                    topic_name,
                    config.partitions_per_topic,
                )
                .await?;
        }

        println!("All topics created.");

        // =====================================================
        // WARMUP
        //
        // Warmup is NOT measured.
        // =====================================================

        if config.warmup_records > 0 {

            println!("Starting warmup...");

            for client_index in 0..config.clients {

                for record in 0..config.warmup_records {

                    let topic_id =
                        record % config.topics;

                    let topic =
                        format!(
                            "benchmark_run_{}_topic_{}",
                            run_no,
                            topic_id
                        );

                    let value =
                        "x".repeat(config.record_size);

                    clients[client_index]
                        .send_topic_data(
                            topic,
                            None,
                            value,
                        )
                        .await?;
                }
            }

            println!("Warmup completed.");
        }

        // =====================================================
        // MEASURED BENCHMARK
        // =====================================================

        println!("Starting measured benchmark...");

        let start =
            Instant::now();

        let mut latencies =
            Vec::with_capacity(total_records);

        let mut errors =
            0usize;

        for client_index in 0..config.clients {

            for record in 0..config.total_records {

                let topic_id =
                    record % config.topics;

                let topic =
                    format!(
                        "benchmark_run_{}_topic_{}",
                        run_no,
                        topic_id
                    );

                let value =
                    "x".repeat(config.record_size);

                let request_start =
                    Instant::now();

                match clients[client_index]
                    .send_topic_data(
                        topic,
                        None,
                        value,
                    )
                    .await
                {
                    Ok(_) => {

                        latencies.push(
                            request_start
                                .elapsed()
                                .as_secs_f64()
                                * 1000.0
                        );
                    }

                    Err(_) => {
                        errors += 1;
                    }
                }
            }
        }

        let elapsed =
            start.elapsed();

        // =====================================================
        // METRICS
        // =====================================================

        latencies.sort_by(|a, b| {
            a.partial_cmp(b).unwrap()
        });

        let successful_operations =
            latencies.len();

        let elapsed_seconds =
            elapsed.as_secs_f64();

        let records_per_sec =
            if elapsed_seconds > 0.0 {
                successful_operations as f64
                    / elapsed_seconds
            } else {
                0.0
            };

        let total_bytes =
            successful_operations
                * config.record_size;

        let mb_per_sec =
            if elapsed_seconds > 0.0 {
                total_bytes as f64
                    / (1024.0 * 1024.0)
                    / elapsed_seconds
            } else {
                0.0
            };

        let avg_latency_ms =
            if successful_operations > 0 {
                latencies.iter().sum::<f64>()
                    / successful_operations as f64
            } else {
                0.0
            };

        let p50_latency_ms =
            percentile(&latencies, 50.0);

        let p95_latency_ms =
            percentile(&latencies, 95.0);

        let p99_latency_ms =
            percentile(&latencies, 99.0);

        let max_latency_ms =
            latencies
                .last()
                .copied()
                .unwrap_or(0.0);

        let result =
            RunResult {
                run_no,

                total_operations:
                    total_records,

                successful_operations,
                errors,

                elapsed_ms:
                    elapsed_seconds * 1000.0,

                records_per_sec,
                mb_per_sec,

                avg_latency_ms,
                p50_latency_ms,
                p95_latency_ms,
                p99_latency_ms,
                max_latency_ms,
            };

        // =====================================================
        // RUN RESULT
        // =====================================================

        println!();
        println!("Results");
        println!("------------------------------------------------------------");

        println!(
            "Records sent:                    {}",
            result.successful_operations
        );

        println!(
            "Total time:                      {:.3} ms",
            result.elapsed_ms
        );

        println!(
            "Throughput:                      {:.2} records/sec",
            result.records_per_sec
        );

        println!(
            "Throughput:                      {:.2} MB/sec",
            result.mb_per_sec
        );

        println!();
        println!("Latency");
        println!("------------------------------------------------------------");

        println!(
            "Average:                         {:.3} ms",
            result.avg_latency_ms
        );

        println!(
            "p50:                             {:.3} ms",
            result.p50_latency_ms
        );

        println!(
            "p95:                             {:.3} ms",
            result.p95_latency_ms
        );

        println!(
            "p99:                             {:.3} ms",
            result.p99_latency_ms
        );

        println!(
            "Max:                             {:.3} ms",
            result.max_latency_ms
        );

        println!();
        println!("Errors");
        println!("------------------------------------------------------------");

        println!(
            "Errors:                          {}",
            result.errors
        );

        println!("============================================================");

        results.push(result);
    }

    // =========================================================
    // FINAL RESULTS
    // =========================================================

    println!();
    println!("============================================================");
    println!("                    FINAL RESULTS");
    println!("============================================================");

    for result in &results {

        println!();
        println!("RUN {}", result.run_no);
        println!("------------------------------------------------------------");

        println!(
            "Records/sec:  {:.2}",
            result.records_per_sec
        );

        println!(
            "MB/sec:       {:.2}",
            result.mb_per_sec
        );

        println!(
            "Avg latency:  {:.3} ms",
            result.avg_latency_ms
        );

        println!(
            "p50:          {:.3} ms",
            result.p50_latency_ms
        );

        println!(
            "p95:          {:.3} ms",
            result.p95_latency_ms
        );

        println!(
            "p99:          {:.3} ms",
            result.p99_latency_ms
        );

        println!(
            "Max:          {:.3} ms",
            result.max_latency_ms
        );

        println!(
            "Errors:       {}",
            result.errors
        );
    }

    // =========================================================
    // AVERAGES
    // =========================================================

    if !results.is_empty() {

        let count =
            results.len() as f64;

        let average_throughput =
            results
                .iter()
                .map(|r| r.records_per_sec)
                .sum::<f64>()
                / count;

        let average_mb_per_sec =
            results
                .iter()
                .map(|r| r.mb_per_sec)
                .sum::<f64>()
                / count;

        let average_latency =
            results
                .iter()
                .map(|r| r.avg_latency_ms)
                .sum::<f64>()
                / count;

        let average_p50 =
            results
                .iter()
                .map(|r| r.p50_latency_ms)
                .sum::<f64>()
                / count;

        let average_p95 =
            results
                .iter()
                .map(|r| r.p95_latency_ms)
                .sum::<f64>()
                / count;

        let average_p99 =
            results
                .iter()
                .map(|r| r.p99_latency_ms)
                .sum::<f64>()
                / count;

        let total_errors =
            results
                .iter()
                .map(|r| r.errors)
                .sum::<usize>();

        println!();
        println!("============================================================");
        println!("                    AVERAGE RESULTS");
        println!("============================================================");

        println!(
            "Throughput:      {:.2} records/sec",
            average_throughput
        );

        println!(
            "Throughput:      {:.2} MB/sec",
            average_mb_per_sec
        );

        println!(
            "Average latency: {:.3} ms",
            average_latency
        );

        println!(
            "p50 latency:     {:.3} ms",
            average_p50
        );

        println!(
            "p95 latency:     {:.3} ms",
            average_p95
        );

        println!(
            "p99 latency:     {:.3} ms",
            average_p99
        );

        println!(
            "Total errors:    {}",
            total_errors
        );

        println!("============================================================");
    }

    Ok(())
}