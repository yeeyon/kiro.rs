//! Performance benchmarks for the perf-optimizations branch.
//!
//! Run with:  cargo bench --release
//!
//! Or if criterion is unavailable: cargo test --release --bench perf_bench -- --nocapture

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};

// ── estimate_tokens micro-bench ──────────────────────────────────────────────
//
// We can't import the private `estimate_tokens` directly, so we replicate
// both the old (Vec<char>) and new (direct iteration) implementations here
// and compare them.  This isolates the allocation overhead.

fn estimate_tokens_old(text: &str) -> i32 {
    let chars: Vec<char> = text.chars().collect();
    let mut chinese_count = 0;
    let mut other_count = 0;
    for c in &chars {
        if *c >= '\u{4E00}' && *c <= '\u{9FFF}' {
            chinese_count += 1;
        } else {
            other_count += 1;
        }
    }
    let chinese_tokens = (chinese_count * 2 + 2) / 3;
    let other_tokens = (other_count + 3) / 4;
    (chinese_tokens + other_tokens).max(1)
}

fn estimate_tokens_new(text: &str) -> i32 {
    let mut chinese_count = 0;
    let mut other_count = 0;
    for c in text.chars() {
        if c >= '\u{4E00}' && c <= '\u{9FFF}' {
            chinese_count += 1;
        } else {
            other_count += 1;
        }
    }
    let chinese_tokens = (chinese_count * 2 + 2) / 3;
    let other_tokens = (other_count + 3) / 4;
    (chinese_tokens + other_tokens).max(1)
}

// ── to_sse_string micro-bench ────────────────────────────────────────────────
//
// Replicate old (format!()) and new (pre-allocated) patterns.

fn to_sse_string_old(event: &str, data: &serde_json::Value) -> String {
    format!(
        "event: {}\ndata: {}\n\n",
        event,
        serde_json::to_string(data).unwrap_or_default()
    )
}

fn to_sse_string_new(event: &str, data: &serde_json::Value) -> String {
    let json = serde_json::to_string(data).unwrap_or_default();
    let mut s = String::with_capacity(event.len() + json.len() + 16);
    s.push_str("event: ");
    s.push_str(event);
    s.push_str("\ndata: ");
    s.push_str(&json);
    s.push_str("\n\n");
    s
}

// ── Test data ────────────────────────────────────────────────────────────────

fn make_text_delta(text: &str) -> serde_json::Value {
    serde_json::json!({
        "type": "content_block_delta",
        "index": 0,
        "delta": {
            "type": "text_delta",
            "text": text
        }
    })
}

const SHORT_TEXT: &str = "Hello, world!";
const MEDIUM_TEXT: &str = "The quick brown fox jumps over the lazy dog. This is a medium-sized chunk of text that might appear in a typical streaming response.";
const CHINESE_TEXT: &str = "这是一段中文文本，用于测试中文字符的 token 估算性能。这段文本包含足够多的字符来产生有意义的基准测试结果。";

fn bench_estimate_tokens(c: &mut Criterion) {
    let mut group = c.benchmark_group("estimate_tokens");

    for (label, text) in [
        ("short_ascii", SHORT_TEXT),
        ("medium_ascii", MEDIUM_TEXT),
        ("chinese", CHINESE_TEXT),
    ] {
        group.bench_with_input(BenchmarkId::new("old_vec_alloc", label), text, |b, t| {
            b.iter(|| black_box(estimate_tokens_old(black_box(t))))
        });
        group.bench_with_input(BenchmarkId::new("new_direct_iter", label), text, |b, t| {
            b.iter(|| black_box(estimate_tokens_new(black_box(t))))
        });
    }
    group.finish();
}

fn bench_to_sse_string(c: &mut Criterion) {
    let mut group = c.benchmark_group("to_sse_string");

    let data_short = make_text_delta(SHORT_TEXT);
    let data_medium = make_text_delta(MEDIUM_TEXT);

    for (label, data) in [("short", &data_short), ("medium", &data_medium)] {
        group.bench_with_input(BenchmarkId::new("old_format", label), data, |b, d| {
            b.iter(|| black_box(to_sse_string_old(black_box("content_block_delta"), black_box(d))))
        });
        group.bench_with_input(BenchmarkId::new("new_prealloc", label), data, |b, d| {
            b.iter(|| black_box(to_sse_string_new(black_box("content_block_delta"), black_box(d))))
        });
    }
    group.finish();
}

// ── HTTP connection reuse bench ──────────────────────────────────────────────
//
// This is the big one: measures the difference between Connection: close
// (old behavior) and keep-alive (new behavior) over a local mock server.

fn bench_connection_reuse(c: &mut Criterion) {
    // Spin up a minimal HTTP server that always returns 200 OK.
    let rt = tokio::runtime::Runtime::new().unwrap();
    let addr = rt.block_on(async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((mut stream, _)) => {
                        tokio::spawn(async move {
                            use tokio::io::{AsyncReadExt, AsyncWriteExt};
                            let mut buf = [0u8; 4096];
                            // Support HTTP keep-alive: loop reading requests
                            // on the same connection until the client closes.
                            loop {
                                match stream.read(&mut buf).await {
                                    Ok(0) => break, // client closed
                                    Ok(_) => {
                                        let resp = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
                                        let _ = stream.write_all(resp).await;
                                        let _ = stream.flush().await;
                                    }
                                    Err(_) => break,
                                }
                            }
                        });
                    }
                    Err(_) => break,
                }
            }
        });

        addr
    });

    let url = format!("http://{}/", addr);

    // Client WITH keep-alive (new behavior - no Connection: close header)
    let client_keepalive = rt.block_on(async {
        reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .connect_timeout(std::time::Duration::from_secs(5))
            .pool_idle_timeout(std::time::Duration::from_secs(300))
            .pool_max_idle_per_host(64)
            .tcp_keepalive(std::time::Duration::from_secs(60))
            .build()
            .unwrap()
    });

    // Client WITHOUT keep-alive (simulates old behavior - Connection: close)
    // We can't set Connection: close per-request in this bench easily,
    // so we disable pooling entirely to simulate the effect.
    let client_no_pool = rt.block_on(async {
        reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .connect_timeout(std::time::Duration::from_secs(5))
            .pool_idle_timeout(std::time::Duration::from_secs(0))
            .pool_max_idle_per_host(0)
            .build()
            .unwrap()
    });

    let mut group = c.benchmark_group("http_connection_reuse");
    group.sample_size(50); // fewer samples since each makes N requests

    for n in [10, 50, 100] {
        group.bench_with_input(BenchmarkId::new("no_pool_old_behavior", n), &n, |b, &n| {
            b.iter(|| {
                rt.block_on(async {
                    for _ in 0..n {
                        let _ = client_no_pool
                            .get(&url)
                            .header("Connection", "close")
                            .send()
                            .await
                            .unwrap();
                    }
                })
            })
        });

        group.bench_with_input(BenchmarkId::new("keepalive_new_behavior", n), &n, |b, &n| {
            b.iter(|| {
                rt.block_on(async {
                    for _ in 0..n {
                        let _ = client_keepalive.get(&url).send().await.unwrap();
                    }
                })
            })
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_estimate_tokens,
    bench_to_sse_string,
    bench_connection_reuse,
);
criterion_main!(benches);
