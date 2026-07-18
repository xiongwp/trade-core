//! SIGTERM graceful-shutdown integration test.
//!
//! Spawns the real `trade-core` (exchange_server) binary, lets it come up and
//! block awaiting an order-system connection, then sends SIGTERM. The signal
//! handler flips an atomic; a watcher thread clears `running` and nudges the
//! blocked `accept()` with a throwaway self-connection, so the process runs its
//! normal drain (`handle.shutdown()`) and exits 0 rather than being SIGKILLed.
//!
//! Unix only (uses `kill`). If this proves flaky in a given environment it can
//! be ignored; the shutdown path is also exercisable manually with `docker
//! stop` / `kill -TERM`.
#![cfg(unix)]

use std::net::TcpListener;
use std::process::Command;
use std::time::{Duration, Instant};

/// Grab an ephemeral port, then release it so the child can rebind it. A tiny
/// TOCTOU window exists but is harmless for a local test.
fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    listener.local_addr().unwrap().port()
}

#[test]
fn exchange_server_drains_on_sigterm() {
    let addr = format!("127.0.0.1:{}", free_port());
    let tmp = std::env::temp_dir().join(format!("tc-sigterm-{}", std::process::id()));
    std::fs::create_dir_all(&tmp).unwrap();

    // ADDR SHARDS STRATEGY JOURNAL_DIR POOL_MB BAND_BPS MD_ADDR
    let mut child = Command::new(env!("CARGO_BIN_EXE_trade-core"))
        .args([&addr, "1", "price-time", "none", "8", "1000", "none"])
        .current_dir(&tmp)
        .env("TC_LOG", "error")
        .spawn()
        .expect("spawn trade-core");

    // Can't probe with a TCP connect: this server serves exactly one
    // order-system connection, so a probe would be treated as that session and
    // shut the server down on its own — masking the signal path. Instead give
    // it a fixed head start to bind and reach its blocking `accept()`. Signal
    // handlers are installed on the first line of `main`, so even a SIGTERM
    // that lands before the watcher thread spawns is honoured (the watcher sees
    // the flag as soon as it starts).
    std::thread::sleep(Duration::from_secs(2));

    let status = Command::new("kill")
        .arg("-TERM")
        .arg(child.id().to_string())
        .status()
        .expect("send SIGTERM");
    assert!(status.success(), "kill -TERM failed");

    // Poll for a clean exit within a generous budget.
    let deadline = Instant::now() + Duration::from_secs(15);
    let exit = loop {
        match child.try_wait().expect("try_wait") {
            Some(status) => break Some(status),
            None if Instant::now() >= deadline => break None,
            None => std::thread::sleep(Duration::from_millis(50)),
        }
    };

    let cleaned = exit.is_some();
    if !cleaned {
        let _ = child.kill();
        let _ = child.wait();
    }
    std::fs::remove_dir_all(&tmp).ok();

    let status = exit.expect("process did not exit after SIGTERM (no graceful shutdown)");
    // Graceful drain returns from main normally -> exit code 0.
    assert!(
        status.success(),
        "expected clean exit after SIGTERM, got {status:?}"
    );
}
