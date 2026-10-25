//! Runs a throw-away Tarantool for integration tests.
#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use tarn_common::Value;
use tarn_iproto::{mp, Client, Opts};

pub const INIT_LUA: &str = include_str!("init.lua");

/// Path of the Tarantool binary (`$TARANTOOL` overrides the default).
pub fn tarantool_bin() -> String {
    std::env::var("TARANTOOL").unwrap_or_else(|_| "/opt/src/tarantool/build/src/tarantool".into())
}

pub struct Tarantool {
    child: Option<Child>,
    pub dir: PathBuf,
    pub addr: String,
}

impl Tarantool {
    pub fn start() -> Tarantool {
        Self::start_with(INIT_LUA, &[])
    }

    pub fn start_with(script: &str, env: &[(&str, &str)]) -> Tarantool {
        static SEQ: AtomicU32 = AtomicU32::new(0);
        let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().subsec_nanos();
        let dir = std::env::temp_dir().join(format!(
            "tarn-iproto-{}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed),
            nanos
        ));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("init.lua"), script).unwrap();
        let bin = tarantool_bin();
        let out = std::fs::File::create(dir.join("stdout.log")).unwrap();
        let err = out.try_clone().unwrap();
        let mut cmd = Command::new(&bin);
        cmd.arg("init.lua").current_dir(&dir).stdin(Stdio::null()).stdout(out).stderr(err);
        for (k, v) in env {
            cmd.env(k, v);
        }
        let mut child = cmd.spawn().unwrap_or_else(|e| panic!("cannot start {bin}: {e} (set $TARANTOOL)"));
        let listen = dir.join("listen.txt");
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Ok(addr) = std::fs::read_to_string(&listen) {
                return Tarantool { child: Some(child), dir, addr: addr.trim().to_string() };
            }
            if let Some(status) = child.try_wait().unwrap() {
                panic!("tarantool exited with {status}: {}", Self::logs_of(&dir));
            }
            if Instant::now() > deadline {
                let _ = child.kill();
                panic!("tarantool did not start: {}", Self::logs_of(&dir));
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn logs_of(dir: &Path) -> String {
        let a = std::fs::read_to_string(dir.join("stdout.log")).unwrap_or_default();
        let b = std::fs::read_to_string(dir.join("tarantool.log")).unwrap_or_default();
        format!("{a}\n{b}")
    }

    pub fn opts(&self) -> Opts {
        Opts {
            addr: self.addr.clone(),
            user: Some("tarn".into()),
            password: Some("tarn".into()),
            connect_timeout: Duration::from_secs(5),
        }
    }

    pub async fn client(&self) -> Client {
        Client::connect(&self.opts()).await.expect("connect")
    }

    /// SIGKILL the server and reap it.
    pub fn kill(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

impl Drop for Tarantool {
    fn drop(&mut self) {
        self.kill();
        if std::env::var_os("TARN_KEEP_TMP").is_none() {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
}

/// Encode values as a MessagePack array.
pub fn tuple(vals: &[Value]) -> Vec<u8> {
    let mut out = Vec::new();
    mp::encode_tuple(&mut out, vals);
    out
}

pub fn int(i: i64) -> Value {
    Value::Int(i)
}

pub fn s(x: &str) -> Value {
    Value::str(x)
}

/// Decode a raw MessagePack value that holds an array.
pub fn decode_array(raw: &[u8]) -> Vec<Value> {
    let mut p = raw;
    let v = mp::decode_tuple(&mut p).expect("array");
    assert!(p.is_empty());
    v
}
