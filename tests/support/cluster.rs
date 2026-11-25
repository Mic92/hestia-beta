//! A sharded store as it is deployed, on one machine: `hestia serve
//! --sharded` processes as cache nodes, a real Envoy in front of them
//! (`tests/fixtures/envoy.yaml`), and the fake bucket behind them.
//!
//! Nodes can be killed, stopped, started again and taken in and out of
//! Envoy's node list while builders drain and readers fetch through the
//! proxy.

use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use hestia::node::NodeClient;
use hestia::pipeline::PipelineContext;

use super::common::pipeline_context_with;
use super::fake_s3::{ACCESS_KEY, FakeS3, REGION, SECRET_KEY};
use super::store::ScratchStore;

const ENVOY_CONFIG: &str = include_str!("../fixtures/envoy.yaml");

fn which(name: &str) -> Option<PathBuf> {
    std::env::split_paths(&std::env::var_os("PATH")?)
        .map(|d| d.join(name))
        .find(|c| c.is_file())
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// One `hestia serve --sharded` process. It keeps its port and its pack
/// cache directory over restarts.
pub struct Node {
    child: Option<Child>,
    pub port: u16,
    dir: PathBuf,
    starts: u32,
}

impl Node {
    pub fn url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    pub fn running(&self) -> bool {
        self.child.is_some()
    }

    /// What the node wrote to stderr, all starts.
    pub fn log(&self) -> String {
        (0..self.starts)
            .filter_map(|n| std::fs::read_to_string(self.dir.join(format!("log-{n}"))).ok())
            .collect()
    }
}

pub struct Cluster {
    pub s3: FakeS3,
    pub nodes: Vec<Node>,
    /// Indexes into `nodes` of those Envoy sends to.
    pub routed: Vec<usize>,
    envoy: Child,
    /// The proxy: what builders and readers talk to.
    pub url: String,
    db_path: PathBuf,
    follow: Option<u64>,
    dir: tempfile::TempDir,
    pub http: reqwest::Client,
}

impl Drop for Cluster {
    fn drop(&mut self) {
        let _ = self.envoy.kill();
        let _ = self.envoy.wait();
        for node in &mut self.nodes {
            if let Some(mut child) = node.child.take() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }
}

impl Cluster {
    /// `nodes` cache nodes behind Envoy. `follow`: how often a node
    /// reloads the shards it holds, in seconds; `None` for never.
    /// `None` when Envoy is not on PATH.
    pub async fn start(nodes: usize, follow: Option<u64>, store: &ScratchStore) -> Option<Self> {
        let envoy_bin = which("envoy")?;
        let dir = tempfile::tempdir().unwrap();
        let port = free_port();
        let config = dir.path().join("envoy.yaml");
        std::fs::write(
            &config,
            ENVOY_CONFIG
                .replace("@PORT@", &port.to_string())
                .replace("@DIR@", &dir.path().display().to_string()),
        )
        .unwrap();
        std::fs::write(dir.path().join("eds.yaml"), eds(&[])).unwrap();
        let log = std::fs::File::create(dir.path().join("envoy.log")).unwrap();
        let envoy = Command::new(envoy_bin)
            .arg("--config-path")
            .arg(&config)
            // Several tests run an Envoy each.
            .args(["--disable-hot-restart", "--log-level", "warn"])
            .stdout(Stdio::null())
            .stderr(log)
            .spawn()
            .unwrap();
        let mut cluster = Cluster {
            s3: FakeS3::start().await,
            nodes: Vec::new(),
            routed: Vec::new(),
            envoy,
            url: format!("http://127.0.0.1:{port}"),
            db_path: store.db_path(),
            follow,
            dir,
            http: reqwest::Client::new(),
        };
        for _ in 0..nodes {
            cluster.add_node().await;
        }
        cluster.route((0..nodes).collect()).await;
        Some(cluster)
    }

    /// Start one more node. Envoy does not send to it before
    /// [`Self::route`] names it.
    pub async fn add_node(&mut self) -> usize {
        let index = self.nodes.len();
        let dir = self.dir.path().join(format!("node-{index}"));
        std::fs::create_dir_all(&dir).unwrap();
        self.nodes.push(Node {
            child: None,
            port: free_port(),
            dir,
            starts: 0,
        });
        self.start_node(index).await;
        index
    }

    /// Start a node that is not running, with the pack cache it left.
    pub async fn start_node(&mut self, index: usize) {
        let node = &mut self.nodes[index];
        assert!(node.child.is_none(), "node {index} is running");
        let log = std::fs::File::create(node.dir.join(format!("log-{}", node.starts))).unwrap();
        node.starts += 1;
        let socket = node.dir.join("hook.sock");
        let _ = std::fs::remove_file(&socket);
        let mut command = Command::new(env!("CARGO_BIN_EXE_hestia"));
        command
            .args(["serve", "--sharded", "--commit-interval", "20"])
            .args(["--branch", "main", "--system", "test-system"])
            .args(["--listen", &format!("127.0.0.1:{}", node.port)])
            .arg("--socket")
            .arg(&socket)
            .arg("--db-path")
            .arg(&self.db_path)
            .arg("--pack-cache")
            .arg(node.dir.join("packs"))
            .env("HESTIA_S3", self.s3.url())
            .env("HESTIA_S3_ENDPOINT", self.s3.endpoint())
            .env("AWS_REGION", REGION)
            .env("AWS_ACCESS_KEY_ID", ACCESS_KEY)
            .env("AWS_SECRET_ACCESS_KEY", SECRET_KEY)
            .env_remove("ACTIONS_RESULTS_URL")
            .stdout(Stdio::null())
            .stderr(log);
        if let Some(seconds) = self.follow {
            command.args(["--follow", &seconds.to_string()]);
        }
        node.child = Some(command.spawn().expect("spawning hestia serve failed"));
        let url = format!("{}/nix-cache-info", node.url());
        for _ in 0..300 {
            if let Some(status) = node.child.as_mut().unwrap().try_wait().unwrap() {
                panic!("node {index} exited early with {status}:\n{}", node.log());
            }
            if let Ok(answer) = self.http.get(&url).send().await
                && answer.status().is_success()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("node {index} did not come up:\n{}", node.log());
    }

    /// SIGKILL: the node gets no chance to upload what it holds.
    pub fn kill(&mut self, index: usize) {
        let mut child = self.nodes[index].child.take().expect("node is running");
        child.kill().unwrap();
        child.wait().unwrap();
    }

    /// SIGTERM, and wait until the node has exited by itself.
    pub async fn stop(&mut self, index: usize) -> std::process::ExitStatus {
        let mut child = self.nodes[index].child.take().expect("node is running");
        // SAFETY: the pid is our own child, not yet waited for.
        unsafe { libc::kill(child.id() as i32, libc::SIGTERM) };
        for _ in 0..3000 {
            if let Some(status) = child.try_wait().unwrap() {
                return status;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let _ = child.kill();
        panic!(
            "node {index} did not exit within 30s of SIGTERM:\n{}",
            self.nodes[index].log()
        );
    }

    /// Tell Envoy which nodes to send to, and wait until it sends to
    /// one of them (or to none).
    pub async fn route(&mut self, nodes: Vec<usize>) {
        let ports: Vec<u16> = nodes.iter().map(|&n| self.nodes[n].port).collect();
        // Envoy watches for a rename onto the file.
        let next = self.dir.path().join("eds.yaml.next");
        std::fs::write(&next, eds(&ports)).unwrap();
        std::fs::rename(&next, self.dir.path().join("eds.yaml")).unwrap();
        self.routed = nodes;
        if self.routed.iter().any(|&n| self.nodes[n].running()) {
            self.wait_ready().await;
        }
    }

    /// Until a request through Envoy is answered by a node.
    pub async fn wait_ready(&mut self) {
        let url = format!("{}/nix-cache-info", self.url);
        for _ in 0..500 {
            if let Some(status) = self.envoy.try_wait().unwrap() {
                panic!("envoy exited with {status}:\n{}", self.envoy_log());
            }
            if let Ok(answer) = self.http.get(&url).send().await
                && answer.status().is_success()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("envoy does not answer:\n{}", self.envoy_log());
    }

    pub fn envoy_log(&self) -> String {
        std::fs::read_to_string(self.dir.path().join("envoy.log")).unwrap_or_default()
    }

    /// A builder that drains `store` through the proxy.
    pub fn builder(&self, store: &ScratchStore) -> PipelineContext {
        builder(self.s3.backend(), &self.url, store)
    }

    pub fn logs(&self) -> String {
        let mut out = String::new();
        for (index, node) in self.nodes.iter().enumerate() {
            out.push_str(&format!("--- node {index} ---\n{}", node.log()));
        }
        out
    }
}

/// A builder that drains `store` through the proxy at `url`.
pub fn builder(
    backend: hestia::backend::Backend,
    url: &str,
    store: &ScratchStore,
) -> PipelineContext {
    PipelineContext {
        sharded: true,
        expand_closure: false,
        node: Some(NodeClient::new(reqwest::Client::new(), url)),
        ..pipeline_context_with(backend, store.database())
    }
}

/// Envoy's node list for these ports.
fn eds(ports: &[u16]) -> String {
    let mut out = String::from(
        "resources:\n\
         - \"@type\": type.googleapis.com/envoy.config.endpoint.v3.ClusterLoadAssignment\n\
         \x20 cluster_name: hestia\n\
         \x20 endpoints:\n\
         \x20   - lb_endpoints:\n",
    );
    if ports.is_empty() {
        out.push_str("        []\n");
    }
    for port in ports {
        out.push_str(&format!(
            "        - endpoint: {{ address: {{ socket_address: {{ address: 127.0.0.1, port_value: {port} }} }} }}\n"
        ));
    }
    out
}

/// A small store path: one file of `size` bytes decided by `seed`.
pub fn fixture_data(seed: u64, size: usize) -> Vec<u8> {
    let mut data = Vec::with_capacity(size + 8);
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    while data.len() < size {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        data.extend_from_slice(&state.to_le_bytes());
    }
    data.truncate(size);
    data
}

/// Add `count` such paths to `store`, seeds `first..first + count`.
pub fn add_fixtures(
    store: &ScratchStore,
    scratch: &Path,
    first: u64,
    count: u64,
    size: usize,
) -> Vec<PathBuf> {
    let sources: Vec<PathBuf> = (first..first + count)
        .map(|seed| {
            let dir = scratch.join(format!("obj-{seed}"));
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("data"), fixture_data(seed, size)).unwrap();
            dir
        })
        .collect();
    // The command line has a length limit.
    sources
        .chunks(500)
        .flat_map(|chunk| store.add_paths(chunk))
        .collect()
}
