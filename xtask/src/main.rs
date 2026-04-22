use std::env;
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{self, Child, Command};
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use clap::{Args, Parser, Subcommand};

const DEFAULT_ADDR: &str = "127.0.0.1:4433";
const DEFAULT_LOG_DIR: &str = "./logs/network";

#[derive(Debug, Parser)]
#[command(name = "xtask")]
struct Cli {
    #[command(subcommand)]
    command: CommandKind,
}

#[derive(Debug, Subcommand)]
enum CommandKind {
    Server(RunServerArgs),
    Client(RunClientArgs),
    Play(PlayArgs),
    Bots(RunBotsArgs),
    Flame(FlameArgs),
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum BuildProfile {
    Dev,
    Release,
}

impl BuildProfile {
    fn cargo_args(self) -> &'static [&'static str] {
        match self {
            Self::Dev => &[],
            Self::Release => &["--release"],
        }
    }

    fn target_dir_name(self) -> &'static str {
        match self {
            Self::Dev => "debug",
            Self::Release => "release",
        }
    }
}

#[derive(Debug, Args, Clone, Copy)]
struct ProfileArgs {
    #[arg(long, conflicts_with = "dev")]
    release: bool,
    #[arg(long, hide = true)]
    dev: bool,
}

impl ProfileArgs {
    fn profile(self) -> BuildProfile {
        if self.release { BuildProfile::Release } else { BuildProfile::Dev }
    }
}

#[derive(Debug, Args)]
struct NetTraceArgs {
    #[arg(long, num_args = 0..=1, default_missing_value = DEFAULT_LOG_DIR)]
    log: Option<String>,
}

impl NetTraceArgs {
    fn apply(&self, command: &mut Command) -> Result<()> {
        if let Some(log_dir) = &self.log {
            fs::create_dir_all(log_dir)
                .with_context(|| format!("failed to create log directory {}", log_dir))?;
            command.env("WIDEV_NET_TRACE", "1");
            command.env("WIDEV_NET_TRACE_DIR", log_dir);
            command.env(
                "WIDEV_NET_TRACE_FLUSH",
                env::var("WIDEV_NET_TRACE_FLUSH").unwrap_or_else(|_| "flow".to_string()),
            );
            command.env(
                "WIDEV_NET_TRACE_CONSOLE",
                env::var("WIDEV_NET_TRACE_CONSOLE").unwrap_or_else(|_| "1".to_string()),
            );
            eprintln!("Network logging enabled: {}", log_dir);
        }

        Ok(())
    }
}

#[derive(Debug, Args)]
struct RunServerArgs {
    #[command(flatten)]
    profile: ProfileArgs,
    #[command(flatten)]
    net_trace: NetTraceArgs,
    #[arg(long = "addr", alias = "bind", default_value = DEFAULT_ADDR)]
    addr: String,
    #[arg(long)]
    game: Option<String>,
}

#[derive(Debug, Args)]
struct RunClientArgs {
    #[command(flatten)]
    profile: ProfileArgs,
    #[arg(long = "addr", default_value = DEFAULT_ADDR)]
    addr: String,
}

#[derive(Debug, Args)]
struct PlayArgs {
    #[command(flatten)]
    profile: ProfileArgs,
    #[command(flatten)]
    net_trace: NetTraceArgs,
    #[arg(long = "addr", alias = "bind", default_value = DEFAULT_ADDR)]
    addr: String,
    #[arg(long)]
    game: Option<String>,
}

#[derive(Debug, Args)]
struct RunBotsArgs {
    #[command(flatten)]
    profile: ProfileArgs,
    #[command(flatten)]
    net_trace: NetTraceArgs,
    #[arg(long = "addr", default_value = DEFAULT_ADDR)]
    addr: String,
    #[arg(long, default_value_t = 600)]
    count: usize,
    #[arg(long, default_value = "ack-move")]
    flow: String,
}

#[derive(Debug, Args)]
struct FlameArgs {
    #[arg(long)]
    pid: Option<u32>,
    #[arg(long)]
    name: Option<String>,
    #[arg(long, default_value_t = 30)]
    duration: u64,
    #[arg(long, default_value_t = 199)]
    frequency: u32,
    #[arg(long, default_value = "server-flame.svg")]
    output: PathBuf,
    #[arg(long)]
    sudo: bool,
}

fn main() {
    if let Err(err) = try_main() {
        eprintln!("{err:#}");
        process::exit(1);
    }
}

fn try_main() -> Result<()> {
    let cli = Cli::parse();
    let root = workspace_root()?;

    match cli.command {
        CommandKind::Server(args) => run_server(&root, args),
        CommandKind::Client(args) => run_client(&root, args),
        CommandKind::Play(args) => run_play(&root, args),
        CommandKind::Bots(args) => run_bots(&root, args),
        CommandKind::Flame(args) => run_flame(args),
    }
}

fn run_server(root: &Path, args: RunServerArgs) -> Result<()> {
    exec_workspace_binary(root, "widev-server", "widev-server", args.profile.profile(), |command| {
        args.net_trace.apply(command)?;
        command.arg(&args.addr);
        if let Some(game) = &args.game {
            command.arg(game);
        }
        Ok(())
    })
}

fn run_client(root: &Path, args: RunClientArgs) -> Result<()> {
    exec_workspace_binary(
        root,
        "widev-desktop-client",
        "widev-desktop-client",
        args.profile.profile(),
        |command| {
            command.arg(&args.addr);
            Ok(())
        },
    )
}

fn run_bots(root: &Path, args: RunBotsArgs) -> Result<()> {
    exec_workspace_binary(
        root,
        "widev-desktop-bots",
        "widev-desktop-bots",
        args.profile.profile(),
        |command| {
            args.net_trace.apply(command)?;
            command
                .arg(&args.addr)
                .arg("--bots")
                .arg(args.count.to_string())
                .arg("--flow")
                .arg(&args.flow);
            Ok(())
        },
    )
}

fn run_play(root: &Path, args: PlayArgs) -> Result<()> {
    let profile = args.profile.profile();
    build_packages(root, &["widev-server", "widev-desktop-client"], profile)?;

    let mut server = workspace_binary_command(root, "widev-server", profile);
    args.net_trace.apply(&mut server)?;
    server.arg(&args.addr);
    if let Some(game) = &args.game {
        server.arg(game);
    }

    let mut server_child = server.spawn().context("failed to start widev-server")?;
    let server_pid = server_child.id();
    let guard = ChildGuard::new(&mut server_child);

    thread::sleep(Duration::from_secs(1));
    eprintln!("Server started on {} (pid {}); launching client", args.addr, server_pid);

    let mut client = workspace_binary_command(root, "widev-desktop-client", profile);
    client.arg(&args.addr);
    let status = client.status().context("failed to start widev-desktop-client")?;

    drop(guard);

    if status.success() {
        Ok(())
    } else {
        bail!("widev-desktop-client exited with status {}", status);
    }
}

fn run_flame(args: FlameArgs) -> Result<()> {
    let pid = match (args.pid, args.name.as_deref()) {
        (Some(_), Some(_)) => bail!("use only one of --pid or --name"),
        (Some(pid), None) => pid,
        (None, Some(name)) => resolve_pid_by_name(name)?,
        (None, None) => resolve_pid_by_name("widev-server")?,
    };

    let output_parent = args.output.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(output_parent)
        .with_context(|| format!("failed to create {}", output_parent.display()))?;

    match env::consts::OS {
        "macos" => run_macos_flame(pid, &args),
        "linux" => run_linux_flame(pid, &args),
        other => bail!("unsupported OS: {}", other),
    }
}

fn run_linux_flame(pid: u32, args: &FlameArgs) -> Result<()> {
    require_tool("perf", "missing 'perf'")?;
    require_tool(
        "inferno-collapse-perf",
        "missing 'inferno-collapse-perf' (install: cargo install inferno)",
    )?;
    require_tool(
        "inferno-flamegraph",
        "missing 'inferno-flamegraph' (install: cargo install inferno)",
    )?;

    let tmpdir = temp_dir("widev-flame")?;
    let perf_data = tmpdir.join("perf.data");
    let perf_script = tmpdir.join("perf.script");
    let folded = tmpdir.join("folded.txt");

    eprintln!("[linux] recording PID={} for {}s at {}Hz", pid, args.duration, args.frequency);

    run_optional_sudo(
        args.sudo,
        "perf",
        &[
            OsString::from("record"),
            OsString::from("-F"),
            OsString::from(args.frequency.to_string()),
            OsString::from("-g"),
            OsString::from("-p"),
            OsString::from(pid.to_string()),
            OsString::from("-o"),
            perf_data.as_os_str().to_owned(),
            OsString::from("--"),
            OsString::from("sleep"),
            OsString::from(args.duration.to_string()),
        ],
    )?;

    let mut script_command = optional_sudo_command(args.sudo, "perf");
    script_command
        .arg("script")
        .arg("-i")
        .arg(&perf_data)
        .stdout(fs::File::create(&perf_script).context("failed to create perf script output")?);
    run_status(&mut script_command, "perf script failed")?;

    let mut collapse = Command::new("inferno-collapse-perf");
    collapse
        .arg(&perf_script)
        .stdout(fs::File::create(&folded).context("failed to create folded output")?);
    run_status(&mut collapse, "inferno-collapse-perf failed")?;

    let mut flamegraph = Command::new("inferno-flamegraph");
    flamegraph
        .arg(&folded)
        .stdout(fs::File::create(&args.output).context("failed to create output svg")?);
    run_status(&mut flamegraph, "inferno-flamegraph failed")?;

    eprintln!("Flamegraph written to {}", args.output.display());
    Ok(())
}

fn run_macos_flame(pid: u32, args: &FlameArgs) -> Result<()> {
    require_tool("sample", "missing 'sample' (part of macOS developer tools)")?;
    let stackcollapse = find_mac_tool("stackcollapse-sample.awk")?.ok_or_else(|| {
        anyhow!("missing stackcollapse-sample.awk (install FlameGraph scripts, e.g. 'brew install flamegraph')")
    })?;
    let flamegraph = find_mac_tool("flamegraph.pl")?.ok_or_else(|| {
        anyhow!(
            "missing flamegraph.pl (install FlameGraph scripts, e.g. 'brew install flamegraph')"
        )
    })?;

    let tmpdir = temp_dir("widev-flame")?;
    let sample_out = tmpdir.join("sample.txt");
    let folded = tmpdir.join("folded.txt");

    let mut interval_ms = 1000 / args.frequency.max(1);
    if interval_ms == 0 {
        interval_ms = 1;
    }

    eprintln!("[macos] sampling PID={} for {}s every {}ms", pid, args.duration, interval_ms);

    run_optional_sudo(
        args.sudo,
        "sample",
        &[
            OsString::from(pid.to_string()),
            OsString::from(args.duration.to_string()),
            OsString::from(interval_ms.to_string()),
            OsString::from("-file"),
            sample_out.as_os_str().to_owned(),
        ],
    )?;

    let mut collapse = Command::new(&stackcollapse);
    collapse
        .arg(&sample_out)
        .stdout(fs::File::create(&folded).context("failed to create folded output")?);
    run_status(&mut collapse, "stackcollapse-sample.awk failed")?;

    let mut flame = Command::new(&flamegraph);
    flame
        .arg(&folded)
        .stdout(fs::File::create(&args.output).context("failed to create output svg")?);
    run_status(&mut flame, "flamegraph.pl failed")?;

    eprintln!("Flamegraph written to {}", args.output.display());
    Ok(())
}

fn resolve_pid_by_name(pattern: &str) -> Result<u32> {
    let output = Command::new("pgrep")
        .arg("-f")
        .arg(pattern)
        .output()
        .with_context(|| format!("failed to run pgrep for pattern '{}'", pattern))?;

    if !output.status.success() {
        bail!("no process matched --name '{}'", pattern);
    }

    let stdout = String::from_utf8(output.stdout).context("pgrep output was not valid utf-8")?;
    let mut matches = stdout
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            line.parse::<u32>().with_context(|| format!("invalid PID from pgrep: '{}'", line))
        })
        .collect::<Result<Vec<_>>>()?;

    let pid = matches
        .drain(..1)
        .next()
        .ok_or_else(|| anyhow!("no process matched --name '{}'", pattern))?;

    if !matches.is_empty() {
        let all = std::iter::once(pid)
            .chain(matches)
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(" ");
        eprintln!("Multiple PIDs matched; using first: {} (all: {})", pid, all);
    }

    Ok(pid)
}

fn workspace_root() -> Result<PathBuf> {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map(Path::to_path_buf)
        .ok_or_else(|| anyhow!("failed to determine workspace root"))
}

fn exec_workspace_binary<F>(
    root: &Path,
    package: &str,
    binary: &str,
    profile: BuildProfile,
    configure: F,
) -> Result<()>
where
    F: FnOnce(&mut Command) -> Result<()>,
{
    build_packages(root, &[package], profile)?;
    let mut command = workspace_binary_command(root, binary, profile);
    configure(&mut command)?;
    let status = command.status().with_context(|| format!("failed to start {}", binary))?;
    process::exit(status.code().unwrap_or(1));
}

fn build_packages(root: &Path, packages: &[&str], profile: BuildProfile) -> Result<()> {
    for package in packages {
        let mut command = Command::new("cargo");
        command.current_dir(root).arg("build").arg("-p").arg(package);
        for arg in profile.cargo_args() {
            command.arg(arg);
        }
        run_status(&mut command, &format!("cargo build failed for package {}", package))?;
    }
    Ok(())
}

fn workspace_binary_command(root: &Path, binary: &str, profile: BuildProfile) -> Command {
    let mut command =
        Command::new(root.join("target").join(profile.target_dir_name()).join(binary));
    command.current_dir(root);
    command
}

fn require_tool(tool: &str, error: &str) -> Result<()> {
    which(tool).map(|_| ()).ok_or_else(|| anyhow!(error.to_string()))
}

fn which(tool: &str) -> Option<PathBuf> {
    env::var_os("PATH").and_then(|path| {
        env::split_paths(&path).map(|entry| entry.join(tool)).find(|candidate| candidate.is_file())
    })
}

fn find_mac_tool(tool: &str) -> Result<Option<PathBuf>> {
    if let Some(path) = which(tool) {
        return Ok(Some(path));
    }

    for candidate in [
        format!("/opt/homebrew/opt/flamegraph/bin/{tool}"),
        format!("/usr/local/opt/flamegraph/bin/{tool}"),
        format!("/opt/homebrew/bin/{tool}"),
        format!("/usr/local/bin/{tool}"),
    ] {
        let path = PathBuf::from(candidate);
        if path.is_file() {
            return Ok(Some(path));
        }
    }

    Ok(None)
}

fn temp_dir(prefix: &str) -> Result<PathBuf> {
    let mut dir = env::temp_dir();
    dir.push(format!("{}-{}", prefix, process::id()));
    dir.push(unique_suffix());
    fs::create_dir_all(&dir).with_context(|| format!("failed to create {}", dir.display()))?;
    Ok(dir)
}

fn unique_suffix() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    nanos.to_string()
}

fn optional_sudo_command(use_sudo: bool, program: &str) -> Command {
    if use_sudo {
        let mut command = Command::new("sudo");
        command.arg(program);
        command
    } else {
        Command::new(program)
    }
}

fn run_optional_sudo(use_sudo: bool, program: &str, args: &[OsString]) -> Result<()> {
    let mut command = optional_sudo_command(use_sudo, program);
    command.args(args);
    run_status(&mut command, &format!("{} failed", program))
}

fn run_status(command: &mut Command, error: &str) -> Result<()> {
    let status = command.status().context(error.to_string())?;
    if status.success() { Ok(()) } else { bail!("{}: {}", error, status) }
}

fn stop_child(child: &mut Child) {
    if let Ok(None) = child.try_wait() {
        let _ = child.kill();
    }
    let _ = child.wait();
}

struct ChildGuard<'a> {
    child: &'a mut Child,
}

impl<'a> ChildGuard<'a> {
    fn new(child: &'a mut Child) -> Self {
        Self { child }
    }
}

impl Drop for ChildGuard<'_> {
    fn drop(&mut self) {
        stop_child(self.child);
    }
}
