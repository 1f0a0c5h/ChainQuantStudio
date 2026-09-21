use std::env;
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;
use std::time::Duration;
use tauri::Manager;

struct DesktopServices {
    project_root: PathBuf,
    children: Mutex<Vec<Child>>,
}

impl Drop for DesktopServices {
    fn drop(&mut self) {
        // The Signal runtime watches this file, drains its queues, sends Telegram
        // STOPPED, and then exits. The desktop shell never force-kills it directly.
        let shutdown = self
            .project_root
            .join(".runtime")
            .join("signal-runtime.shutdown");
        let _ = std::fs::create_dir_all(shutdown.parent().unwrap_or(&self.project_root));
        let _ = std::fs::File::create(shutdown);
        std::thread::sleep(Duration::from_secs(2));
        if let Ok(children) = self.children.get_mut() {
            for child in children.iter_mut() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }
}

fn port_is_open(port: u16) -> bool {
    let address = SocketAddr::from(([127, 0, 0, 1], port));
    TcpStream::connect_timeout(&address, Duration::from_millis(250)).is_ok()
}

fn project_root() -> PathBuf {
    if let Some(root) = env::var_os("CHAIN_STUDIO_ROOT") {
        return PathBuf::from(root);
    }
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("src-tauri must remain inside studio-dashboard")
        .to_path_buf()
}

fn hidden_command(program: &Path) -> Command {
    let mut command = Command::new(program);
    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    #[cfg(target_os = "windows")]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x08000000);
    }
    command
}

fn codex_home_from<F>(get: F) -> Option<PathBuf>
where
    F: Fn(&str) -> Option<std::ffi::OsString>,
{
    get("CODEX_HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .or_else(|| {
            get(if cfg!(target_os = "windows") {
                "USERPROFILE"
            } else {
                "HOME"
            })
            .filter(|value| !value.is_empty())
            .map(|profile| PathBuf::from(profile).join(".codex"))
        })
}

fn codex_home() -> Option<PathBuf> {
    codex_home_from(|key| env::var_os(key))
}

fn start_local_services(root: &Path) -> Result<Vec<Child>, String> {
    let mut children = Vec::new();
    if !port_is_open(8765) {
        let python = root.join(".venv").join("Scripts").join("python.exe");
        let mut gateway = hidden_command(&python);
        gateway
            .current_dir(root)
            .args(["-m", "quant_signal_agent.studio.gateway"]);
        if let Some(home) = codex_home() {
            gateway.env("CODEX_HOME", home);
        }
        children.push(
            gateway
                .spawn()
                .map_err(|error| format!("Gateway: {error}"))?,
        );
    }
    if !cfg!(debug_assertions) && !port_is_open(3000) {
        let dashboard = root.join("studio-dashboard");
        let pnpm = env::var_os("CHAIN_PNPM")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("pnpm.cmd"));
        let mut web = hidden_command(&pnpm);
        web.current_dir(dashboard).args(["run", "start"]);
        children.push(web.spawn().map_err(|error| format!("Dashboard: {error}"))?);
    }
    Ok(children)
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .setup(|app| {
            let root = project_root();
            let children = start_local_services(&root).map_err(std::io::Error::other)?;
            app.manage(DesktopServices {
                project_root: root,
                children: Mutex::new(children),
            });
            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("error while running CHA!N Quant Studio");
}

#[cfg(test)]
mod tests {
    use super::codex_home_from;
    use std::collections::HashMap;
    use std::ffi::OsString;
    use std::path::PathBuf;

    #[test]
    fn codex_home_honors_override() {
        let values = HashMap::from([
            ("CODEX_HOME", OsString::from("D:\\portable\\codex")),
            ("USERPROFILE", OsString::from("C:\\Users\\tester")),
        ]);
        assert_eq!(
            codex_home_from(|key| values.get(key).cloned()),
            Some(PathBuf::from("D:\\portable\\codex"))
        );
    }

    #[test]
    fn codex_home_uses_current_os_user_profile() {
        let profile_key = if cfg!(target_os = "windows") {
            "USERPROFILE"
        } else {
            "HOME"
        };
        let values = HashMap::from([(profile_key, OsString::from("device-user"))]);
        assert_eq!(
            codex_home_from(|key| values.get(key).cloned()),
            Some(PathBuf::from("device-user").join(".codex"))
        );
    }
}
