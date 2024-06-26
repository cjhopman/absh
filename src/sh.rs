use std::process::Child;
use std::process::Command;
use std::process::Output;
use std::process::Stdio;

#[cfg(not(windows))]
pub fn spawn_sh(script: &str) -> anyhow::Result<Child> {
    Ok(Command::new("/bin/sh")
        .args(&["-ec", &script])
        .stdin(Stdio::null())
        .spawn()?)
}

#[cfg(windows)]
pub fn spawn_sh(script: &str) -> anyhow::Result<Child> {
    Ok(Command::new("powershell.exe")
        .args(&["-Command", &script])
        .stdin(Stdio::null())
        .spawn()?)
}

#[cfg(not(windows))]
pub fn run_sh(script: &str) -> anyhow::Result<Output> {
    Ok(Command::new("/bin/sh")
        .args(&["-ec", &script])
        .stdin(Stdio::null())
        .output()?)
}

#[cfg(windows)]
pub fn run_sh(script: &str) -> anyhow::Result<Output> {
    Ok(Command::new("powershell.exe")
        .args(&["-Command", &script])
        .stdin(Stdio::null())
        .output()?)
}
