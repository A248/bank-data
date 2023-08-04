/*
 * bank-data
 * Copyright © 2023 Centre for Policy Dialogue
 *
 * bank-data is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * bank-data is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with bank-data. If not, see <https://www.gnu.org/licenses/>
 * and navigate to version 3 of the GNU General Public License.
 */

mod download;
mod merge;
mod http;
mod common;
mod parse;
mod analysis;

use std::{env};
use std::ffi::OsString;
use async_std::path::{Path, PathBuf};
use log::LevelFilter;
use simplelog::{ColorChoice, Config, TerminalMode, TermLogger};
use async_std::{fs, io, io::WriteExt, task};
use crate::download::Download;
use crate::merge::MergeXL;
use eyre::Result;

fn main() -> core::result::Result<(), eyre::Error> {

    if let Err(env::VarError::NotPresent) = env::var("RUST_BACKTRACE") {
        env::set_var("RUST_BACKTRACE", "1");
    }
    stable_eyre::install()?;

    TermLogger::init(
        LevelFilter::Info, Config::default(), TerminalMode::default(), ColorChoice::Auto
    )?;
    task::block_on(async_main())
}

struct Console {
    stdout: io::Stdout,
    stdin: io::Stdin
}

impl Console {
    fn new() -> Self {
        Self {
            stdout: io::stdout(),
            stdin: io::stdin()
        }
    }

    /// Prints a line to STDOUT
    async fn output(&mut self, line: &[u8]) -> Result<()> {
        self.stdout.write_all(line).await?;
        self.stdout.write_all(b"\n").await?;
        Ok(self.stdout.flush().await?)
    }

    /// Asks the user a question
    async fn input(&mut self, question: &[u8]) -> Result<String> {
        let mut answer = String::new();
        self.stdout.write_all(question).await?;
        self.stdout.flush().await?;
        self.stdin.read_line(&mut answer).await?;
        // Remove newline characters
        answer.retain(|c| !['\n', '\r', '\t'].contains(&c));
        Ok(answer)
    }
}

async fn async_main() -> Result<()> {

    let mut console = Console::new();
    // Find the user's data directory
    let data_dir = if let Some(from_env_var) = env::var_os("DATA_DIR") {
        log::info!("Detected data directory from environment: {}", from_env_var.to_string_lossy());
        PathBuf::from(from_env_var)
    } else {
        let mut data_dir = console.input(b"Define the dataset directory (default: data):").await?;
        if data_dir.is_empty() {
            data_dir.push_str("data");
        }
        console.output(format!("Using data directory '{}'", &data_dir).as_bytes()).await?;
        PathBuf::from(data_dir)
    };
    // Create that directory if it doesn't exist
    fs::create_dir_all(&data_dir).await?;
    loop {
        let choice = console.input(
            b"Choose whether to download new datasets, or condense the existing ones
                     \nWARNING: The downloader WILL get you IP-banned by Bangladesh Bank
                     \nUSE THE DOWNLOADER WITH CAUTION

                     \n1. Download new
                     \n2. Condense existing
                     \nYour choice:").await?;
        match choice.as_str() {
            "1" => {
                console.output(b"Downloading new datasets").await?;
                let download = Download::new(&data_dir);
                download.download_all().await?;
                break
            }
            "2" => {
                console.output(b"Merging existing datasets").await?;
                let destination_prefix = OsString::from("./output");
                let merge_xl = MergeXL::default();
                merge_xl.load_all_from(&data_dir).await?;
                merge_xl.write_to(&destination_prefix).await?;
                console.output(b"-- Critical reminders! --").await?;
                console.output(b"Please note if you are using CPI data, there is sometimes a base year change in 2012-2013").await?;
                break
            }
            _ => {
                console.output(b"Invalid answer. Try again.").await?;
            }
        }
    }
    console.output(b"\nProgram finished").await?;
    Ok(())
}

pub struct DataDir(PathBuf);

impl DataDir {
    pub fn path(&self) -> &Path {
        &self.0
    }
}
