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

use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use async_std::{fs, task};
use async_std::path::Path;
use dashmap::{DashMap, DashSet};
use eyre::{Result, WrapErr};
use futures::stream::FuturesUnordered;
use async_std::stream::StreamExt;
use async_std::sync::RwLock;
use calamine::{DataType, Reader};
use chrono::Month;
use futures::TryFutureExt;
use smallvec::SmallVec;

#[derive(Default)]
pub struct MergeXL {
    sheets: RwLock<HashMap<Box<str>, Arc<Sheet>>>
}

/// Loads a specific excel file into memory
fn load_all_sheets(source: &Path) -> Result<impl IntoIterator<Item=calamine::Range<DataType>>> {
    log::info!("Loading excel file from {}", source.to_string_lossy());
    let mut workbook = calamine::open_workbook_auto(source)
        .wrap_err_with(|| format!("While loading excel file {}", source.to_string_lossy()))?;
    Ok(workbook
        .worksheets()
        .into_iter()
        .filter_map(|(sheet_name, range)| {
            if sheet_name == "Cover Page" || sheet_name == "Contents" {
                None
            } else {
                Some(range)
            }
        }))
}

impl MergeXL {
    /// Writes the data in memory to the given destination
    pub async fn write_to(self, destination: &Path) -> Result<()> {
        Ok(())
    }

    /// Loads all excel files from the given data directory into memory
    pub async fn load_all_from(&self, data_dir: &Path) -> Result<()> {

        let mut tasks = FuturesUnordered::new();
        let mut files = fs::read_dir(data_dir).await?;

        while let Some(file) = files.next().await.transpose()? {

            if file.file_name().to_string_lossy().starts_with('.') {
                // Hidden file; skip it
                continue;
            }
            tasks.push(task::spawn_blocking(move || {
                let file = file.path();
                load_all_sheets(&file)
            }).and_then(|sheets| async {
                for sheet in sheets {
                    self.merge_data(sheet).await?;
                }
                Ok(())
            }));
        }
        while let Some(status) = tasks.next().await.transpose()? {
            // Keep polling
        }
        log::info!("Loaded and merged rows from all data files");
        Ok(())
    }

    /// Accepts and merges more data loaded from another spreadsheet
    async fn merge_data(&self, sheet: calamine::Range<DataType>) -> Result<()> {
        if sheet.is_empty() {
            return Ok(());
        }
        let (height, width) = (sheet.height(), sheet.width());
        // Important: check columns starting from the left, BEFORE rows
        for check_col in 0..width {
            for check_row in 0..height {
                match sheet.get((check_row, check_col)) {
                    Some(DataType::String(value)) => {
                        if value == "July" {
                            // Hooray, we found the start of the data
                        }
                    }
                    _ => continue
                }
            }
        }
        Ok(())
    }

    /// Gets or creates a sheet by name
    async fn get_or_create_sheet(&self, name: &str) -> Arc<Sheet> {
        {
            let sheets = self.sheets.read().await;
            if let Some(sheet) = sheets.get(name) {
                return sheet.clone();
            }
            // Release read lock
        }
        let mut sheets = self.sheets.write().await;
        if let Some(existing) = sheets.get(name) {
            return existing.clone();
        }
        let new = Arc::new(Sheet::default());
        sheets.insert(name.into(), new.clone());
        new
    }
}

#[derive(Default)]
struct Sheet {
    columns: DashSet<Column>,
    rows: DashMap<Timestamp, RowData>
}

#[derive(PartialEq, Eq, Clone, Hash)]
struct Column {
    categorization: SmallVec<[Box<str>; 4]>,
    label: Box<str>,
    index: u8
}

impl Sheet {
    fn ensure_column(&self, column: &Column) {
        self.columns.insert(column.clone());
    }

    fn add_row(&self, timestamp: Timestamp, row: RowData) {
        row.data
            .iter()
            .for_each(|(col, _val)| self.ensure_column(col));
        self.rows.insert(timestamp, row);
    }
}

struct RowData {
    data: HashMap<Column, Box<str>>
}

#[derive(Eq, PartialEq, Hash, PartialOrd)]
struct Timestamp {
    day: u8,
    month: Month,
    year: u32
}

impl Ord for Timestamp {
    fn cmp(&self, other: &Self) -> Ordering {
        match (
            self.year.cmp(&other.year),
            self.month.number_from_month().cmp(&other.month.number_from_month()),
            self.day.cmp(&other.day)
        ) {
            (Ordering::Less, _, _) => {
                Ordering::Less
            }
            (Ordering::Greater, _, _) => {
                Ordering::Greater
            }
            (Ordering::Equal, Ordering::Less, _) => {
                Ordering::Less
            }
            (Ordering::Equal, Ordering::Greater, _) => {
                Ordering::Greater
            }
            (Ordering::Equal, Ordering::Equal, Ordering::Less) => {
                Ordering::Less
            }
            (Ordering::Equal, Ordering::Equal, Ordering::Greater) => {
                Ordering::Greater
            }
            (Ordering::Equal, Ordering::Equal, Ordering::Equal) => {
                Ordering::Equal
            }
        }
    }
}

