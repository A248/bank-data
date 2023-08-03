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

use std::borrow::Cow;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fmt::{Debug, Display, Formatter, Write};
use std::hash::{Hash, Hasher};
use std::mem;
use std::sync::Arc;
use arc_interner::ArcIntern;
use async_std::{fs, task};
use async_std::fs::{DirEntry, OpenOptions};
use async_std::path::{Path, PathBuf};
use dashmap::{DashMap, DashSet};
use eyre::{Result, WrapErr};
use futures::stream::FuturesUnordered;
use async_std::stream::StreamExt;
use async_std::sync::RwLock;
use calamine::{DataType, Range, Reader};
use futures::TryFutureExt;
use smallvec::SmallVec;
use crate::analysis::{AnalysisError, AnalysisResult, SheetAnalyzer};
use crate::common::*;

#[derive(Default)]
pub struct MergeXL {
    sheets: RwLock<HashMap<mem::Discriminant<Timestamp>, Arc<Sheet>>>
}

#[derive(Debug, Eq, PartialEq)]
pub enum FileStatus {
    XlsUnsupported,
    Success,
    ErrorsAnalyzing(Vec<String>)
}

/// For working with csv-async, we need AsRef<[u8]>. This wrapper imposes zero overhead.
/// An excellent example of the beauty of Rust.
struct CowAsU8<'d>(Cow<'d, str>);

impl AsRef<[u8]> for CowAsU8<'_> {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl MergeXL {
    /// Writes the data in memory to the given destination
    pub async fn write_to(self, destination: &OsStr) -> Result<()> {
        let mut tasks = FuturesUnordered::new();
        for (identifier, sheet) in self.sheets.into_inner() {
            tasks.push(async move {

                let mut hasher = DefaultHasher::new();
                identifier.hash(&mut hasher);
                let hash = hasher.finish();

                let mut destination = destination.to_os_string();
                destination.push(&format!("-timestamp-{}.csv", hash));
                let destination = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .open(destination).await?;
                let mut writer = csv_async::AsyncWriter::from_writer(destination);
                if let Some(sheet) = Arc::into_inner(sheet) {

                    let columns = sheet.columns.into_iter().collect::<Vec<_>>();
                    // Write the header
                    let mut header = Vec::new();
                    header.push(String::from("timestamp-primary-key"));
                    for column in &columns {
                        header.push(column.display_full_labeling());
                    }
                    writer.write_record(&header).await?;

                    // Write all the data
                    for (timestamp, mut data) in sheet.rows {
                        let mut record = Vec::new();

                        // Timestamp comes first
                        record.push(CowAsU8(Cow::Owned(timestamp.to_string())));
                        // Then the regular data columns
                        for column in columns.iter() {
                            let item = data.data.remove(column);
                            let item = if let Some(item) = item {
                                Cow::Owned(item.into_string())
                            } else {
                                Cow::Borrowed("NA")
                            };
                            record.push(CowAsU8(item));
                        }
                        writer.write_record(record).await?;
                    }
                    writer.flush().await?;
                    Ok(())
                } else {
                    Err(eyre::eyre!("Sheet not held exclusively"))
                }
            })
        }
        while let Some(_) = tasks.next().await.transpose()? {
            // Keep polling
        }
        Ok(())
    }

    /// Loads all excel files from the given data directory into memory
    pub async fn load_all_from(&self, data_dir: &Path) -> Result<()> {

        let mut tasks = FuturesUnordered::new();
        let mut files = fs::read_dir(data_dir).await?;

        while let Some(file) = files.next().await.transpose()? {

            let merge_file = MergeFile {
                merge_xl: &self,
                file
            };
            tasks.push(async move { merge_file.merge().await });
        }
        let mut file_statuses = HashMap::<PathBuf, FileStatus>::new();
        while let Some(output) = tasks.next().await.transpose()? {
            if let Some((path, status)) = output {
                file_statuses.insert(path, status);
            }
            // Keep polling
        }
        if file_statuses.is_empty() {
            log::warn!("No files loaded. Did you specify the correct data directory?");
            return Ok(());
        }
        let report_intro = "Loaded and merged rows from input data files.\n-- Report --";
        let mut report = String::from(report_intro);
        {
            let xls_unsupported = file_statuses
                .iter()
                .filter_map(|(path, status)| {
                    if status == &FileStatus::XlsUnsupported {
                        Some(path)
                    } else {
                        None
                    }
                })
                .map(PathBuf::as_path)
                .map(Path::to_string_lossy)
                .collect::<Vec<_>>()
                .join(", ");
            if !xls_unsupported.is_empty() {
                report.push_str("\nXLS files are unsupported. XLS files: ");
                report.push_str(&xls_unsupported);
            }
        }
        {
            let other_failures = file_statuses
                .iter()
                .filter_map(|(path, status)| {
                    if let FileStatus::ErrorsAnalyzing(errors) = status {
                        Some(format!(
                            "  {}:\n    {}", path.to_string_lossy(), errors.join("\n    ")
                        ))
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
                .join("\n");
            if !other_failures.is_empty() {
                report.push_str("\nFailures while loading files:\n");
                report.push_str(&other_failures);
            }
        }
        if report == report_intro {
            report.push_str("\n  Hooray, all sheets loaded with pure success.\n");
        }
        log::info!("{}", report);
        Ok(())
    }

    /// Gets or creates a sheet by name
    pub async fn get_or_create_sheet(&self, timestamp_variant: &Timestamp) -> Arc<Sheet> {
        let variant = mem::discriminant(timestamp_variant);
        {
            let sheets = self.sheets.read().await;
            if let Some(sheet) = sheets.get(&variant) {
                return sheet.clone();
            }
            // Release read lock
        }
        let mut sheets = self.sheets.write().await;
        if let Some(existing) = sheets.get(&variant) {
            return existing.clone();
        }
        let new = Arc::new(Sheet::default());
        sheets.insert(variant, new.clone());
        new
    }
}

struct MergeFile<'m> {
    merge_xl: &'m MergeXL,
    file: DirEntry
}

impl MergeFile<'_> {
    async fn merge(&self) -> Result<Option<(PathBuf, FileStatus)>> {
        let filename = self.file.file_name();
        let filename = filename.to_string_lossy();
        if filename.starts_with('.') {
            // Hidden file; skip it
            return Ok(None);
        }
        let file = self.file.path();
        Ok(Some(if filename.ends_with(".xls") {
            (file, FileStatus::XlsUnsupported)
        } else {
            self.perform_merge_data(file).await?
        }))
    }

    async fn perform_merge_data(&self, file: PathBuf) -> Result<(PathBuf, FileStatus)> {
        task::spawn_blocking(move || {
            let sheets = blocking_load_all_sheets(&file)?;
            Ok((file, sheets))
        }).and_then(|(file, sheets)| async move {
            let filename = file.to_string_lossy();
            let mut errors = vec![];
            for (name, sheet) in sheets {
                let analyzer = SheetAnalyzer {
                    source: &filename,
                    name: &name,
                    sheet
                };
                if let Err(error) = analyzer.merge_data(&self.merge_xl).await {
                    errors.push(format!("{}: {}", name, error));
                }
            }
            let status = if errors.is_empty() { FileStatus::Success } else { FileStatus::ErrorsAnalyzing(errors) };
            Ok((file, status))
        }).await
    }
}


/// Loads a specific excel file into memory
/// Threading: calamine's blocking I/O happens here and not later
fn blocking_load_all_sheets(source: &Path) -> Result<impl IntoIterator<Item=(String, Range<DataType>)>> {
    let source_filename = source.to_string_lossy();
    log::info!("Loading excel file from {}", source_filename);
    let mut workbook =  calamine::open_workbook_auto(source)
        .wrap_err_with(|| format!("While loading excel file {}", source_filename))?;
    log::info!("Loaded file {}", source_filename);
    Ok(workbook
        .worksheets()
        .into_iter()
        .filter(|(sheet_name, _)| {
            sheet_name != "Cover Page" && sheet_name != "Contents" && !sheet_name.starts_with("Appendix")
        }))
}

#[derive(Default)]
pub struct Sheet {
    columns: DashSet<Column>,
    rows: DashMap<Timestamp, RowData>
}

/// A column in a sheet. Because the central bank likes to exquisitely detail its columns,
/// columns tend to fall within a categorization, e.g. Scheduled Bank Branches >
/// Group Bank Branches >
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Column {
    /// The label categorization is guaranteed to be non-empty
    label_categorization: SmallVec<[ColumnLabel; 6]>
}

#[derive(Clone, Debug, Default)]
pub struct RowData {
    data: HashMap<Column, Box<str>>
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ColumnLabel(ArcIntern<str>);

impl Column {
    pub fn new(label_categorization: impl IntoIterator<Item=ColumnLabel>) -> AnalysisResult<Self> {
        let label_categorization = label_categorization.into_iter().collect::<SmallVec<_>>();
        if label_categorization.is_empty() {
            Err(AnalysisError::unsupported("Label categorization is empty"))
        } else {
            Ok(Self {
                label_categorization
            })
        }
    }

    fn display_full_labeling(&self) -> String {
        let mut builder = String::new();
        for label in &self.label_categorization {
            builder.push_str(label.as_ref());
            builder.push('.');
        }
        builder.pop();
        builder
    }
}

impl Display for Column {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_char('[')?;
        for label in &self.label_categorization {
            f.write_str(label.as_ref())?;
            f.write_char(',')?;
        }
        f.write_char(']')
    }
}

impl ColumnLabel {
    pub fn create(label: &str) -> Option<Self> {
        let label = label.trim();
        let is_number = label.parse::<u8>();
        if is_number.is_ok() {
            // Column labels are not allowed to be numbers
            // Commonly the Bangladesh Bank writes numbers on each column, just because
            None
        } else {
            Some(Self(ArcIntern::from(label)))
        }
    }
}

impl AsRef<str> for ColumnLabel {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

impl Sheet {
    fn ensure_column(&self, column: &Column) {
        self.columns.insert(column.clone());
    }

    pub fn add_row(&self, timestamp: Timestamp, row: RowData) {
        row.data
            .iter()
            .for_each(|(col, _val)| self.ensure_column(col));

        // Try to insert the row. If there is another value at the timestamp, combine them
        let previous_row = self.rows.insert(timestamp.clone(), row);
        if let Some(previous_row) = previous_row {
            // Combine them
            self.rows.alter(&timestamp, |_, new_row| previous_row.combine(new_row));
        }
    }
}

impl RowData {
    pub fn populate<V>(&mut self, column: &Column, value: V) where V: Into<Box<str>> {
        self.data.insert(column.clone(), value.into());
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    fn combine(mut self, other: Self) -> Self {
        self.data.extend(other.data);
        self
    }
}



