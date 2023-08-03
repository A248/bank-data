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

use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::num::NonZeroU16;
use std::ops::{Deref, RangeBounds};
use std::str::FromStr;
use calamine::{DataType, Range};
use log::Level;
use crate::common::*;
use crate::merge::{Column, ColumnLabel, MergeXL, RowData};

const UNSUPPORTED_SHEETS: [(&'static str, &'static str); 4] = [
    // We can't read the sheets pertaining to government bonds, which use daily timestamps
    ("BD(Govt) Treasury Bond", "Government securities/bonds sheet unsupported"),
    // Neither can we read those detailing interest rates due to horizontal data
    ("Fixed Deposit Account (Interest after maturity)", "Bank/interest rate sheet unsupported"),
    // This sheet has no timestamps at all
    ("BANK WISE ANNOUNCED INTEREST RATE STRUCTURE", "Bank rate announcements unsupported"),
    // Neither does this one
    ("PROFIT RATE STRUCTURE OF THE ISLAMIC BANKS", "Islamic banks sheet unsupported")
];

const SKIPPED_LABEL_ELEMENTS: [&'static str; 1] = ["Weight"];

const INFLATION_OLD_BASE_MARKER: &str = "(OB)";
const INFLATION_NEW_BASE_MARKER: &str = "(NB)";

#[derive(Debug)]
pub struct SheetAnalyzer<'p> {
    pub source: &'p str,
    pub name: &'p str,
    pub sheet: Range<DataType>
}

impl Display for SheetAnalyzer<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "sheet {} from {}", self.name, self.source)
    }
}

pub type AnalysisResult<T> = Result<T, AnalysisError>;

#[derive(Debug)]
pub enum AnalysisError {
    Unsupported{ reason: String },
    NoData,
    OtherFailure(ErrorBox)
}

#[derive(Debug)]
pub struct ErrorBox(Box<dyn Error + Send + Sync + 'static>);

impl Display for AnalysisError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unsupported { reason: what } => {
                write!(f, "Format unsupported: {}", what)
            },
            Self::NoData => f.write_str("No non-provisional data"),
            Self::OtherFailure(error) => {
                write!(f, "Other: {}", error)
            }
        }
    }
}

impl Display for ErrorBox {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl Error for AnalysisError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::OtherFailure(ErrorBox(error)) => Some(error.deref()),
            _ => None
        }
    }
}

impl AnalysisError {
    pub fn unsupported<M>(reason: M) -> Self where M: Into<String> {
        Self::Unsupported { reason: reason.into() }
    }
}

impl From<ErrorBox> for AnalysisError {
    fn from(value: ErrorBox) -> Self {
        Self::OtherFailure(value)
    }
}

impl<E> From<E> for ErrorBox where E: Error + Send + Sync + 'static {
    fn from(value: E) -> Self {
        Self(Box::new(value))
    }
}

#[derive(Clone, Debug)]
struct FirstYearlyTimestamp {
    value: YearlyTimestamp,
    cell: (usize, usize)
}

enum CellAsTimestamp<'d> {
    None,
    MayNeedContext(&'d str),
    YearlyTimestamp(YearlyTimestamp),
    TimestampIsProvisional
}

trait CellInspector {
    fn inspect_if_unsupported(&self, string_value: &str) -> AnalysisResult<()>;

    fn inspect_if_skippable(&self, string_value: &str) -> bool;
}

struct NoOpInspector {}
impl CellInspector for NoOpInspector {
    fn inspect_if_unsupported(&self, _: &str) -> AnalysisResult<()> {
        Ok(())
    }

    fn inspect_if_skippable(&self, _: &str) -> bool {
        false
    }
}

struct SheetSupportInspector<'s, const M: usize, const N: usize> {
    banned_cell_values_to_reasons: [(&'s str, &'s str); M],
    skipped_cell_values: [&'s str; N]
}

impl<const M: usize, const N: usize> CellInspector for SheetSupportInspector<'_, M, N> {
    fn inspect_if_unsupported(&self, value: &str) -> AnalysisResult<()> {

        for (banned_value, reason) in &self.banned_cell_values_to_reasons {
            if value.contains(*banned_value) {
                // Gotcha! We can't read these sheets
                return Err(AnalysisError::unsupported(*reason));
            }
        }
        Ok(())
    }

    fn inspect_if_skippable(&self, value: &str) -> bool {
        for skipped_value in &self.skipped_cell_values {
            if value == *skipped_value {
                return true;
            }
        }
        false
    }
}

/// Attempts to read a cell as a timestamp. If successful, it is guaranteed the timestamp
/// is a year.
///
/// This function has two purposes. It is used on the initial scan to find the first timestamp
/// value in the sheet, checking along the way for signs that the sheet is unsupported via
/// the provided inspector.
///
/// Later, it is used again to load each timestamp as the data is collected from the rows.
/// No checks are necessary for signs the sheet is unsupported.
fn read_cell_as_timestamp<'d, I>(data_type: &'d DataType, inspector: &I) -> AnalysisResult<CellAsTimestamp<'d>>
    where I: CellInspector {

    /// Attempts to read an integer value as a calendar yeear
    fn try_as_calendar_year(year: u16) -> CellAsTimestamp<'static> {
        const INDEPENDENCE_YEAR: u16 = 1971;

        if year >= INDEPENDENCE_YEAR && year <= current_year() {
            let calendar_year = Year(NonZeroU16::new(year).unwrap());
            CellAsTimestamp::YearlyTimestamp(YearlyTimestamp::Calendar(calendar_year))
        } else {
            CellAsTimestamp::None
        }
    }
    Ok(match data_type {
        // Integer types
        DataType::Int(year) => try_as_calendar_year(*year as u16),
        DataType::Float(year) => try_as_calendar_year(year.round() as u16),
        // Date types
        DataType::DateTime(_) | DataType::Duration(_) => {
            // Calamine should probably remove these enum variants if the feature is unset
            log::trace!("Dates feature of calamine is not enabled for {}", data_type);
            CellAsTimestamp::None
        },
        // String
        DataType::String(value) | DataType::DateTimeIso(value) | DataType::DurationIso(value) => {
            let value = &mut value.as_str();

            // Check for unsupported cells
            inspector.inspect_if_unsupported(&value)?;

            for provisional_marker in ["P", "p", "(P)", "(p)"] {
                if value.ends_with(provisional_marker) {
                    let prior = &value[..value.len() - provisional_marker.len()];
                    // Identify both provisional years and months
                    if let Ok(YearlyTimestamp::Fiscal(_)) = YearlyTimestamp::from_str(prior) {
                        return Ok(CellAsTimestamp::TimestampIsProvisional);
                    } else if let Ok(_) = Month::from_str(prior) {
                        return Ok(CellAsTimestamp::TimestampIsProvisional);
                    }
                }
            }
            // Make allowances for asterisks and other characters
            // 'R' superscript means revised, ® can also be removed
            if let Some(last_char) = value.chars().rev().next() {
                match last_char {
                    '*' | 'R' | '®' => {
                        let byte_count = last_char.len_utf8();
                        *value = &value[..value.len() - byte_count];
                    },
                    _ => {}
                }
            }
            // Inflation sheet uses these values to signify the change of base year
            // The base year identifier is added only for data recorded in both bases
            // Keep data using the new base, ignore and discard data explicitly of the old base
            if value.ends_with(INFLATION_OLD_BASE_MARKER) {
                return Ok(CellAsTimestamp::None);
            }
            // Keep data which uses the new base
            if value.ends_with(INFLATION_NEW_BASE_MARKER) {
                *value = &value[..value.len() - INFLATION_NEW_BASE_MARKER.len()];
            }
            if let Ok(timestamp) = YearlyTimestamp::from_str(value) {
                CellAsTimestamp::YearlyTimestamp(timestamp)
            } else {
                CellAsTimestamp::MayNeedContext(value)
            }
        },
        // Misc
        DataType::Empty | DataType::Error(_) | DataType::Bool(_) => CellAsTimestamp::None
    })
}

impl SheetAnalyzer<'_> {
    /// Determines the first (yearly) timestamp value in the sheet. This value is critical
    /// and tells us whether the sheet is valid at all, or parsable by our algorithm.
    ///
    /// The data starts from the first timestamp and proceeds downwards. Usually, the
    /// first timestamp is a year and the subsequent values in the period column contain
    /// plain months such as "July", "August" which refer back to the previous month.
    /// However, this is not guaranteed; biannual and quarterly data is another possibility.
    /// Moreover, oftentimes, yearly data preceeds monthly data.
    fn find_first_timestamp<I: CellInspector>(&self, inspector: &I) -> AnalysisResult<FirstYearlyTimestamp> {

        let sheet = &self.sheet;

        // Important: check columns starting from the left, BEFORE rows
        for cur_col in 0..sheet.width() {

            // Scan the years until we receive a year
            for cur_row in 0..sheet.height() {
                match read_cell_as_timestamp(&sheet[(cur_row, cur_col)], inspector)? {
                    CellAsTimestamp::YearlyTimestamp(timestamp) => {
                        return Ok(FirstYearlyTimestamp {
                            value: timestamp,
                            cell: (cur_row, cur_col)
                        });
                    },
                    CellAsTimestamp::TimestampIsProvisional => {
                        // Provisional data encountered. Stop everything. We have nothing.
                        // Hereafter, everything (all the rows) will be provisional
                        return Err(AnalysisError::NoData);
                    },
                    CellAsTimestamp::None | CellAsTimestamp::MayNeedContext(_) => () /* do nothing */
                }
            }
        }
        Err(AnalysisError::unsupported("No timestamp found"))
    }
}

#[derive(Clone, Debug)]
struct ColumnInfo {
    column: Column,
    indexed_labels: HashMap<usize, ColumnLabel>,
    /// Which column index does this represent in the sheet
    index_in_sheet: usize
}

impl Display for ColumnInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.column)
    }
}

impl SheetAnalyzer<'_> {
    /// Accepts and merges more data loaded from another spreadsheet
    pub async fn merge_data(&self, merge_xl: &MergeXL) -> AnalysisResult<()> {
        if self.sheet.is_empty() {
            Err(AnalysisError::NoData)

        } else {
            let inspector = SheetSupportInspector {
                banned_cell_values_to_reasons: UNSUPPORTED_SHEETS,
                skipped_cell_values: SKIPPED_LABEL_ELEMENTS
            };
            let FirstYearlyTimestamp {
                value: start_year, cell: (data_start_row, timestamp_col)
            } = self.find_first_timestamp(&inspector)?;

            let supported_sheet = SupportedSheet {
                analyzer: &self,
                data_start_row,
                timestamp_col
            };
            let columns = supported_sheet.load_columns(
                supported_sheet.find_label_range(&inspector)?
            )?;
            if log::log_enabled!(Level::Debug) {
                let mut column_display = String::new();
                for column in columns.clone()    {
                    column_display.push_str(&format!("{}", column));
                }
                log::debug!("Loaded columns [{}]", column_display)
            }
            supported_sheet.read_rows_into(start_year, columns, merge_xl).await
        }
    }
}

#[derive(Clone, Debug)]
struct SupportedSheet<'a, 'p> {
    analyzer: &'a SheetAnalyzer<'p>,
    data_start_row: usize,
    timestamp_col: usize
}

impl Display for SupportedSheet<'_, '_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} [where data starts in ({}, {})", self.analyzer, self.data_start_row, self.timestamp_col)
    }
}

impl SupportedSheet<'_, '_> {
    fn cell(&self, row: usize, col: usize) -> &DataType {
        &self.analyzer.sheet[(row, col)]
    }

    /// Finds the range of cells constituting the label. Starts from the beginning of the data
    /// and progresses upwards until a string cell signifying the start of the label is found.
    /// Then continues to read string cells until an empty cell or the end of the document.
    fn find_label_range<I: CellInspector>(&self, inspector: &I) -> AnalysisResult<std::ops::Range<usize>> {
        if self.data_start_row == 0 {
            return Err(AnalysisError::unsupported("Data starts in the first row. No labels possible"));
        }
        // First, find the top of the label text, something like "Period" or "End of period"
        let mut label_start_index = None;
        for row_cursor in 0..self.data_start_row {
            if let DataType::String(value) = self.cell(row_cursor, self.timestamp_col) {
                if value.contains("Period") || value.contains("period") {
                    // We've found the beginning of the label
                    label_start_index = Some(row_cursor);
                    break;
                }
            }
        }
        let label_start_index = match label_start_index {
            None => return Err(AnalysisError::unsupported("Unable to find label start index")),
            Some(idx) => idx
        };
        // Now scan cells in case of reaching skippable label values
        for row_cursor in label_start_index..self.data_start_row {
            if let DataType::String(value) = self.cell(row_cursor, self.timestamp_col) {
                if inspector.inspect_if_skippable(value) {
                    return Ok(label_start_index..row_cursor);
                }
            }
        }
        Ok(label_start_index..self.data_start_row)
        /*
        loop {
            let cell = self.cell(row_cursor, self.timestamp_col);
            match cell {
                DataType::Empty => {
                    // Keep going until reaching the label
                }
                DataType::String(value) => {
                    if inspector.inspect_if_skippable(value) {
                        // Keep going until reaching the label
                        log::info!("Skipping label element {} purposefully in {}", value, self);
                    } else {
                        // Yes!
                        break Ok(());
                    }
                }
                _ => break Err(AnalysisError::unsupported(
                    format!("Not allowed data type {:?} in row {} of {}", cell, row_cursor, self)
                ))
            }
            if row_cursor == 0 {
                break Err(AnalysisError::unsupported("No label position found"));
            }
            row_cursor -= 1;
        }?;
        let label_bottom_index = row_cursor;
        log::info!("Found label_bottom_index at {} in {}", label_bottom_index, self);
        // Now scan upwards until we receive an empty cell or end of document
        loop {
            if row_cursor == 0 {
                break Ok(0..=label_bottom_index);
            }
            row_cursor -= 1;
            let cell = self.cell(row_cursor, self.timestamp_col);
            match cell {
                DataType::Empty => {
                    // Okay, so the last row was the top index of the label
                    let last_row = row_cursor + 1;
                    break Ok(last_row..=label_bottom_index);
                },
                DataType::String(value) => {
                    if !value.contains("Period") && !value.contains("period") {
                        // This is probably the header of the document or something like that
                        break Ok((row_cursor + 1)..=label_bottom_index);
                    }
                    // Keep going
                },
                _ => break Err(AnalysisError::unsupported(
                    format!("Not allowed data type {:?} in row {} of {}", cell, row_cursor, self)
                ))
            }
        }*/
    }

    /// Generates column information. If there is no detected column at the specified column index,
    /// yields None.
    ///
    /// The columns MUST be generated in sequence starting from the left. The implementation of this
    /// method assumes reliance on this contract.
    fn generate_column_info<R>(&self, label_range: R, col_index: usize,
                               previous_columns: &HashMap<usize, ColumnInfo>) -> AnalysisResult<Option<ColumnInfo>>
        where R: IntoIterator<Item=usize> + Clone + RangeBounds<usize> {

        // We mainly need the categorization vector. The additional index is used for the look-behind trick
        let mut label_categorization = Vec::new();
        let mut indexed_labels = HashMap::new();

        for row_cursor in label_range.clone() {
            let label = match self.cell(row_cursor, col_index) {

                DataType::Empty => {
                    // An empty cell means we need to try the adjacent column to the left
                    // This trick relies on the order of iteration on behalf of the caller
                    fn find_label_from_previous_column<R>(label_range: &R, col_index: usize,
                                                          row_cursor: usize, indexed_labels: &HashMap<usize, ColumnLabel>,
                                                          previous_columns: &HashMap<usize, ColumnInfo>)
                        -> Option<ColumnLabel> where R: RangeBounds<usize> {

                        if let Some(previous_column) = previous_columns.get(&(col_index - 1)) {
                            log::trace!("Attempting to use previous column for label transplant {:?}", previous_column);
                            if let Some(candidate) = previous_column.indexed_labels.get(&row_cursor) {
                                // Before we assume this is the right label, we need to check for proper nesting
                                // E.g., the broadest categories always come first
                                // In other words, if these two columns are the same, their top category should also be
                                let proper_nesting = if row_cursor == 0 || !label_range.contains(&(row_cursor - 1)) {
                                    // No need to check nesting. This is the top category
                                    true
                                } else {
                                    let last_row = row_cursor - 1;
                                    previous_column.indexed_labels.get(&last_row) == indexed_labels.get(&last_row)
                                };
                                if proper_nesting {
                                    // Everything good
                                    // I love this trick
                                    log::debug!("We did the trick!");
                                    return Some(candidate.clone());
                                } else {
                                    // This isn't valid categorisation
                                    log::trace!("Not valid categorisation");
                                }
                            } else {
                                log::trace!("No candidate found at all");
                            }
                        }
                        None
                    }
                    // An empty label is yielded if and only if there really is nothing
                    find_label_from_previous_column(&label_range, col_index, row_cursor, &indexed_labels, previous_columns)
                },
                // These return empty label parts if and only if the value is a number
                // See ColumnLabel#create for more information
                DataType::String(value) => ColumnLabel::create(value.as_str()),
                other => ColumnLabel::create(&other.to_string())
            };
            if let Some(label) = label {
                log::trace!("Found label for ({}, {})", row_cursor, col_index);
                label_categorization.push(label.clone());
                indexed_labels.insert(row_cursor, label);
            }
        }
        Ok(if label_categorization.is_empty() {
            None
        } else {
            Some(ColumnInfo {
                column: Column::new(label_categorization)?,
                indexed_labels,
                index_in_sheet: col_index
            })
        })
    }

    fn load_columns<R>(&self, label_range: R) -> AnalysisResult<Vec<ColumnInfo>>
        where R: IntoIterator<Item=usize> + Clone + Debug + RangeBounds<usize> {

        let mut columns = HashMap::new();

        for col_index in (self.timestamp_col + 1)..self.analyzer.sheet.width() {
            let column_info = self.generate_column_info(label_range.clone(), col_index, &columns)?;
            if let Some(column_info) = column_info {
                columns.insert(col_index, column_info);
            } else {
                // No more columns; we can stop
                break;
            };
        }
        Ok(columns.into_iter().map(|(_, col)| col).collect())
    }

    async fn read_rows_into(&self, start_year: YearlyTimestamp,
                            columns: Vec<ColumnInfo>, output: &MergeXL) -> AnalysisResult<()> {
        // Monthly and quarterly data relies on identifying the last-seen year from prior rows
        let mut current_year = match start_year {
            YearlyTimestamp::Fiscal(fy) => fy,
            YearlyTimestamp::Calendar(cy) => cy
        };

        for row_cursor in self.data_start_row..self.analyzer.sheet.height() {

            // First, figure out the timestamp of this row
            let timestamp_cell = self.cell(row_cursor, self.timestamp_col);
            let timestamp = match read_cell_as_timestamp(timestamp_cell, &NoOpInspector {})? {
                CellAsTimestamp::MayNeedContext(timestamp_str) => {

                    // Try to parse as month, quarter, or halfyear
                    if let Ok(month) = Month::from_str(timestamp_str) {
                        Timestamp::Monthly(MonthlyReport {
                            year: current_year,
                            month,
                        })
                    } else if let Ok(quarter) = Quarter::from_str(timestamp_str) {
                        Timestamp::Quarterly(current_year, quarter)
                    } else if let Ok(halfyear) = HalfYear::from_str(timestamp_str) {
                        Timestamp::BiAnnually(current_year, halfyear)

                    // Otherwise, we've either hit the end of document or an error
                    } else if timestamp_str.contains("Source") || timestamp_str.contains("Note") {
                        // Hooray, we've reached the end of the document!
                        // The central bank typically leaves these mentions at the very end of the column
                        break;
                    } else {
                        return Err(AnalysisError::unsupported(format!(
                            "Found invalid timestamp (non-parsable) {} in row {}", timestamp_cell, row_cursor
                        )));
                    }
                }
                CellAsTimestamp::None => {
                    if let DataType::Empty = timestamp_cell {
                        // Yes! We're done
                        break;
                    } else {
                        return Err(AnalysisError::unsupported(format!(
                            "Found invalid timestamp (cell type) {} in row {}", timestamp_cell, row_cursor
                        )));
                    }
                }
                CellAsTimestamp::YearlyTimestamp(yearly_timestamp) => {
                    current_year = Year::from(yearly_timestamp);
                    Timestamp::from(yearly_timestamp)
                }
                CellAsTimestamp::TimestampIsProvisional => {
                    // We're done, stop reading
                    break;
                }
            };
            let mut row_data = RowData::default();
            for column_info in columns.iter() {
                let value = self.cell(row_cursor, column_info.index_in_sheet);
                if let DataType::Empty = value {
                    // It's empty. Skip it. If all the cells are empty, that's fine.
                } else {
                    let value = value.to_string();
                    row_data.populate(&column_info.column, value);
                }
            }
            if columns.len() != row_data.len() {
                let percent_full = row_data.len() as f32 / columns.len() as f32;
                if percent_full < 0.15 {
                    // Probably a nothing row worth skipping
                    continue;
                } else if percent_full < 0.80 {
                    //log::warn!("Percent full at {}% in row {} of {} having {}",
                    //    percent_full * 100.0, row_cursor, self, columns.iter().map(|c| format!("{}", c)).collect::<Vec<_>>().join(","));
                }
            }
            let sheet = output.get_or_create_sheet(&timestamp).await;
            sheet.add_row(timestamp, row_data);
        }
        Ok(())
    }
}
