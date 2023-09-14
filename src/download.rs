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
use std::fmt::{Display, Formatter};
use std::num::NonZeroU16;
use std::sync::atomic::{AtomicUsize, Ordering};
use async_std::path::{Path, PathBuf};
use async_std::stream::StreamExt;
use eyre::Result;
use futures::stream::FuturesUnordered;
use hyper::Uri;
use crate::common::{current_year, MonthlyReport, Year, Month};
use crate::http::{Connection, DownloadHandler};

const WEBSITE_PREFIX: &'static str = "https://www.bb.org.bd/pub/monthly/econtrds";
const XL_EXTENSIONS: [SheetExtension; 2] = [SheetExtension::Xlsx, SheetExtension::Xls];

pub struct Download<'d> {
    data_dir: &'d Path,
    total_hit_count: AtomicUsize
}

impl<'d> Download<'d> {
    pub fn new(data_dir: &'d Path) -> Self {
        Self {
            data_dir,
            total_hit_count: AtomicUsize::default()
        }
    }

    async fn download_year(&self, year: Year) -> Result<YearlyReport> {

        let mut outcomes = HashMap::new();

        for month in Month::values() {

            let report = MonthlyReport {
                month, year
            };
            let (status, hit_count) = report.download_if_possible(self.data_dir).await?;
            outcomes.insert(month, status);
            self.total_hit_count.fetch_add(hit_count, Ordering::AcqRel);
        }
        Ok(YearlyReport { year, outcomes })
    }

    pub async fn download_all(&self) -> Result<()> {
        // Parallelize per year
        let mut yearly_reports = FuturesUnordered::new();
        for year in 2013..=current_year() {
            let year = Year(NonZeroU16::new(year).expect("Non-zero year"));
            yearly_reports.push(self.download_year(year));
        }
        let mut total_downloads = 0;
        while let Some(YearlyReport { year, outcomes }) = yearly_reports.next().await.transpose()? {
            let download_count = outcomes
                .iter()
                .filter(|(_month, status)| {
                    if let ReportStatus::Downloaded(_ext) = **status {
                        true
                    } else {
                        false
                    }
                })
                .count();
            let missing_months = outcomes
                .iter()
                .filter_map(|(month, status)| {
                    if let ReportStatus::Missing = status {
                        Some(month)
                    } else {
                        None
                    }
                })
                .map(Month::name)
                .collect::<Vec<_>>();
            if missing_months.is_empty() {
                log::info!("Downloaded {} files for {}.", download_count, year);
            } else {
                let missing_months = missing_months.join(", ");
                log::info!(
                    "Downloaded {} files for {}. However, data is unavailable for months {}.",
                    download_count, year, missing_months
                );
            }
            total_downloads += download_count;
        }
        let total_hit_count = self.total_hit_count.load(Ordering::Acquire);
        log::info!(
            "Accessed {} URLs and downloaded {} files total from the central bank website.",
            total_hit_count, total_downloads
        );
        Ok(())
    }
}

struct YearlyReport {
    year: Year,
    outcomes: HashMap<Month, ReportStatus>
}

impl MonthlyReport {

    async fn attempt_urls<DH>(&self, connection: &mut Connection<'_, DH>)
        -> Result<ReportStatus> where DH: DownloadHandler {

        fn populate_urls(month: &str, year: &str, extension: SheetExtension) -> [String; 4] {
            let prefix = WEBSITE_PREFIX;
            [
                format!("{}/et{}{}.{}", prefix, month, year, extension),
                format!("{}/econtrends_{}{}.{}", prefix, month, year, extension),
                format!("{}/ET{}{}.{}", prefix, month, year, extension),
                format!("{}/{}{}/statisticaltable.{}", prefix, month, year, extension)
            ]
        }

        async fn attempt_urls_using<const M: usize, const Y: usize, DH>(months: [&str; M],
                                                                        years: [&str; Y],
                                                                        connection: &mut Connection<'_, DH>)
            -> Result<ReportStatus> where DH: DownloadHandler {

            for month in months {
                for year in years {
                    for extension in XL_EXTENSIONS {
                        for url in populate_urls(month, year, extension) {
                            if connection.download(url).await? {
                                return Ok(ReportStatus::Downloaded(extension));
                            }
                        }
                    }
                }
            }
            Ok(ReportStatus::Missing)
        }
        let month = self.month.name();
        let lower_month = month.to_lowercase();
        let short_month = &month[0..3];
        let lower_short_month = &lower_month[0..3];

        let year = self.year.to_string();
        let short_year = &year[2..];

        attempt_urls_using(
            [month, &lower_month, short_month, lower_short_month],
            [&year, short_year],
            connection
        ).await
    }

    async fn download_if_possible(&self, data_dir: &Path) -> Result<(ReportStatus, usize)> {
        let mut filename_prefix = format!("{}-{}.", self.year, self.month.as_numeric());
        for extension in XL_EXTENSIONS {
            filename_prefix.push_str(extension.value());
            if data_dir.join(&filename_prefix).exists().await {
                return Ok((ReportStatus::ExistsPreviously(extension), 0));
            }
            for _ in extension.value().chars() {
                filename_prefix.pop();
            }
        }
        // No existing files found; try URLs to download
        let handler = Handler {
            data_dir,
            filename_prefix: &filename_prefix,
        };
        let website_prefix = WEBSITE_PREFIX.parse::<Uri>()?;
        let host = website_prefix.host().expect("No host");
        let mut connection = Connection::open_connection(&handler, host).await?;
        let download_outcome = self.attempt_urls(&mut connection).await?;
        let hit_count = connection.hit_count();
        Ok((download_outcome, hit_count))
    }

}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum ReportStatus {
    ExistsPreviously(SheetExtension),
    Downloaded(SheetExtension),
    Missing
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum SheetExtension {
    Xlsx,
    Xls
}

impl SheetExtension {
    fn value(&self) -> &'static str {
        match self {
            Self::Xlsx => "xlsx",
            Self::Xls => "xls"
        }
    }
}

impl Display for SheetExtension {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.value())
    }
}

#[derive(Debug)]
struct Handler<'h> {
    data_dir: &'h Path,
    filename_prefix: &'h str
}

impl Handler<'_> {
    fn filename(&self, url: &str) -> Result<String> {
        for extension in XL_EXTENSIONS {
            if url.ends_with(extension.value()) {
                return Ok(format!("{}{}", self.filename_prefix, extension));
            }
        }
        Err(eyre::eyre!("No extension while attempting {} from url {}", self.filename_prefix, url))
    }
}

impl<'h> DownloadHandler for Handler<'h> {
    fn destination_file(&self, url: &str) -> Result<PathBuf> {
        let filename = self.filename(url)?;
        Ok(self.data_dir.join(filename))
    }
}

