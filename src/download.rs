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

use async_std::path::{Path, PathBuf};
use async_std::stream::StreamExt;
use chrono::{Datelike, Month};
use eyre::Result;
use futures::stream::FuturesUnordered;
use hyper::Uri;
use crate::common::MonthlyReport;
use crate::http::{Connection, DownloadHandler};

const WEBSITE_PREFIX: &'static str = "https://www.bb.org.bd/pub/monthly/econtrds";
const XL_EXTENSIONS: [&'static str; 2] = ["xlsx", "xls"];

fn current_year() -> i32 {
    let current_year = chrono::Utc::now();
    let current_year = current_year.year();
    log::info!("Current year is {}", current_year);
    current_year
}

pub struct Download<'d> {
    pub data_dir: &'d Path
}

impl Download<'_> {
    async fn download_year(&self, year: i32) -> Result<YearlyReport> {
        let mut missing_months = Vec::new();
        let mut month = Month::January;
        loop {
            let next_month = month.succ();
            let report = MonthlyReport {
                month, year
            };
            if !report.download_if_possible(self.data_dir).await? {
                missing_months.push(month);
            }
            if next_month == Month::January {
                break;
            }
            month = next_month
        }
        Ok(YearlyReport { year, missing_months })
    }

    pub async fn download_all(&self) -> Result<()> {
        // Parallelize per year
        let mut yearly_reports = FuturesUnordered::new();
        for year in 2023..=current_year() {
            yearly_reports.push(self.download_year(year));
        }
        while let Some(YearlyReport { year, missing_months }) = yearly_reports.next().await.transpose()? {
            let missing_months = missing_months
                .iter()
                .map(Month::name)
                .map(Box::from)
                .collect::<Vec<_>>()
                .join(", ");
            log::warn!("In {}, data is unavailable for {}", year, missing_months);
        }
        Ok(())
    }
}

struct YearlyReport {
    year: i32,
    missing_months: Vec<Month>
}

impl MonthlyReport {

    async fn attempt_urls<DH>(&self, connection: &mut Connection<'_, DH>)
        -> Result<bool> where DH: DownloadHandler {

        fn populate_urls(month: &str, year: &str, extension: &str) -> [String; 4] {
            let prefix = WEBSITE_PREFIX;
            [
                format!("{}/et{}{}.{}", prefix, month, year, extension),
                format!("{}/econtrends_{}{}.{}", prefix, year, month, extension),
                format!("{}/ET{}{}.{}", prefix, year, month, extension),
                format!("{}/{}{}/statisticaltable.{}", prefix, year, month, extension)
            ]
        }

        async fn attempt_urls_using<const M: usize, const Y: usize, DH>(months: [&str; M],
                                                                    years: [&str; Y],
                                                                    connection: &mut Connection<'_, DH>)
            -> Result<bool> where DH: DownloadHandler {

            for month in months {
                for year in years {
                    for extension in XL_EXTENSIONS {
                        for url in populate_urls(month, year, extension) {
                            if connection.download(url).await? {
                                return Ok(true);
                            }
                        }
                    }
                }
            }
            Ok(false)
        }
        let month = self.month.name();
        let lower_month = month.to_lowercase();
        let short_month = &month[0..3];
        let lower_short_month = &lower_month[0..3];

        let year = self.year.to_string();
        let short_year = &year[0..2];

        attempt_urls_using(
            [month, &lower_month, short_month, lower_short_month],
            [&year, short_year],
            connection
        ).await
    }

    async fn download_if_possible(&self, data_dir: &Path) -> Result<bool> {
        let mut filename_prefix = format!("{}-{}.", self.year, self.month.number_from_month());
        for extension in XL_EXTENSIONS {
            filename_prefix.push_str(extension);
            if data_dir.join(&filename_prefix).exists().await {
                return Ok(true);
            }
            for _ in extension.chars() {
                filename_prefix.pop();
            }
        }
        let handler = Handler {
            data_dir,
            filename_prefix: &filename_prefix,
        };
        let website_prefix = WEBSITE_PREFIX.parse::<Uri>()?;
        let host = website_prefix.host().expect("No host");
        let mut connection = Connection::open_connection(&handler, host).await?;
        let downloaded = self.attempt_urls(&mut connection).await?;
        Ok(downloaded)
    }

}


struct Handler<'h> {
    data_dir: &'h Path,
    filename_prefix: &'h str
}

impl Handler<'_> {
    fn filename(&self, url: &str) -> Result<String> {
        for extension in XL_EXTENSIONS {
            if url.ends_with(extension) {
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

