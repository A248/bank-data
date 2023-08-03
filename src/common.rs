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
use std::fmt::{Display, Formatter};
use std::num::NonZeroU16;
use assert_matches::assert_matches;
use chrono::Datelike;

pub fn current_year() -> u16 {
    let current_year = chrono::Utc::now();
    let current_year = current_year.year() as u16;
    current_year
}

// Structs

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Year(pub NonZeroU16);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd)]
pub struct MonthlyReport {
    pub year: Year,
    pub month: Month
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd)]
pub enum Timestamp {
    CalendarYear(Year),
    FiscalYear(Year),
    BiAnnually(Year, HalfYear),
    Quarterly(Year, Quarter),
    Monthly(MonthlyReport)
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd)]
pub enum YearlyTimestamp {
    Calendar(Year),
    Fiscal(Year)
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum HalfYear {
    JanThruJun = 0,
    JulThruDec = 1
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Quarter {
    JanFebMar = 0,
    AprMayJun = 1,
    JulAugSep = 2,
    OctNovDec = 3
}


#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd)]
pub struct Month {
    inner: chrono::Month
}

pub trait MonthBounds {
    fn start_and_end_month(&self) -> (Month, Month);
}

impl MonthlyReport {
    pub fn new(year: Year, month: Month) -> Self {
        Self { year, month }
    }
}

impl Timestamp {
    /// How long a period the timestamp covers, in months
    fn length_of_period_in_months(&self) -> u8 {
        match self {
            Self::FiscalYear(_) | Self::CalendarYear(_) => 12,
            Self::BiAnnually(..) => 6,
            Self::Quarterly(..) => 3,
            Self::Monthly(..) => 1,
        }
    }
}

impl Month {
    pub fn from_chrono(chrono: chrono::Month) -> Self {
        Self { inner: chrono }
    }

    pub fn name(&self) -> &str {
        self.inner.name()
    }

    pub fn as_numeric(&self) -> u8 {
        self.inner.number_from_month() as u8
    }
}

macro_rules! gen_month_constants {
    ($which:ident) => {
        #[allow(non_snake_case)]
        impl Month {
            #[allow(non_upper_case_globals)]
            pub const $which: Self = Self { inner: chrono::Month::$which };
        }
    };
    ($which:ident, $($further:ident),+) => {
        gen_month_constants!($which);
        gen_month_constants!($($further),+);
    }
}
gen_month_constants!(January, February, March, April, May, June, July, August, September, October, November, December);
impl Month {
    pub fn values() -> [Self; 12] {
        [Self::January, Self::February, Self::March, Self::April, Self::May, Self::June,
            Self::July, Self::August, Self::September, Self::October, Self::November, Self::December]
    }
}

impl From<YearlyTimestamp> for Timestamp {
    fn from(value: YearlyTimestamp) -> Self {
        match value {
            YearlyTimestamp::Calendar(y) => Self::CalendarYear(y),
            YearlyTimestamp::Fiscal(y) => Self::FiscalYear(y)
        }
    }
}

impl From<YearlyTimestamp> for Year {
    fn from(value: YearlyTimestamp) -> Self {
        match value {
            YearlyTimestamp::Calendar(y) => y,
            YearlyTimestamp::Fiscal(y) => y
        }
    }
}

// Implement ordering so that we can perform sort operations later

impl Ord for Timestamp {
    fn cmp(&self, other: &Self) -> Ordering {
        let order_period = Ord::cmp(&self.length_of_period_in_months(), &other.length_of_period_in_months());
        if order_period != Ordering::Equal {
            // Put longer periods first
            return order_period.reverse();
        }
        // Otherwise, order first by year, then other details
        match *self {
            Self::CalendarYear(year) => {
                let other_year = assert_matches!(other, Self::CalendarYear(oy) => oy);
                year.cmp(other_year)
            }
            Self::FiscalYear(year) => {
                let other_year = assert_matches!(other, Self::FiscalYear(oy) => oy);
                year.cmp(other_year)
            },
            Self::BiAnnually(year, halfyear) => {
                let (other_year, other_halfyear) = assert_matches!(other, Self::BiAnnually(oy, ohy) => (oy, ohy));
                let order_year = year.cmp(other_year);
                if order_year != Ordering::Equal {
                    order_year
                } else {
                    halfyear.cmp(other_halfyear)
                }
            },
            Self::Quarterly(year, quarter) => {
                let (other_year, other_quarter) = assert_matches!(other, Self::Quarterly(oy, oq) => (oy, oq));
                let order_year = year.cmp(other_year);
                if order_year != Ordering::Equal {
                    order_year
                } else {
                    quarter.cmp(other_quarter)
                }
            },
            Self::Monthly(report) => {
                let other_report = assert_matches!(other, Self::Monthly(r) => r);
                report.cmp(other_report)
            }
        }
    }
}

impl Ord for MonthlyReport {
    fn cmp(&self, other: &Self) -> Ordering {
        let order_year = self.year.cmp(&other.year);
        if order_year != Ordering::Equal {
            order_year
        } else {
            self.month.cmp(&other.month)
        }
    }
}

impl Ord for Month {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_numeric().cmp(&other.as_numeric())
    }
}

// Display enables us to write all of these structs as strings later on

impl Display for Year {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl Display for Month {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.name()[0..3])
    }
}

impl Display for MonthlyReport {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // e.g. 2009-01
        write!(f, "{}-{:02}", self.year, self.month.as_numeric())
    }
}

impl Display for Timestamp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match *self {
            Self::CalendarYear(year) => year.fmt(f),
            Self::FiscalYear(year) => {
                let next_year = (year.0.get() % 100) + 1;
                write!(f, "{}-{}", year, next_year)
            },
            Self::BiAnnually(year, half_year) => {
                // e.g. 2009 Jan-Jun
                write!(f, "{} {}", year, half_year)
            },
            Self::Quarterly(year, quarter) => {
                // e.g. 2014 Jul-Sep
                write!(f, "{} {}", year, quarter)
            },
            Self::Monthly(report) => report.fmt(f)
        }
    }
}

impl Month {
    pub fn trim_start_matches_from<'v>(&self, value: &'v str) -> &'v str {
        let short_name = &self.name()[..3];
        if value.starts_with(short_name) {
            // Trim the short name
            if value.len() >= 4 && &value[3..4] == &self.name()[3..4] {
                // Matches four-character short name
                &value[4..]
            } else {
                // Matches three-character short name
                &value[3..]
            }
        } else {
            value
        }
    }

    pub fn trim_end_matches_from<'v>(&self, value: &'v str) -> &'v str {
        let name = self.name();
        for name in [&name[..4], &name[..3]] {
            if value.ends_with(name) {
                return &value[..value.len() - name.len()];
            }
        }
        value
    }
}

impl Quarter {
    pub fn as_str(&self) -> &str {
        match *self {
            Self::JanFebMar => "Jan-Mar",
            Self::AprMayJun => "Apr-Jun",
            Self::JulAugSep => "Jul-Sep",
            Self::OctNovDec => "Oct-Dec"
        }
    }
}

impl MonthBounds for Quarter {
    fn start_and_end_month(&self) -> (Month, Month) {
        match *self {
            Self::JanFebMar => (Month::January, Month::March),
            Self::AprMayJun => (Month::April, Month::June),
            Self::JulAugSep => (Month::July, Month::September),
            Self::OctNovDec => (Month::October, Month::December)
        }
    }
}

impl Display for Quarter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl HalfYear {
    pub fn as_str(&self) -> &str {
        match *self {
            Self::JanThruJun => "Jan-Jun",
            Self::JulThruDec => "Jul-Dec"
        }
    }
}

impl MonthBounds for HalfYear {
    fn start_and_end_month(&self) -> (Month, Month) {
        match *self {
            HalfYear::JanThruJun => (Month::January, Month::June),
            HalfYear::JulThruDec => (Month::July, Month::December)
        }
    }
}

impl Display for HalfYear {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use super::*;
    #[test]
    fn size_of_timestamp() {
        assert!(std::mem::size_of::<Timestamp>() <= std::mem::size_of::<u64>())
    }

    #[test]
    fn order_halfyear() {
        assert!(HalfYear::JanThruJun < HalfYear::JulThruDec);
        assert!(HalfYear::JulThruDec > HalfYear::JanThruJun);
        assert_eq!(Ordering::Equal, HalfYear::JanThruJun.cmp(&HalfYear::JanThruJun));
    }

    #[test]
    fn order_quarter() {
        let mut quarters = vec![
            Quarter::AprMayJun, Quarter::OctNovDec, Quarter::JanFebMar, Quarter::JulAugSep
        ];
        quarters.sort();
        assert_eq!(
            vec![Quarter::JanFebMar, Quarter::AprMayJun, Quarter::JulAugSep, Quarter::OctNovDec],
            quarters
        )
    }

    #[test]
    fn trim_using_september() {
        let month = Month::September;
        assert_eq!("", month.trim_start_matches_from("Sep"));
        assert_eq!("--1", month.trim_start_matches_from("Sep--1"));
        assert_eq!("", month.trim_end_matches_from("Sep"));
        assert_eq!("1--", month.trim_end_matches_from("1--Sep"));
        assert_eq!("", month.trim_start_matches_from("Sept"));
        assert_eq!("--1", month.trim_start_matches_from("Sept--1"));
        assert_eq!("", month.trim_end_matches_from("Sept"));
        assert_eq!("1--", month.trim_end_matches_from("1--Sept"));

        assert_eq!("Dec", month.trim_end_matches_from("Dec"), "Irrelevant");
        assert_eq!("Dec", month.trim_start_matches_from("Dec"), "Irrelevant");
    }

    #[test]
    fn trim_using_january() {
        let month = Month::January;
        assert_eq!("-Mar", month.trim_start_matches_from("Jan-Mar"));
    }

    #[test]
    fn display_report() {
        let year_2009 = Year(NonZeroU16::new(2009).unwrap());
        assert_eq!("2009-01",
                   MonthlyReport::new(year_2009, Month::January).to_string());
        assert_eq!("2009-11",
                   MonthlyReport::new(year_2009, Month::November).to_string());
    }

    #[test]
    fn quarter_bounds() {
        for quarter in Quarter::values() {
            let (start, end) = quarter.start_and_end_month();
            assert_eq!(2, end.as_numeric() - start.as_numeric());
        }
    }

    #[test]
    fn halfyear_bounds() {
        for halfyear in HalfYear::values() {
            let (start, end) = halfyear.start_and_end_month();
            assert_eq!(5, end.as_numeric() - start.as_numeric());
        }
    }

    #[test]
    fn all_months_present() {
        let mut months_map = HashSet::new();
        months_map.extend(Month::values());
        assert_eq!(12, months_map.len());
    }
}
