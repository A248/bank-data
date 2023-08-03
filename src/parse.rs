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


// Parsing

use std::num::NonZeroU16;
use std::str::FromStr;
use std::sync::OnceLock;
use regex::Regex;
use crate::common::*;

#[derive(Debug, PartialEq, Eq)]
pub struct CannotParse(());

impl CannotParse {
    /// Legacy method I keep here in case I need to refactor CannotParse
    fn simply() -> Self {
        Self(())
    }
}

impl<E> From<E> for CannotParse where E: std::error::Error + Send + Sync + 'static {
    fn from(_error: E) -> Self {
        Self(())
    }
}

macro_rules! impl_from_str_with_pat {
    ($strct:ty, $pat_constant:ident, $pat:literal, $unchecked_create:ident, $tests_module:ident) => {

        static $pat_constant: OnceLock<Regex> = OnceLock::new();

        impl FromStr for $strct {
            type Err = CannotParse;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                let pattern = $pat_constant.get_or_init(||
                    Regex::new($pat).expect("Regex compilation failure")
                );
                if pattern.shortest_match(value) != Some(value.len()) {
                    return Err(CannotParse::simply());
                }
                $unchecked_create(value)
            }
        }

        #[cfg(test)]
        mod $tests_module {
            use super::*;
            #[test]
            fn pattern_validation() {
                // Test for non-panicking pattern
                let _res: Result<$strct, CannotParse> = "".parse();
            }
        }
    }
}

fn impl_parse_year(value: &str) -> Result<Year, CannotParse> {
    let year: u16 = value.parse()?;
    Ok(Year(NonZeroU16::try_from(year)?))
}

impl_from_str_with_pat!(Year, YEAR_PATTERN, "^[0-9]{4}$", impl_parse_year, test_year_from_str);

fn impl_parse_report(value: &str) -> Result<MonthlyReport, CannotParse> {
    let year: u16 = value[0..4].parse()?;
    let month: u8 = value[5..].parse()?;
    Ok(MonthlyReport {
        year: Year(NonZeroU16::try_from(year)?),
        month: Month::try_from(month)?
    })
}

impl_from_str_with_pat!(
    MonthlyReport, MONTHLY_REPORT_PATTERN, "^[0-9]{4}-([0-9]{2}|[0-9])$", impl_parse_report, test_report_from_str
);

impl FromStr for YearlyTimestamp {
    type Err = CannotParse;

    fn from_str(value: &str) -> Result<Self, Self::Err> {

        // Strip trailing whitespace
        let value = value.trim_end_matches(char::is_whitespace);

        // Goal is to allow for whitespace inside fiscal years, e.g. "2009 - 10" is also valid
        const FISCAL_YEAR_LEN: usize = "2009-10".len();
        // However, calendar years are parsed rather simply and more strictly
        const CALENDAR_YEAR_LEN: usize = "2009".len();

        if value.len() == CALENDAR_YEAR_LEN {
            return Ok(YearlyTimestamp::Calendar(Year::from_str(value)?));
        }
        if value.len() >= FISCAL_YEAR_LEN {
            let year: Year = value[0..4].parse()?;
            // Need to validate rest of the string
            let suffix = &value[4..];
            if suffix.len() >= 2 {
                // Break apart the last two characters
                let last_two_chars = &suffix[suffix.len() - 2..];
                let interior = &suffix[..suffix.len() - 2];

                // The last two characters should be the next year
                let next_year: u16 = last_two_chars.parse()?;

                // The interior, excluding whitespace, should be '-'
                if interior.trim() == "-" {
                    assert_eq!(
                        (year.0.get() + 1) % 100,
                        next_year,
                        "Invalid fiscal year"
                    );
                    return Ok(YearlyTimestamp::Fiscal(year));
                }

            }
        }
        Err(CannotParse::simply())
    }
}


impl Timestamp {
    /// Reparses a timestamp from a value we already displayed.
    fn from_displayed_value(value: &str) -> Result<Self, CannotParse> {
        if let Ok(year) = Year::from_str(value) {
            return Ok(Self::CalendarYear(year));
        }
        if let Ok(report) = MonthlyReport::from_str(value) {
            return Ok(Self::Monthly(report));
        }
        // Length for both quarterly and biannual data
        const PARTIAL_YEAR_LEN: usize = "2009 Jan-Jun".len();
        if value.len() == PARTIAL_YEAR_LEN {
            // Parse year
            if let Ok(year) = value[0..4].parse::<Year>() {
                // Parse remainder
                let remainder = &value[5..];
                if let Ok(quarter) = Quarter::from_str(remainder) {
                    return Ok(Self::Quarterly(year, quarter));
                }
                if let Ok(halfyear) = HalfYear::from_str(remainder) {
                    return Ok(Self::BiAnnually(year, halfyear));
                }
            }
        }
        Err(CannotParse::simply())
    }
}

impl TryFrom<(Year, &str)> for Timestamp {
    type Error = CannotParse;

    fn try_from((year, remainder): (Year, &str)) -> Result<Self, Self::Error> {
        if let Ok(month) = Month::from_str(remainder) {
            Ok(Timestamp::Monthly(MonthlyReport::new(year, month)))
        } else if let Ok(quarter) = Quarter::from_str(remainder) {
            Ok(Timestamp::Quarterly(year, quarter))
        } else if let Ok(halfyear) = HalfYear::from_str(remainder) {
            Ok(Timestamp::BiAnnually(year, halfyear))
        } else {
            Err(CannotParse::simply())
        }
    }
}

macro_rules! impl_from_str_using_start_end_months {
    ($strct:ident, $values:expr) => {
        impl $strct {
            pub fn values() -> impl IntoIterator<Item=Self> {
                $values
            }
        }

        impl FromStr for $strct {
            type Err = CannotParse;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                // Several requirements
                // 1. Permit empty spaces between months and hyphen
                // 2. Handle trailing periods before the start or end month
                // 3. Allow four- or three-letter month names

                let value = value.trim();
                if value.ends_with('.') {
                    return Self::from_str(value.trim_end_matches('.'));
                }
                for try_me in Self::values() {
                    let (start_month, end_month) = try_me.start_and_end_month();

                    // Remove leading month e.g. Jul/July
                    let value = start_month.trim_start_matches_from(value);
                    // Remove trailing month e.g. Sep/Sept
                    let mut value = end_month.trim_end_matches_from(value);

                    if value.starts_with('.') {
                        value = value.trim_start_matches('.');
                    }
                    if value.trim() == "-" {
                        return Ok(try_me);
                    }
                }
                Err(CannotParse::simply())
            }
        }
    }
}

impl_from_str_using_start_end_months!(HalfYear, [HalfYear::JanThruJun, HalfYear::JulThruDec]);
impl_from_str_using_start_end_months!(Quarter, [Quarter::JanFebMar, Quarter::AprMayJun, Quarter::JulAugSep, Quarter::OctNovDec]);

impl TryFrom<u8> for Month {
    type Error = CannotParse;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Ok(Self::from_chrono(chrono::Month::try_from(value)?))
    }
}

impl FromStr for Month {
    type Err = CannotParse;

    fn from_str(value: &str) -> Result<Self, Self::Err> {

        // Strip trailing whitespace
        let mut value = value.trim_end_matches(char::is_whitespace);
        // Remove trailing period if it exists
        if value.ends_with('.') {
            value = &value[..value.len() - 1];
        }

        let chrono = match chrono::Month::from_str(value) {
            Ok(mo) => mo,
            Err(_) => {
                if value == "Fabruary" {
                    // Hooray for spelling
                    return Ok(Self::February);
                }
                return Err(CannotParse::simply())
            }
        };
        Ok(Self::from_chrono(chrono))
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use super::*;

    #[test]
    fn parse_year() {
        assert_eq!(Year(NonZeroU16::new(2009).unwrap()), "2009".parse::<Year>().unwrap());
        assert_matches!("20090".parse::<Year>(), Err(_));
        assert_matches!("02009".parse::<Year>(), Err(_));
        assert_matches!("hello".parse::<Year>(), Err(_))
    }

    #[test]
    fn parse_fiscal_year() {
        fn is_fiscal_year(value: &str) -> bool {
            if let Ok(YearlyTimestamp::Fiscal(_)) = YearlyTimestamp::from_str(value) {
                true
            } else {
                false
            }
        }
        assert!(is_fiscal_year("2022-23"));
        assert!(is_fiscal_year("2009-10"));
        assert!(is_fiscal_year("2010-11"));
        assert!(is_fiscal_year("2017-18"));
        assert!(!is_fiscal_year("July"));
        assert!(!is_fiscal_year("hello"));
    }

    #[test]
    fn parse_calendar_year() {
        fn is_calendar_year(value: &str) -> bool {
            if let Ok(YearlyTimestamp::Calendar(_)) = YearlyTimestamp::from_str(value) {
                true
            } else {
                false
            }
        }
        assert!(is_calendar_year("2022"));
        assert!(is_calendar_year("2009"));
        assert!(is_calendar_year("2010"));
        assert!(is_calendar_year("2017"));
        assert!(!is_calendar_year("July"));
        assert!(!is_calendar_year("hello"));
    }

    #[test]
    fn parse_quarter() {
        fn assert_parse_quarter(expected: Quarter, from_what: &str) {
            let year = Year(NonZeroU16::new(2009).unwrap());
            assert_eq!(Ok(expected), Quarter::from_str(from_what));
            assert_eq!(Ok(Timestamp::Quarterly(
                year, expected
            )), Timestamp::try_from((year, from_what)));
        }
        assert_parse_quarter(Quarter::JanFebMar, "Jan-Mar");
        assert_parse_quarter(Quarter::JanFebMar, "Jan- Mar");
        assert_parse_quarter(Quarter::JanFebMar, "Jan -Mar");
        assert_parse_quarter(Quarter::JanFebMar, "Jan  - Mar");
        assert_parse_quarter(Quarter::OctNovDec, "Oct.-Dec");
        assert_parse_quarter(Quarter::OctNovDec, "Oct-Dec.");
        assert_parse_quarter(Quarter::JulAugSep, "Jul-Sep");
        assert_parse_quarter(Quarter::JulAugSep, "Jul- Sep");
        assert_parse_quarter(Quarter::JulAugSep, "July- Sep");
    }
}
