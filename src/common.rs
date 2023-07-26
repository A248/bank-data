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
use chrono::Month;

#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd)]
pub struct MonthlyReport {
    pub month: Month,
    pub year: i32
}

impl Ord for MonthlyReport {
    fn cmp(&self, other: &Self) -> Ordering {
        match (
            self.year.cmp(&other.year),
            self.month.number_from_month().cmp(&other.month.number_from_month())
        ) {
            (Ordering::Less, _, _) => {
                Ordering::Less
            }
            (Ordering::Greater, _, _) => {
                Ordering::Greater
            }
            (Ordering::Equal, Ordering::Less) => {
                Ordering::Less
            }
            (Ordering::Equal, Ordering::Greater) => {
                Ordering::Greater
            }
            (Ordering::Equal, Ordering::Equal) => {
                Ordering::Equal
            }
        }
    }
}
