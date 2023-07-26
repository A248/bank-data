#  bank-data
#  Copyright © 2023 Centre for Policy Dialogue
#
#  bank-data is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  bank-data is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with bank-data. If not, see <https://www.gnu.org/licenses/>
#  and navigate to version 3 of the GNU General Public License.

import calendar
import datetime


# Logging system which includes date and colors automatically
def log_msg(log: str, color: str):
    print("\033[94m[", end="")
    print(datetime.datetime.now(), end="")
    print("]", end="")
    print(color, end=" ")
    print(log)


# Info message
def info(log: str):
    log_msg(log, "]\033[92m")


# Warning message
def warn(log: str):
    log_msg("WARNING: " + log, "\033[93m")


class Month:
    def __init__(self, month_index: int):
        self.month_index = month_index

    def short_name(self):
        arr = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
        return arr[self.month_index]

    def numerical(self):
        # Only developers use zero-based indices
        return self.month_index + 1

    def __str__(self):
        return calendar.month_name[1 + self.month_index]

    @staticmethod
    def all_months():
        return [Month(month_index) for month_index in range(12)]


# Represents a monthly report released by the central bank
class MonthlyDataReport:
    def __init__(self, year: int, month: Month):
        self.year = year
        self.month = month

    def locate_excel_file(self, data_dir: str) -> str:
        return f"{data_dir}/{self.year}-{self.month.numerical()}.xlsx"

    def locate_possible_urls(self):
        short_year = str(self.year)[2:]
        short_month = self.month.short_name()
        capital_short_name = short_month[0].upper() + short_month[1:]
        lower_month = str(self.month).lower()
        return [
            f"https://www.bb.org.bd/pub/monthly/econtrds/et{self.month.short_name()}{short_year}.xlsx",
            f"https://www.bb.org.bd/pub/monthly/econtrds/econtrends_{self.month}{self.year}.xlsx",
            f"https://www.bb.org.bd/pub/monthly/econtrds/econtrends_{lower_month}{self.year}.xlsx",
            f"https://www.bb.org.bd/pub/monthly/econtrds/ET{capital_short_name}{short_year}.xlsx",
            f"https://www.bb.org.bd/pub/monthly/econtrds/econtrends_{short_month}{self.year}.xlsx",
            f"https://www.bb.org.bd/pub/monthly/econtrds/{short_month}{short_year}/statisticaltable.xlsx",
            f"https://www.bb.org.bd/pub/monthly/econtrds/{short_month}{short_year}/statisticaltable.xls",
            f"https://www.bb.org.bd/pub/monthly/econtrds/{lower_month}{short_year}/statisticaltable.xlsx"
        ]

    def __str__(self):
        return "monthly report for " + str(self.month) + " " + str(self.year)

    def has_no_data(self):
        return (self.month.short_name(), self.year) in [
            ("nov", 2015)
        ]
