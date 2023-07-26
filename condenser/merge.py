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

import openpyxl

class MergeXL:
    def __init__(self):
        self.sheets = dict()

    def add_source(self, source_xl: str):
        workbook = openpyxl.load_workbook(open(source_xl, 'rb'))


    def write_to(self, output: str):
        pass

    def get_or_create_sheet(self, name: str):
        existing = self.sheets[name]
        if existing is not None:
            return existing
        new = Sheet(set(), dict())
        self.sheets[name] = new
        return new

class Sheet:
    def __init__(self, columns: set, rows: dict):
        self.columns = columns
        self.rows = rows

    def ensure_column(self, column):
        self.columns.add(column)

    def add_row(self, timestamp, row):
        for (col, val) in row.data:
            self.ensure_column(col)
        self.rows[timestamp] = row


class RowData:
    def __init__(self, data: dict):
        self.data = data


class Timestamp:
    def __init__(self, day: int, month: int, year: int):
        self.day = day
        self.month = month
        self.year = year
