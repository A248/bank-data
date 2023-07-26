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

import datetime
from os import path
import requests

from common import info, warn, MonthlyDataReport, Month


def current_year():
    the_year = datetime.datetime.now().year
    info("The current year is " + str(the_year))
    return the_year


class Download:
    def __init__(self, data_dir):
        self.data_dir = data_dir

    # Download all the new spreadsheets
    def download_new(self):
        for year in range(2012, current_year() + 1):
            for month in Month.all_months():
                report = MonthlyDataReport(year, month)
                if report.has_no_data():
                    # One of the year/month combinations for which the central bank has no data
                    continue
                if self.download_if_possible(report):
                    info(f"Data from {month} {year} available")

    # True means file exists or was downloaded, false if the URL does not exist (the bank has not posted the data)
    def download_if_possible(self, report: MonthlyDataReport) -> bool:
        filename = report.locate_excel_file(self.data_dir)
        # If the file already exists, all good
        if path.isfile(filename):
            return True
        urls = report.locate_possible_urls()
        for url in urls:
            # Download the URL and save it to the file
            # https://stackoverflow.com/questions/16694907/download-large-file-in-python-with-requests
            with requests.get(url, stream=True) as request:
                if request.status_code == 404:
                    # Report not yet published
                    warn(f"URL {url} failed")
                    continue
                request.raise_for_status()
                if request.url not in urls:
                    # Signifies a URL redirect
                    # The bank's website tends to redirect users rather than giving 404s
                    warn(f"URL {url} failed")
                    continue
                info(f"Downloading {url}...")
                with open(filename, 'wb') as file:
                    for chunk in request.iter_content(chunk_size=8192):
                        file.write(chunk)
                return True
        return False
