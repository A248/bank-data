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

import os

from common import info, warn
from download import Download
from merge import MergeXL


# Start of program to let user choose behavior
def main():
    info("Program started")
    data_dir = input("Define the dataset directory (default: data):")
    if data_dir == "":
        data_dir = "data"
    elif data_dir.__len__() == 1:
        warn(
            "One-character data directories are not allowed (A mistake commonly caused by jumping to the question)"
        )
        exit(1)
    os.makedirs(data_dir)
    # Keep asking the user until we receive a valid answer
    while True:
        option = input("Choose whether to download new datasets, or condense the existing ones"
                       "\n1. Download new"
                       "\n2. Condense existing"
                       "\nYour choice:")
        if option == "1":
            download_new(data_dir)
        elif option == "2":
            condense_existing(data_dir)
        else:
            # Invalid answer so keep asking
            continue
        # Found a valid answer and the action already happened
        break
    info("Program finished")


def condense_existing(data_dir):
    info("Condensing existing datasets")
    mergexl = MergeXL()
    for file in os.listdir(data_dir):
        mergexl.add_source(os.path.join(data_dir, file))
    mergexl.write_to("merged.xlsx")


def download_new(data_dir):
    info("Downloading new datasets")
    download = Download(data_dir)
    download.download_new()


if __name__ == "__main__":
    main()
