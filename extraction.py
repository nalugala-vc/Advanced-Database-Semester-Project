import pandas as pd
import mysql.connector


class ExtractionClass:

    # step 1 : extract data
    def extract_data(self, csv_file):
        kiva_loans_data = pd.read_csv(csv_file)
        return kiva_loans_data
