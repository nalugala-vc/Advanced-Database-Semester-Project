import pandas as pd
import mysql.connector


class LoadingClass:
    def load_data_one(self, tranformed_data, connection):
        cursor = connection.cursor()
        # explain what a connection cursor is

        # creating country table
        create_country_table = '''
            CREATE TABLE IF NOT EXISTS country_table (
                id INT AUTO_INCREMENT PRIMARY KEY,
                country VARCHAR(255),
                iso VARCHAR(255)
            )
         '''
        cursor.execute(create_country_table)

        # formatting
        country_data = tranformed_data[['country', 'ISO']].copy()

        #removing duplicates
        country_data['country'] = country_data['country'].str.lower().str.strip()
        country_data['ISO'] = country_data['ISO'].str.lower().str.strip()
        country_data = country_data.drop_duplicates(subset='country')

        country_data['country'] = country_data['country'].str.title()
        country_data['ISO'] = country_data['ISO'].str.upper()

        country_records = country_data.values.tolist()
        # explain why we change it to a list

        # inserting records into country table
        insert_country_records = '''
            INSERT INTO country_table (country,iso) VALUES (%s,%s)
        '''

        cursor.executemany(insert_country_records, country_records)

        # sector table
        create_sector_table = '''
            CREATE TABLE IF NOT EXISTS sector_table (
                id INT AUTO_INCREMENT PRIMARY KEY,
                sector_name VARCHAR(255)  
            )  
        '''
        cursor.execute(create_sector_table)
        sector_data = tranformed_data[['sector']].copy()
        sector_data = sector_data.drop_duplicates()
        sector_records = sector_data.values.tolist()

        insert_sector_records = '''
            INSERT INTO sector_table (sector_name) VALUES (%s)
        '''

        cursor.executemany(insert_sector_records, sector_records)

        # loan theme type table
        create_loan_theme_type_table = '''
                    CREATE TABLE IF NOT EXISTS loan_theme_type_table (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        loan_theme_type VARCHAR(255)   
                    ) 
                '''
        cursor.execute(create_loan_theme_type_table)
        loan_theme_data = tranformed_data[['Loan Theme Type']].copy()
        loan_theme_data = loan_theme_data.drop_duplicates()
        loan_theme_records = loan_theme_data.values.tolist()

        insert_loan_theme_records = '''
                    INSERT INTO loan_theme_type_table (loan_theme_type) VALUES (%s)
                '''

        cursor.executemany(insert_loan_theme_records, loan_theme_records)

        connection.commit()
        cursor.close()
        connection.close()

        return 'loaded data successfully'

    # loading for qs2
    def load_data_two(self,transformed_data, connection):
        cursor = connection.cursor()

        create_region_table = '''
            CREATE TABLE IF NOT EXISTS region_table(
                id INT AUTO_INCREMENT PRIMARY KEY,
                region VARCHAR(255)
            )
        '''

        cursor.execute(create_region_table)

        region_data = transformed_data[['region']].copy()
        region_data = region_data.drop_duplicates()
        region_records = region_data.values.tolist()

        insert_region_table = '''
            INSERT INTO region_table (region) VALUES (%s)
        '''

        cursor.executemany(insert_region_table, region_records)

        create_repayment_interval_table = '''
            CREATE TABLE IF NOT EXISTS repayment_interval_table(
                id INT AUTO_INCREMENT PRIMARY KEY,
                repayment_interval VARCHAR(255)
            )
        '''

        cursor.execute(create_repayment_interval_table)

        repayment_interval_data = transformed_data[['repayment_interval']].copy()
        repayment_interval_data = repayment_interval_data.drop_duplicates()
        repayment_records = repayment_interval_data.values.tolist()

        insert_repayment_interval_table = '''
                INSERT INTO repayment_interval_table (repayment_interval) VALUES (%s)
            '''

        cursor.executemany(insert_repayment_interval_table, repayment_records)

        create_expected_return_time_table = '''
                CREATE TABLE IF NOT EXISTS expected_return_time_table(
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    expected_return_time DATETIME
                )
            '''

        cursor.execute(create_expected_return_time_table)

        expected_return_time_data = transformed_data[['expected_return_time']].copy()
        expected_return_time_data = expected_return_time_data.drop_duplicates()
        print(expected_return_time_data)
        expected_return_time_data['expected_return_time'] = expected_return_time_data['expected_return_time'].astype(
            str)
        expected_return_time = expected_return_time_data.values.tolist()

        insert_expected_return_time_table = '''
                    INSERT INTO expected_return_time_table (expected_return_time) VALUES (%s)
                '''

        cursor.executemany(insert_expected_return_time_table, expected_return_time)

        connection.commit()
        cursor.close()
        connection.close()

        return 'Loading successful'

    # loading for qs3
    def load_data_three(self,transformed_data, connection):
        cursor = connection.cursor()

        create_date_table = '''
            CREATE TABLE IF NOT EXISTS date_table(
                id INT AUTO_INCREMENT PRIMARY KEY,
                date DATE
            )
        '''

        cursor.execute(create_date_table)

        date_data = pd.to_datetime(transformed_data['date'])
        date_data = date_data.drop_duplicates()
        date_records = [(date,) for date in date_data]

        insert_date_table = '''
            INSERT INTO date_table (date) VALUES (%s)
        '''

        cursor.executemany(insert_date_table, date_records)

        connection.commit()
        cursor.close()
        connection.close()

        # region table is already created

        return 'Loading successful'

