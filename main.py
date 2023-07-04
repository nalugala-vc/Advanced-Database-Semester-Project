import pandas as pd
import mysql.connector
from extraction import ExtractionClass
from transformation import TransformationClass
from loading import LoadingClass


# create mysql connection
def mysql_connection():
    try:
        connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='romulemia01',
            database='etl_pipeline'
        )
        return connection
    except Exception as e:
        # Handle any errors that occurred during the loading process
        print(f"An error occurred: {str(e)}")


# connection = mysql_connection()
# cursor = connection.cursor()


# closing mysql connection
def close_connection(connection):
    connection.commit()
    connection.close()


# executing etl pipeline
def execute_etl_pipeline(csv_file, required_columns):
    # connect to database
    connection = mysql_connection()

    # Step 1: Extraction
    extraction = ExtractionClass()
    data = extraction.extract_data(csv_file)

    # Step 2: Transformation
    transformation = TransformationClass()
    transformed_data = transformation.transform_data_three(data, required_columns)

    # Step 3: Loading
    loading = LoadingClass()
    loading.load_data_three(transformed_data, connection)

    return 'etl pipelining successful'

#extablishing mysql connection
connection = mysql_connection()

# qsn 1
# The type of loans mostly applied for
# csv_file = '../loan_themes_by_region.csv'
# required_columns = ['Loan Theme Type', 'sector', 'country', 'ISO']
# execute_etl_pipeline(csv_file, required_columns)

# qsn 2
# The repayment trend in the different regions with the passing of time
# csv_file = '../kiva_loans.csv'
# required_columns = ['region','disbursed_time','term_in_months','posted_time','repayment_interval','country']
# execute_etl_pipeline(csv_file, required_columns)

#qsn 3
# The demographic trend of the fundraisers across different regions in relation to time of the year
csv_file = '../kiva_loans.csv'
required_columns = ['id','date','loan_amount','funded_amount','lender_count','country','region','borrower_genders']
etl_pipeline = execute_etl_pipeline(csv_file,required_columns)


print(etl_pipeline)
