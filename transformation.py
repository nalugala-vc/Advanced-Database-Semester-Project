import pandas as pd
import mysql.connector


class TransformationClass:
    # step 2 : transform data
    # transforming depending on The type of loans mostly applied for

    def transform_data_one(self, extracted_data, required_columns):
        transformed_df = extracted_data[required_columns].copy()
        # we should explain why we use .copy()

        # cleaning data for the country dimension table
        transformed_df.loc[transformed_df['country'] == 'Kosovo', 'ISO'] = 'XK'
        transformed_df.loc[transformed_df['country'] == "Cote D'Ivoire", 'ISO'] = 'CIV'

        return transformed_df

    def transform_data_two(self, extracted_data, required_columns):
        # we will use repayment interval
        transformed_df = extracted_data[required_columns].copy()

        transformed_df['disbursed_time'] = pd.to_datetime(transformed_df['disbursed_time']).dt.strftime(
            '%Y-%m-%d %H:%M:%S')
        transformed_df['posted_time'] = pd.to_datetime(transformed_df['posted_time']).dt.strftime('%Y-%m-%d %H:%M:%S')

        # subtract posted time from dibursment time
        transformed_df['time_diff'] = pd.to_datetime(transformed_df['posted_time']) - pd.to_datetime(
            transformed_df['disbursed_time'])

        # finding median
        median_time_diff = transformed_df['time_diff'].median()

        # creating approximate values for dibursment time
        null_distributed_time = transformed_df['disbursed_time'].isnull()
        transformed_df.loc[null_distributed_time, 'disbursed_time'] = pd.to_datetime(
            transformed_df.loc[null_distributed_time, 'posted_time']) + median_time_diff

        # sorting regions that were empty
        transformed_df['region'] = transformed_df['region'].fillna(transformed_df['country'])

        # expected return time

        # Convert 'time_in_months' to numeric
        transformed_df['term_in_months'] = transformed_df['term_in_months'].transform(
            lambda x: pd.DateOffset(months=x))

        transformed_df['expected_return_time'] = pd.to_datetime(transformed_df['disbursed_time']) + transformed_df['term_in_months']

        return transformed_df

    def transform_data_three(self,extracted_data, required_columns):
        transformed_df = extracted_data[required_columns].copy()

        transformed_df['region'] = transformed_df['region'].fillna(transformed_df['country'])
        transformed_df = transformed_df.rename(columns={'funded_amount': 'amount'})

        return transformed_df
