import argparse
import logging
import sys
import apache_beam as beam
from apache_beam.dataframe.io import read_csv
from apache_beam.dataframe.frames import DeferredSeries, DeferredDataFrame

logging.basicConfig(datefmt='%Y/%m/%d %H:%M:%S', level=logging.INFO,
                    format='%(asctime)s: %(name)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.StreamHandler(sys.stdout)
                    ])


def filter_results(df: DeferredDataFrame) -> DeferredDataFrame:
    """
    Apply transformations to a pandas dataframe which
    filter out transactional amounts under 20 and those with
    a timestamp during or after 2010
    :param df: Beam DeferredDataFrame
    :return: Filtered Beam DeferredDataFrame
    """
    df = df[df['transaction_amount'] > 20]
    return df[df['timestamp'] >= '2010-01-01']


def convert_to_date(df: DeferredDataFrame) -> DeferredSeries:
    """
    Convert the timestamp field to a date datatype
    :param df: Beam Dataframe
    :return: Beam DeferredSeries
    """
    return df['timestamp'].astype('datetime64').apply(lambda x: x.date())


def aggregate_results(df: DeferredSeries) -> DeferredSeries:
    """
    Sum total amount by date and return a grouped dataframe
    :param df: Pandas Dataframe
    :return: Pandas series
    """
    return df.groupby('date').transaction_amount.sum().rename('total_amount')


def run(file: str) -> None:
    """Entry point"""
    logger = logging.getLogger(__name__)
    logger.info(f'Starting Application: input path: {file}')
    with beam.Pipeline(runner='DirectRunner') as p:
        df = p | read_csv(file)
        logger.info('Filtering Results')
        df = filter_results(df)
        logger.info('Converting Date')
        df['date'] = convert_to_date(df)
        logger.info('Aggregating Results')
        agg = aggregate_results(df)
        logger.info('Writing to CSV')
        agg.to_csv('output/results')
    logger.info("Application Complete")


if __name__ == '__main__':

    # path = 'gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv'
    parser = argparse.ArgumentParser()
    parser.add_argument("path")
    args = parser.parse_args()
    run(args.path)