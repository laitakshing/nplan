from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import pubsub_v1
from google.cloud import bigquery
import apache_beam as beam
import logging
import datetime
import json
import argparse
from top import TopDistinctFn
import time


def parse_pubsub(line):

    record = json.loads(str(line))
    return record


def run(input_topic="projects/tak-playground/topics/one_min", schema='time:TIMESTAMP, top3:STRING', window_size=60, pipeline_args=None):
    """
    Details dataflow data transfomation:
    1. transfer the string message to json
    2. transfer the json to a list of key value tuple e.g.[("FUTU",252),("GME",80)]
    3. Assign hopping window
    4. Flapmap the list of tuple
    5. Group the stock tuple by key with the mean price
    6. Use TopDistinctFn to get the top 3 highest price stocks
    7. Add timestamp to the result
    8. Write the result to Bigquery

    Args:
        input_topic (str): PubSub topics
        schema (str): Big Query table schema
        window_size(int): how long is the period of window in second
        pipeline_args (str): Any other beam pipeline options
    """
    pipeline_options = PipelineOptions(
        pipeline_args, streaming=True, save_main_session=True
    )

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Read PubSub Messages" >> beam.io.ReadFromPubSub(topic=input_topic).with_output_types(bytes)
            | "Decode" >> beam.Map(lambda x: x.decode('utf-8'))
            | "Parse PubSub Messages" >> beam.Map(parse_pubsub)
            | "Parse PubSub Messages to tuple" >> beam.Map(lambda elem: [(k, v) for k, v in elem.items()])
            | 'Add Window info' >> beam.WindowInto(beam.window.SlidingWindows(window_size, 10))
            | "split all stock" >> beam.FlatMap(lambda e: e)
            | 'take mean per key' >> beam.combiners.Mean.PerKey()
            | 'Top 3 stocks' >> beam.CombineGlobally(
                TopDistinctFn(n=3, compare=lambda a, b: a[1] < b[1])).without_defaults()
            | 'Add time stamp' >> beam.Map(lambda e: dict((("time", time.time()), ("top3", str(e)))))
            | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(f'tak-playground:testing.table_{str(int(window_size/60))}min', schema=schema,    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND, create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
        )


if __name__ == '__main__':  # noqa
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_topic",
        help="The Cloud Pub/Sub topic to read from.\n"
        '"projects/<PROJECT_NAME>/topics/<TOPIC_NAME>".',
    )
    parser.add_argument(
        "--window_size",
        help="Output file's window size in number of minutes.",
    )
    known_args, pipeline_args = parser.parse_known_args()

    run(
        pipeline_args,
        input_topic=known_args.input_topic,
        window_size=known_args.window_size
    )
