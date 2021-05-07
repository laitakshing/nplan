from google.cloud import pubsub_v1
import os
import json
import requests
from datetime import datetime

# References an existing topic
PROJECT_ID = "tak-playground"
# projects/tak-playground/topics/one_min
topic_name = "one_min"


def get_stream_data(request=None, stock_list="futu,aapl,pdd,msft,fb,bidu,gme,baba,bili,kod&types=quote", token="Tpk_45a453752ec8497f86a9527198cc7af2"):
    """
    Access endpoint by useing request, transfer it into json and then publish to pubsub topic.

    Args:
        request: cloud function return HTTP body, we won't use heree
        stock_list (str): list of stocks, default 10 stocks
        token: IEXcloud endpoint token
    """

    publisher = pubsub_v1.PublisherClient()

    topic_path = publisher.topic_path(PROJECT_ID, topic_name)
    try:
        response = requests.get(
            f"https://sandbox.iexapis.com/stable/stock/market/batch?symbols={stock_list}&token={token}")
        j = response.json()
        final_dict = {}
        for k, v in j.items():
            final_dict[k] = v['quote']['latestPrice']

        publish_future = publisher.publish(
            topic_path, data=json.dumps(final_dict).encode('utf-8'))
        publish_future.result()  # Verify the publish succeeded
        return 'Message published.'
    except Exception as e:
        print(e)
        return (e, 500)
