
from datetime import datetime
from plistlib import dumps

from airflow import DAG
from airflow.operators.python import PythonOperator
import websocket
import json
from kafka import KafkaProducer
import uuid
import threading

from kafka import KafkaProducer
from queue import Queue

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms=5000)
message_queue = Queue()
def format_data(res):
    data = {}
    data['id'] = str(uuid.uuid4())
    data['symbol'] = res.get('s', 'Unknown')
    data['price'] = res.get('p', 'Unknown')
    data['timestamp'] = res.get('t', 'Unknown')
    data['volume'] = res.get('v', 'Unknown')
    return data




# Fonction WebSocket : Connexion et gestion des messages reçus

def on_message(ws, message):
    try:
        message = json.loads(message)
        if "data" in message:
            for res in message["data"]:
                formatted_data = format_data(res)
                print(f"Message reçu : {formatted_data}")  # Affiche chaque donnée reçue en temps réel
                message_queue.put(formatted_data)
    except json.JSONDecodeError:
        print("Erreur lors du décodage du message.")

def on_error(ws, error):
    print("Erreur : ", error)

def on_close(ws, close_status_code, close_msg):
    print(f"### Connexion fermée ###\nCode : {close_status_code}, Message : {close_msg}")

def on_open(ws):
    ws.send('{"type":"subscribe","symbol":"AAPL"}')
    ws.send('{"type":"subscribe","symbol":"AMZN"}')
    ws.send('{"type":"subscribe","symbol":"BINANCE:BTCUSDT"}')
    ws.send('{"type":"subscribe","symbol":"IC MARKETS:1"}')

def connect_to_websocket():
    ws = websocket.WebSocketApp(
    "wss://ws.finnhub.io?token=cv7rk19r01qpecih8e40cv7rk19r01qpecih8e4g",
    on_message=on_message,
    on_error=on_error,
    on_close=on_close
)
    ws.on_open = on_open
    return ws

def send_to_kafka(queue_name):
    while True:
        message = queue_name.get()
        if message is None:
            break
        producer.send('new_price4', value=json.dumps(message).encode('utf-8'))
        producer.flush()
        print(f"Message envoyé au topic")

ls

def stream_data():
    import json
    import requests

    kafka_thread = threading.Thread(target=send_to_kafka, args=(message_queue,))
    kafka_thread.start()


    connexion = connect_to_websocket()
    connexion.run_forever()
    message_queue.put(None)
    kafka_thread.join()


stream_data()

# with DAG('user_automation',
#          default_args=default_args,
#          schedule_interval='@daily'
#          Catchup=False) as dag:
#     streaming_task = PythonOperator(
#         task_id='stream_data_from_api'
#         python_callable=stream_data
#     )
