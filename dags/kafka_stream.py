from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import websocket
import json
import uuid

default_args = {
    'owner': 'Warsame',
    'start_date': datetime(2025, 3, 14, 23,00)
}


def format_data(res):
    data = {}
    data['id'] = uuid.uuid4()
    data['symbol'] = res.get('s', 'Unknown')
    data['price'] = res.get('p', 'Unknown')
    data['timestamp'] = res.get('t', 'Unknown')
    data['volume'] = res.get('v', 'Unknown')
    return data


def get_data():
    data_received = []

    def on_message(ws, message):
        try:
            message = json.loads(message)
            if "data" in message:
                for res in message["data"]:
                    #formatted_data = format_data(res)
                    #data_received.append(formatted_data)
                    print(json.dumps(res, indent=3))  # Affiche chaque donnée reçue en temps réel
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

    ws = websocket.WebSocketApp(
        "wss://ws.finnhub.io?token=cv7rk19r01qpecih8e40cv7rk19r01qpecih8e4g",
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.on_open = on_open
    ws.run_forever()  # Ceci est bloquant, donc rien après cette ligne ne s'exécutera.

    # Pas de "return" ici, car ça arrêterait immédiatement l'écoute




def stream_data():
    import json
    import requests
    res = get_data()
    res = format_data(res)
    print(json.dumps(res, indent=3))

stream_data()

# with DAG('user_automation',
#          default_args=default_args,
#          schedule_interval='@daily'
#          Catchup=False) as dag:
#     streaming_task = PythonOperator(
#         task_id='stream_data_from_api'
#         python_callable=stream_data
#     )