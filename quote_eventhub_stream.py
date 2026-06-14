import websocket
import json
import sys
from azure.eventhub import EventHubProducerClient, EventData
from dotenv import load_dotenv
import os
# --- Configuration ---
load_dotenv()
FINNHUB_API_KEY = os.getenv("finnhub_api_key")
AZURE_CONNECTION_STR = os.getenv("AZURE_CONNECTION_STR")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME")

# Initialize the Azure Event Hub Producer Client globally
# This maintains an active TCP connection pool for optimal throughput
try:
    producer = EventHubProducerClient.from_connection_string(
        conn_str=AZURE_CONNECTION_STR, 
        eventhub_name=EVENT_HUB_NAME
    )
except Exception as e:
    print(f"❌ Failed to initialize Event Hub Client: {e}")
    sys.exit(1)

def on_message(ws, message):
    """Callback triggered when Finnhub pushes market data."""
    msg_json = json.loads(message)
    
    if msg_json.get('type') == 'trade':
        trades = msg_json['data']
        
        # Initialize an Event Hub execution batch
        event_data_batch = producer.create_batch()
        
        print(json.dumps(trades, indent=2))  # Debug: Print the raw trade data received
        
        for trade in trades:
            
              # Debug: Print the raw trade data received
            # Structuring a normalized schema for ingestion
            payload = {
                "symbol": trade['s'],
                "price": trade['p'],
                "volume": trade['v'],
                "conditions": trade['c'],  # Trade conditions (e.g., regular, auction)
                "timestamp": trade['t']  # Epoch millisecond
            }
            
            
            
            event_data = EventData(json.dumps(payload))
            
            try:
                # Attempt to pack the event into the current batch
                event_data_batch.add(event_data)
            except ValueError:
                # The batch is full (max size reached). Send it immediately.
                print("🔄 Batch size limit reached. Flushing current batch to Azure...")
                producer.send_batch(event_data_batch)
                
                # Create a brand new batch and add the skipped event
                event_data_batch = producer.create_batch()
                event_data_batch.add(event_data)
        
        # Fire off any remaining events left in the batch
        if len(event_data_batch) > 0:
            try:
                producer.send_batch(event_data_batch)
                print(f"🚀 Successfully sent {len(event_data_batch)} trade events to Event Hub.")
            except Exception as e:
                print(f"⚠️ Error dispatching batch to Azure: {e}")

def on_error(ws, error):
    print(f"⚠️ WebSocket Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print(f"🔌 WebSocket connection closed. Status: {close_status_code}")

def on_open(ws):
    print("🛰️ Connected to Finnhub Stream. Initializing subscriptions...")
    # Add your target tracking tickers here
    symbols = [
                
                "AAPL",
                "MSFT",
                "GOOGL",
                "AMZN",
                "TSLA",
                "NVDA",
                "META",
                "NFLX",  
                "JPM",
                "BAC",
                "GS",
                "WMT",
                "KO",
                "DIS",
                "CTSH",
                "JNJ"
            ]
    for symbol in symbols:
        ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))

if __name__ == "__main__":
    socket_url = f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}"
    
    ws = websocket.WebSocketApp(
        socket_url,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.on_open = on_open

    try:
        ws.run_forever()
    except KeyboardInterrupt:
        print("\nStopping script gracefully...")
    finally:
        # Crucial: Clean up resources and close the AMQP connection pool
        print("🔒 Closing Azure Event Hub connection...")
        producer.close()