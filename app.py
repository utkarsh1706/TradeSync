from flask import Flask
from schema import *
import json
import time
from mongoengine import connect, ValidationError
import redis
from dotenv import load_dotenv
import os
import threading

# Load environment variables
load_dotenv()

mongoURI = os.getenv("mongoURI")
redisPassword = os.getenv("redisPassword")
redisHost = os.getenv("redisHost")
redisPort = os.getenv("redisPort")

app = Flask(__name__)

# Function to connect to MongoDB and Redis
def initialize_connections():
    # Connect to MongoDB
    try:
        connect(db="StockBrokerSystem", host=mongoURI)
    except Exception as e:
        print(e)

    # Connect to Redis
    try:
        redisClient = redis.Redis(host=redisHost, port=redisPort, password=redisPassword)
        redisClient.ping()
        print("Connected to Redis")
        return redisClient
    except redis.ConnectionError as e:
        print("Redis not connected due to ", e)
        return None

# Initialize connections once on the first call
redisClient = initialize_connections()

# Function to update trades
def updateTrades():
    if redisClient:
        try:
            tradeDataList = redisClient.lrange("tradeData", 0, -1)
            for tradeData in tradeDataList:
                try:
                    trade = json.loads(tradeData)
                    if not Trade.objects(unique_id=trade['unique_id']):
                        newTrade = Trade(
                            unique_id=trade['unique_id'],
                            execution_timestamp=trade['execution_timestamp'],
                            price=trade['price'],
                            qty=trade['qty'],
                            bid_order_id=trade['bid_order_id'],
                            ask_order_id=trade['ask_order_id']
                        )
                        newTrade.save()
                except (json.JSONDecodeError, ValidationError) as e:
                    print(f"Error saving trade: {e}")
        except Exception as e:
            print(f"Error fetching trade data from Redis: {e}")

# Function to update orders
def updateOrders():
    if redisClient:
        try:
            keys = redisClient.keys('order:*')
            for orderKey in keys:
                try:
                    orderData = redisClient.hgetall(orderKey)
                    if orderData:
                        orderData = {k.decode('utf-8'): v.decode('utf-8') for k, v in orderData.items()}
                        # Update or save order logic
                except Exception as e:
                    print(f"Error processing order {orderKey}: {e}")
        except Exception as e:
            print(f"Error fetching order keys from Redis: {e}")

# Background task to run updates every 30 seconds
def run_updates():
    while True:
        print("Running updates...")
        updateTrades()
        updateOrders()
        time.sleep(30)  # Wait for 30 seconds before the next run

# Start the background task in a separate thread
@app.before_first_request
def start_background_task():
    thread = threading.Thread(target=run_updates)
    thread.daemon = True
    thread.start()

@app.route('/update')
def home():
    return "Trade and Order Updates Microservice Running"

if __name__ == "__main__":
    app.run(debug=True, use_reloader=False)
