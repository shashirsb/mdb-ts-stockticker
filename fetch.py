from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS
from pymongo import MongoClient

app = Flask(__name__, static_folder='.')
CORS(app)

client = MongoClient("mongodb+srv://mainuser:<password>@cluster0.mongodb.net/?retryWrites=true&w=majority")
db = client["marketdata"]

@app.get('/api/ko')
def ko():
    cur = db["stock_ticks_v2"].find(
        {"ts": {"$exists": True}},
        {"_id": 0, "ts": 1, "validStart": 1, "price_raw": 1, "price_adjusted": 1, "volume": 1, "eventTags": 1}
    ).sort("ts", 1)
    
    out = [
        {
            "ts": doc["ts"].strftime('%Y-%m-%d'),
            "validStart": doc["validStart"].strftime('%Y-%m-%d'),
            "price_raw": doc["price_raw"],
            "price_adjusted": doc["price_adjusted"],
            "volume": doc["volume"],
            "eventTags": doc["eventTags"]
        }
        for doc in cur
    ]
    return jsonify(out)

@app.get('/')
def index():
    return send_from_directory('.', 'index.html')

if __name__ == '__main__':
    app.run(debug=True, port=5001)
