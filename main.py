import pymongo
import os
import json
from dotenv import load_dotenv

load_dotenv()
connectionStr = os.environ.get("CONN_STR")


if __name__ == '__main__':
    client = pymongo.MongoClient(connectionStr)
    # create a database fora school
    db = client['house']

    # create a collection
    collection = db.prices

    # inserting into collection
    priceInfo = [
        {
            "location": "Banglore",
            "price": 40,
            "bedroom": 10,
            "floor": 15
        },
        {
            "location": "Banglore",
            "price": 20,
            "bedroom": 2,
            "floor": 3
        },
        {
            "location": "Pune",
            "price": 15,
            "bedroom": 8,
            "floor": 1
        },
        {
            "location": "Banglore",
            "price": 30,
            "bedroom": 6,
            "floor": 1
        },

        {
            "location": "Banglore",
            "price": 50,
            "bedroom": 5,
            "floor": 7
        },
        {
            "location": "Delhi",
            "price": 25,
            "bedroom": 5,
            "floor": 7
        },
    ]

    # collection.insert_many(priceInfo)

    # aggreation pipline operation
    # input --> $match --> #group --> $sort --> $project  --> output

    documents = db.prices.aggregate([
        {"$match": {
            "location": "Banglore"
        }},
        {
            "$group": {
                "_id": "$_id",
                "price": {
                    "$first": "$price"
                },
                "floorrooms": {
                    "$sum": {
                        "$add": ["$bedroom", "$floor"]
                    }
                }
            }
        },
        {
            "$sort": {
                "price": 1,
                "floorrooms": -1
            }
        },

    ])

    o = []
    with open("data.json", "a") as f:
        for document in documents:
            o.append({
                "id": str(document["_id"]),
                "price": document["price"],
                "floorrooms": document["floorrooms"]
            })
            print(document)
        f.write(json.dumps(o))
