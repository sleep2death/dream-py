from flask import Flask
import redis
import pymongo

import torch
from diffusers import StableDiffusionPipeline

pipe = StableDiffusionPipeline.from_pretrained("runwayml/stable-diffusion-v1-5", torch_dtype=torch.float16, revision="fp16")
pipe = pipe.to("cuda")

import threading
import json
import time

from datetime import datetime, timezone
from enum import IntEnum

class dStatus(IntEnum):
    Pending = 0
    Processing = 1
    Done = 2
    Failed = 3
    Nsfw = 4

# redis
r = redis.Redis("localhost", 6379)

# mongodb
m = pymongo.MongoClient("mongodb://localhost:27017/")
mDreams = m["DreamWalker"]["dreams"]

def process(): 
    while True:
        # pop the task_id from queue
        _, task_id = r.blpop("DQ", 0)
        id = task_id.decode("utf-8")
        dream_id = "d:"+id+":temp"

        # get temp dream's details from redis
        ba = r.get(dream_id)
        if ba == None:
            print("task not found:", id)
            continue
        dream = json.loads(ba)

        # update redis
        dream["status"] = 1
        str = json.dumps(dream)
        r.set(dream_id, str)

        time.sleep(3)

        print("task done:", dream)

        # update redis
        dream["status"] = 2
        str = json.dumps(dream)
        r.set(dream_id, str)


        # update mongodb
        fTime = datetime.now(timezone.utc)
        res = mDreams.update_one({"_id": dream["_id"]}, {"$set": {"finished": fTime, "status": dream["status"]}})

        # print("mongo update result:", res.modified_count, res.matched_count)
        if res.modified_count != 1:
            print("dream status not updated:", dream["_id"])




t0 = threading.Thread(target=process)
t0.start()


app = Flask(__name__)

@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"
