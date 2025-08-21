from importlib import reload , import_module

from temporalio import activity

import os
import json

from datetime import datetime

import requests

MQ_URL = "http://localhost:8000/MQ/"
TARGET = "MQ"

@activity.defn
async def RUN(payload) -> dict:
    JOB_ID     = payload.get("uuid",'')
    if JOB_ID == '':
        res = requests.get(MQ_URL+TARGET+"/poll")
        if res.status_code != 200:
            return { "status": res.status_code }
            
        print(res.status_code,res.text)
        JOB_ID = res.json().get("uuid","")
        payload = res.json()
    else:
        res = requests.get(MQ_URL+TARGET+"/poll/"+JOB_ID)
        
        if res.status_code != 200:
            return { "status": res.status_code }
                    
        print(res.status_code,res.text)
        JOB_ID = res.json().get("uuid","")
        payload = res.json()

    if JOB_ID == '':
        return payload

    ### ack
    if payload['status'] == 'Pending':
        requests.post(MQ_URL+TARGET+"/ack/"+JOB_ID)
        print(res.status_code,res.json())
    ###
    print('\r\n',payload,'\r\n')
    
    agent_id = payload.get("topic",'')
    inputs =   payload.get("payload","{}")   #json.loads(payload.get("payload","{}") )
    shared = json.loads(inputs)
    token =    {} #shared.get("token",{})

    ## topic
    if not os.path.exists("./"+agent_id.replace('.', '/')+".py"):
        requests.post(MQ_URL+TARGET+"/status/", json={"uuid": JOB_ID, "status": "BadFormat","detail": {"stamp": datetime.now().isoformat()}})
        print(res.status_code,res.json())
        return {"status":"BadFormat"}

    agt = import_module(agent_id)
    reload(agt)

    ## processing
    requests.post(MQ_URL+TARGET+"/status/", json={"uuid": JOB_ID, "status": "Processing","detail": {"stamp": datetime.now().isoformat()}})
    print(res.status_code,res.json())

    try:
        result = agt.pocketflow(shared,token)
        ## Completed
        requests.post(MQ_URL+TARGET+"/status/", json={"uuid": JOB_ID, "status": "Completed","detail": result })
        print(res.status_code,res.json())
        return result
        
    except Exception as e:
        print(f"发生未知错误: {e}")
        raise 