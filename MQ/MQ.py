from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, Any
from uuid import uuid4
from datetime import datetime, timezone
import redis  #.asyncio as redis
from enum import Enum
from fastapi.testclient import TestClient
import httpx
import os
import json

app = FastAPI(
    title="MQ Server",
    version="1.0",
    description="A simple mq server using redisdb 9",
    #root_path="/MQ"
)

r = redis.Redis(host="localhost", port=6379, db=9,decode_responses=True)

# ----------- webhooks ------------- #
webhook = "https://www.example.com/oauth2/MQ"

async def send_to_webhook(payload, url=webhook):
    payload["_id"] = payload.get("uuid", None)
    try:
        async with httpx.AsyncClient(verify=False) as client:
            response = await client.post(url, json=payload, timeout=20.0)
            if 200 <= response.status_code < 300:
                print(f"Webhook 发送成功: {response.status_code}")
            else:
                print(f"Webhook 返回错误状态码: {response.status_code}")
                save_failed_webhook(payload)
    except Exception as e:
        print(f"Webhook 发送失败: {e}")
        save_failed_webhook(payload)

    delete_message_if_completed(payload["target"], payload["uuid"])

def save_failed_webhook(payload):
    uuid = payload.get("uuid", "unknown")

    # 当前时间，精确到毫秒
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%f")[:-3]  # 毫秒精度
    filename = f"{timestamp}.json"

    # 目录结构：webhook_failures/<uuid>/
    dir_path = os.path.join("webhook_failures", uuid)
    os.makedirs(dir_path, exist_ok=True)

    path = os.path.join(dir_path, filename)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        print(f"Webhook 消息保存至文件: {path}")
    except Exception as e:
        print(f"保存 webhook 消息失败: {e}")

import os
import json
import httpx
from typing import Optional
from datetime import datetime

def list_uuid_folders_by_creation(base_dir="webhook_failures"):
    if not os.path.exists(base_dir):
        print(f"📁 目录不存在: {base_dir}")
        return []

    folders = [
        os.path.join(base_dir, name)
        for name in os.listdir(base_dir)
        if (
            os.path.isdir(os.path.join(base_dir, name)) and
            not name.startswith(".")  # 忽略隐藏文件夹
        )
    ]

    # 按最后修改时间排序（模拟创建时间）
    folders.sort(key=lambda f: os.path.getmtime(f))

    results = []
    for folder in folders:
        mod_time = os.path.getmtime(folder)
        readable_time = datetime.fromtimestamp(mod_time).strftime("%Y-%m-%d %H:%M:%S")
        results.append((os.path.basename(folder), readable_time))

    return results 

async def resend_failed_webhooks(uuid: str, webhook_url: Optional[str] = None):

    def remove_empty_dirs(path: str):
        """
        递归删除空目录，删除目录必须物理空才行。
        """
        if not os.path.isdir(path):
            return
    
        print(f"{path} 递归删除空目录直到不能删除为止")
    
        for root, dirs, files in os.walk(path, topdown=False):
            for d in dirs:
                dir_path = os.path.join(root, d)
                try:
                    if not os.listdir(dir_path):  # 物理空目录
                        os.rmdir(dir_path)
                        print(f"🗑️ 删除空目录: {dir_path}")
                except Exception as e:
                    print(f"删除空目录失败: {dir_path}, 错误: {e}")
    
        # 最后删传入的目录，必须物理空才删
        try:
            if not os.listdir(path):
                os.rmdir(path)
                print(f"🗑️ 删除空目录: {path}")
        except Exception as e:
            print(f"删除空目录失败: {path}, 错误: {e}")


    ######
    base_dir = os.path.join("webhook_failures", uuid)
    
    if not os.path.isdir(base_dir):
        print(f"⚠️ UUID 文件夹不存在: {base_dir}")
        return

    files = sorted([
        f for f in os.listdir(base_dir)
        if os.path.isfile(os.path.join(base_dir, f)) and not f.startswith(".")
    ])

    if not files:
        print(f"📂 UUID 文件夹为空: {base_dir}")
        remove_empty_dirs(base_dir)
        return

    if webhook_url is None:
        print("❗ webhook_url 未指定，取消发送")
        return

    for file in files:
        file_path = os.path.join(base_dir, file)
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
                if not content.strip():
                    print(f"⚠️ 文件为空，已删除跳过: {file_path}")
                    os.remove(file_path)
                    continue

                payload = json.loads(content)

            payload["_id"] = payload.get("uuid")

            async with httpx.AsyncClient(verify=False) as client:
                response = await client.post(webhook_url, json=payload, timeout=20.0)

            if 200 <= response.status_code < 300:
                print(f"✅ Webhook 发送成功: {file_path}")
                os.remove(file_path)
            else:
                print(f"❌ Webhook 返回失败状态码 ({response.status_code}): {file_path}")
                # 发送失败，停止并保留剩余文件和目录
                return

        except json.JSONDecodeError as jde:
            print(f"🚨 JSON 解析错误，删除该文件: {file_path}，错误: {jde}")
            os.remove(file_path)
            continue
        except Exception as e:
            print(f"🚨 处理文件时出错 {file_path}: {e}")
            # 发送异常，停止并保留剩余文件和目录
            return

    # 全部文件处理完毕，递归清理空目录
    remove_empty_dirs(base_dir)
    
def delete_message_if_completed(target: str, uuid: str):
    msg = r.hgetall(make_msg_key(uuid))
    if not msg:
        return False

    status = msg.get("status")
    if not status:
        return False

    deletable_statuses = {
        MessageStatus.completed.value,
        MessageStatus.failed.value,
        MessageStatus.format_error.value,
        MessageStatus.cancelled.value,
    }

    if status in deletable_statuses:
        # 删除消息哈希
        r.delete(make_msg_key(uuid))
        # 从队列zset移除
        r.zrem(f"queue:{target}", uuid)
        print(f"消息 {uuid} 因状态 {status} 被删除")
        return True

    return False

    
# ----------- Enums ----------- #
class MessageStatus(str, Enum):
    pending = "Pending"
    acked = "Acked"
    started = "Started"
    intermediate = "Processing"
    completed = "Completed"
    failed = "Failed"
    format_error = "BadFormat"
    cancelled = "Cancelled"

# ----------- Models ----------- #
class MessageCreate(BaseModel):
    topic: str
    payload: Any
    target: str
    resource: str
    priority: Optional[int] = 10

class PriorityUpdateRequest(BaseModel):
    uuid: str
    target: str
    new_priority: int

class MessageCancelRequest(BaseModel):
    uuid: str
    target: str

class MessageStatusUpdate(BaseModel):
    uuid: str
    status: MessageStatus
    detail: Optional[Any] = None

def make_msg_key(uuid: str) -> str:
    return f"msg:{uuid}"

# ----------- Core Logic ----------- #
def save_message(data: MessageCreate) -> str:
    uuid = str(uuid4())
    timestamp = datetime.now(timezone.utc).isoformat()
    msg_key = make_msg_key(uuid)

    r.hset(msg_key, mapping={
        "uuid": uuid,
        "topic": data.topic,
        "payload": str(data.payload),
        "target": data.target,
        "resource": data.resource,
        "priority": data.priority,
        "timestamp": timestamp,
        "status": MessageStatus.pending.value
    })
    r.zadd(f"queue:{data.target}", {uuid: data.priority})
    return uuid

def update_priority(uuid: str, target: str, new_priority: int):
    msg_key = make_msg_key(uuid)
    msg = r.hgetall(msg_key)
    if not msg:
        raise ValueError("Message not found")

    if msg.get("target") != target:
        raise PermissionError("System not authorized")

    if msg.get("status") != MessageStatus.pending.value:
        raise ValueError("Only messages with 'Pending' status can be updated")

    r.hset(msg_key, "priority", new_priority)
    r.zadd(f"queue:{target}", {uuid: new_priority})
    return True

def cancel_message(uuid: str, target: str):
    msg_key = make_msg_key(uuid)
    msg = r.hgetall(msg_key)
    if not msg:
        raise ValueError("Message not found")

    if msg.get("target") != target:
        raise PermissionError("System not authorized")

    if msg.get("status") != MessageStatus.pending.value:
        raise ValueError("Only messages with 'Pending' status can be cancelled")

    # 更新状态为 Cancelled
    r.hset(msg_key, "status", MessageStatus.cancelled.value)
    # 从队列中移除
    r.zrem(f"queue:{target}", uuid)
    return True

def get_next_message(target: str):
    items = r.zrange(f"queue:{target}", 0, 0)
    if not items:
        return None

    msg = r.hgetall(make_msg_key(items[0]))
    if not msg:
        return None

    status = msg.get(b'status') or msg.get('status')
    if isinstance(status, bytes):
        status = status.decode()

    if status == MessageStatus.pending.value:
        return msg

    return None

def ack_message(target: str, uuid: str):
    msg = r.hgetall(make_msg_key(uuid))
    if not msg:
        raise ValueError("Message not found")

    if msg.get("target") != target:
        raise PermissionError("System not authorized")

    if msg.get("status") != MessageStatus.pending.value:
        raise ValueError("Only 'Pending' messages can be acknowledged")

    r.hset(make_msg_key(uuid), "status", MessageStatus.acked.value)

def update_status(uuid: str, status: MessageStatus, detail: Any, target: str):
    msg = r.hgetall(make_msg_key(uuid))
    if msg.get("target") != target:
        raise PermissionError("System not authorized")
    r.hset(make_msg_key(uuid), mapping={
        "status": status.value,
        "result": str(detail) if detail else ""
    })

def query(uuid: str, target: str):
    msg = r.hgetall(make_msg_key(uuid))
    if msg.get("target") != target:
        raise PermissionError("System not authorized")
    return msg

def get_topic_stats(target: str, topic: str, status: str):
    keys = r.keys("msg:*")
    count = 0
    for key in keys:
        msg = r.hgetall(key)
        if (
            msg.get("target") == target and
            msg.get("topic") == topic and
            msg.get("status") == status
        ):
            count += 1
    return {"topic": topic, "status": status, "count": count}
        
# ----------- API Routes ----------- #
@app.get("/MQ/webhook")
async def resend_webhook():
    todos = list_uuid_folders_by_creation()   
    for f in todos:
        print(f[0])
        await resend_failed_webhooks(f[0], webhook)
    
@app.post("/MQ/send")
async def send(data: MessageCreate):
    uuid = save_message(data)
    msg = r.hgetall(make_msg_key(uuid))
    await send_to_webhook(msg)
    return {"uuid": uuid}
    
@app.post("/MQ/priority")
async def priority_update(data: PriorityUpdateRequest):
    try:
        update_priority(data.uuid, data.target, data.new_priority)
        return {"status": "priority updated"}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except PermissionError:
        raise HTTPException(status_code=403, detail="Unauthorized")

@app.post("/MQ/cancel")
async def cancel(data: MessageCancelRequest):
    try:
        cancel_message(data.uuid, data.target)
        msg = r.hgetall(make_msg_key(data.uuid))
        await send_to_webhook(msg)
        return {"status": "cancelled"}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except PermissionError:
        raise HTTPException(status_code=403, detail="Unauthorized")

@app.get("/MQ/{target}/poll")
async def poll(target: str):
    return get_next_message(target)

@app.get("/MQ/{target}/poll/{uuid}")
async def poll_by_uuid(target: str, uuid: str):
    msg = r.hgetall(make_msg_key(uuid))
    if not msg:
        raise HTTPException(status_code=404, detail="Message not found")
    if msg.get("target") != target:
        raise HTTPException(status_code=403, detail="Unauthorized access")
    return msg

@app.post("/MQ/{target}/ack/{uuid}")
async def ack(target: str, uuid: str):
    try:
        ack_message(target, uuid)
        msg = r.hgetall(make_msg_key(uuid))
        await send_to_webhook(msg)
        return {"status": "acknowledged"}
    except PermissionError:
        raise HTTPException(status_code=403, detail="Unauthorized")
    
@app.post("/MQ/{target}/status")
async def update(target: str, status: MessageStatusUpdate):
    try:
        update_status(status.uuid, status.status, status.detail, target)
        msg = r.hgetall(make_msg_key(status.uuid))
        await send_to_webhook(msg)
        return {"status": "updated"}
    except PermissionError:
        raise HTTPException(status_code=403, detail="Unauthorized")

@app.get("/MQ/{target}/query/{uuid}")
async def query_status(target: str, uuid: str):
    try:
        return query(uuid, target)
    except PermissionError:
        raise HTTPException(status_code=403, detail="Unauthorized")

@app.get("/MQ/{target}/stats/{topic}")
async def stats(target: str, topic: str, status: str):
    return get_topic_stats(target, topic, status)

    