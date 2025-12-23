import json
import time
import uuid
from typing import List

import pika
from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Boolean, text
from sqlalchemy.orm import sessionmaker, declarative_base

DATABASE_URL = "postgresql://user:password@postgres/gozon_db"
RABBITMQ_HOST = "rabbitmq"
QUEUE_NAME = "orders_queue"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class OrderModel(Base):
    __tablename__ = "orders"
    id = Column(String, primary_key=True, index=True)
    user_id = Column(Integer)
    item = Column(String)
    price = Column(Integer)
    status = Column(String, default="NEW")

class OutboxModel(Base):
    __tablename__ = "outbox"
    id = Column(Integer, primary_key=True, index=True)
    event_type = Column(String)
    payload = Column(String)
    published = Column(Boolean, default=False)

def init_db():
    retries = 5
    while retries > 0:
        try:
            Base.metadata.create_all(bind=engine)
            return
        except Exception:
            retries -= 1
            time.sleep(2)

init_db()

app = FastAPI()

class OrderCreate(BaseModel):
    user_id: int
    item: str
    price: int

def get_rabbit_connection():
    params = pika.ConnectionParameters(host=RABBITMQ_HOST)
    return pika.BlockingConnection(params)

def publish_event(event_data: dict):
    connection = get_rabbit_connection()
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.basic_publish(
        exchange='',
        routing_key=QUEUE_NAME,
        body=json.dumps(event_data),
        properties=pika.BasicProperties(delivery_mode=2)
    )
    connection.close()

def process_outbox():
    db = SessionLocal()
    try:
        events = db.query(OutboxModel).filter(OutboxModel.published == False).all()
        for event in events:
            try:
                data = json.loads(event.payload)
                publish_event(data)
                event.published = True
            except Exception as e:
                print(f"Error publishing: {e}")
        db.commit()
    finally:
        db.close()

@app.post("/orders/")
def create_order(order: OrderCreate, background_tasks: BackgroundTasks):
    db = SessionLocal()
    order_id = str(uuid.uuid4())
    
    new_order = OrderModel(
        id=order_id,
        user_id=order.user_id,
        item=order.item,
        price=order.price,
        status="NEW"
    )
    
    event_payload = {
        "event": "ORDER_CREATED",
        "order_id": order_id,
        "user_id": order.user_id,
        "price": order.price
    }
    
    outbox_entry = OutboxModel(
        event_type="ORDER_CREATED",
        payload=json.dumps(event_payload)
    )

    try:
        db.add(new_order)
        db.add(outbox_entry)
        db.commit()
        db.refresh(new_order)
        
        background_tasks.add_task(process_outbox)
        
        return {"id": new_order.id, "status": "PROCESSING"}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()