import json
import time
import threading
import uuid
from typing import List

import pika
from fastapi import FastAPI, HTTPException, Body, Depends
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Boolean
from sqlalchemy.orm import sessionmaker, declarative_base, Session

DATABASE_URL = "postgresql://user:password@postgres/gozon_db"
RABBITMQ_HOST = "rabbitmq"
PUBLISH_QUEUE = "orders_queue"
CONSUME_QUEUE = "payments_results"

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
    __tablename__ = "orders_outbox"
    id = Column(Integer, primary_key=True, index=True)
    payload = Column(String)
    published = Column(Boolean, default=False)

class InboxModel(Base):
    __tablename__ = "orders_inbox"
    message_id = Column(String, primary_key=True)

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

class OrderResponse(BaseModel):
    id: str
    user_id: int
    item: str
    price: int
    status: str
    
    class Config:
        orm_mode = True

def get_rabbit_connection():
    params = pika.ConnectionParameters(host=RABBITMQ_HOST)
    return pika.BlockingConnection(params)

def publish_raw(channel, queue, data):
    channel.queue_declare(queue=queue, durable=True)
    channel.basic_publish(
        exchange='',
        routing_key=queue,
        body=json.dumps(data),
        properties=pika.BasicProperties(delivery_mode=2)
    )

def start_outbox_relay():
    while True:
        db = SessionLocal()
        try:
            events = db.query(OutboxModel).filter(OutboxModel.published == False).limit(10).all()
            if not events:
                time.sleep(2)
                continue

            connection = get_rabbit_connection()
            channel = connection.channel()

            for event in events:
                try:
                    data = json.loads(event.payload)
                    publish_raw(channel, PUBLISH_QUEUE, data)
                    
                    event.published = True
                    db.commit()
                    print(f"Sent event for order {data.get('order_id')}")
                except Exception as e:
                    print(f"Error sending event {event.id}: {e}")
            
            connection.close()
        except Exception as e:
            print(f"Relay loop error: {e}")
            time.sleep(5)
        finally:
            db.close()

def process_payment_result(ch, method, properties, body):
    db = SessionLocal()
    try:
        data = json.loads(body)
        order_id = data.get("order_id")
        status = data.get("status")
        
        message_id = f"RESULT_{order_id}_{status}"
        
        if db.query(InboxModel).filter_by(message_id=message_id).first():
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        order = db.query(OrderModel).filter(OrderModel.id == order_id).first()
        if order:
            if status == "PAID":
                order.status = "FINISHED"
            else:
                order.status = "CANCELLED"
            
            db.add(InboxModel(message_id=message_id))
            db.commit()
            print(f"Order {order_id} updated to {order.status}")
        
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print(f"Consumer Error: {e}")
        db.rollback()
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    finally:
        db.close()

def start_result_consumer():
    while True:
        try:
            connection = get_rabbit_connection()
            channel = connection.channel()
            channel.queue_declare(queue=CONSUME_QUEUE, durable=True)
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue=CONSUME_QUEUE, on_message_callback=process_payment_result)
            channel.start_consuming()
        except Exception as e:
            print(f"Connection failed: {e}")
            time.sleep(5)

@app.on_event("startup")
def startup_event():
    threading.Thread(target=start_outbox_relay, daemon=True).start()
    threading.Thread(target=start_result_consumer, daemon=True).start()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.post("/orders/", response_model=OrderResponse)
def create_order(order: OrderCreate, db: Session = Depends(get_db)):
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
        payload=json.dumps(event_payload)
    )

    try:
        db.add(new_order)
        db.add(outbox_entry)
        db.commit()
        db.refresh(new_order)
        return new_order
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/orders/", response_model=List[OrderResponse])
def get_orders(db: Session = Depends(get_db)):
    return db.query(OrderModel).all()

@app.get("/orders/{order_id}", response_model=OrderResponse)
def get_order_status(order_id: str, db: Session = Depends(get_db)):
    order = db.query(OrderModel).filter(OrderModel.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    return order