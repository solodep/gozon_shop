import json
import time
import threading
import pika
from fastapi import FastAPI, HTTPException, Body
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Boolean
from sqlalchemy.orm import sessionmaker, declarative_base

DATABASE_URL = "postgresql://user:password@postgres/gozon_db"
RABBITMQ_HOST = "rabbitmq"
CONSUME_QUEUE = "orders_queue"
PUBLISH_QUEUE = "payments_results"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class Account(Base):
    __tablename__ = "accounts"
    user_id = Column(Integer, primary_key=True, index=True)
    balance = Column(Integer, default=0)

class Inbox(Base):
    __tablename__ = "inbox"
    message_id = Column(String, primary_key=True)

class Outbox(Base):
    __tablename__ = "outbox"
    id = Column(Integer, primary_key=True, index=True)
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

def get_rabbit_connection():
    params = pika.ConnectionParameters(host=RABBITMQ_HOST)
    return pika.BlockingConnection(params)

def publish_event(event_data: dict):
    try:
        connection = get_rabbit_connection()
        channel = connection.channel()
        channel.queue_declare(queue=PUBLISH_QUEUE, durable=True)
        channel.basic_publish(
            exchange='',
            routing_key=PUBLISH_QUEUE,
            body=json.dumps(event_data),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        connection.close()
    except Exception as e:
        print(f"Publish error: {e}")

def process_payment_logic(ch, method, properties, body):
    db = SessionLocal()
    try:
        data = json.loads(body)
        order_id = data.get("order_id")
        user_id = data.get("user_id")
        price = data.get("price")
        
        message_id = f"{order_id}_PAYMENT"

        if db.query(Inbox).filter_by(message_id=message_id).first():
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        user_acc = db.query(Account).filter_by(user_id=user_id).with_for_update().first()

        payment_status = "FAILED"
        if user_acc and user_acc.balance >= price:
            user_acc.balance -= price
            payment_status = "PAID"
        
        inbox_entry = Inbox(message_id=message_id)
        db.add(inbox_entry)
        
        result_event = {
            "event": "PAYMENT_RESULT",
            "order_id": order_id,
            "status": payment_status
        }
        
        outbox_entry = Outbox(payload=json.dumps(result_event))
        db.add(outbox_entry)
        
        db.commit()
        
        publish_event(result_event)
        
        outbox_entry.published = True
        db.commit()

        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print(f"Error processing: {e}")
        db.rollback()
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    finally:
        db.close()

def start_consumer():
    while True:
        try:
            connection = get_rabbit_connection()
            channel = connection.channel()
            channel.queue_declare(queue=CONSUME_QUEUE, durable=True)
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue=CONSUME_QUEUE, on_message_callback=process_payment_logic)
            channel.start_consuming()
        except Exception:
            time.sleep(5)

@app.on_event("startup")
def startup_event():
    t = threading.Thread(target=start_consumer, daemon=True)
    t.start()

class AccountCreate(BaseModel):
    user_id: int
    balance: int

@app.post("/accounts/")
def create_account(acc: AccountCreate):
    db = SessionLocal()
    try:
        existing = db.query(Account).filter_by(user_id=acc.user_id).first()
        if existing:
            existing.balance += acc.balance
        else:
            new_acc = Account(user_id=acc.user_id, balance=acc.balance)
            db.add(new_acc)
        db.commit()
        return {"status": "ok"}
    finally:
        db.close()

@app.get("/accounts/{user_id}")
def get_balance(user_id: int):
    db = SessionLocal()
    acc = db.query(Account).filter_by(user_id=user_id).first()
    db.close()
    if not acc:
        raise HTTPException(status_code=404, detail="User not found")
    return {"user_id": acc.user_id, "balance": acc.balance}