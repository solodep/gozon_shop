import json
import time
import threading
import pika
from fastapi import FastAPI, HTTPException
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
    __tablename__ = "payments_inbox" 
    message_id = Column(String, primary_key=True)

class Outbox(Base):
    __tablename__ = "payments_outbox"
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

def start_outbox_relay():
    while True:
        db = SessionLocal()
        try:
            events = db.query(Outbox).filter(Outbox.published == False).limit(10).all()
            if not events:
                time.sleep(2)
                continue

            connection = get_rabbit_connection()
            channel = connection.channel()
            channel.queue_declare(queue=PUBLISH_QUEUE, durable=True)

            for event in events:
                try:
                    data = json.loads(event.payload)
                    channel.basic_publish(
                        exchange='',
                        routing_key=PUBLISH_QUEUE,
                        body=json.dumps(data),
                        properties=pika.BasicProperties(delivery_mode=2)
                    )
                    event.published = True
                    db.commit()
                    print(f"Sent result for {data.get('order_id')}")
                except Exception as e:
                    print(f"Relay Error: {e}")
            
            connection.close()
        except Exception as e:
            print(f"Relay Loop error: {e}")
            time.sleep(5)
        finally:
            db.close()

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
        
        db.add(Inbox(message_id=message_id))
        
        result_event = {
            "order_id": order_id,
            "status": payment_status
        }
        db.add(Outbox(payload=json.dumps(result_event)))
        
        db.commit()
        ch.basic_ack(delivery_tag=method.delivery_tag)
        print(f"Processed order {order_id}: {payment_status}")

    except Exception as e:
        print(f"Consumer Error: {e}")
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
            print("Consumer started...")
            channel.start_consuming()
        except Exception:
            time.sleep(5)

@app.on_event("startup")
def startup_event():
    threading.Thread(target=start_consumer, daemon=True).start()
    threading.Thread(target=start_outbox_relay, daemon=True).start()

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