from faker import Faker
import json
import time
import random

fake = Faker()

def get_registered_user():
    return {
         "name": fake.name(),
         "address": fake.address(),
         "created_at": fake.year()
            }

def get_customer_user():
    return {
        "user_id": fake.uuid4(),
        "user_name": fake.name(),
        "revenue": round(random.uniform(100, 1000), 2),  # Random revenue between 100 and 1000
        "timestamp": int(time.time() * 1000)  # Current timestamp in milliseconds
    }
    
    
if __name__ == "__main__":
    print(get_registered_user())
    print(get_customer_user())
