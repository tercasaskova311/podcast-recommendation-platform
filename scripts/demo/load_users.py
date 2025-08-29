#!/usr/bin/env python3
import os, json, uuid, random
from datetime import date
from faker import Faker
from pymongo import MongoClient, ASCENDING
from config.settings import MONGO_URI, MONGO_DB, MONGO_COLLECTION_USERS, NUM_USERS


def random_dob(fake: Faker) -> str:
    # 1950-01-01 .. 2007-12-31 (~18 years old minimum)
    d = fake.date_between_dates(date(1950,1,1), date(2007,12,31))
    return d.isoformat()

def main():
    fake = Faker()
    mc = MongoClient(MONGO_URI)
    coll = mc[MONGO_DB][MONGO_COLLECTION_USERS]

    # Ensure unique index on "id" (string)
    coll.create_index([("id", ASCENDING)], unique=True)

    current_count = coll.count_documents({})
    if current_count > 0:
        print(f"[INFO] Users collection already has {current_count} docs. Nothing to do.")
        return

    genders = ["male", "female", "nonbinary"]
    users = []
    for _ in range(NUM_USERS):
        users.append({
            "id": str(uuid.uuid4()),            # unique id (string)
            "name": fake.first_name(),
            "surname": fake.last_name(),
            "date_of_birth": random_dob(fake),  # "YYYY-MM-DD"
            "gender": random.choice(genders),
        })

    # Insert
    res = coll.insert_many(users, ordered=False)
    print(f"[INFO] Inserted {len(res.inserted_ids)} users into '{MONGO_COLLECTION_USERS}'.")

if __name__ == "__main__":
    main()
