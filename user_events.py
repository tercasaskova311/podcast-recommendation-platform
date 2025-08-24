import uuid
import json
import random
from faker import Faker


def make_date(start_date, end_date):
    """Creates random dates within a given range.
    
    Parameters:
    start_date -- the start of the dates range (type: str or Datetime object)
    end_data -- the end of the dates range (type: str or Datetime object)
    
    Returns:
    fake_date_string -- the generated date (type: str)
    """

    fake_maker = Faker()
    fake_date = fake_maker.date_time_between(start_date=start_date, 
                                             end_date=end_date)
    fake_date_string = str(fake_date)
    return fake_date_string

###############################################################################################################
### I simulated events happening between yesterday and tomorrow. You can change this if necessary.          ###
### I know that we have to simulate an on-line scenario, but I couldn't take current dates for each event   ###
### (that is, 'datetime.now()') because results would be implausible (that is, there would be ms intervals) ###
###############################################################################################################
NUMBER_OF_USERS = 300
START_DATE = "-1d"
END_DATE = "+1d"
user_ids = [str(uuid.uuid4()) for _ in range(NUMBER_OF_USERS)]
event_ids = {"pause": 1,
             "like": 2,
             "skip": 3,
             "rate": 4,
             "complete": 5}
rating_scale = [n + 1 for n in range(5)]


with open("top_episodes.json") as episodes_file:
    top_episodes = json.load(episodes_file)

user_events_data = {f"user{number}": {} for number, _ in enumerate(user_ids)}
for user_number, user_id in enumerate(user_ids):
    print(f"USER: {user_id}")
    print(" ")
    device = random.sample(population=["ios","android","web"], k=1)[0]
    number_of_episodes = random.randint(1,5)
    for episode in range(number_of_episodes):
        episode_id = random.sample(population=top_episodes, k=1)[0]["id"] 
        print(f"Episode ID: {episode_id}")
        user_event = random.sample(population=sorted(event_ids.keys()), k=1)[0]
        user_event_time = make_date(start_date=START_DATE,
                                    end_date=END_DATE)
        user_event_id = event_ids[user_event] 
        if user_event == "rate":
            rating = random.sample(population=rating_scale, k=1)[0]
        else:
            rating = None
        print(f"Action: {user_event}")
        print(f"Action ID: {user_event_id}")
        print(f"Action time: {user_event_time}")
        print(f"Rating: {rating}")
        print(" ")
        user_events_data[f"user{user_number}"]["user_id"] = user_id
        user_events_data[f"user{user_number}"]["device"] = device
        user_events_data[f"user{user_number}"][f"episode{episode}"] = {"episode_id": episode_id,
                                                                       "event": user_event,
                                                                       "event_id": user_event_id,
                                                                       "rating": rating,
                                                                       "event_time": user_event_time}
    print(" ")
    print("---")
    print(" ")
with open("user_events_data.json", "w") as output_file:
    json.dump(obj=user_events_data,
              fp=output_file)
    

