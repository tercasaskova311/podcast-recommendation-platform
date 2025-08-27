import os
import json
import random

import pymongo
import pandas as pd

import gradio as gr


# ENV VARIABLES TAKEN FROM main/spark/config/settings.py 
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "podcasts")
MONGO_COLLECTION_FINAL_RECS = os.getenv("MONGO_COLLECTION_FINAL_RECS", "final_recommendations")
K = 10

# GET TOP-TRENDING EPISODES, SORT THEM ACCORDING TO ENGAGEMENT SCORE
with open("top_episodes.json") as podcasts_file:
    top_episodes = json.load(podcasts_file) 
for episode in top_episodes:
    episode["score"] = round(number=random.uniform(0,1),
                             ndigits=3)
top_episodes = sorted(top_episodes,
                      key=lambda x : x["score"],
                      reverse=True) 

# SELECT TOP K EPISODES TO PLOT IN THE DASHBOARD 
top_k = pd.DataFrame({"title": [item["podcast"] for item in top_episodes[:K]],
                      "score": [item["score"] for item in top_episodes[:K]]})

with gr.Blocks() as dashboard: 
    gr.BarPlot(value=top_k, 
               x="title", 
               y="score",
               sort="-y",
               y_lim=[min(top_k["score"])-0.1,1],
               x_title = "Podcast",
               y_title="Engagement Score",
               x_label_angle=0,
               title=f"Top {K} Podcasts by Engagement Score")
    
    all_recommendations_df = gr.DataFrame()
    def read_mongo_recommendations():
        """Reads recommendations from Mongo and stores them all in a single dictionary,
        then converts in a DataFrame component for the Gradio app (i.e, the dashboard). 
        """
        client = pymongo.MongoClient(MONGO_URI)
        database = client[MONGO_DB]
        collection = database[MONGO_COLLECTION_FINAL_RECS]
        recommendations_data = collection.find()
        # IF I UNDERSTAND CORRECTLY, EACH ENTRY OF THE COLLECTION IS A DICTIONARY
        # WITH KEYS 'user_id', 'episode_id', and 'als_score'. I STACK THOSE DICTS
        # INTO A SINGLE ONE TO CONVERT IT TO PANDAS (NECESSARY FOR GRADIO)
        all_recommendations = {"user_id": [],
                               "episode_id": [],
                               "als_score": []}
        for data in recommendations_data:
            all_recommendations["user_id"].append(data["user_id"])
            all_recommendations["episode_id"].append(data["episode_id"])
            all_recommendations["als_score"].append(data["als_score"])
        all_recommendations_df = gr.DataFrame(pd.DataFrame(all_recommendations))
        return all_recommendations_df
    # EVERY TIME THE DASHBOARD IS LAUNCHED OR REFRESHED, THE MONGO DATABASE IS READ BY
    # 'read_mongo_recommendations()' AND THE RESULTING DATAFRAME IS DISPLAYED IN THE DASHBOARD
    gr.Interface(fn=read_mongo_recommendations,
                 inputs=None,
                 outputs=all_recommendations_df,
                 title="Live recommendations",
                 live=True)
    refresh_button = gr.Button(value="Refresh the page")
    refresh_button.click(None, js="window.location.reload()")
    dashboard.launch()