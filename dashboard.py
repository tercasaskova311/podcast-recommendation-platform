import json
import random
import uuid

import pandas as pd

import gradio as gr


# GET TOP-TRENDING EPISODES
with open("top_episodes.json") as podcasts_file:
    top_episodes = json.load(podcasts_file)

# SORT THEM ACCORDING TO ENGAGEMENT SCORE 
for episode in top_episodes:
    episode["score"] = round(number=random.uniform(0,1),
                             ndigits=3)
top_episodes = sorted(top_episodes,
                      key=lambda x : x["score"],
                      reverse=True) 

# SELECT TOP K EPISODES TO PLOT IN THE DASHBOARD 
K = 10
top_k = pd.DataFrame({"title": [item["podcast"] for item in top_episodes[:K]],
                      "score": [item["score"] for item in top_episodes[:K]]})

# SIMULATE USER DATA. I NEEDED THIS TO BUILD THE DASHBOARD, IT WILL DISAPPEAR IN THE FINAL VERSION.
# THE SIMULATED DATA ARE SAVED TO CSV TO SIMULATE A SITUATION WHERE THE DASHBOARD GETS THE DATA FROM SOME LOCATION
NUMBER_OF_USERS = 300
user_ids = [str(uuid.uuid4()) for _ in range(NUMBER_OF_USERS)]
simulated_recommendations_df = pd.DataFrame({"user": [uid for uid in user_ids],
                                             "next_recommended_episode": [0 for n in user_ids]})
simulated_recommendations_df.to_csv(path_or_buf="recommendations.csv",
                                    sep=",",
                                    index=False)

# MAKE THE DASHBOARD
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
    df = gr.DataFrame(value="recommendations.csv")
    def read_new_recommendations():
        df = gr.DataFrame(value="recommendations.csv")
        return df
    gr.Interface(fn=read_new_recommendations,
                 inputs=None,
                 outputs=df,
                 title="Live recommendations",
                 live=True)
    refresh_button = gr.Button(value="Refresh the page")
    refresh_button.click(None, js="window.location.reload()")   # TO-DO: trigger automatic refreshes with js
    dashboard.launch()