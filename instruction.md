# INSTRUCTIONS TO RUN THE INFRASTRUCTURE

## INITIALIZE THE SYSTEM
1. First of all, delete all the contents of the project folder `data/delta`.
2. Start Docker, and when it’s ready, run **make init** from the CLI in the root of the project.
3. Wait until the container `airflow-airflow-init-1` exits (this is normal, as it initializes the Airflow system). After that, you should see all the other containers running.

## RUN DEMO DATA
For now, we have to run all the scripts manually:

1. From your CLI, run **docker exec -it airflow-airflow-scheduler-1 bash**, which connects you to the container’s terminal. Then move to the project folder (**cd /opt/project**) and run the demo delta scripts:  
   **python3 scripts/demo/load_delta.py**
2. Next, run the analysis script:  
   **python3 spark/pipelines/analyze_transcripts_pipeline.py**  
   Once it has finished, you should see the podcast database in MongoDB (localhost:8083, user: `admin`, password: `pass`).
3. Then run the user simulation script:  
   **python3 scripts/streaming/user_events_simulation.py**  
   Wait a couple of minutes to give the streaming job time to process the events. You should see the folder `/data/delta/user_vs_episode_daily` populated.
4. Finally, run the two scripts for similarities and recommendations. First:  
   **python3 spark/pipelines/training_user_events_pipeline.py**  
   and then:  
   **python3 spark/pipelines/final_recommendation.py**  
   You should see MongoDB populated!

Now you can work with the data and start integrating the dashboard! :)
