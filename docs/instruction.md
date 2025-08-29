# INSTRUCTIONS TO RUN THE INFRASTRUCTURE

# HOW TO RUN THE INFRASTRUCTURE

1. Run **make init** (wait until it finishes).  
   The only container that should be stopped after this command is `airflow-airflow-init-1` (it is only used to initialize Airflow).  

2. Connect to the Airflow interface at `http://localhost:8081`  
   (username: `airflow`, password: `airflow`).  
   Go to **Admin > Connections**, search for `spark_default`, click **Edit parameter**, and change the value in the **host** field to `local[*]`. Save it.  

3. Go to the **DAGs** page, run the demo DAG that initializes the system, and once it has finished, run the recommendation DAG.  

---

### LAST STEP  
After these operations, we have simulated the functionality of the system.  
To check the results, go to `http://localhost:8084`.  

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
