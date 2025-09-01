# Instruction

# How to run the infrastructure

1. Run **make init** (wait until it finishes).  
   The only container that should be stopped after this command is `airflow-airflow-init-1` (it is only used to initialize Airflow).  

2. Connect to the Airflow interface at `http://localhost:8081`  
   (username: `airflow`, password: `airflow`).  
   Go to **Admin > Connections**, search for `spark_default`, click **Edit parameter**, and change the value in the **host** field to `local[*]`. Save it.  

3. Go to the **DAGs** page, run the demo DAG that initializes the system, and once it has finished, run the recommendation DAG.  

---

### Last step  
After these operations, we have simulated the functionality of the system.  
To check the results, go to `http://localhost:8084`.  


