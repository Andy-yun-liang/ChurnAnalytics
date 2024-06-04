import psycopg2
import os

#postgres connection to my simulated crm data, credentials are saved in my .bashrc file
#data source: https://github.com/carl24k/fight-churn

pg_conn = psycopg2.connect(
    # dbname='os.environ.get("DBNAME")',
    # user=os.environ.get("PUSER"),
    # password=os.environ.get("PPASS"),
    dbname="churn",
    user="postgres",
    password="postgrespassword",
    host="localhost",
    port="5432"
)
cur = pg_conn.cursor()

try:
    # Wrap insert statements in a transaction
    pg_conn.autocommit = False

    # Your existing schema and table creation code
    schema_name = 'user_events'
    create_schema = f"CREATE SCHEMA IF NOT EXISTS {schema_name};"

    cur.execute(create_schema)
    pg_conn.commit()

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.event_metrics(
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id int,
    metric_date date,
    metric_id int, 
    metric_value int
    );
    """

    cur.execute(create_table_query)
    pg_conn.commit()
    print("DONE")

    metrics = ['post', 'newfriend', 'like', 'adview', 'dislike', 'unfriend', 'message', 'reply']

    for metric in metrics:

        inserting_agg_events = f"""
        INSERT INTO {schema_name}.event_metrics (account_id,metric_date,metric_id,metric_value)
        WITH date_range AS (  
        
            SELECT periods::timestamp AS metric_intervals
            FROM generate_series('2020-01-01','2020-12-31','7 days'::interval) AS periods

        ), event_data AS (    
        
            SELECT e.account_id AS account_id, e.event_time AS event_time,e.event_type_id AS metric_id , et.event_type_name AS event_type 
            FROM socialnet7."event" AS e 
            LEFT JOIN socialnet7.event_type AS et 
            ON e.event_type_id = et.event_type_id 

        ), metric_count AS (   
        
            SELECT e.account_id, d.metric_intervals::date AS metric_date,e.metric_id ,COUNT(*) AS metric_value
            FROM event_data AS e
            INNER JOIN date_range AS d 
            ON e.event_time < d.metric_intervals + '1 day'::interval
            AND e.event_time >= d.metric_intervals - '28 days'::interval
            WHERE e.event_type = '{metric}'
            GROUP BY e.account_id,d.metric_intervals,e.metric_id

        )
        
        SELECT * FROM metric_count
        ON CONFLICT DO NOTHING;
        """

        cur.execute(inserting_agg_events)
        print(f"finished inserting {metric} in the table")

    # Commit the transaction
    pg_conn.commit()

except psycopg2.Error as e:
    # Rollback the transaction if an exception occurs
    print("Error inserting data:", e)
    pg_conn.rollback()

finally:
    # Reset autocommit and close the cursor and connection
    pg_conn.autocommit = True
    cur.close()
    pg_conn.close()
