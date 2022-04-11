import snowflake.connector
import pandas as pd

def sf_connector(user_name, database_name, schema_name, table_name):

    con = snowflake.connector.connect(
    user= user_name,
    account= "colgatepalmolivedev.us-central1.gcp",
    authenticator= 'externalbrowser',
    warehouse= "DEVELOPER_WH",
    database= database_name,
    schema= schema_name,
    )
    cur = con.cursor()

    preview_query = "SELECT * FROM " + table_name + " LIMIT 10 "
    try:
        cur.execute(preview_query)
        columns = []
        for val in cur.description:
            columns.append(val[0])
        rows = 0
        while True:
        # print("Inside while")
            dat = cur.fetchall()
            if not dat:
                break
        # print(cur.description)
            df = pd.DataFrame(dat, columns=columns)
            rows += df.shape[0]
    finally:
        cur.close()