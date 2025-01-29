import os
from google.cloud.sql.connector import Connector, IPTypes
import pg8000
import sqlalchemy
import google.auth

# credentials, project = google.auth.default()

# initialize Connector object
connector = Connector()

INSTANCE_CONNECTION_NAME = "lateral-plane-409204:us-central1:test-db"
DB_USER = "postgres"
DB_PASS = "123qweasdzxc"
DB_NAME = "postgres"

# function to return the database connection object
def getconn():
    conn = connector.connect(
        INSTANCE_CONNECTION_NAME,
        "pg8000",
        user=DB_USER,
        password=DB_PASS,
        db=DB_NAME
    )
    print("Connection complete ... ")
    return conn

def main(request):
    print("Preparing pool ... ")
    # create connection pool with 'creator' argument to our connection object function
    pool = sqlalchemy.create_engine(
        "postgresql+pg8000://",
        creator=getconn,
    )

    with pool.connect() as db_conn:

        # query database
        result = db_conn.execute(sqlalchemy.text("SELECT * from home_user")).fetchall()
        print("Ran query ... ")
        # commit transaction (SQLAlchemy v2.X.X is commit as you go)
        db_conn.commit()

        # Do something with the results
        for row in result:
            print(row)
        connector.close()
    
    return 'Ok'

if __name__ == '__main__':
    main('')
