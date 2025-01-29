import functions_framework
import os

from google.cloud.sql.connector import Connector, IPTypes
import pg8000

import sqlalchemy


def connect_with_connector() -> sqlalchemy.engine.base.Engine:
    """
    Initializes a connection pool for a Cloud SQL instance of Postgres.

    Uses the Cloud SQL Python Connector package.
    """
    # Note: Saving credentials in environment variables is convenient, but not
    # secure - consider a more secure solution such as
    # Cloud Secret Manager (https://cloud.google.com/secret-manager) to help
    # keep secrets safe.

    instance_connection_name = "lopezsamuel-pf-dev:us-central1:test-cloudrun" # e.g. 'project:region:instance'
    db_user = "sam"  # e.g. 'my-db-user'
    db_pass = "123456789"  # e.g. 'my-db-password'
    db_name = "test-db"  # e.g. 'my-database'

    ip_type = IPTypes.PRIVATE if os.environ.get("PRIVATE_IP") else IPTypes.PUBLIC

    # initialize Cloud SQL Python Connector object
    connector = Connector()

    def getconn() -> pg8000.dbapi.Connection:
        conn: pg8000.dbapi.Connection = connector.connect(
            instance_connection_name,
            "pg8000",
            user=db_user,
            password=db_pass,
            db=db_name,
            ip_type=ip_type,
        )
        return conn

    # The Cloud SQL Python Connector can be used with SQLAlchemy
    # using the 'creator' argument to 'create_engine'
    pool = sqlalchemy.create_engine(
        "postgresql+pg8000://",
        creator=getconn,
        # ...
    )
    return pool

@functions_framework.http
def main(request):
    print("Preparing pool ... ")
    # create connection pool with 'creator' argument to our connection object function
    pool = connect_with_connector()
    

    with pool.connect() as db_conn:
        # Insert specific rows into the table
        ids_to_insert = [2030, 2040, 2050]
        for id_value in ids_to_insert:
            db_conn.execute(sqlalchemy.text("INSERT INTO andreas (ID) VALUES (:id)"), {"id": id_value})
        
        print("Inserted rows: ", ids_to_insert)

        # query database
        result = db_conn.execute(sqlalchemy.text("SELECT * from andreas")).fetchall()
        print("Ran query ... ")
        
        # commit transaction (SQLAlchemy v2.X.X is commit as you go)
        db_conn.commit()

        # Do something with the results
        for row in result:
            print(row)

    return 'Ok'

