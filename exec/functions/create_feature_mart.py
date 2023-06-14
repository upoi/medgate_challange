import psycopg2

def create_feature_mart():
    """ create tables in the PostgreSQL database"""
    commands = (
        "DROP SCHEMA IF EXISTS dev_feature_mart CASCADE;",
        "CREATE SCHEMA dev_feature_mart;"
        """
        CREATE TABLE dev_feature_mart.patient_features (
            patient_id               VARCHAR(255) PRIMARY KEY,
            age                      INTEGER check (age >= 0),
            last_case_closed         TIMESTAMP,
            has_open_case            BOOLEAN,
            case_cnt                 INTEGER,
            updated_at               TIMESTAMP NOT NULL
        );
        """
        )
    conn = None
    try:
        # connect to the PostgreSQL server
        conn = psycopg2.connect("dbname='db_name' user='db_user' host='localhost' password='db_password'")
        
        cur = conn.cursor()
        # create table one by one
        for command in commands:
            cur.execute(command)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


if __name__ == '__main__':
    create_feature_mart()