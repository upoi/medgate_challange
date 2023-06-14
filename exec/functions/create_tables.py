import psycopg2

def create_tables():
    """ 
    create tables in the PostgreSQL database
    TODO: impose more constraints like patient_date_of_birth < today
    """
    commands = (
        "DROP SCHEMA IF EXISTS dev_analytics_base CASCADE;",
        "CREATE SCHEMA dev_analytics_base;"
        """
        CREATE TABLE dev_analytics_base.patients (
            patient_id               VARCHAR(255) PRIMARY KEY,
            patient_name             VARCHAR(255) NOT NULL,
            patient_email            VARCHAR(255),
            patient_phone            VARCHAR(255) NOT NULL,
            patient_address          VARCHAR(255) NOT NULL,
            patient_city             VARCHAR(255) NOT NULL,
            patient_state            VARCHAR(255) NOT NULL,
            patient_zip              INTEGER NOT NULL,
            patient_country          VARCHAR(255) NOT NULL,
            patient_date_of_birth    TIMESTAMP,
            updated_at               TIMESTAMP  NOT NULL
        );
        """,
        """ 
        CREATE TABLE dev_analytics_base.cases (
            case_id                         VARCHAR(255) PRIMARY KEY,
            case_type                       INTEGER NOT NULL,
            patient_id                      VARCHAR(255) NOT NULL,
            case_datetime                   TIMESTAMP NOT NULL,
            case_closed                     BOOLEAN NOT NULL,
            case_closed_datetime            TIMESTAMP,
            case_closed_reason              VARCHAR(255) NOT NULL,
            updated_at                      TIMESTAMP NOT NULL,
            CONSTRAINT fk_patient
                FOREIGN KEY (patient_id) 
                    REFERENCES dev_analytics_base.patients (patient_id)
                    ON DELETE SET NULL
                    
            );
        """,
        """
        CREATE TABLE dev_analytics_base.case_class (
            case_id                         VARCHAR(255) NOT NULL,
            icpc_code                       VARCHAR(255) NOT NULL,
            PRIMARY KEY (case_id, icpc_code),
            CONSTRAINT fk_case
                FOREIGN KEY (case_id) 
                    REFERENCES dev_analytics_base.cases (case_id)
                    ON DELETE SET NULL
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
    create_tables()