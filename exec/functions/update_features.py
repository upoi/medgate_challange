import pandas as pd
import sqlalchemy as sa
from datetime import date
from datetime import datetime

def update_features():
    engine = sa.create_engine('postgresql+psycopg2://db_user:db_password@localhost/db_name')

    # calculate patient age
    patient = pd.read_sql(
        """
        SELECT 
            patient_id,
            patient_date_of_birth
        FROM dev_analytics_base.patients 
        """, con=engine)
    patient["age"] = patient["patient_date_of_birth"].apply(lambda x : (date.today().year - x.year))
    
    # obey constraint of patient_features table
    patient.loc[patient['age'] < 0, 'age'] = None 
    
    # calculate case_open, most recet case and case count
    patient_cases = pd.read_sql(
        """
        SELECT 
            p.patient_id,
            p.patient_date_of_birth,
            c.case_id,
            c.case_type,
            c.case_datetime,
            c.case_closed,
            c.case_closed_datetime
        FROM dev_analytics_base.patients p 
        JOIN dev_analytics_base.cases c USING (patient_id)
        """, con=engine)

    patient_open_case = patient_cases.loc[patient_cases.groupby('patient_id').case_closed.idxmin()].reset_index(drop=True)
    patient_open_case['has_open_case'] = patient_open_case['case_closed']
    patient_open_case[['patient_id', 'has_open_case']]

    patient_last_case_close = patient_cases.loc[patient_cases.groupby('patient_id').case_closed_datetime.idxmax()].reset_index(drop=True)
    patient_last_case_close['last_case_closed'] = patient_open_case['case_closed_datetime']
    patient_last_case_close[['patient_id', 'last_case_closed']]

    patient_cases_cnt = patient_cases.groupby('patient_id', as_index=False).agg(count=('patient_id', 'count'))
    patient_cases_cnt['case_cnt'] = patient_cases_cnt['count']

    patient_features = patient[['patient_id', 'age']].\
        merge(patient_open_case[['patient_id', 'has_open_case']], on='patient_id', how='left').\
        merge(patient_last_case_close[['patient_id', 'last_case_closed']], on='patient_id', how='left').\
        merge(patient_cases_cnt[['patient_id', 'case_cnt']], on='patient_id', how='left')

    patient_features['updated_at'] = datetime.now()
    patient_features

    # recreate patients_features table and fill with update
    with engine.begin() as conn: 
            conn.exec_driver_sql(f"DROP TABLE IF EXISTS dev_feature_mart.patient_features")
            conn.exec_driver_sql(
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
            patient_features.to_sql(name="patient_features", schema="dev_feature_mart", con=conn, index=False, if_exists="append")
    
    