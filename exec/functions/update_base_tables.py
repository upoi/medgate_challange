from functions.upsert import upsert
import pandas as pd

def update_base_tables(path_to_update: str):
    #TODO: add parameters for reading raw data from different paths
    
    # read data
    patients = pd.read_csv(f'{path_to_update}/patients.csv')
    cases = pd.read_json(f'{path_to_update}/cases.ndjson', lines=True)

    # data formatting and cleaning
    cases['case_datetime'] = pd.to_datetime(cases['case_datetime'], unit='ms', origin='unix')
    cases['case_closed_datetime'] = pd.to_datetime(cases['case_closed_datetime'], unit='ms', origin='unix')
    #TODO: remove closing time from open cases

    patients['patient_date_of_birth'] = pd.to_datetime(patients['patient_date_of_birth'])
    patients['updated_at'] = pd.to_datetime(patients['updated_at'])
    #TODO: clean and format phone num and address fields

    # extract icpc codes and creat case - icpc relation table
    case_class = cases.loc[:, ('case_id', 'icpc_codes')]
    case_class['icpc_code'] = case_class['icpc_codes'].str.split(" ")
    case_class = case_class.explode('icpc_code')

    case_class = case_class.drop(columns='icpc_codes', axis=1)
    cases = cases.drop(columns='icpc_codes', axis=1)
        
    # update database tables
    upsert(patients, "dev_analytics_base", "patients")
    upsert(cases, "dev_analytics_base", "cases")
    upsert(case_class, "dev_analytics_base", "case_class", pk_columns=['case_id', 'icpc_code'])
    