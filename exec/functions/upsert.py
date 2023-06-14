import sqlalchemy as sa
import pandas as pd

def upsert(df: pd.DataFrame, schema: str, target_table: str, pk_columns=None):
    """
    Perform an "upsert" on a PostgreSQL table from a DataFrame.
    Constructs an INSERT … ON CONFLICT statement, uploads the DataFrame to a
    temporary table, and then executes the INSERT. If PK is not in first column use 
    Parameters
    ----------
    df : pandas.DataFrame
        The DataFrame to be upserted.
    target_table : str
        The name of the target table.
    engine : sqlalchemy.engine.Engine
        The SQLAlchemy Engine to use.
    schema : str, optional
        The name of the schema containing the target table.
    pk_columns : str[], optional
        If the primary key is not the first column or consists of multiple colums list them here
    """
    
    engine = sa.create_engine("postgresql+psycopg2://db_user:db_password@localhost/db_name")
    
    with engine.begin() as conn:
              
        #create temporary table and upload DataFrame
        conn.exec_driver_sql(
            f"CREATE TABLE {schema}.{target_table}_tmp AS SELECT * FROM {schema}.{target_table} WHERE false"
        )
        df.to_sql(name=f"{target_table}_tmp", schema=schema, con=conn, index=False, if_exists="append")

        # merge temp_table into target_table
        cols_list = ', '.join(df.columns)
        updates = ', '.join([f"{e} = EXCLUDED.{e}" for e in df.columns[1:]])

        # if primary specified use it for conflict checking
        if pk_columns is None:
            upsertq =f"""\
                INSERT INTO {schema}.{target_table} ({cols_list}) 
                SELECT * FROM {schema}.{target_table}_tmp
                ON CONFLICT ({df.columns[0]}) DO
                    UPDATE SET {updates}
            """
        else:
            primary_key = ', '.join(pk_columns)
            upsertq =f"""\
                INSERT INTO {schema}.{target_table} ({cols_list}) 
                SELECT * FROM {schema}.{target_table}_tmp
                ON CONFLICT ({primary_key}) DO
                    UPDATE SET {updates}    
            """
        
        conn.exec_driver_sql(upsertq)
        conn.exec_driver_sql(f"DROP TABLE {schema}.{target_table}_tmp")
        
        # conn.close()
    
        # step 3 - confirm results
        # result = conn.exec_driver_sql(f"SELECT * FROM {schema}.{target_table} limit 200").all()
        # print(result) 
        