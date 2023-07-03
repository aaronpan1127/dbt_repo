import pyspark.sql.functions as F

def model(dbt, session):
    dbt.config(
        materialized = "table",
        create_notebook = True
    )
    df = dbt.ref("UnbilledAccrualResultSummary")

    # session.sql(f"""insert into dbt_poc.audit (log_identifier,model, state, time) values ('test','python_test', 'end', null)""")

    final_df = df.limit(500)

    return final_df