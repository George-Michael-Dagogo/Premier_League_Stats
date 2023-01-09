import bitdotio
import pandas as pd

# This is to provide a reproducible csv,
# you can ignore and use your own csv
df_test = pd.DataFrame(
    data=[[0, 1, 2], [3, 4, 5]],
    columns=['a', 'b', 'c'])
df_test.to_csv('test.csv', index=False)

# Connect with bit.io API key credentials
b = bitdotio.bitdotio('v2_3xntW_C6CBDbsKrZ9m9YbcHfUgCkn')

# Create table, if it does not already exist
create_table_sql = """
    CREATE TABLE "<YOUR_USERNAME>/<YOUR_REPO>"."test" (
      a integer,
      b integer,
      c integer
    )
    """

with b.get_connection() as conn:
    cursor = conn.cursor()
    cursor.execute(create_table_sql)
    
# Copy csv from a local file
copy_table_sql = """
    COPY "<YOUR_USERNAME>/<YOUR_REPO>"."test" FROM stdin WITH CSV HEADER DELIMITER as ',';
    """

with open('test.csv', 'r') as f:
    with b.get_connection() as conn:
        cursor = conn.cursor()
        cursor.copy_expert(sql=copy_table_sql, file=f)
        
# Note: you can also create a new repo from Python using the API, if needed