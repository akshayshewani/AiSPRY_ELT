import json, sqlite3, csv

sqlite_keywords = set([
        "ABORT", "ACTION", "ADD", "AFTER", "ALL", "ALTER", "ANALYZE", "AND", "AS", "ASC", "ATTACH", "AUTOINCREMENT",
        "BEFORE", "BEGIN", "BETWEEN", "BY", "CASCADE", "CASE", "CAST", "CHECK", "COLLATE", "COLUMN", "COMMIT",
        "CONFLICT", "CONSTRAINT", "CREATE", "CROSS", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "DATABASE",
        "DEFAULT", "DEFERRABLE", "DEFERRED", "DELETE", "DESC", "DETACH", "DISTINCT", "DROP", "EACH", "ELSE", "END",
        "ESCAPE", "EXCEPT", "EXCLUSIVE", "EXISTS", "EXPLAIN", "FAIL", "FOR", "FOREIGN", "FROM", "FULL", "GLOB", 
        "GROUP", "HAVING", "IF", "IGNORE", "IMMEDIATE", "IN", "INDEX", "INDEXED", "INITIALLY", "INNER", "INSERT", 
        "INSTEAD", "INTERSECT", "INTO", "IS", "ISNULL", "JOIN", "KEY", "LEFT", "LIKE", "LIMIT", "MATCH", "NATURAL", 
        "NO", "NOT", "NOTNULL", "NULL", "OF", "OFFSET", "ON", "OR", "ORDER", "OUTER", "PLAN", "PRAGMA", "PRIMARY", 
        "QUERY", "RAISE", "RECURSIVE", "REFERENCES", "REGEXP", "REINDEX", "RELEASE", "RENAME", "REPLACE", "RESTRICT", 
        "RIGHT", "ROLLBACK", "ROW", "SAVEPOINT", "SELECT", "SET", "TABLE", "TEMP", "TEMPORARY", "THEN", "TO", 
        "TRANSACTION", "TRIGGER", "UNION", "UNIQUE", "UPDATE", "USING", "VACUUM", "VALUES", "VIEW", "VIRTUAL", 
        "WHEN", "WHERE", "WITH", "WITHOUT"
    ])

def infer_data_type_sqlite(value):
    if isinstance(value, int):
        return "INTEGER"
    elif isinstance(value, float):
        return "REAL"
    elif isinstance(value, str):
        return "TEXT"
    elif isinstance(value, bytes):
        return "BLOB"
    else:
        return "TEXT"  # Default to TEXT for any other types or None values


def clean_columns(columns, sqlite_keywords):

    # Process columns: replace spaces with underscores and avoid SQLite keywords
    renamed_columns = []
    for col in columns:
        # Replace spaces with underscores
        new_col = col.replace(' ', '_')
        # If the column is a reserved keyword, append '_col' to the name
        if new_col.upper() in sqlite_keywords:
            new_col = new_col + '_col'
        renamed_columns.append(new_col)

    print('Cleaned Column Names...')
    
    return renamed_columns


def clean_json(json_file):
    
    json_objects = []
    
    with open(json_file, 'r') as file:
        for line in file:
            line = line.strip()  # Remove leading/trailing whitespaces
            if line:  # Ensure the line isn't empty
                try:
                    json_objects.append(json.loads(line))
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON: {e}")

    print('Cleaned JSON File...')
    
    return json_objects


def clean_csv(csv_file):

    csv_data = []

    with open(csv_file, 'r') as file:
        reader = csv.DictReader(file)  # Reads each row as a dictionary

        for row in reader:
            csv_data.append(row)  # Append each row (as a dict) to the list

    print('Cleaned CSV File...')
    
    return csv_data

def load_data_to_sqlite(data, table_name, db_file):
  
    # Connect to the database
    conn = sqlite3.connect(db_file)
    c = conn.cursor()

    # Get the column names from the first dictionary
    columns = list(data[0].keys())
    
    columns = clean_columns(columns, sqlite_keywords)

    first_row = data[0]
    column_types = []
    for cleaned_column_name, value in zip(columns, first_row.values()):
        col_type = infer_data_type_sqlite(value)
        column_types.append(f"{cleaned_column_name} {col_type}")


    drop_table_query = f"DROP TABLE IF EXISTS {table_name}"
    c.execute(drop_table_query)

    # Create the table with new column names
    create_table_query = f"CREATE TABLE {table_name} ({', '.join(column_types)})"
    c.execute(create_table_query)

    # Insert data into the table
    insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['?' for _ in columns])})"
    for row in data:
        # print(insert_query)
        # print(row.values())
        
        c.execute(insert_query, tuple(row.values()))
        

    # Commit changes and close the connection
    conn.commit()
    conn.close()
    print(f'Data Inserted to CSV File...{table_name}')


    
# Example usage

customer_json_file = "/Users/akshayshewani/Documents/AiSPRY_ELT/data/customers.json"
order_csv_file = "/Users/akshayshewani/Documents/AiSPRY_ELT/data/orders_12_20.csv"

customer_table_name = "customers"
orders_table_name = 'orders'
db_file = "./test.db"

customer_data = clean_json(customer_json_file)
order_data = clean_csv(order_csv_file)


load_data_to_sqlite(order_data, customer_table_name, db_file)
load_data_to_sqlite(customer_data, orders_table_name, db_file)

 