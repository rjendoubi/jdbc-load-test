import locale
locale.setlocale(locale.LC_ALL, '')

from csv import writer
from datetime import datetime, timedelta
from dotenv import load_dotenv
from impala.dbapi import connect
from os import getenv


cleanup = True


def setup_target(run_label):
    load_dotenv()
    USERNAME=getenv("JLT_USERNAME", "USERNAME_REQUIRED")
    PASSWORD=getenv("JLT_PASSWORD", "PASSWORD_REQUIRED")
    JDBC_URL=getenv("JLT_JDBC_URL", "JDBC_URL_REQUIRED")

    print(f"JDBC URL: {JDBC_URL}")
    # Do it this hand-rolled way as urllib.parse chokes on some Impala
    # JDBC URLs, I think due to the multiple scheme components.
    HOST = JDBC_URL.split('/')[2]
    HOST = HOST.split(':')[0]
    PORT = 443

    connection_kwargs = {
        "host": HOST,
        "port": PORT,
        "auth_mechanism": "LDAP",
        "user": USERNAME,
        "password": PASSWORD,
        "use_http_transport": True,
        "http_path": "cliservice",
        "use_ssl": True
    }
    print("Initiating Impala connection with params:")
    print({ x: (connection_kwargs[x] if x != 'password' else 'xxxxxxxxx') for x in connection_kwargs.keys()})
    conn = connect(**connection_kwargs)

    dbcursor = conn.cursor()

    if dbcursor.database_exists(run_label):
        print(f"Re-creating database '{run_label}'")
        dbcursor.execute(f"DROP DATABASE `{run_label}`")
    else:
        print(f"Creating database '{run_label}'")

    dbcursor.execute(f"CREATE DATABASE `{run_label}`")

    return conn

def prepare_values_str (record):
    values_str = "("
    for val in record:
        if type(val) == str:
            values_str += f"'{val}',"
        else:
            values_str += f"{val},"
    values_str = values_str[0:-1]  # Trim trailing comma
    values_str += ")"
    return values_str


def load_test(conn, db_name, csv, cleanup=True):
    # TODO: update with faker.generate() in a way that doesn't unduly
    # slow down the overall time measurement. Maybe cache to a file?
    # TODO: Make the schema configurable
    test_record = (1, "foo", 2.345, 10000000000, "bar")
    values_str = prepare_values_str(test_record)
    col_schema_clause = "(my_int INT, my_str STRING, my_flt FLOAT, my_big BIGINT, my_str2 STRING)"
    print(values_str)
    print(col_schema_clause)

    # TODO: Make all iteration params configurable
    row_magnitudes = range(0, 2)  # range(0,5)
    insert_multiples = [1, 2, 5]
    insert_magnitudes = range(0, 2)  # range(0,4)

    for row_mag in row_magnitudes:
        num_rows = 10**row_mag
        for ins_mag in insert_magnitudes:
            for ins_mult in insert_multiples:
                num_inserts = ins_mult * (10**ins_mag)
                table_name = f"insert_{num_rows}_rows_{num_inserts}_times"
                fq_table = f"`{db_name}`.`{table_name}`"
                create_sql = f"CREATE TABLE {fq_table} {col_schema_clause}"
                print("    " + f"Creating table {fq_table}")

                dbcursor = conn.cursor()
                dbcursor.execute(create_sql)
                print("        ... table created.")

                print(f"    Inserting {num_rows} rows {num_inserts} times")
                insert_sql = f"INSERT INTO {fq_table} VALUES " + ", ".join([values_str] * num_rows)

                start = datetime.now()

                for i in range(1, num_inserts+1):
                    dbcursor.execute(insert_sql)

                duration = datetime.now() - start
                seconds = (duration / timedelta(seconds=1))
                print(f"        ... Took {seconds:n} seconds.")

                row = (table_name,
                    num_rows,
                    num_inserts,
                    (duration / timedelta(microseconds=1)),
                    seconds)

                csv.writerow(row)

                if (cleanup):
                    print(f"    Dropping table {fq_table}")
                    dbcursor.execute(f"DROP TABLE {fq_table}")
                    print("        ... table dropped 🧹")


run_label = "jdbc_load_test_" + datetime.now().strftime('%Y%m%d_%H%M%S')
print(f"Run label (database name) is '{run_label}'")

print("Setting up connection and creating database...")
conn = setup_target(run_label)  # create target database
print("    Database setup complete")

with open(f"{run_label}.csv", "w") as outputfile:
    print("Running load test...")
    csv = writer(outputfile)
    csv.writerow((
        "Table",
        "Rows per Batch",
        "Num Inserts",
        "Duration in Microseconds",
        "Duration in Seconds"))
    load_test(conn, run_label, csv)
    print("    ... test complete!")



if (cleanup):
    print(f"Dropping database '{run_label}'")
    conn.cursor().execute(f"DROP DATABASE `{run_label}`")
    print("    ... database dropped 👍🏼")