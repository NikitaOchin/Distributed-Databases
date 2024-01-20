from threading import Thread
import psycopg2
import time


def table_create(table_name):
    # Connect to an existing database
    conn = psycopg2.connect("dbname=postgres user=postgres password=example")

    # Open a cursor to perform database operations
    cur = conn.cursor()

    # Execute a command: Create new schema dist_db
    cur.execute('''
    CREATE SCHEMA IF NOT EXISTS dist_db;
    ''')

    # Execute a command: drop table if exists
    cur.execute(f'''
    DROP TABLE IF EXISTS dist_db.{table_name};
    ''')

    # Execute a command: this creates a new table
    cur.execute(f'''
    CREATE TABLE IF NOT EXISTS dist_db.{table_name}
    (
        user_id serial PRIMARY KEY,
        Counter integer,
        Version integer
    );
    ''')

    # Insert user id = 1
    cur.execute(f'''
    INSERT INTO dist_db.{table_name} (Counter, Version) VALUES (0, 0);
    ''')

    # Make the changes to the database persistent
    conn.commit()

    # Close communication with the database
    cur.close()
    conn.close()


def counter_update(table_name, update_func):
    # Connect to an existing database
    conn = psycopg2.connect("dbname=postgres user=postgres password=example")

    # Open a cursor to perform database operations
    cursor = conn.cursor()

    # Pass data to fill a query placeholders and let Psycopg perform
    # the correct conversion (no more SQL injections!)
    for i in range(10000):
        update_func(conn, cursor, table_name)

    # Close communication with the database
    cursor.close()
    conn.close()


def way_1_lost_update(conn, cursor, table_name):
    cursor.execute(f"SELECT Counter FROM dist_db.{table_name} WHERE user_id = 1")
    counter = cursor.fetchone()[0]
    counter = counter + 1
    cursor.execute(f"UPDATE dist_db.{table_name} set Counter = {counter} WHERE user_id = 1;")
    conn.commit()


def way_2_in_place_update(conn, cursor, table_name):
    cursor.execute(f"UPDATE dist_db.{table_name} set Counter = Counter + 1 WHERE user_id = 1;")
    conn.commit()


def way_3_row_level_locking(conn, cursor, table_name):
    cursor.execute(f"SELECT Counter FROM dist_db.{table_name} WHERE user_id = 1 FOR UPDATE")
    counter = cursor.fetchone()[0]
    counter = counter + 1
    cursor.execute(f"UPDATE dist_db.{table_name} set Counter = {counter} WHERE user_id = 1;")
    conn.commit()


def way_4_optimistic_concurrency_control(conn, cursor, table_name):
    while True:
        cursor.execute(f"SELECT Counter, Version FROM dist_db.{table_name} WHERE user_id = 1")
        (counter, version) = cursor.fetchone()
        counter = counter + 1
        cursor.execute(f"UPDATE dist_db.{table_name} set Counter = {counter}, Version = {version} + 1 WHERE user_id = 1 AND version = {version};")
        conn.commit()
        count = cursor.rowcount
        if count > 0:
            break


def main(table_name, update_func):
    # Create table for test
    table_create(table_name)

    # Initiate time count
    print(f'Start at {time.strftime("%H:%M:%S", time.localtime())}')
    start_time = time.time()

    # Run Threads
    threads = [Thread(target=counter_update, args=(table_name, update_func)) for user in range(10)]
    [thread.start() for thread in threads]
    [thread.join() for thread in threads]

    # Print Result
    print("Completed by {all_time_minute}:{all_time_second} minutes at {now}. Whole seconds is {all_time}".
          format(all_time_minute=int(time.time() - start_time) // 60,
                 all_time_second=int(time.time() - start_time) % 60,
                 all_time=int(time.time() - start_time),
                 now=time.strftime("%H:%M:%S", time.localtime())
                 )
          )


if __name__ == '__main__':
    main(f'user_counter_v1', way_1_lost_update)
    main(f'user_counter_v2', way_2_in_place_update)
    main(f'user_counter_v3', way_3_row_level_locking)
    main(f'user_counter_v4', way_4_optimistic_concurrency_control)
