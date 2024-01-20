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


def counter_update(table_name):
    # Connect to an existing database
    conn = psycopg2.connect("dbname=postgres user=postgres password=example")

    # Open a cursor to perform database operations
    cursor = conn.cursor()

    # Pass data to fill a query placeholders and let Psycopg perform
    # the correct conversion (no more SQL injections!)
    for i in range(10000):
        cursor.execute(f"SELECT Counter FROM dist_db.{table_name} WHERE user_id = 1")
        counter = cursor.fetchone()[0]
        counter = counter + 1
        cursor.execute(f"UPDATE dist_db.{table_name} set Counter = {counter} WHERE user_id = 1;")
        conn.commit()

    # Close communication with the database
    cursor.close()
    conn.close()

def main():
    table_name = 'user_counter_v1'
    table_create(table_name)
    print(f'Start at {time.strftime("%H:%M:%S", time.localtime())}')
    # Initiate time count
    start_time = time.time()

    threads = [Thread(target=counter_update, args=(table_name,)) for user in range(10)]
    [thread.start() for thread in threads]
    [thread.join() for thread in threads]

    print("Completed by {all_time_minute}:{all_time_second} minutes at {now}. Whole seconds is {all_time}".
          format(all_time_minute=int(time.time() - start_time) // 60,
                 all_time_second=int(time.time() - start_time) % 60,
                 all_time=int(time.time() - start_time),
                 now=time.strftime("%H:%M:%S", time.localtime())
                 )
          )

if __name__ == '__main__':
    main()
