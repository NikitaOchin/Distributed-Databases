import hazelcast
from threading import Thread
import time
from hazelcast.config import Config


def counter_update_without_locks(key, map):
    for i in range(100):
        counter = map.get(key)
        map.put(key, counter + 1)


def pessimistic_update_method(key, map):
    for i in range(10000):
        map.lock(key)
        try:
            counter = map.get(key)
            map.put(key, counter + 1)
        finally:
            map.unlock(key)


def optimistic_update_method(key, map):
    for i in range(10000):
        while True:
            counter = map.get(key)
            if map.replace_if_same(key, counter, counter + 1):
                break


def atomic_update(al):
    for i in range(100):
        al.increment_and_get()


def counter_map(update_func, key):
    # Get the Distributed Map from Cluster.
    map = hz.get_map("my-distributed-map").blocking()

    # Standard Put and Get
    map.put(key, 0)

    # Run Threads
    threads = [Thread(target=update_func, args=(key, map,)) for user in range(10)]
    [thread.start() for thread in threads]
    [thread.join() for thread in threads]

    print(f'Counter after all equal to {map.get(key)}')


def counter_atomic(update_func, key):
    # Get the AtomicLong for Cluster
    al = hz.cp_subsystem.get_atomic_long("counter").blocking()

    # Standard Set
    al.set(0)

    # Run Threads
    threads = [Thread(target=update_func, args=(al,)) for user in range(10)]
    [thread.start() for thread in threads]
    [thread.join() for thread in threads]

    print(f'Counter after all equal to {al.get()}')


def main(update_func, key, type='Map'):
    # Initiate time count
    print(f'Start at {time.strftime("%H:%M:%S", time.localtime())}')
    start_time = time.time()

    if type == 'Map':
        counter_map(update_func, key)
    elif type == 'Atomic':
        counter_atomic(update_func, key)
    else:
        print('There is no function')

    # Print Result
    print("Completed by {all_time_minute}:{all_time_second} minutes at {now}. Whole seconds is {all_time}".
          format(all_time_minute=int(time.time() - start_time) // 60,
                 all_time_second=int(time.time() - start_time) % 60,
                 all_time=int(time.time() - start_time),
                 now=time.strftime("%H:%M:%S", time.localtime())
                 )
          )


if __name__ == '__main__':
    hz = hazelcast.HazelcastClient()

    # main(counter_update_without_locks, 'without_locks', 'Map')
    # main(pessimistic_update_method, 'pessimistic_update_method', 'Map')
    # main(optimistic_update_method, 'optimistic_update_method', 'Map')
    main(atomic_update, 'atomic_update', 'Atomic')

    # Shutdown this Hazelcast Client
    hz.shutdown()
