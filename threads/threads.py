import threading
import time
from worker import worker


def spawn_thread(proc_id: int, request_buckets: dict) -> None:
    thread_id, threads = 0, []
    # We iterate over the request maps and create a thread for each one
    for _, request_bucket in request_buckets.items():
        # Create thread object
        thread = threading.Thread(
            target=worker.worker,
            args=(proc_id, thread_id, request_bucket),
            name=f"ExtractThread {proc_id}-{thread_id}",
        )

        # We append the thread object to the list of threads
        threads.append(thread)
        thread_id += 1

    # We loop over the threads and start them
    for i, thread in enumerate(threads):
        print(f"  starting thread {proc_id}-{i}")
        thread.start()

    # We loop over the threads and join them, meaning, we wait for them to finish
    for i, thread in enumerate(threads):
        thread.join()

    print("###############################")
    print(f" Process {proc_id} finished!")
    print("###############################")
    time.sleep(0.5)
