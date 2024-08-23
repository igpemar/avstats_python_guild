from threads import threads
from utils import utils
import multiprocessing
import pickle
import time


M_PROC = 10
N_THREADS = 4
N_AIRPORTS = 1500


def main():
    request_map = pickle.load(open("request_map.pckl", "rb"))
    processes, process_id = [], 0
    request_map = dict(list(request_map.items())[:N_AIRPORTS])
    # Bundle the requests into buckets
    request_buckets = utils.bucket_inputs(
        request_map, M_PROC, N_THREADS, method="random"
    )

    # We iterate over the request buckets and create a process for every N_THREADS
    for bucket_id in range(0, len(request_buckets), N_THREADS):
        target_bucket = {}
        for i in range(N_THREADS):
            target_bucket[bucket_id + i] = request_buckets[bucket_id + i]
        # We create a process object
        process = multiprocessing.Process(
            target=threads.spawn_thread, args=(process_id, target_bucket)
        )
        processes.append(process)

        process_id += 1

        # We append the process object to the list of processess

    # We loop over the processes and start them
    started = []
    for ith, process in enumerate(processes):
        print(f"  starting process {ith}/{len(processes)-1}")
        process.start()
        started.append(process)

    # We loop over the processes and join them, meaning, we wait for them to finish before continuing
    for process in processes:
        process.join()


if __name__ == "__main__":
    start = time.time()
    main()
    print(f"Total time: {round(time.time()-start,2)}s")
