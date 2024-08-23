from threads import threads
from utils import utils
import multiprocessing, time

M_PROC = 10
N_THREADS = 2  # Number of threads per process (not used in this example)


def worker_function(process_id, inputs) -> None:
    # Do something
    pass


def main():
    processes = []  # List where we will stoer all the process objects
    all_inputs = {}

    # Split the inputs into manageable buckets
    input_buckets = utils.bucket_inputs(all_inputs, M_PROC, N_THREADS, method="random")

    # Iterate over the input buckets and create a process for each one
    for bucket_id in range(0, len(input_buckets), N_THREADS):
        # We pick out N_THREADS number of buckets at a time
        target_bucket = {}
        for i in range(N_THREADS):
            target_bucket[bucket_id + i] = input_buckets[bucket_id + i]

        # We create a process object
        process = multiprocessing.Process(
            target=threads.spawn_thread,
            args=(f"{bucket_id:02}", target_bucket),
            name=f"Process-{bucket_id:02}",
        )

    # We append the process object to the list of processess
    processes.append(process)

    # We loop over the processes and start them
    started = []  # List to keep track of all the processes that have been started
    for ith, process in enumerate(processes):
        print(f"  starting process {ith}/{len(processes)-1}")
        process.start()
        started.append(process)

    # Wait for all the started processes to finish before continuing
    for process in started:
        process.join()  # The join() method ensures that the main program waits for this process to complete

    # Transform Phase starts here
    # ....


if __name__ == "__main__":
    start = time.time()
    main()
    print(f"Total time: {round(time.time()-start,2)}s")
