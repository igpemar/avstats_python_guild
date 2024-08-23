import time
import random

WAIT_FACTOR = 0.1
MIN_COMPLEXITY = 1000
MAX_COMPLEXITY = 100000000


def simulate_external_api_call(proc_id: int, thread_id: int):
    # Calls an external API (I/O Bound)
    print(f"{proc_id}-{thread_id}: Calling external API...")
    # time.sleep(random.uniform(0.5*K, 2.0*K))  # Simulate random API call delay


def simulate_api_response_time(proc_id: int, thread_id: int):
    # Waits for the API to respond (I/O Bound)
    print(f"{proc_id}-{thread_id}: Waiting for API response...")
    time.sleep(
        random.uniform(1.0 * WAIT_FACTOR, 6.0 * WAIT_FACTOR)
    )  # Simulate random response wait time


def simulate_api_response_handling(proc_id: int, thread_id: int):
    # Handles API response
    print(f"{proc_id}-{thread_id}: Handling API response...")
    # time.sleep(random.uniform(0.5*K, 2.0*K))  # Simulate random API handling time


def simulate_write_to_disk(proc_id: int, thread_id: int):
    # Writes response to disk (I/O Bound)
    print(f"{proc_id}-{thread_id}: Writing response to disk...")
    time.sleep(
        random.uniform(0.3 * WAIT_FACTOR, 1.0 * WAIT_FACTOR)
    )  # Simulate random disk write time


def simulate_data_validation(proc_id: int, thread_id: int):
    # Applies Data Validation Layer (CPU Bound)
    print(f"{proc_id}-{thread_id}: Applying data validation...")
    time.sleep(
        random.uniform(1.0 * WAIT_FACTOR, 30 * WAIT_FACTOR)
    )  # Simulate random data validation processing time


def simulate_data_processing(proc_id: int, thread_id: int):
    # Applies Data Processing Layer (CPU Bound)
    print(f"{proc_id}-{thread_id}: Data processing...")
    complexity = random.randint(MIN_COMPLEXITY, MAX_COMPLEXITY)
    # Simulate CPU-intensive operation
    result = 0
    for i in range(complexity):
        result += (i**2) % 1234567  # Arbitrary CPU-bound calculation


def simulate_write_to_database(proc_id: int, thread_id: int):
    # Writes to Database (I/O Bound)
    print(f"{proc_id}-{thread_id}: Writing data to database...")
    time.sleep(
        random.uniform(0.1 * WAIT_FACTOR, 10) * WAIT_FACTOR
    )  # Simulate random database write time


# Mock function to simulate the work of a worker in terms of lead time
def worker(proc_id: int, thread_id: int, request_bucket: dict) -> None:
    print("  starting worker")
    for request, data in request_bucket.items():
        for flight_type in ["DEPARTURES", "ARRIVALS"]:
            for date_range in data:
                print(
                    f"{proc_id}-{thread_id}: processing request {request} {flight_type} from {date_range[0]} to {date_range[1]}"
                )

                # Step 1
                simulate_external_api_call(proc_id, thread_id)
                # Step 2
                simulate_api_response_time(proc_id, thread_id)
                # Step 3
                simulate_api_response_handling(proc_id, thread_id)
                # Step 4
                simulate_write_to_disk(proc_id, thread_id)
                # Step 5
                simulate_data_validation(proc_id, thread_id)
                # Step 6
                simulate_data_processing(proc_id, thread_id)
                # Step 7
                simulate_write_to_database(proc_id, thread_id)

    print("  worker finished!")
    print("###############################")
