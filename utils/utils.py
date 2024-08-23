import time
import random


def bucket_inputs(
    date_map: dict, N_PROC: int = 4, M_THREADS: int = 2, method: str = "sequential"
) -> list[dict]:
    batches = []
    if M_THREADS > 0:
        number_of_batches = N_PROC * M_THREADS
    for i in range(number_of_batches):
        batches.append({})
    i = 0
    if method == "sequential":
        for k, v in date_map.items():
            batches[i][k] = v
            i = increment_batch_number(i, number_of_batches)
    elif method == "random":
        shuffle = True
        if number_of_batches == 1:
            shuffle = False
        max_length, tries = 1, 0
        keys = list(date_map.keys())
        if shuffle:
            random.shuffle(keys)
        for key in keys:
            i = random.randint(0, number_of_batches - 1)
            while len(batches[i]) == max_length and tries < number_of_batches:
                i = increment_batch_number(i, number_of_batches)
                tries += 1
            tries = 0
            batches[i][key] = date_map[key]
            max_length = max(max_length, len(batches[i]))

    print(f"bundler: split {len(date_map)} airports into {len(batches)} bucket")
    for i, batch in enumerate(batches):
        print(f"bucket {i}: {len(batch)} airports")
    time.sleep(1)
    return batches


def increment_batch_number(i: int, number_of_batches: int):
    if i == number_of_batches - 1:
        return 0
    return i + 1
