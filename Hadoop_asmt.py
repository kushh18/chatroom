import threading

# Read input file and store it in memory
with open('input_file.txt', 'r') as f:
    input_data = f.read()

# Define Map function
def map_fn(chunk):
    word_counts = {}
    for word in chunk.split():
        if word not in word_counts:
            word_counts[word] = 1
        else:
            word_counts[word] += 1
    return word_counts.items()

# Define Partition function
def partition_fn(key_value_pairs, num_partitions):
    partitions = [[] for _ in range(num_partitions)]
    for key, value in key_value_pairs:
        partition_idx = hash(key) % num_partitions
        partitions[partition_idx].append((key, value))
    return partitions

# Define Reduce function
def reduce_fn(partition):
    word_counts = {}
    for key, value in partition:
        if key not in word_counts:
            word_counts[key] = value
        else:
            word_counts[key] += value
    return word_counts.items()

# Define number of threads
num_threads = 4

# Divide input data into chunks
chunk_size = len(input_data) // num_threads
chunks = [input_data[i:i+chunk_size] for i in range(0, len(input_data), chunk_size)]

# Create Map and Reduce threads
map_threads = []
reduce_threads = []
for i in range(num_threads):
    # Map threads
    map_thread = threading.Thread(target=map_fn, args=(chunks[i],))
    map_threads.append(map_thread)
    # Reduce threads
    partition = partition_fn(map_thread.result(), num_threads)
    for j in range(num_threads):
        reduce_thread = threading.Thread(target=reduce_fn, args=(partition[j],))
        reduce_threads.append(reduce_thread)

# Start threads
for thread in map_threads + reduce_threads:
    thread.start()

# Wait for threads to finish
for thread in map_threads + reduce_threads:
    thread.join()

# Combine results from Reduce functions
word_counts = {}
for thread in reduce_threads:
    for key, value in thread.result():
        if key not in word_counts:
            word_counts[key] = value
        else:
            word_counts[key] += value

# Print word counts
for key, value in word_counts.items():
    print(key, value)
