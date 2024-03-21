from pyspark.sql import SparkSession
from queue import Queue
import multiprocessing
import user_script  # Import the user script
import threading

# Initialize a lock for controlling access to the available_workers list
lock = threading.Lock()

def distribute_tasks(tasks):
    """
    Distribute tasks to worker nodes for processing.
    """
    return spark.sparkContext.parallelize(tasks, len(tasks))

def assign_tasks_to_workers(task_queue, available_workers):
    """
    Assign tasks to available workers.
    """
    # Check if there are any available workers
    with lock:
        if not available_workers:
            print("No available workers. Waiting for workers to become available.")
            return
    
    # Proceed with task assignment
    while not task_queue.empty():
        with lock:
            if not available_workers:
                # No available workers, wait until one becomes available
                print("No available workers. Waiting for workers to become available.")
                continue
            task = task_queue.get()
            worker = available_workers.pop()
        solve_task(worker, task)

def solve_task(worker, task):
    """
    Solve the task assigned to the worker.
    """
    task_type, index = task
    if task_type == 'row':
        user_script.solve_row(index, puzzle)
    elif task_type == 'column':
        user_script.solve_column(index, puzzle)
    elif task_type == 'subgrid':
        user_script.solve_subgrid(index, puzzle)
    
    # After solving the task, mark worker as available
    with lock:
        available_workers.append(worker)

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Sudoku Solver") \
    .getOrCreate()

# Define Sudoku puzzle (in the user script)
puzzle = user_script.get_puzzle()

# Define tasks for workers
tasks = [('row', i) for i in range(9)] + [('column', i) for i in range(9)] + [('subgrid', i) for i in range(9)]
task_queue = Queue()
for task in tasks:
    task_queue.put(task)

# Define the number of workers available
num_workers = multiprocessing.cpu_count()  # Number of available CPU cores

# Initialize available workers list
available_workers = [i for i in range(num_workers)]

# Assign tasks to available workers
assign_tasks_to_workers(task_queue, available_workers)

# Validate the puzzle
valid = user_script.is_valid_puzzle(puzzle)
if valid:
    print("Sudoku puzzle solved successfully.")
else:
    print("Error: Invalid Sudoku puzzle.")

# Stop Spark session
spark.stop()