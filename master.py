from pyspark import SparkContext, SparkConf
import urllib.request
import websockets
import asyncio
import json

# Initialize Spark session
sparkconf = SparkConf().setAppName("Sudoku Solver") \
                        .setMaster("spark://10.0.0.4:7077") \
                        .set("spark.driver.host", "145.220.74.141") \
                        .set("spark.driver.bindAddress", "10.0.0.4") \
                        .set("spark.driver.port","50243") \
                        .set("spark.executor.memoryOverhead", "512m")  # Additional overhead for each executor


sparkcontext = SparkContext(conf=sparkconf)

# URL of the file server where the script is saved
file_server_url = "http://file_server_ip:port/user_script.py"
# Path to save the script locally
local_script_path = "user_script.py"

# # Download the user script from the file server
# urllib.request.urlretrieve(file_server_url, local_script_path)

# Import the user script
import user_script

# Distribute user_script.py to all workers
sparkcontext.addFile(local_script_path)

# Define Sudoku puzzle (in the user script)
puzzle = user_script.get_puzzle()

# Define tasks for workers
tasks = [('cell', (i, j)) for i in range(9) for j in range(9)]
# task_rdd = sparkcontext.parallelize(tasks)

# Solve tasks using Spark
def solve_task(task, puzzle):
    cell_type, (row, col) = task
    if cell_type == 'cell':
        print("Solving task: ", task)
        result = user_script.solve_cell(row, col, puzzle)
        print(result)
        return result
    return None

def update_puzzle(solution):
    print("updating puzzle")
    print_puzzle(puzzle)
    row, col, value = solution
    puzzle[row][col] = value

# Function to print the puzzle in a formatted way
def print_puzzle(puzzle):
    for row in puzzle:
        print(' '.join(map(str, row)))

# Get the puzzle
current_puzzle = puzzle.copy()

unsolved_tasks = tasks.copy()  # Maintain a copy of unsolved tasks
while unsolved_tasks:
    task = unsolved_tasks.pop(0)  # Get the first unsolved task
    print("Unsolved tasks remaining:", len(unsolved_tasks))
    print (unsolved_tasks)
    # Solve task using Spark, passing the current puzzle version
    solved_task = sparkcontext.parallelize([task]).map(lambda t: solve_task(t, puzzle)).filter(lambda x: x is not None).collect()[0]
    
    # Update puzzle
    update_puzzle(solved_task)

# Validate the puzzle
valid = user_script.is_valid_puzzle(puzzle)
if valid:
    print("Sudoku puzzle solved successfully.")
    print_puzzle(puzzle)
else:
    print("Error: Invalid Sudoku puzzle.")
    print_puzzle(puzzle)

# Stop Spark session
sparkcontext.stop()