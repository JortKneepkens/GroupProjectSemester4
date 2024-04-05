from pyspark import SparkContext, SparkConf
import os
import user_script  # Import the user script

# # Function to determine network location of the worker
# def determine_network_location():
#     # Example logic: Check environment variable to determine network location
#     print(os.environ.get("INTERNAL_NETWORK"))
#     if os.environ.get("INTERNAL_NETWORK"):
#         print("Internal")
#         return "internal"
#     else:
#         print("Internal")
#         return "external"

# Function to set spark.driver.host dynamically based on network location
def set_driver_host():
    print(os.environ)
    internal_network = os.environ.get("INTERNAL_NETWORK")
    print(internal_network)

    if internal_network == "true":
        print("Internal 10.0.0.4")
        return "10.0.0.4"  # Replace with the internal IP address
    elif internal_network == "false":
        print("External 145.220.74.141")
        return "145.220.74.141"  # Replace with the external IP address
    else:
        raise ValueError("INTERNAL_NETWORK must be set to 'true' or 'false'")

# Initialize Spark session
sparkconf = SparkConf().setAppName("Sudoku Solver") \
                        .setMaster("spark://10.0.0.4:7077") \
                        # .set("spark.driver.host", "145.220.74.141") \
                        # .set("spark.driver.bindAddress", "10.0.0.4") \
                        # .set("spark.driver.port","50243")

# Set spark.driver.host dynamically
driver_host = set_driver_host()
print(driver_host)
sparkconf.set("spark.driver.host", driver_host).set("spark.driver.bindAddress", "10.0.0.4").set("spark.driver.port","50243")

sparkcontext = SparkContext(conf=sparkconf)


# Distribute user_script.py to all workers
sparkcontext.addFile("user_script.py")

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