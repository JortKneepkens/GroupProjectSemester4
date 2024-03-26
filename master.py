from pyspark import SparkContext, SparkConf
import user_script  # Import the user script

# Initialize Spark session
sparkconf = SparkConf().setAppName("Sudoku Solver").setMaster("spark://192.168.0.4:7077")
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
    print(puzzle)
    row, col, value = solution
    puzzle[row][col] = value

# Get the number of executors (workers)
num_workers = sparkcontext._jsc.sc().getExecutorMemoryStatus().size()

# Print the number of workers
print("Number of workers:", num_workers)

# Distribute tasks among workers
current_puzzle = puzzle.copy()

# Solve tasks using Spark, passing the current puzzle version
# solved_tasks_rdd = task_rdd.map(lambda task: solve_task(task, current_puzzle)).filter(lambda x: x is not None)
# solved_tasks_rdd.foreach(update_puzzle)
# print(task_rdd.count())
# print("distributing tasks")
# Check if there are more tasks to distribute
# if not task_rdd.isEmpty():
#     # Some tasks are remaining, redistribute them
#     task_rdd = sparkcontext.parallelize(task_rdd.collect())

# Assign tasks to workers and continuously update puzzle
unsolved_tasks = tasks.copy()  # Maintain a copy of unsolved tasks
while unsolved_tasks:
    task = unsolved_tasks.pop(0)  # Get the first unsolved task
    print(unsolved_tasks.count())
    print (unsolved_tasks)
    # Solve task using Spark, passing the current puzzle version
    solved_task = sparkcontext.parallelize([task]).map(lambda t: solve_task(t, puzzle)).filter(lambda x: x is not None).collect()[0]
    
    # Update puzzle
    update_puzzle(solved_task)

# Validate the puzzle
valid = user_script.is_valid_puzzle(puzzle)
if valid:
    print("Sudoku puzzle solved successfully.")
    print(puzzle)
else:
    print("Error: Invalid Sudoku puzzle.")

# Stop Spark session
sparkcontext.stop()