from pyspark import SparkContext, SparkConf
import user_script  # Import the user script

# Initialize Spark session
sparkconf = SparkConf().setAppName("Sudoku Solver").setMaster("spark://ubuntu-server-2204:7077")
sparkcontext = SparkContext(conf=sparkconf)

# Define Sudoku puzzle (in the user script)
puzzle = user_script.get_puzzle()
print(puzzle)
# Define tasks for workers
tasks = [('row', i) for i in range(9)] + [('column', i) for i in range(9)] + [('subgrid', i) for i in range(9)]
task_rdd = sparkcontext.parallelize(tasks)
print(tasks)
# Solve tasks using Spark
def solve_task(task):
    task_type, index = task
    if task_type == 'row':
        user_script.solve_row(index, puzzle)
    elif task_type == 'column':
        user_script.solve_column(index, puzzle)
    elif task_type == 'subgrid':
        user_script.solve_subgrid(index, puzzle)
    return task

solved_tasks_rdd = task_rdd.map(solve_task)

# Collect solved tasks
solved_tasks = solved_tasks_rdd.collect()
print(solved_tasks)
# Validate the puzzle
valid = user_script.is_valid_puzzle(puzzle)
if valid:
    print("Sudoku puzzle solved successfully.")
else:
    print("Error: Invalid Sudoku puzzle.")

print(puzzle)
# Stop Spark session
sparkcontext.stop()
