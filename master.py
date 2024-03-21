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
task_rdd = sparkcontext.parallelize(tasks)

# Solve tasks using Spark
def solve_task(task):
    cell_type, (row, col) = task
    if cell_type == 'cell':
        print("solving task ")
        return user_script.solve_cell(row, col, puzzle)
    return None

def update_puzzle(solution):
    print("updating puzzle ")
    row, col, value = solution
    puzzle[row][col] = value

solved_tasks_rdd = task_rdd.map(solve_task).filter(lambda x: x is not None)
solved_tasks_rdd.foreach(update_puzzle)

# Validate the puzzle
valid = user_script.is_valid_puzzle(puzzle)
if valid:
    print("Sudoku puzzle solved successfully.")
else:
    print("Error: Invalid Sudoku puzzle.")

# Stop Spark session
sparkcontext.stop()