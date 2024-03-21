from pyspark import SparkContext, SparkConf
#import user_script  # Import the user script

def get_puzzle():
    """
    Define and return the Sudoku puzzle.
    """
    puzzle = [
        [5, 3, 0, 0, 7, 0, 0, 0, 0],
        [6, 0, 0, 1, 9, 5, 0, 0, 0],
        [0, 9, 8, 0, 0, 0, 0, 6, 0],
        [8, 0, 0, 0, 6, 0, 0, 0, 3],
        [4, 0, 0, 8, 0, 3, 0, 0, 1],
        [7, 0, 0, 0, 2, 0, 0, 0, 6],
        [0, 6, 0, 0, 0, 0, 2, 8, 0],
        [0, 0, 0, 4, 1, 9, 0, 0, 5],
        [0, 0, 0, 0, 8, 0, 0, 7, 9]
    ]
    return puzzle

def solve_row(row, puzzle):
    """
    Solve the specified row of the Sudoku puzzle.
    """
    solve(puzzle, [row], range(9))  # Pass a list containing the row

def solve_column(column, puzzle):
    """
    Solve the specified column of the Sudoku puzzle.
    """
    solve(puzzle, range(9), [column])  # Pass a list containing the column

def solve_subgrid(subgrid, puzzle):
    """
    Solve the specified 3x3 subgrid of the Sudoku puzzle.
    """
    start_row, start_col = 3 * (subgrid // 3), 3 * (subgrid % 3)
    solve(puzzle, range(start_row, start_row + 3), range(start_col, start_col + 3))

def is_valid_puzzle(puzzle):
    """
    Check if the entire Sudoku puzzle is valid.
    """
    for row in range(9):
        for col in range(9):
            if puzzle[row][col] != 0:
                num = puzzle[row][col]
                if not is_valid(puzzle, row, col, num):
                    return False
    return True

def is_valid(puzzle, row, col, num):
    """
    Check if placing 'num' in position (row, col) of the puzzle is valid.
    """
    # Check if 'num' already exists in the row or column
    for i in range(9):
        if puzzle[row][i] == num or puzzle[i][col] == num:
            return False

    # Check the subgrid
    start_row, start_col = 3 * (row // 3), 3 * (col // 3)
    for i in range(3):
        for j in range(3):
            if puzzle[i + start_row][j + start_col] == num:
                return False

    return True

def solve(puzzle, rows, cols):
    """
    Backtracking function to solve the Sudoku puzzle.
    """
    for row in rows:
        for col in cols:
            if puzzle[row][col] == 0:
                for num in range(1, 10):
                    if is_valid(puzzle, row, col, num):
                        puzzle[row][col] = num
                        if solve(puzzle, rows, cols):
                            return True
                        puzzle[row][col] = 0  # Backtrack
                return False
    return True

# Initialize Spark session
sparkconf = SparkConf().setAppName("Sudoku Solver").setMaster("spark://ubuntu-server-2204:7077")
sparkcontext = SparkContext(conf=sparkconf)

# Define Sudoku puzzle (in the user script)
puzzle = get_puzzle()
print(puzzle)
# Define tasks for workers
tasks = [('row', i) for i in range(9)] + [('column', i) for i in range(9)] + [('subgrid', i) for i in range(9)]
task_rdd = sparkcontext.parallelize(tasks)
print(tasks)
# Solve tasks using Spark
def solve_task(task):
    task_type, index = task
    if task_type == 'row':
        solve_row(index, puzzle)
    elif task_type == 'column':
        solve_column(index, puzzle)
    elif task_type == 'subgrid':
        solve_subgrid(index, puzzle)
    return task

solved_tasks_rdd = task_rdd.map(solve_task)

# Collect solved tasks
solved_tasks = solved_tasks_rdd.collect()
print(solved_tasks)
# Validate the puzzle
valid = is_valid_puzzle(puzzle)
if valid:
    print("Sudoku puzzle solved successfully.")
else:
    print("Error: Invalid Sudoku puzzle.")

print(puzzle)
# Stop Spark session
sparkcontext.stop()


