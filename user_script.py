def get_puzzle():
    # Example puzzle (replace it with your actual puzzle)
    # puzzle = [
    #     [5, 3, 0, 0, 7, 0, 0, 0, 0],
    #     [6, 0, 0, 1, 9, 5, 0, 0, 0],
    #     [0, 9, 8, 0, 0, 0, 0, 6, 0],
    #     [8, 0, 0, 0, 6, 0, 0, 0, 3],
    #     [4, 0, 0, 8, 0, 3, 0, 0, 1],
    #     [7, 0, 0, 0, 2, 0, 0, 0, 6],
    #     [0, 6, 0, 0, 0, 0, 2, 8, 0],
    #     [0, 0, 0, 4, 1, 9, 0, 0, 5],
    #     [0, 0, 0, 0, 8, 0, 0, 7, 9]
    # ]
    puzzle = [
        [1, 2, 3, 4, 5, 6, 7, 8, 0],
        [6, 0, 0, 1, 9, 5, 0, 0, 0],
        [0, 9, 8, 0, 0, 0, 0, 6, 0],
        [8, 0, 0, 0, 6, 0, 0, 0, 3],
        [4, 0, 0, 8, 0, 3, 0, 0, 1],
        [7, 0, 0, 0, 2, 0, 0, 0, 6],
        [0, 6, 0, 0, 0, 0, 2, 8, 0],
        [0, 0, 0, 4, 1, 9, 0, 0, 5],
        [0, 0, 0, 0, 8, 0, 0, 7, 0]
    ]
    return puzzle

def is_valid_move(puzzle, row, col, num):
    # Check if the move is valid for the given row, column, and subgrid
    return (
        all(num != puzzle[row][i] for i in range(9)) and
        all(num != puzzle[i][col] for i in range(9)) and
        all(num != puzzle[row//3*3 + i][col//3*3 + j] for i in range(3) for j in range(3))
    )

def solve_sudoku(puzzle):
    empty_cell = find_empty_cell(puzzle)
    if empty_cell is None:
        return True  # Puzzle is solved

    row, col = empty_cell
    for num in range(1, 10):
        if is_valid_move(puzzle, row, col, num):
            puzzle[row][col] = num
            if solve_sudoku(puzzle):  # Recursive call to solve the entire puzzle
                return True  # Return True if the puzzle is solved
            puzzle[row][col] = 0  # Backtrack if the solution is not valid
    return False  # Return False if no solution found

# def solve_cell(row, col, puzzle):
#     if puzzle[row][col] == 0:
#         for num in range(1, 10):
#             if is_valid_move(puzzle, row, col, num):
#                 puzzle[row][col] = num
#                 if solve_sudoku(puzzle):  # Attempt to solve the puzzle
#                     print("Returning found solution for cell")
#                     return row, col, num  # Return solution tuple if the puzzle is solved
#                 puzzle[row][col] = 0  # Backtrack if the solution is not valid
#         return row, col, 0  # Return None if no valid move found
#     return row, col, puzzle[row][col]  # Return the same number if the cell is already filled

def solve_cell(row, col, puzzle):
    if puzzle[row][col] == 0:
        valid_moves = []
        for num in range(1, 10):
            if is_valid_move(puzzle, row, col, num):
                valid_moves.append(num)
                if len(valid_moves) > 1:
                    # If there are more than one valid move, return (row, col, 0)
                    return row, col, 0
        
        if len(valid_moves) == 1:
            # If there is only one valid move, return the solution
            num = valid_moves[0]
            puzzle[row][col] = num
            return row, col, num
        else:
            # If no valid move found, return (row, col, 0)
            return row, col, 0
    else:
        # If the cell is already filled, return the number in the cell
        return row, col, puzzle[row][col]

def find_empty_cell(puzzle):
    for row in range(9):
        for col in range(9):
            if puzzle[row][col] == 0:
                return row, col
    return None  # No empty cell found

def is_valid_puzzle(puzzle):
    # Implement logic to validate if the Sudoku puzzle is solved correctly
    # Check rows, columns, and subgrids for duplicates
    def is_valid(arr):
        seen = set()
        for num in arr:
            if num != 0 and num in seen:
                return False
            seen.add(num)
        return True

    for i in range(9):
        if not is_valid(puzzle[i]):  # Check rows
            return False
        if not is_valid([puzzle[j][i] for j in range(9)]):  # Check columns
            return False
        if not is_valid([puzzle[i//3*3 + j//3][i%3*3 + j%3] for j in range(9)]):  # Check subgrids
            return False
    return True