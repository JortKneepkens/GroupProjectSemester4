import os
import sys
from pyspark import SparkContext, SparkConf
import ftplib
import websockets
import asyncio
import json
import importlib
import cloudpickle

# Initialize Spark session
sparkconf = SparkConf().setAppName("Sudoku Solver") \
                        .setMaster("spark://10.0.0.4:7077") \
                        .set("spark.driver.host", "145.220.74.141") \
                        .set("spark.driver.bindAddress", "10.0.0.4") \
                        .set("spark.driver.port","50243") \
                        .set("spark.executor.memoryOverhead", "512m")  # Additional overhead for each executor


sparkcontext = SparkContext(conf=sparkconf)

ftp_server = "192.168.0.2"
ftp_username = "sparkmaster"
ftp_password = "P@ssword"

async def retrieve_file(ftp_server, ftp_username, ftp_password, remote_filename, local_filename):
    try:
        # Connect to the FTP server
        ftp = ftplib.FTP(ftp_server)
        ftp.login(ftp_username, ftp_password)

        # Download the file
        with open(local_filename, 'wb') as local_file:
            ftp.retrbinary('RETR ' + remote_filename, local_file.write)

        print(f"File '{remote_filename}' downloaded successfully to '{local_filename}'")
        
        return True
    except Exception as e:
        print(f"Error downloading file from FTP: {e}")
        return False
    finally:
        # Close the FTP connection
        ftp.quit()

# Define tasks for workers
tasks = [('cell', (i, j)) for i in range(9) for j in range(9)]

puzzle = None
user_script_module = None

# Solve tasks using Spark
def solve_task(task, puzzle):
    cell_type, (row, col) = task
    if cell_type == 'cell':
        print("Solving task: ", task)
        result = user_script_module.solve_cell(row, col, puzzle)
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

# Define the cleanup function
def cleanup(local_filename):
    # Delete the file from the local filesystem
    print("removing file: " + local_filename)
    os.remove(local_filename)
    print("following file removed: " + local_filename)

# Serialize the cleanup function
serialized_cleanup = cloudpickle.dumps(cleanup)

# Pass the serialized cleanup function to every worker
sparkcontext.broadcast(serialized_cleanup)

async def main():
    global puzzle
    global user_script_module
    uri = "ws://192.168.0.9:8181"  # Replace with the WebSocket server URI
    print(f"Connecting to {uri}...")
    async with websockets.connect(uri) as websocket:
        print("Connected.")
        # Receive messages until the connection is closed
        async for message in websocket:
            try:
                parsed_message = json.loads(message)
                message_type = parsed_message.get("Type")
                message_content = parsed_message.get("Content")
                if message_type == "File_Uploaded":
                    print(f"Received message: {message_content}")
                    local_filename = message_content  # Use the message content as the local filename
                    if await retrieve_file(ftp_server, ftp_username, ftp_password, message_content, local_filename):
                        sparkcontext.addFile(local_filename)
                        # Import the downloaded file as a module
                        user_script_module = importlib.import_module(local_filename[:-3])  # Remove the ".py" extension
                        user_script_puzzle = user_script_module.get_puzzle()
                        puzzle = user_script_puzzle.copy()

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
                        valid = user_script_module.is_valid_puzzle(puzzle)
                        if valid:
                            print("Sudoku puzzle solved successfully.")
                            print_puzzle(puzzle)
                        else:
                            print("Error: Invalid Sudoku puzzle.")
                            print_puzzle(puzzle)
                        
                        # Unload the module to free up memory
                        del sys.modules[local_filename[:-3]]
                        importlib.invalidate_caches()
                        
                        # Delete the file from the local filesystem
                        os.remove(local_filename)
                        # Invoke the cleanup function on each worker
                        sparkcontext.parallelize([1]).foreach(lambda x: cloudpickle.loads(serialized_cleanup)(local_filename))

            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
            except Exception as e:
                print(f"Error processing message: {e}")

asyncio.run(main())
# Stop Spark session
sparkcontext.stop()