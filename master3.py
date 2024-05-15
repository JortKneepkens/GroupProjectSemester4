import itertools
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

websocket_uri = "ws://10.0.0.14:8181"  # Replace with the WebSocket server URI

ftp_server = "192.168.0.2"
ftp_username = "sparkmaster"
ftp_password = "P@ssword"

# Define character space
CHARACTER_SPACE = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

user_script_module = None
user_script_filename = None
hashed_password = None
password_found = False

async def retrieve_file(ftp_server, ftp_username, ftp_password, remote_filename, local_filename):
    try:
        # Connect to the FTP server
        ftp = ftplib.FTP(ftp_server)
        ftp.login(ftp_username, ftp_password)

        # Download the file
        with open(local_filename, 'wb') as local_file:
            ftp.retrbinary('RETR ' + remote_filename, local_file.write)

        print(f"File '{remote_filename}' downloaded successfully to '{local_filename}'")
        
        # Log the content of the downloaded file
        with open(local_filename, 'r') as f:
            file_content = f.read()
            print("File content:")
            print(file_content)

        return True
    except Exception as e:
        print(f"Error downloading file from FTP: {e}")
        return False
    finally:
        # Close the FTP connection
        ftp.quit()

async def delete_file_from_ftp(ftp_server, ftp_username, ftp_password, filename):
    try:
        # Connect to the FTP server
        ftp = ftplib.FTP(ftp_server)
        ftp.login(ftp_username, ftp_password)

        # Delete the file
        ftp.delete(filename)
        print(f"File '{filename}' deleted successfully from FTP server.")
        return True
    except Exception as e:
        print(f"Error deleting file from FTP: {e}")
        return False
    finally:
        # Close the FTP connection
        ftp.quit()

async def load_user_script():
    global user_script_module
    try:
        # Load or reload the user script module
        user_script_module = importlib.import_module(user_script_filename[:-3])  # Remove the ".py" extension
        print("User script loaded")
    except Exception as e:
        print(f"Error loading user script: {e}")

async def crack_password(task):
    global password_found
    try:
        if not password_found:  # Continue cracking only if password is not found
            print(task)
            candidate = ''.join(task)
            if user_script_module.crack_password("sha1", hashed_password, candidate):
                password_found = True  # Set flag if password is found
                return candidate
    except Exception as e:
        print(f"Error cracking password: {e}")
        return None

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
    global hashed_password
    global user_script_module
    global user_script_filename
    print(f"Connecting to {websocket_uri}...")
    while True: 
        try:
            async with websockets.connect(websocket_uri) as websocket:
                print("Connected.")
                # Receive messages until the connection is closed
                async for message in websocket:
                    try:
                        parsed_message = json.loads(message)
                        message_type = parsed_message.get("Type")
                        message_content = parsed_message.get("Content")
                        if message_type == "File_Uploaded":
                            print(f"Received message: {message_content}")
                            user_script_filename = message_content
                            if await retrieve_file(ftp_server, ftp_username, ftp_password, message_content, user_script_filename):
                                sparkcontext.addFile(user_script_filename)
                                await load_user_script()  # Load user script once
                        elif message_type == "Message":
                            print("Received hashed password:", message_content)
                            hashed_password = message_content
                            # Ensure user script is loaded before cracking password
                            if user_script_module is not None:
                                # Generate dynamic task chunks based on available workers and network conditions
                                print("User script module")
                                tasks = generate_password_tasks(5)
                                print("Tasks:")
                                print(tasks)
                                # Execute tasks using Spark
                                results = sparkcontext.parallelize(tasks).map(crack_password).collect()
                                print(results)
                                # Process results
                                if any(results):
                                    print("Password cracked:", results[0])
                                    # Perform further actions with the cracked password
                                    return  # Terminate task execution once password is found
                                else:
                                    print("Password not cracked.")
                            else:
                                print("No user script module")
                    except json.JSONDecodeError as e:
                        print(f"Error decoding JSON: {e}")
                    except Exception as e:
                        print(f"Error processing message: {e}")
        except Exception as e:
                print(f"Error connecting to WebSocket server: {e}")
                print(e)
                print("Retrying...")
                await asyncio.sleep(10)  # Wacht 5 seconden voordat opnieuw wordt geprobeerd
        finally:
            # Cleanup resources when WebSocket connection is closed or error occurs
            if user_script_filename:
                print("Deleting file")
                # Delete the file from the FTP server
                await delete_file_from_ftp(ftp_server, ftp_username, ftp_password, user_script_filename)
                
                # Unload the module to free up memory
                if user_script_module:
                    del sys.modules[user_script_filename[:-3]]
                    importlib.invalidate_caches()
                
                # Delete the file from the local filesystem
                os.remove(user_script_filename)
                # Invoke the cleanup function on each worker
                sparkcontext.parallelize([1]).foreach(lambda x: cloudpickle.loads(serialized_cleanup)(user_script_filename))

# Define tasks for workers
def generate_password_tasks(max_length):
    #Generate all possible password combinations up to the specified maximum length.
    tasks = []
    try:
        print("Generating tasks up to length:", max_length)
        for length in range(1, max_length + 1):
            print("Generating tasks for length:", length)
            for combination in itertools.product(CHARACTER_SPACE, repeat=length):
                tasks.append(combination)
        print("Total tasks generated:", len(tasks))
    except Exception as e:
        print(f"Error generating tasks: {e}")
        print(e)
    return tasks

asyncio.run(main())
# Stop Spark session
sparkcontext.stop()