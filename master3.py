import itertools
import os
import sys
from pyspark import SparkContext, SparkConf, AccumulatorParam
import ftplib
import websockets
import asyncio
import json
import importlib
import cloudpickle
import time

# Initialize Spark session
sparkconf = SparkConf().setAppName("Password Cracker") \
                        .setMaster("spark://10.0.0.18:7077") \
                        .set("spark.driver.host", "10.0.0.18") \
                        .set("spark.driver.bindAddress", "10.0.0.18") \
                        .set("spark.driver.port","10020") \
                        .set("spark.shuffle.compress", "false") \
                        .set("spark.shuffle.spill.compress", "false") \
                        .set("spark.broadcast.compress", "false") \
                        .set("spark.network.timeout", "800s") \
                        .set("spark.executor.heartbeatInterval", "60s") \
                        .set("spark.dynamicAllocation.enabled", "false") \
                        .set("spark.speculation", "true") \
                        .set("spark.speculation.quantile", "0.75") \
                        .set("spark.speculation.multiplier", "1.5") \
                        .set("spark.shuffle.service.enabled", "false") \
                        .set("spark.blockManager.port", "10021") \
                        .set("spark.executor.port", "10022") \
                        .set("spark.executor.port.maxRetries", "50") \
                        .set("spark.broadcast.port", "10023") \
                        .set("spark.fileserver.port", "10024") \
                        .set("spark.replClassServer.port", "10025") \
                        .set("spark.port.maxRetries", "50")
                        # .set("spark.driver.host", "145.220.74.141") \
                        # .set("spark.rpc.message.maxSize", "512") \

sparkcontext = SparkContext(conf=sparkconf)

websocket_uri = "ws://10.0.0.19:8181" 

ftp_server = "192.168.0.2"
ftp_username = "sparkmaster"
ftp_password = "P@ssword"

CHARACTER_SPACE = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

user_script_module = None
user_script_filename = None
hashed_password = None

# # Initialize shared accumulator for indicating password found status
# password_found = sparkcontext.accumulator(False, PasswordFoundAccumulatorParam())

async def retrieve_file(ftp_server, ftp_username, ftp_password, remote_filename, local_filename):
    try:
        # Connect to the FTP server
        ftp = ftplib.FTP(ftp_server)
        ftp.login(ftp_username, ftp_password)

        # Download the file
        with open(local_filename, 'wb') as local_file:
            ftp.retrbinary('RETR ' + remote_filename, local_file.write)

        print(f"File '{remote_filename}' downloaded successfully to '{local_filename}'")
        
        with open(local_filename, 'r') as f:
            file_content = f.read()
            print("File content:")
            print(file_content)

        return True
    except Exception as e:
        print(f"Error downloading file from FTP: {e}")
        return False
    finally:
        ftp.quit()

async def delete_file_from_ftp(ftp_server, ftp_username, ftp_password, filename):
    try:
        # Connect to the FTP server
        ftp = ftplib.FTP(ftp_server)
        ftp.login(ftp_username, ftp_password)

        ftp.delete(filename)
        print(f"File '{filename}' deleted successfully from FTP server.")
        return True
    except Exception as e:
        print(f"Error deleting file from FTP: {e}")
        return False
    finally:
        ftp.quit()

async def load_user_script():
    global user_script_module
    try:
        # Load or reload the user script module
        user_script_module = importlib.import_module(user_script_filename[:-3])  # Remove the ".py" extension
        print("User script loaded")
    except Exception as e:
        print(f"Error loading user script: {e}")

# def execute_task(chunk):
#     print("Executing task at worker")
#     print("Chunk:")
#     print(chunk)
#     try:
#         for task in chunk:
#             if user_script_module.crack_password("sha1", hashed_password, task):
#                 print(f"password found: {task}")
#                 return task
#     except Exception as e:
#         print(f"Error cracking password: {e}")
#         return []
def execute_task(chunk):
    print("Executing task at worker")
    print("Chunk:")
    print(chunk)
    results = []  # Initialize an empty list to store results
    try:
        for task in chunk:
            if user_script_module.crack_password("sha1", hashed_password, task):
                print(f"password found: {task}")
                results.append(task)
                break  # If a password is found, break the loop
    except Exception as e:
        print(f"Error cracking password: {e}")
    return results  # Always return a list
    
# Process chunks independently
def process_chunks(chunk):
    passwords = []
    for combination in chunk:
        try:
            password = execute_task(combination)
            if password is not None:
                passwords.append(password)
        except Exception as e:
            print(f"Error processing chunk: {e}")
    return passwords

def generate_combinations():
    print("Making combinations")
    max_password_length = 6
    for length in range(1, max_password_length + 1):
        for combination in itertools.product(CHARACTER_SPACE, repeat=length):
            generated_combination = ''.join(combination)
            yield str(generated_combination)

# def generate_combinations():
#     print("Generating combinations...")
#     combinations = itertools.product(CHARACTER_SPACE, repeat=6)  # Example: Adjust the repeat value as needed
#     chunk_size = 1000  # Example: Adjust chunk size as needed
#     while True:
#         chunk = list(itertools.islice(combinations, chunk_size))
#         if not chunk:
#             break
#         yield chunk

# def generate_chunks(chunk_size, combinations_generator):
#     chunk = []
#     for _ in range(chunk_size):
#         try:
#             combination = next(combinations_generator)
#             chunk.append(combination)
#         except StopIteration:
#             break
#     return chunk, combinations_generator

# def generate_chunk(chunk_size):
#     chunk = []
#     combinations_generator = generate_combinations()
#     for _ in range(chunk_size):
#         try:
#             combination = next(combinations_generator)
#             chunk.append(combination)
#         except StopIteration:
#             break
#     yield chunk

# Dynamically allocate chunks to workers
def allocate_chunks(chunk_size):
    combinations_generator = generate_combinations()
    print("Allocate chunks")
    while True:
        chunk = []
        print("Nieuwe chunk")
        for _ in range(chunk_size):
            try:
                combination = next(combinations_generator)
                chunk.append(combination)
            except StopIteration:
                print("Breaking because StopIteration")
                break
        if chunk:
            yield chunk
        else:
            print("No more chunks")
            break  # Break out of the loop when there are no more chunks

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
    tried_passwords_count = 0
    print(f"Connecting to {websocket_uri}...")
    while True: 
        try:
            async with websockets.connect(websocket_uri) as websocket:
                print("Connected.")
                async for message in websocket:
                    try:
                        parsed_message = json.loads(message)
                        token = parsed_message.get("WsToken")
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
                            if user_script_module is not None:
                                start_time = time.time()  # Record the start time
                                chunk_size = 750000
                                # combinations_generator = generate_combinations()
                                # while True:
                                #     combinations_chunk, combinations_generator = generate_chunks(chunk_size, combinations_generator)
                                #     if combinations_chunk:
                                #         print(combinations_chunk)
                                #         password = sparkcontext.parallelize([combinations_chunk]).map(lambda chunk: execute_task(chunk)).filter(lambda x: x is not None).collect()
                                #         print(f"Password: {password}")
                                #         if password:
                                #             print("Password found:", password[0]) 
                                #             end_time = time.time()  # Record the end time
                                #             elapsed_time = end_time - start_time  # Calculate the elapsed time
                                #             print(f"Elapsed time: {elapsed_time} seconds")
                                #             break 
                                #     else:
                                #         print("No more combinations to try.")
                                #         break
                                generated_chunks = allocate_chunks(chunk_size)
                                while True:
                                    next_chunk = next(generated_chunks)
                                    if next_chunk:
                                        rdd = sparkcontext.parallelize(next_chunk)
                                        _ = rdd.unpersist()
                                        passwords = rdd.mapPartitions(execute_task).collect()
                                        print(rdd.cache())
                                        # passwords = sparkcontext.parallelize(next_chunk).mapPartitions(execute_task).collect()
                                        print("passwords: ")
                                        print(passwords)
                                        tried_passwords_count += len(next_chunk) 
                                        elapsed_time = time.time() - start_time
                                        await websocket.send(json.dumps({
                                            "WsToken": token,
                                            "Type": "Status_Update",
                                            "Tried_Passwords": tried_passwords_count,
                                            "Elapsed_Time": elapsed_time
                                        }))
                                        if any(passwords):
                                            print("Password found:", [password for password in passwords if password])
                                            end_time = time.time()  # Record the end time
                                            elapsed_time = end_time - start_time  # Calculate the elapsed time
                                            print(f"Elapsed time: {elapsed_time} seconds")
                                            message = {
                                                "WsToken": token,
                                                "Type": "Password_Found",
                                                "Content": passwords[0]
                                            }
                                            await websocket.send(json.dumps(message))
                                            break
                                    else:
                                        print("No more chunks")
                                        break
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
                await asyncio.sleep(10)
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

asyncio.run(main())
# Stop Spark session
sparkcontext.stop()