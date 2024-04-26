# save this as myscript.py
import datetime

current_time = datetime.datetime.now()
with open("/home/ubuntu/output.txt", "a") as file:  # Append mode
    file.write(f"{current_time}\n")
