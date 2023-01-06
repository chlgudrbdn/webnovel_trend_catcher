import os
import time
from multiprocessing import Process, freeze_support
import os
import time
from multiprocessing import Process, freeze_support
from multiprocessing import Manager
def task(idx, count):
    print(f"PID : {os.getpid()}")
    logic = sum([i ** 2 for i in range(count)])
    return idx, logic

# if __name__ == "__main__":
# freeze_support()

job = [("첫 번째", 10 ** 7), ("두 번째", 10 ** 7), ("세 번째", 10 ** 7), ("네 번째", 10 ** 7)]

start = time.time()

process = []
for idx, count in job:
    p = Process(target=task, args=(idx, count))
    p.start()
    process.append(p)

for p in process:
    p.join()

print(f"End Time : {time.time() - start}s")

start = time.time()

for idx, count in job:
    task(idx, count)

print(f"End Time : {time.time() - start}s")
""""""
