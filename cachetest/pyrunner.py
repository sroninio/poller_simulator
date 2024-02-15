import subprocess
times_list = []
for data_set_size in range(1, 80000, 100):
    process = subprocess.run(f'taskset 0x8 ./tt.out {data_set_size}', shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    time_in_nano = float(process.stdout.decode().split()[-1])
    print (f"data set im cache lines {data_set_size} avg access time is nano {time_in_nano}")
    times_list.append(time_in_nano)
import ipdb;ipdb.set_trace()
