import os

path="./"
os_list=sorted(os.listdir(path))
# print(os_list)
prefix="storage/index/IVF_Flat/"
for item in os_list:
  print(prefix+item)