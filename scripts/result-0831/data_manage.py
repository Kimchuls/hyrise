import os


def ivfflat_build():
  f="ivfflat_build.txt"
  ff=open(f,"r").readlines()
  result=""
  for line in ff:
    item=line.split(", ")
    avg=0.0
    for it in item:
      time = it.split("/")
      time_float = float(time[0])+float(time[1])
      avg+=time_float
    avg/=len(item)
    result+=str(avg)+", "
  print(result)
    
  
ivfflat_build()