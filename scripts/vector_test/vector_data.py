import os
import random

# f=open("vector_load_data.sh","w")
# elements=1000
# dim=16
# f.write("create table a(b vector(16));\n")
# for i in range(elements):
#     list0=[random.random()*10 for _ in range(dim)]
#     str_list="["
#     for item in list0:
#         str_list+="%.12f"%item+', '
#     str_list=str_list[:-2]+"]"
#     f.write("insert into a values (vector '{0}');\n".format(str_list))
#     # pass
# f.close()

f=open("float_load_data.sh","w")
elements=1000
dim=16
f1=""
for i in range(16):
    f1+="b{0} float, ".format(i)
f1=f1[:-2]
f.write("create table b({0});\n".format(f1))
for i in range(elements):
    list0=[random.random()*10 for _ in range(dim)]
    str_list=""
    for item in list0:
        str_list+="%.12f"%item+', '
    str_list=str_list[:-2]
    f.write("insert into b values ({0});\n".format(str_list))
    # pass
f.close()