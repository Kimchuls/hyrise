#include <stdio.h>
#include <iomanip>
#include <iostream>
#include <string>
#include "readfile.cpp"
using namespace std;

int main() {
  // {  //base
  //   string basename = "gist_query";
  //   string filename = "../gist/" + basename + ".fvecs";
  //   size_t nb, d;
  //   float* xb = fvecs_read(filename.c_str(), &d, &nb);
  //   string writefile = "../gist/" + basename + "_load_data.sh";
  //   FILE* writes = fopen(writefile.c_str(), "w");
  //   /*----------------------------------------------------------------------------*/
  //   for (int i = 0; i < d * nb; i++) {
  //     fprintf(writes, "%.6f", xb[i]);
  //     if (i % d == d - 1) {
  //       fprintf(writes, "\n");
  //     } else {
  //       fprintf(writes, ",");
  //     }
  //   }
  //   fclose(writes);
  // }
  {
    string basename = "gist_base";
    string filename = "../gist/" + basename + ".fvecs";
    size_t nb, d;
    float* xb = fvecs_read(filename.c_str(), &d, &nb);
    //   cout << d << " " << nb << endl;
    string writefile = "../gist/" + basename + "_load_data.sh";
    FILE* writes = fopen(writefile.c_str(), "w");
    fprintf(writes, "create table %s(data vector(%ld));\n", basename.c_str(), d);
    /*----------------------------------------------------------------------------*/
    for (int i = 0; i < d * nb; i++) {
      // if(i/d==5)break;
      if (i % d == 0) {
        fprintf(writes, "insert into %s values (vector \'[", basename.c_str());
      }
      fprintf(writes, "%.6f", xb[i]);
      if (i % d == d - 1) {
        fprintf(writes, "]\');\n");
      } else {
        fprintf(writes, ", ");
      }
    }
    fclose(writes);
  }
  // {  //GT

  //   string basename = "sift_groundtruth";
  //   string filename = "./" + basename + ".ivecs";
  //   size_t nb, d;
  //   int* xb = ivecs_read(filename.c_str(), &d, &nb);
  //   string writefile = "./" + basename + "_load_data.sh";
  //   FILE* writes = fopen(writefile.c_str(), "w");
  //   /*----------------------------------------------------------------------------*/
  //   for (int i = 0; i < d * nb; i++) {
  //     fprintf(writes, "%d", xb[i]);
  //     if (i % d == d - 1) {
  //       fprintf(writes, "\n");
  //     } else {
  //       fprintf(writes, ",");
  //     }
  //   }
  //   fclose(writes);
  // }
  return 0;
}