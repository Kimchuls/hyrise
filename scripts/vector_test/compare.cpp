#include <stdio.h>
#include <algorithm>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
using namespace std;

int main() {
  string ansFile = "/home/jin467/github_download/hyrise/cmake-build-debug/output.sh";
  printf("loading output\n");
  // FILE* f1=fopen(ansFile.c_str(),"r");
  auto script1 = ifstream{ansFile};
  auto command = string{};
  vector<vector<int>> ansList;
  while (getline(script1, command)) {
    std::istringstream iss(command);
    string token;
    vector<int> strList;
    while (std::getline(iss, token, ' ')) {
      strList.push_back(stoi(token));
    }
    sort(strList.begin(), strList.end());
    // for(int i=0;i<strList.size();i++)
    // printf("%d ",strList[i]);
    // exit(0);
    ansList.push_back(strList);
  }
  string GTFile = "./sift/sift_groundtruth_load_data.sh";
  printf("loading GT\n");
  auto script2 = ifstream{GTFile};
  //   auto command = string{};
  vector<vector<int>> GTList;
  while (getline(script2, command)) {
    std::istringstream iss(command);
    string token;
    vector<int> strList;
    while (std::getline(iss, token, ',')) {
      strList.push_back(stoi(token));
    }
    sort(strList.begin(), strList.end());
    GTList.push_back(strList);
  }
  using t = vector<vector<int>>::size_type;
  using v = vector<int>::size_type;
  std::stringstream result;
  float avg = 0.0;
  printf("geting result\n");
  std::cout<<GTList.size()<<std::endl;
  for (t i = 0; i < ansList.size(); i++) {
    vector<int> l1 = ansList[i], l2 = GTList[i];
    v f1 = 0, f2 = 0, all = l1.size(), recall = 0;
    // cout << "size: " << l1.size() << " " << l2.size() << endl;
    while (f1 < l1.size() && f2 < l2.size()) {
      if (l1[f1] == l2[f2]) {
        recall++;
        f1++;
        f2++;
      } else if (l1[f1] < l2[f2])
        f1++;
      else
        f2++;
      //   cout << "iter: " << f1 << " " << f2 << endl;
    }
    // printf("%ld\n",all);
    result << recall * 1.0 / all << "\n";
    avg += recall * 1.0 / all;
    if (i % 50 == 0) {
      // printf("loading %ld\n", i);
    }
  }
  FILE* op = fopen("result.txt", "w");
  fprintf(op, "%s\n", result.str().c_str());
  fprintf(op, "%f", avg / ansList.size());
  return 0;
}