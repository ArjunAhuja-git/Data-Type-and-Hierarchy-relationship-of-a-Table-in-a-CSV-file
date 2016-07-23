#include <algorithm>
#include <cmath>
#include <cstdio>
#include <iostream>
#include <limits.h>
#include <vector>
using namespace std;

int main() {
  /* Enter your code here. Read input from STDIN. Print output to STDOUT */
  int t;
  cin >> t;
  int n;
  while (t--) {
    cin >> n;
    char c;
    int prev;
    bool br = false;
    for (int i = 0; i < n; i++) {
      int mina = INT_MAX;
      for (int j = 0; j < n; j++) {
        cin >> c;
        mina = min(mina, (int)c);
      }
      if (i == 0) {
        prev = mina;
      } else {
        if (mina < prev) {
          br = true;
          cout << "NO" << endl;
          break;
        } else
          prev = mina;
      }
    }
    if (!br)
      cout << "YES" << endl;
  }
  return 0;
}
