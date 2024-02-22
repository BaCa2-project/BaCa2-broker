#include <iostream>
using namespace std;
int main() {
    int n;
    cin >> n;
    int range[n];
    int maxVal = 0;
    for(int i = 0; i < n; ++i) {
        cin >> range[i];
        if(range[i] > maxVal) {
            maxVal = range[i];
        }
    }
    int values[maxVal];
    values[0] = 0;
    values[1] = 1;
    for(int i = 2; i < maxVal; i++) {
        values[i] = values[i - 2] + values[i - 1];
    }
    for(int i = 0; i < n; i++) {
        for(int j = 0; j < range[i]; j++) {
            cout << values[j];
        }
        cout << endl;
    }
}
