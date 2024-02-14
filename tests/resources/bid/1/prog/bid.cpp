#include <iostream>
#include <vector>
#include <string>

using namespace std;

constexpr int SIZE = 1e6 + 10;
using Vertex = int;

string bida(vector<Vertex> graph[], int n){
	for (int i = 0; i < n; i++){
		if (graph[i].size() % 2 != 0)
			return "NIE";
	}
	return "TAK";
}

int main(){
	ios_base::sync_with_stdio(false);
	cin.tie(nullptr);
	
	int n, m, s;
	
	cin >> n >> m >> s;
	vector<Vertex> graph[SIZE];
	for (int i = 0; i<m; i++){
		Vertex a, b;
		cin >> a >> b;
		graph[a].push_back(b);
		graph[b].push_back(a);
	}
	
	cout << bida(graph, n);
}
