#include <algorithm>
#include <assert.h>
#include <complex>
#include <ctime>
#include <iostream>
#include <list>
#include <map>
#include <math.h>
#include <memory.h>
#include <queue>
#include <set>
#include <stack>
#include <fstream>
#include <stack>
#include <stdio.h>
#include <string>
#include <vector>
//#include <cstdLL>
#include <bitset>
#include <iomanip> // for std::setprecision()

using namespace std;

// typedef LL_fast64_t Li;
// typedef LL_fast32_t ll;
// typedef LL_fast8_t ii;
typedef long long LL;
typedef long l;

#define arjun main()
#define FOR(i, x, y) for (LL i = (x); i <= (y); ++i)
#define sd(x) scanf("%lld", &x)
#define p(e) printf("\n");
// typedef makepair mpr;

#define fast                                                                   \
  ios_base::sync_with_stdio(false);                                            \
  cin.tie(NULL);
#define mpr(x, y) make_pair(x, y)

// const ll arrsz=2*10e9+1;

namespace patch {
template <typename T> std::string to_string(const T &n) {
  std::ostringstream stm;
  stm << n;
  return stm.str();
}
}

// usage use a>=b>=0;
// Ifa≥b,then a 1000000007; b <a/2.
/**
 *If d divides both a and b, and d = ax + by for some integers x and y, then
 *necessarily
 *d = gcd(a, b).
**/
LL findgcd(LL a, LL b) {
  if (b == 0)
    return a;
  return findgcd(b, a % b);
}

/**
 * find the min of two numbers!
 */

LL minm(LL a, LL b) {
  return !(b < a) ? a : b; // or: return !comp(b,a)?a:b; for version (2)
}

/**
 * If p is prime, then for every 1 ≤ a < p,
 * pow(a,p−1) ≡ 1 (1000000007; p). ---> (pow(a,p−1)-1)%p ==0
 */

bool primetest(LL a) {
  for (size_t i = 2; i <= sqrt(a); i++) {
    if (a % i == 0)
      return true;
  }
  return false;
}

/**
 * program for sorting an array in descending order
 * end here is the number of elements not the index
 */

LL *sort_descending(LL *array, LL start, LL end) {
  sort(array + start, array + end + 1, std::greater<LL>());
  return array;
}

/**
 *function similar to inbuilt max,finds maximum of two numbers
 */

LL maxm(LL a, LL b) {
  return !(b > a) ? a : b; // or: return !comp(b,a)?a:b; for version (2)
}

/**
 *function that calculates nCk using the property (n-1)C(k-1) + (n-1)Ck ,using
 *dp
 */
LL nCk_dp_array[60][60] = {{0}};
LL nCk_dp_for_non_fixed_n_k(LL n, LL k) {
  nCk_dp_array[0][0] = 1;
  for (size_t i = 0; i <= n; i++) {
    for (size_t j = 0; j <= i; j++) {
      if (j == 0 || j == i) {
        nCk_dp_array[i][j] = 1;
      } else {
        nCk_dp_array[i][j] =
            nCk_dp_array[i - 1][j - 1] + nCk_dp_array[i - 1][j];
      }
    }
  }
  return nCk_dp_array[n][k];
}

LL bfs(LL n,std::vector< pair<LL,LL> > *adj,LL* value){
      // Mark all the vertices as not visited
    bool *visited = new bool[n+1];
    for(int i = 0; i <= n; i++)
        visited[i] = false;
 
    // Create a queue for BFS
    list<LL> queue;
 
    // Mark the current node as visited and enqueue it
    visited[1] = true;
    queue.push_back(1);
 
    // 'i' will be used to get all adjacent vertices of a vertex
    LL s;
    std::vector< pair<LL,LL> >::iterator i;
    LL count = 1;
    LL *dist = new LL[n+1];
    for (LL i = 0; i < n+1; ++i)
    {
      dist[i] = 0;
    }
    while(!queue.empty())
    {
        // Dequeue a vertex from queue and print it
        s = queue.front();
        queue.pop_front();
 
        // Get all adjacent vertices of the dequeued vertex s
        // If a adjacent has not been visited, then mark it visited
        // and enqueue it
        for(i = adj[s].begin(); i != adj[s].end(); ++i)
        {
            if(!visited[(*i).first])
            {
                dist[(*i).first] = maxm((*i).second,dist[s]+(*i).second);
                visited[(*i).first] = true;
                if(dist[(*i).first]>value[(*i).first])
                {
                  //
                }
                else{
                  queue.push_back((*i).first);
                  count++;
                }
            }
        }
    }
    return n-count;
}
#define MAXN 1025

vector<int> tree[MAXN];
vector<int> centroidTree[MAXN];
bool centroidMarked[MAXN];

/* method to add edge between to nodes of the undirected tree */
void addEdge(int u, int v)
{
    tree[u].push_back(v);
    tree[v].push_back(u);
}

/* method to setup subtree sizes and nodes in current tree */
void DFS(int src, bool visited[], int subtree_size[], int* n)
{
    /* mark node visited */
    visited[src] = true;

    /* increase count of nodes visited */
    *n += 1;

    /* initialize subtree size for current node*/
    subtree_size[src] = 1;

    vector<int>::iterator it;

    /* recur on non-visited and non-centroid neighbours */
    for (it = tree[src].begin(); it!=tree[src].end(); it++)
        if (!visited[*it] && !centroidMarked[*it])
        {
            DFS(*it, visited, subtree_size, n);
            subtree_size[src]+=subtree_size[*it];
        }
}

int getCentroid(int src, bool visited[], int subtree_size[], int n)
{
    /* assume the current node to be centroid */
    bool is_centroid = true;

    /* mark it as visited */
    visited[src] = true;

    /* track heaviest child of node, to use in case node is 
       not centroid */
    int heaviest_child = 0;

    vector<int>::iterator it;

    /* iterate over all adjacent nodes which are children 
       (not visited) and not marked as centroid to some 
       subtree */
    for (it = tree[src].begin(); it!=tree[src].end(); it++)
        if (!visited[*it] && !centroidMarked[*it])
        {
            /* If any adjacent node has more than n/2 nodes,
             * current node cannot be centroid */
            if (subtree_size[*it]>n/2)
                is_centroid=false;

            /* update heaviest child */
            if (heaviest_child==0 ||
                subtree_size[*it]>subtree_size[heaviest_child])
                heaviest_child = *it;
        }

    /* if current node is a centroid */
    if (is_centroid && n-subtree_size[src]<=n/2)
        return src;

    /* else recur on heaviest child */
    return getCentroid(heaviest_child, visited, subtree_size, n);
}

/* function to get the centroid of tree rooted at src.
 * tree may be the original one or may belong to the forest */
int getCentroid(int src)
{
    bool visited[MAXN];

    int subtree_size[MAXN];

    /* initialize auxiliary arrays */
    memset(visited, false, sizeof visited);
    memset(subtree_size, 0, sizeof subtree_size);

    /* variable to hold number of nodes in the current tree */
    int n = 0;

    /* DFS to set up subtree sizes and nodes in current tree */
    DFS(src, visited, subtree_size, &n);

    for (int i=1; i<MAXN; i++)
        visited[i] = false;

    int centroid = getCentroid(src, visited, subtree_size, n);

    centroidMarked[centroid]=true;

    return centroid;
}

/* function to generate centroid tree of tree rooted at src */
/* function to generate centroid tree of tree rooted at src */
int decomposeTree(int root)
{
    //printf("decomposeTree(%d)\n", root);
 
    /* get sentorid for current tree */
    int cend_tree = getCentroid(root);
 
    printf("%d %d\n", cend_tree,root);
 
    vector<int>::iterator it;
 
    /* for every node adjacent to the found centroid
     * and not already marked as centroid */
    for (it=tree[cend_tree].begin(); it!=tree[cend_tree].end(); it++)
    {
        if (!centroidMarked[*it])
        {
            /* decompose subtree rooted at adjacent node */
            int cend_subtree = decomposeTree(*it);
 
            /* add edge between tree centroid and centroid of subtree */
            centroidTree[cend_tree].push_back(cend_subtree);
            centroidTree[cend_subtree].push_back(cend_tree);
        }
    }
 
    /* return centroid of tree */
    return cend_tree;
}
// driver function

std::string toBinary(LL n)
{
    if(n==0) return "0";
    std::string r;
    while(n!=0) {r=(n%2==0 ?"0":"1")+r; n/=2;}
    return r;
}

int main()
{
    /* number of nodes in the tree */
    int n = 16;
    LL k;
    int t;
    cin>>t;
    string a;
    LL ans = 0;
    ofstream myfile;
    myfile.open ("new.txt");
    int count = 1;
    while(t--)
    {
      ans = 0;
      cin>>n>>k;
      LL* arr[4];
      string* s[4];
      for (int i = 0; i < 4; ++i)
      {
        arr[i] = new LL[n];
      }
      for (int i = 0; i < 4; ++i)
      {
        for (int j = 0; j < n; ++j)
        {
          cin>>arr[i][j];
        }
      }
      LL *soar =  new LL[n*n];
      LL cou = 0;
      for (int i = 0; i < n; ++i)
      {
        for (int j = 0; j < n; ++j)
        {
          soar[cou] = arr[2][i]^arr[3][j];
          cou++;
        }
      }
      sort(soar,soar+cou);
      vector<LL> v(soar,soar+cou);
      vector<LL>::iterator it1 , it2;
      for (int i = 0; i < n; ++i)
      {
        for (int j = 0; j < n; ++j)
        {
          LL x = k^arr[0][i]^arr[1][j];
          it1 = upper_bound(v.begin(), v.end(), x); 
              /* points to eight element in v */ 

          it2 = lower_bound(v.begin(), v.end(), x);
          ans+=(it1-it2);
        }
      }
      myfile<<"Case #"<<count<<": "<<ans<<endl;
      count++;
    }
    myfile.close();
    return 0;
}