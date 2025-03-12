#include <iostream>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <string>

using namespace std;

class Node {
public:
    static int cnt;
    int val_;
    Node *next_;
    Node() : val_(0), next_(nullptr) {};
    Node(int val) : val_(val), next_(nullptr) {};
    Node(int val, Node *next) : val_(val), next_(next) {};

    virtual void a() {
        std::cout << "a" << std::endl;
    }
};
int Node::cnt = 0;

struct ListNode {
    int val_;
    ListNode *next_;
    ListNode() : val_(0), next_(nullptr) {}
    ListNode(int x) : val_(x), next_(nullptr) {}
};

class BNode : public Node {
    void a() override {
        std::cout << "b" << std::endl;
    }
};

class CNode : public Node {
    void a() override {
        std::cout << "c" << std::endl;
    }
};

class BloomFilter {
public:
    BloomFilter (uint32_t size, int hash_num) {
        size_ = size;
        hash_num_ = hash_num;
        bit_map.resize((size + 7) / 8, 0);
    }
    
    uint32_t hash(string& s, int seed) {
        uint32_t prime = 0x01000193;
        uint32_t hash_ret = 0x811c9dc5 ^ seed;
        
        for (char c : s) {
            hash_ret ^= c;
            hash_ret *= prime;
        }
        return hash_ret % size_;
    }

    void add(string& s) {
        for (int i = 0; i < hash_num_; i++) {
            uint32_t idx = hash(s, i);
            int byte_pos = idx / 8;
            int bit_pos = idx % 8;
            bit_map[byte_pos] |= (1 << bit_pos);
        }
    }
    bool check(string& s) {
        for (int i = 0; i < hash_num_; i++) {
            uint32_t idx = hash(s, i);
            int byte_pos = idx / 8;
            int bit_pos = idx % 8;
            int ret = bit_map[byte_pos] & (1 << bit_pos);
            if (!ret) return false;
        }
        return true;
    }
private:
    vector<char> bit_map;
    uint32_t size_;  // 位总数
    int hash_num_; // 哈希函数总数
};


class TreeNode{
public:
    int val;
    TreeNode *left, *right;
    TreeNode() : val(0), left(nullptr), right(nullptr) {};
    TreeNode(int val) : val(val), left(nullptr), right(nullptr) {};
};

void prevScan(TreeNode* root) {
    if (!root) return;
    std::cout << root->val << " ";
    prevScan(root->left);
    prevScan(root->right);
}



int main() {
    // int a[6] = {3, -1, 2, 5, 6, 9};
    // std::mutex mutex_;
    // std::condition_variable condion_variable_;
    // std::unique_lock<std::mutex> lock(mutex_);

    // int b = 0;
    // condion_variable_.wait(lock, [&b] {
    //     int x = 1;
    //     b += x;
    //     return b == 2;
    // });

    // Node* root = new Node();
    // Node* broot = new BNode();
    // Node* croot = new CNode();
    // root->a();
    // broot->a();
    // croot->a();

    
    // int *n;
    // int *m;
    // n = (int *)malloc(sizeof(int) * 10);
    // m = n;


    // m = (int *)malloc(sizeof(int) * 10);
    // memcpy(m, n, sizeof(int) * 10);
    
    BloomFilter *bf = new BloomFilter(100, 3);
    string s1 = "apple", s2 = "banna", s3 = "apple";
    bf->add(s1);
    bf->add(s2);

    cout << bf->check(s1) << bf->check(s2) << bf->check(s3) << endl;
    
    return 0;
}
