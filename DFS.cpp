#include <iostream>
#include <fstream>
#include <filesystem>
#include <vector>
#include <map>

using namespace std;
namespace fs = std::filesystem;

class Node {
public:
    int id;
    bool active;
    fs::path directory;

    Node(int id) {
        this->id = id;
        this->active = true;
        this->directory = "node_" + to_string(id);

        if (!fs::exists(directory))
            fs::create_directory(directory);
    }

    void fail() { active = false; }
    void recover() { active = true; }
};

class DistributedFS {
private:
    vector<Node> nodes;

    // metadata: filename → node IDs storing it
    map<string, vector<int>> metadata;

    const int REPLICATION = 3;

public:
    DistributedFS(int totalNodes) {
        for (int i = 1; i <= totalNodes; i++)
            nodes.emplace_back(i);

        cout << "[DFS] Initialized with " << totalNodes << " nodes.\n";
    }

    // Upload file + replicate to 3 nodes
    void upload(string filename) {
        if (!fs::exists(filename)) {
            cout << "Error: File not found.\n";
            return;
        }

        vector<int> usedNodes;
        int replicated = 0;

        try {
            for (auto &node : nodes) {
                if (node.active) {
                    fs::copy(filename, node.directory / filename,
                             fs::copy_options::overwrite_existing);

                    usedNodes.push_back(node.id);
                    replicated++;
                }
                if (replicated == REPLICATION)
                    break;
            }
        } catch (const fs::filesystem_error &e) {
            cout << "Error during file replication: " << e.what() << "\n";
            return;
        }

        if (replicated < REPLICATION) {
            cout << "Error: Not enough active nodes for 3 replicas!\n";
            return;
        }

        metadata[filename] = usedNodes;

        cout << "[UPLOAD SUCCESS] File replicated to nodes: ";
        for (int id : usedNodes) cout << id << " ";
        cout << "\n\n";
    }

    // Download from any active node
    void download(string filename) {
        if (!metadata.count(filename)) {
            cout << "Error: File not found in DFS.\n";
            return;
        }

        try {
            for (int nodeID : metadata[filename]) {
                Node &node = nodes[nodeID - 1];

                if (node.active) {
                    fs::copy(node.directory / filename,
                             "downloaded_" + filename,
                             fs::copy_options::overwrite_existing);

                    cout << "[DOWNLOAD SUCCESS] File downloaded from Node "
                         << nodeID << "\n";
                    return;
                }
            }
        } catch (const fs::filesystem_error &e) {
            cout << "Error during download: " << e.what() << "\n";
            return;
        }

        cout << "[ERROR] All replicas are unavailable. File cannot be downloaded.\n";
    }

    // Delete file from all nodes
    void deleteFile(string filename) {
        if (!metadata.count(filename)) {
            cout << "Error: File not found.\n";
            return;
        }

        try {
            for (int nodeID : metadata[filename]) {
                fs::remove(nodes[nodeID - 1].directory / filename);
            }
        } catch (const fs::filesystem_error &e) {
            cout << "Error during deletion: " << e.what() << "\n";
            return;
        }

        metadata.erase(filename);

        cout << "[DELETE SUCCESS] File removed from DFS.\n\n";
    }

    // List files with replicas
    void listFiles() {
        if (metadata.empty()) {
            cout << "(Empty) No files stored.\n\n";
            return;
        }

        cout << "\nFILES IN DFS:\n";
        for (auto &entry : metadata) {
            cout << " - " << entry.first << " → Nodes: ";
            for (int nodeID : entry.second) cout << nodeID << " ";
            cout << "\n";
        }
        cout << endl;
    }

    // Fail a node + check warnings
    void failNode(int id) {
        if (id < 1 || id > (int)nodes.size()) {
            cout << "Error: Invalid node ID " << id << ".\n";
            return;
        }
        nodes[id - 1].fail();
        cout << "[NODE FAILED] Node " << id << " is inactive.\n";

        checkReplicaHealth();
        cout << "\n";
    }

    // Recover a node
    void recoverNode(int id) {
        if (id < 1 || id > (int)nodes.size()) {
            cout << "Error: Invalid node ID " << id << ".\n";
            return;
        }
        nodes[id - 1].recover();
        cout << "[NODE RECOVERED] Node " << id << " is active.\n";

        checkReplicaHealth();
        cout << "\n";
    }

    // Show all nodes and their status
    void showNodes() {
        cout << "\nNODE STATUS:\n";
        for (auto &node : nodes) {
            cout << "Node " << node.id << ": "
                 << (node.active ? "Active" : "Failed") << "\n";
        }
        cout << endl;
    }

    // NEW FEATURE: Automatic warnings if replicas < 2
    void checkReplicaHealth() {
        for (auto &entry : metadata) {
            string file = entry.first;
            vector<int> &nodeList = entry.second;

            int activeCount = 0;
            for (int id : nodeList) {
                if (nodes[id - 1].active)
                    activeCount++;
            }

            if (activeCount < 2) {
                cout << "WARNING: File '" << file
                     << "' has only " << activeCount
                     << " active replicas! Data loss risk!\n";
            }
        }
    }
};

int main() {
    DistributedFS dfs(4); // 4 nodes recommended for triple replication

    string cmd, arg;

    cout << "\n=== DISTRIBUTED FILE SYSTEM ===\n";
    cout << "Commands: upload, download, delete, list, fail, recover, nodes, exit\n\n";

    while (true) {
        cout << "DFS> ";
        cin >> cmd;

        if (cmd == "upload") {
            cin >> arg;
            dfs.upload(arg);
        }
        else if (cmd == "download") {
            cin >> arg;
            dfs.download(arg);
        }
        else if (cmd == "delete") {
            cin >> arg;
            dfs.deleteFile(arg);
        }
        else if (cmd == "list") {
            dfs.listFiles();
        }
        else if (cmd == "fail") {
            cin >> arg;
            int nodeId = stoi(arg);
            dfs.failNode(nodeId);
        }
        else if (cmd == "recover") {
            cin >> arg;
            int nodeId = stoi(arg);
            dfs.recoverNode(nodeId);
        }
        else if (cmd == "nodes") {
            dfs.showNodes();
        }
        else if (cmd == "exit") {
            break;
        }
        else {
            cout << "Invalid command.\n";
        }
    }

    return 0;
}
