# 🚀 Social Network Analysis on Twitter Data Using Apache Spark GraphFrames

## 🧩 Problem Statement

**Analyzing Social Networks using GraphX/GraphFrame.**  
In this part, you will use Spark GraphX/GraphFrame to analyze social network data. You are free to  
choose any one of the Social network datasets available from the SNAP repository.  
You will use this dataset to construct a GraphX/GraphFrame graph and run some queries and algorithms  
on the graph. You will need to perform the following steps:

---

## 📥 2.1 Loading Data

Load the data into a GraphFrame or RDD using Spark. Define a parser so that you can identify and  
extract relevant fields. Note that edges are directed, so if your dataset has undirected relationships,  
you might need to convert those into 2 directed relationships. That is, if your dataset contains an  
undirected friendship relationship between X and Y, then you might need to create 2 edges: one from  
X to Y and the other from Y to X.

---

## 🧱 2.2 Create Graphs

Define edge and vertex structure and create property graphs.

---

## 🔍 2.3 Running Queries

Run the following queries using the GraphX/GraphFrame API and write your output to a file on the  
cluster.

### a. 🔺 Outdegree

Find the top 5 nodes with the highest outdegree and find the count of the number of outgoing  
edges in each.

### b. 🔻 Indegree

Find the top 5 nodes with the highest indegree and find the count of the number of incoming edges  
in each.

### c. 📊 PageRank

Calculate PageRank for each of the nodes and output the top 5 nodes with the highest PageRank  
values. You are free to define any suitable parameters.

### d. 🧬 Connected Components

Run the connected components algorithm on it and find the top 5 components with the largest  
number of nodes.

### e. 🔺 Triangle Counts

Run the triangle counts algorithm on each of the vertices and output the top 5 vertices with the  
largest triangle count. In case of ties, you can randomly select the top 5 vertices.


<img width="1061" height="651" alt="image" src="https://github.com/user-attachments/assets/3cc832f9-ab4b-4645-b993-0cf7714973c2" />

<img width="1045" height="635" alt="image" src="https://github.com/user-attachments/assets/9ebce86c-78d4-4a07-8be7-c21f9a7790b8" />

<img width="1082" height="645" alt="image" src="https://github.com/user-attachments/assets/c8448cc7-017e-4aca-996e-5b95caf89d18" />

<img width="1012" height="608" alt="image" src="https://github.com/user-attachments/assets/2f84a44b-42d4-4f1a-9d9c-e11b057dd43b" />

<img width="1078" height="657" alt="image" src="https://github.com/user-attachments/assets/199a3dc4-64f1-412d-a958-fc49dfaf8d68" />
