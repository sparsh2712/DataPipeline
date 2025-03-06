# Website Navigation Map System for Selenium Automation

## Overview

This system reduces API token usage by creating a **website navigation map**. The map structures webpages, elements, and interactions as a graph, where pages and elements are represented as nodes and interactions (such as clicks or form submissions) as edges. By using this graph, you can navigate through websites using **Breadth-First Search (BFS)** to find the shortest path from a start page to an output element and automate the interaction with **Selenium**.

## Steps to Implement

### 1. **Build the Navigation Graph**

Represent each page and its elements as nodes, and the actions (click, input) as edges. 

- Define pages as nodes with unique identifiers (URLs).
- Define elements as nodes (with identifiers like `id`, `class`, or `name`).
- Define interactions as edges between nodes.

### 2. **Implement BFS Traversal**

Create a BFS function that:

- Takes the start and target nodes as input.
- Performs BFS to find the shortest path in the graph.
- Returns the sequence of actions needed to reach the target element.

### 3. **Integrate Selenium Automation**

For each action in the BFS path, use Selenium to perform the necessary interaction:

- Click elements.
- Input text.
- Other actions based on the graph.
