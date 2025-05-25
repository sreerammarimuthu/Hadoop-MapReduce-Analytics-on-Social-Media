# Social Media Graph Analytics with Hadoop MapReduce (Java)

This project explores social media behavior analytics using Hadoop MapReduce in Java. Working with large synthetic datasets, it performs 8 core ETL tasks across a network of users, pages, and interactions. Each implemented from scratch to demonstrate distributed computing principles, optimization strategies, and system-level thinking in big data environments. Optimized and original versions of all 8 tasks are included.

## Contents

- `data/` – Subsets of the input datasets and their documentation.
- `java/` – Standard MapReduce implementations using Java (Tasks A–H).
- `java-ov/` – Optimized versions of `java/` with improved logic and performance.
- `utils/` – Supporting files for the analysis (e.g., timestamp generator, dataset creator).
- `output/` – Output results for each task.

## Tasks Summary

| Task | Focus |
|------|-------|
| A | Filter users by nationality |
| B | Find top 10 most visited pages |
| C | Count users by country code |
| D | Match owners with relationship counts |
| E | Calculate total and distinct page accesses per user |
| F | Identify users who haven’t visited their friend’s page |
| G | Detect inactive users based on access timestamps |
| H | Determine whether an owner is 'More Popular' based on relationships |



