# 🎧 Podcast Analytics & Recommendation Platform

This project implements a scalable data platform for analyzing podcast content, tracking user behavior, and providing intelligent recommendations to listeners and content producers.

-- add more later --

## 🧠 Project Overview

The system ingests both **real-time events** (from users) and **batch transcripts + metadata** (about podcasts), processes the data using Apache Spark, stores it in a **Delta Lake**, and then exposes structured insights and personalized recommendations via **MongoDB** to be consumed by frontend applications.

-- add more later --

##  Architecture

Below the suggested architecture for the project

## 🗺️ Architecture Diagram

![Architecture Diagram](./docs/project_architecture.png)


## 🏗️ Project structure

-- to do --

## How to Set Up Podscript

1. Install Go (https://golang.org/doc/install)
2. Run:
   ```bash
   go install github.com/deepakjois/podscript@latest

Add podscript to your path:
export PATH="$PATH:$(go env GOPATH)/bin"
