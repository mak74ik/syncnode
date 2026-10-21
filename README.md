# SyncNode

> **Lightweight asynchronous file synchronization node with verified SHA-256 integrity and sandboxed transfer.**  


[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://github.com/mak74ik/syncnode/blob/main/LICENSE)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)

---

## 🌟 Overview

`SyncNode` provides direct, socket-based asynchronous file synchronization between nodes without intermediate cloud relays. All transfers are strictly sandboxed to prevent path traversal attacks, and files are indexed with SHA-256 integrity checks.

---

## 🚀 Usage

### 1. Start Server Node
```bash
python3 main.py serve --port 8080 --storage ./my_files
```

### 2. Upload a File to Remote Node
```bash
python3 main.py upload document.pdf --host 127.0.0.1 --port 8080
```

### 3. List Files on Remote Node
```bash
python3 main.py list --host 127.0.0.1 --port 8080
```

### 4. Download a File
```bash
python3 main.py download document.pdf --out ./downloaded.pdf --host 127.0.0.1 --port 8080
```

---

## 🧪 Testing

Run automated end-to-end integration tests:
```bash
python3 -m unittest discover -s tests
```

---

## 👥 Credits

Created & maintained by [Yoshi](https://github.com/mak74ik)
- 🛡️ **Security & Code Review**: Verified & Sandboxed against Path Traversal
- 📦 **Architecture**: Asynchronous Socket File Protocol
