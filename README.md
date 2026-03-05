# 🗄️ Hệ Thống Lưu Trữ Phân Tán Key-Value

Một hệ thống lưu trữ phân tán key-value đơn giản nhưng mạnh mẽ, được xây dựng bằng Python với các tính năng:
- **Consistent Hashing** để phân phối dữ liệu
- **Replication** để chịu lỗi
- **Automatic Failover** khi node bị lỗi
- **Self-healing** tự động đồng bộ dữ liệu

---

## 📋 Mục Lục

- [Tính Năng](#-tính-năng)
- [Kiến Trúc](#-kiến-trúc)
- [Yêu Cầu](#-yêu-cầu)
- [Cài Đặt](#-cài-đặt)
- [Hướng Dẫn Sử Dụng](#-hướng-dẫn-sử-dụng)
  - [Khởi Động Cluster](#1-khởi-động-cluster)
  - [Sử Dụng Client](#2-sử-dụng-client)
  - [Chạy Tests](#3-chạy-tests)
- [API Reference](#-api-reference)
- [Cấu Trúc Thư Mục](#-cấu-trúc-thư-mục)
- [Cấu Hình](#-cấu-hình)
- [Troubleshooting](#-troubleshooting)
- [Development](#-development)
- [License](#-license)

---

## ✨ Tính Năng

### Core Features
- ✅ **PUT/GET/DELETE** operations cơ bản
- ✅ **Consistent Hashing** để phân phối dữ liệu đều
- ✅ **Data Replication** với replication factor có thể cấu hình (mặc định: 2)
- ✅ **Automatic Failover** khi node bị lỗi
- ✅ **Node Discovery** tự động tìm và kết nối các peers
- ✅ **Heartbeat Monitoring** phát hiện node lỗi
- ✅ **Data Synchronization** đồng bộ định kỳ giữa các nodes
- ✅ **Self-healing** khôi phục dữ liệu tự động khi node restart

### Client Features
- ✅ **Automatic Retry** với exponential backoff
- ✅ **Connection Pooling** cho hiệu suất tốt
- ✅ **Load Balancing** giữa các nodes
- ✅ **Statistics Tracking** theo dõi performance

### Testing & Monitoring
- ✅ **Interactive Test Suite** với 8 kịch bản test
- ✅ **Real-time Monitoring** trạng thái cluster
- ✅ **Detailed Logging** cho debugging

---

## 🏗️ Kiến Trúc

```
┌─────────────────────────────────────────────────────────┐
│                    CLIENT APPLICATION                    │
│                     (client.py)                          │
└────────────┬────────────────────────────────────────────┘
             │
             │ TCP Socket Communication
             │
    ┌────────┴────────┬────────────┬────────────┐
    │                 │            │            │
┌───▼───┐        ┌───▼───┐   ┌───▼───┐   ┌───▼───┐
│ Node 1│◄──────►│ Node 2│◄─►│ Node 3│◄─►│ Node N│
│:5001  │        │:5002  │   │:5003  │   │:500N  │
└───────┘        └───────┘   └───────┘   └───────┘
    │                 │            │            │
    │    Heartbeat    │            │            │
    │    Replication  │            │            │
    │    Sync Data    │            │            │
    └─────────────────┴────────────┴────────────┘
```

### Consistent Hashing Ring

```
           Key Hash: 12345
                ↓
        ┌───────────────┐
     90 │               │ 10
        │   Node 1      │
        │   (hash: 20)  │
        │               │
        └───────────────┘
       /                 \
    80                     20
     /                       \
┌───────┐                 ┌───────┐
│Node 3 │                 │Node 2 │
│(60)   │                 │(40)   │
└───────┘                 └───────┘
    \                       /
     \                     /
      70 ←──────────────→ 30
```

**Cách hoạt động:**
1. Mỗi key được hash thành một số (0 - 2^128)
2. Tìm N nodes gần nhất theo chiều kim đồng hồ
3. Data được replicate đến N nodes đó

---

## 📦 Yêu Cầu

### Hệ Thống
- **Python**: 3.7 trở lên
- **OS**: Windows, Linux, macOS
- **RAM**: Tối thiểu 512MB mỗi node
- **Network**: TCP/IP connectivity giữa các nodes

### Thư Viện Python
```
# Tất cả đều là built-in libraries
socket
json
threading
time
hashlib
logging
```

Không cần cài đặt thêm packages nào! 🎉

---

## 🚀 Cài Đặt

### Bước 1: Clone Repository

```bash
git clone https://github.com/yourusername/distributed-kv-store.git
cd distributed-kv-store
```

### Bước 2: Kiểm Tra Python Version

```bash
python --version
# Hoặc
python3 --version
```

Đảm bảo >= Python 3.7

### Bước 3: (Optional) Tạo Virtual Environment

```bash
# Tạo virtual environment
python -m venv venv

# Kích hoạt
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate
```

### Bước 4: Verify Installation

```bash
# Kiểm tra file tồn tại
ls -la
# Nên thấy: node.py, client.py, manual_test.py
```

---

## 📖 Hướng Dẫn Sử Dụng

### 1. Khởi Động Cluster

#### Khởi Động Node Đầu Tiên (Seed Node)

```bash
# Terminal 1
python node.py 5001
```

**Output mong đợi:**
```
✓ Node đã khởi tạo: 127.0.0.1:5001 tại 127.0.0.1:5001
✓ Node đã khởi động thành công tại 127.0.0.1:5001
✓ Thread gửi heartbeat đã khởi động
✓ Thread phát hiện lỗi đã khởi động
✓ Thread đồng bộ định kỳ đã khởi động
✓ Tất cả background threads đã khởi động
```

#### Khởi Động Nodes Tiếp Theo

```bash
# Terminal 2
python node.py 5002 127.0.0.1 5001

# Terminal 3
python node.py 5003 127.0.0.1 5001
```

**Giải thích tham số:**
```
python node.py <port> [seed_host] [seed_port]

<port>       : Port để node này lắng nghe
[seed_host]  : IP của seed node (optional)
[seed_port]  : Port của seed node (optional)
```

#### Xác Nhận Cluster Hoạt Động

Trong logs của các nodes, bạn sẽ thấy:
```
✓ Node 127.0.0.1:5002 đã tham gia cluster
✓ Node 127.0.0.1:5003 đã tham gia cluster
♥ Nhận heartbeat từ 127.0.0.1:5002
```

---

### 2. Sử Dụng Client

#### Interactive Client

```bash
# Terminal 4
python client.py
```

**Sử dụng default config hoặc nhập custom:**
```
Cấu hình Cluster:
Nhập địa chỉ node (định dạng: host:port)
Nhấn Enter trên dòng trống để sử dụng mặc định

Node 1: [Enter để dùng localhost:5001,5002,5003]
Node 2: 
Node 3: 

✓ Đã kết nối tới cluster với 3 node(s)
```

#### Các Lệnh Client

```bash
# Lưu dữ liệu
> PUT name John
✓ PUT name = John

# Lấy dữ liệu
> GET name
✓ GET name = John

# Xóa dữ liệu
> DELETE name
✓ DELETE name

# Xem trạng thái cluster
> STATUS

# Xem thống kê
> STATS

# Thoát
> EXIT
```

#### Sử Dụng Client Trong Code

```python
from client import KVStoreClient

# Tạo client
client = KVStoreClient([
    ("localhost", 5001),
    ("localhost", 5002),
    ("localhost", 5003)
])

# PUT
client.put("user:1", "Alice")
client.put("user:2", "Bob")

# GET
value = client.get("user:1")
print(f"User 1: {value}")  # User 1: Alice

# DELETE
client.delete("user:2")

# Xem thống kê
stats = client.lay_thong_ke_client()
print(f"Success rate: {stats['thanh_cong']/stats['so_request']*100:.1f}%")
```

---

### 3. Chạy Tests

#### Test Suite Tương Tác

```bash
python manual_test.py
```

**Menu chính:**
```
╔══════════════════════════════════════════════════════════════════════╗
║               TEST THỦ CÔNG HỆ THỐNG KV PHÂN TÁN                    ║
╚══════════════════════════════════════════════════════════════════════╝

📋 CÁC KỊCH BẢN TEST:

  1. Test Thao Tác Cơ Bản (PUT/GET/DELETE)
  2. Test Nhân Bản Dữ Liệu
  3. Test Consistent Hashing
  4. Test Tính Nhất Quán
  5. Test Failover Khi Node Lỗi
  6. Test Recovery Sau Khi Node Khôi Phục
  7. Test Phân Phối Tải
  8. Test Đồng Bộ Dữ Liệu

🔧 CÔNG CỤ:

  9. Xem Trạng Thái Cluster
 10. Xem Dữ Liệu Trên Từng Node
 11. Xem Thống Kê Client
 12. Xóa Tất Cả Dữ Liệu Test

 0. Thoát
```

#### Ví Dụ: Chạy Test Failover

1. Chọn option **5** từ menu
2. Làm theo hướng dẫn từng bước
3. Khi được yêu cầu, tắt Node 2 bằng Ctrl+C
4. Quan sát hệ thống tự động failover
5. Chạy test **6** để test recovery

#### Test Tự Động (Nâng Cao)

```python
# test_automated.py
from client import KVStoreClient
import time

def test_basic_operations():
    client = KVStoreClient([("localhost", 5001)])
    
    # Test PUT
    assert client.put("test_key", "test_value") == True
    
    # Test GET
    assert client.get("test_key") == "test_value"
    
    # Test DELETE
    assert client.delete("test_key") == True
    assert client.get("test_key") == None
    
    print("✓ Basic operations test passed")

def test_replication():
    client = KVStoreClient([("localhost", 5001)])
    
    # PUT data
    client.put("replicated_key", "replicated_value")
    time.sleep(2)  # Wait for replication
    
    # Check on different nodes
    client2 = KVStoreClient([("localhost", 5002)])
    client3 = KVStoreClient([("localhost", 5003)])
    
    value2 = client2.get("replicated_key")
    value3 = client3.get("replicated_key")
    
    # At least one replica should exist
    assert value2 == "replicated_value" or value3 == "replicated_value"
    print("✓ Replication test passed")

if __name__ == "__main__":
    test_basic_operations()
    test_replication()
    print("\n✓ All tests passed!")
```

---

## 📚 API Reference

### Node API

#### Khởi Tạo Node

```python
from node import Node

node = Node(
    node_id="127.0.0.1:5001",      # Unique ID
    host="127.0.0.1",               # Host to bind
    port=5001,                      # Port to listen
    he_so_nhan_ban=2                # Replication factor
)
```

#### Các Method Chính

```python
# Khởi động node
node.bat_dau()

# Tham gia cluster
node.tham_gia_cluster(seed_host="127.0.0.1", seed_port=5001)

# Dừng node
node.dung_lai()

# Lấy nodes chịu trách nhiệm cho một key
nodes = node.lay_cac_node_chiu_trach_nhiem("my_key")
```

### Client API

#### Khởi Tạo Client

```python
from client import KVStoreClient

client = KVStoreClient(
    cac_node=[
        ("localhost", 5001),
        ("localhost", 5002),
        ("localhost", 5003)
    ],
    timeout=5.0  # Socket timeout in seconds
)
```

#### Operations

```python
# PUT - Lưu key-value
success = client.put(key, value, hien_thi=True)
# Returns: bool

# GET - Lấy value
value = client.get(key, hien_thi=True)
# Returns: str | None

# DELETE - Xóa key
success = client.delete(key, hien_thi=True)
# Returns: bool
```

#### Monitoring

```python
# Lấy thống kê từ một node
stats = client.lay_thong_ke_node(chi_so_node=0)
# Returns: dict với các fields:
# - thoi_gian_hoat_dong: uptime in seconds
# - so_key: số keys được lưu
# - so_peer: số peers
# - so_lan_put/get/delete: số operations
# - so_lan_nhan_ban: số lần replicate

# Lấy thống kê client
client_stats = client.lay_thong_ke_client()
# Returns: dict với các fields:
# - so_request: tổng requests
# - thanh_cong: successful requests
# - that_bai: failed requests
# - so_lan_thu_lai: số retry

# Hiển thị trạng thái cluster
client.hien_thi_trang_thai_cluster()
```

### Protocol Messages

#### Request Format

```json
{
    "command": "PUT|GET|DELETE|JOIN|HEARTBEAT|REPLICATE|GET_ALL_DATA|SYNC_DATA|GET_STATS",
    "key": "key_name",           // For PUT/GET/DELETE
    "value": "value_data",       // For PUT
    "node_id": "host:port",      // For JOIN/HEARTBEAT
    "host": "127.0.0.1",         // For JOIN
    "port": 5001,                // For JOIN
    "data": {...}                // For SYNC_DATA
}
```

#### Response Format

```json
{
    "status": "success|error",
    "value": "...",              // For GET
    "message": "...",            // For error
    "data": {...},               // For GET_ALL_DATA
    "stats": {...},              // For GET_STATS
    "peers": {...}               // For JOIN
}
```

---

## 📁 Cấu Trúc Thư Mục

```
distributed-kv-store/
│
├── README.md                    # Tài liệu này
├── bug_analysis_and_fixes.md   # Phân tích lỗi và fix
│
├── node.py                      # Node server implementation
│   ├── class Node
│   │   ├── __init__()          # Khởi tạo
│   │   ├── bat_dau()           # Start server
│   │   ├── _xu_ly_put()        # Handle PUT
│   │   ├── _xu_ly_get()        # Handle GET
│   │   ├── _xu_ly_delete()     # Handle DELETE
│   │   ├── _thread_gui_heartbeat()      # Heartbeat sender
│   │   ├── _thread_phat_hien_loi()      # Failure detector
│   │   ├── _thread_dong_bo_dinh_ky()    # Sync thread
│   │   └── tham_gia_cluster()           # Join cluster
│
├── client.py                    # Client implementation
│   ├── class KVStoreClient
│   │   ├── put()               # PUT operation
│   │   ├── get()               # GET operation
│   │   ├── delete()            # DELETE operation
│   │   ├── _gui_request()      # Send request with retry
│   │   └── hien_thi_trang_thai_cluster()  # Show status
│   └── interactive_client()    # CLI client
│
├── manual_test.py               # Interactive test suite
│   ├── class ManualTestSuite
│   │   ├── test_1_basic_operations()    # Test PUT/GET/DELETE
│   │   ├── test_2_replication()         # Test replication
│   │   ├── test_3_consistent_hashing()  # Test hashing
│   │   ├── test_4_consistency()         # Test consistency
│   │   ├── test_5_failover()            # Test failover
│   │   ├── test_6_recovery()            # Test recovery
│   │   ├── test_7_load_distribution()   # Test load balancing
│   │   └── test_8_sync()                # Test sync
├── cli_node_client.py           # CLI client chọn node cụ thể
│   └── Cho phép gửi request trực tiếp đến từng node trong clustertest_1_basic_operations()   
│   │   ├──Test PUT/GET/DELETE
│   │   ├── STATUS   cluster      
│    
│     
└── node.log                     # Log file (auto-generated)
```

---

## ⚙️ Cấu Hình

### Node Configuration

```python
# Trong node.py, class Node.__init__()

# Replication Factor
he_so_nhan_ban = 2              # Mỗi key có 2 bản sao

# Heartbeat Settings
thoi_gian_timeout_heartbeat = 10  # 10 giây không heartbeat = lỗi
khoang_thoi_gian_heartbeat = 3    # Gửi heartbeat mỗi 3 giây

# Sync Settings
# Trong _thread_dong_bo_dinh_ky():
time.sleep(30)                    # Đồng bộ mỗi 30 giây
```

### Client Configuration

```python
# Trong client.py, class KVStoreClient.__init__()

timeout = 5.0                     # Socket timeout 5 giây
```

### Logging Configuration

```python
# Trong node.py

logging.basicConfig(
    level=logging.INFO,           # Đổi thành DEBUG để xem chi tiết
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('node.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
```

### Network Configuration

```python
# Port range cho cluster
DEFAULT_PORTS = [5001, 5002, 5003, 5004, 5005]

# Host binding
DEFAULT_HOST = "127.0.0.1"       # Localhost only
# Hoặc
DEFAULT_HOST = "0.0.0.0"         # Listen trên tất cả interfaces
```

---

## 🔧 Troubleshooting

### Vấn Đề 1: "Address already in use"

**Triệu chứng:**
```
OSError: [Errno 48] Address already in use
```

**Nguyên nhân:** Port đã được process khác sử dụng

**Giải pháp:**
```bash
# Tìm process đang dùng port
# Linux/Mac:
lsof -i :5001
kill -9 <PID>

# Windows:
netstat -ano | findstr :5001
taskkill /PID <PID> /F

# Hoặc dùng port khác:
python node.py 5004
```

---

### Vấn Đề 2: Nodes không kết nối được với nhau

**Triệu chứng:**
```
⚠ Timeout kết nối tới 127.0.0.1:5001
✗ Lỗi tham gia cluster
```

**Check list:**
1. ✅ Seed node đã khởi động chưa?
2. ✅ Firewall có block port không?
3. ✅ Host/port có đúng không?

**Giải pháp:**
```bash
# 1. Verify seed node đang chạy
netstat -an | grep 5001

# 2. Test kết nối
telnet localhost 5001
# hoặc
nc -zv localhost 5001

# 3. Check firewall (Linux)
sudo ufw status
sudo ufw allow 5001/tcp
```

---

### Vấn Đề 3: Dữ liệu không được replicate

**Triệu chứng:** PUT trên node 1 nhưng GET từ node 2 trả về None

**Debug steps:**
```bash
# 1. Check cluster status
python client.py
> STATUS

# 2. Check logs
tail -f node.log | grep "nhân bản"

# 3. Verify replication factor
# Trong node.py, đảm bảo:
he_so_nhan_ban = 2

# 4. Wait cho sync cycle (30s)
# Hoặc trigger manual sync
```

**Giải pháp:**
- Đợi 30 giây cho sync cycle
- Kiểm tra network latency
- Xem logs để debug

---

### Vấn Đề 4: Node crash sau khi restart

**Triệu chứng:**
```
Exception in thread _thread_dong_bo_dinh_ky
```

**Nguyên nhân:** Node chưa kịp load peers trước khi sync

**Giải pháp:** Đã được fix với `time.sleep(10)` ở đầu sync thread

---

### Vấn Đề 5: "JSON không hợp lệ"

**Triệu chứng:**
```
✗ JSON không hợp lệ: ...
```

**Nguyên nhân:** 
- Network packet bị cắt
- Encoding issues

**Giải pháp:**
```python
# Đảm bảo data có newline terminator
sock.sendall(json.dumps(data).encode() + b"\n")

# Read cho đến khi thấy newline
while b"\n" not in buffer:
    buffer += sock.recv(4096)
```

---

### Vấn Đề 6: High memory usage

**Triệu chứng:** Node sử dụng nhiều RAM

**Nguyên nhân:** 
- Quá nhiều keys
- Socket leaks
- Thread leaks

**Monitor:**
```python
# Thêm vào stats
import psutil
process = psutil.Process()
memory_mb = process.memory_info().rss / 1024 / 1024
print(f"Memory: {memory_mb:.1f} MB")
```

**Giải pháp:**
- Implement eviction policy
- Fix socket leaks (xem bug_analysis_and_fixes.md)
- Limit max keys per node

---

## 👨‍💻 Development

### Running in Development Mode

```bash
# Enable debug logging
# Sửa trong node.py:
logging.basicConfig(level=logging.DEBUG)

# Run với verbose output
python node.py 5001 2>&1 | tee node1.log
```

### Code Style

Dự án tuân theo:
- **PEP 8** Python style guide
- **Type hints** cho function signatures
- **Docstrings** cho tất cả public methods
- **Vietnamese comments** cho giải thích logic

### Adding New Features

**Ví dụ: Thêm TTL (Time To Live) cho keys**

```python
# Trong class Node:

def __init__(self, ...):
    # ...
    self.key_ttl = {}  # {key: expiry_time}
    
def _xu_ly_put(self, key, value, ttl=None):
    with self.khoa_du_lieu:
        self.du_lieu[key] = value
        if ttl:
            self.key_ttl[key] = time.time() + ttl
    # ...

def _thread_cleanup_expired():
    while self.dang_chay:
        now = time.time()
        expired = [k for k, exp in self.key_ttl.items() if exp < now]
        for key in expired:
            self.du_lieu.pop(key, None)
            self.key_ttl.pop(key, None)
        time.sleep(60)
```

### Testing Checklist

Trước khi commit:
- [ ] Chạy tất cả 8 tests trong manual_test.py
- [ ] Test với 1, 2, 3, 5 nodes
- [ ] Test failover và recovery
- [ ] Check không có socket leaks
- [ ] Verify logs không có errors
- [ ] Test trên Windows và Linux

---

## 🐛 Known Issues

Xem file `bug_analysis_and_fixes.md` để biết chi tiết các lỗi đã phát hiện và cách fix.

**Summary:**
- ⚠️ Socket có thể leak trong một số edge cases
- ⚠️ Race condition khi cluster topology thay đổi
- ⚠️ Sync chỉ từ 1 peer (đã có fix proposed)

---

## 🗺️ Roadmap

### Version 1.1 (Planned)
- [ ] Implement majority voting trong sync
- [ ] Fix tất cả socket leaks
- [ ] Add TTL support
- [ ] Add authentication
- [ ] Metrics export (Prometheus format)

### Version 2.0 (Future)
- [ ] Write-Ahead Log (WAL) để persistence
- [ ] Raft consensus algorithm
- [ ] Multi-datacenter support
- [ ] Read/Write quorums
- [ ] Range queries
- [ ] Compression

---

## 📄 License

MIT License

Copyright (c) 2024

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

---

## 🙏 Acknowledgments

Dự án này được xây dựng dựa trên các concepts từ:
- **Amazon Dynamo** paper
- **Consistent Hashing** by Karger et al.
- **Distributed Systems** course materials

---

## 📞 Support

**Gặp vấn đề?**
1. Xem [Troubleshooting](#-troubleshooting)
2. Check [Known Issues](#-known-issues)
3. Review `bug_analysis_and_fixes.md`

**Contributions welcome!** 🎉

---

## 🎓 Learning Resources

Muốn hiểu sâu hơn? Đọc thêm:
- [Dynamo Paper](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf)
- [Consistent Hashing](https://en.wikipedia.org/wiki/Consistent_hashing)
- [CAP Theorem](https://en.wikipedia.org/wiki/CAP_theorem)
- [Distributed Systems Course](https://pdos.csail.mit.edu/6.824/)

---
