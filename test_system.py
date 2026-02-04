"""
Test Tự Động Cho Hệ Thống KV Phân Tán
Kiểm tra tính nhất quán, replication, và fault tolerance
"""

import time
import sys
from client import KVStoreClient

# Cấu hình nodes
NODES = [
    ("127.0.0.1", 5001),
    ("127.0.0.1", 5002),
    ("127.0.0.1", 5003)
]


class TestRunner:
    def __init__(self):
        self.client = KVStoreClient(NODES, timeout=3.0)
        self.test_passed = 0
        self.test_failed = 0
        self.test_total = 0
    def assert_equal(self, actual, expected, test_name):
        """Kiểm tra giá trị có khớp không"""
        self.test_total += 1
        if actual == expected:
            self.test_passed += 1
            print(f"  ✓ {test_name}")
            return True
        else:
            self.test_failed += 1
            print(f"  ✗ {test_name}")
            print(f"    Mong đợi: {expected}, Nhận được: {actual}")
            return False

    def assert_true(self, condition, test_name):
        """Kiểm tra điều kiện đúng"""
        self.test_total += 1
        if condition:
            self.test_passed += 1
            print(f"  ✓ {test_name}")
            return True
        else:
            self.test_failed += 1
            print(f"  ✗ {test_name}")
            return False

    def retry_operation(self, operation, max_retries=3, delay=1):
        """Thử lại một operation nếu thất bại"""
        for attempt in range(max_retries):
            try:
                result = operation()
                return result
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"    ⚠ Retry ({attempt+1}/{max_retries})...: {e}")
                    time.sleep(delay)
                else:
                    print(f"    ✗ Thất bại sau {max_retries} lần thử: {e}")
                    return None
    
    def print_header(self, title):
        """In tiêu đề test"""
        print("\n" + "=" * 70)
        print(f" {title}")
        print("=" * 70)
    
    def print_summary(self):
        """In tổng kết kết quả test"""
        print("\n" + "=" * 70)
        print(" TỔng KẾT KẾT QUẢ TEST")
        print("=" * 70)
        print(f"Tổng số test: {self.test_total}")
        print(f"✓ Thành công: {self.test_passed}")
        print(f"✗ Thất bại: {self.test_failed}")
        if self.test_total > 0:
            success_rate = (self.test_passed / self.test_total) * 100
            print(f"Tỷ lệ thành công: {success_rate:.1f}%")
        print("=" * 70)
    def wait_for_sync(self, seconds=2):
        """Đợi để dữ liệu được đồng bộ"""
        print(f"  ⏳ Đợi {seconds}s để đồng bộ dữ liệu...")
        time.sleep(seconds)
    
    # ==================== CÁC BÀI TEST ====================
    
    def test_basic_operations(self):
        """Test các thao tác cơ bản: PUT, GET, DELETE"""
        self.print_header("TEST 1: CÁC THAO TÁC CƠ BẢN")
        
        # Test PUT
        result = self.client.put("test_key", "test_value", hien_thi=False)
        self.assert_true(result, "PUT key mới")
        
        # Test GET
        value = self.client.get("test_key", hien_thi=False)
        self.assert_equal(value, "test_value", "GET key vừa PUT")
        
        # Test UPDATE
        result = self.client.put("test_key", "updated_value", hien_thi=False)
        self.assert_true(result, "UPDATE key hiện có")
        
        value = self.client.get("test_key", hien_thi=False)
        self.assert_equal(value, "updated_value", "GET key sau khi UPDATE")
        
        # Test DELETE
        result = self.client.delete("test_key", hien_thi=False)
        self.assert_true(result, "DELETE key")
        
        value = self.client.get("test_key", hien_thi=False)
        self.assert_equal(value, None, "GET key đã xóa trả về None")
    
    def test_replication(self):
        """Test tính năng nhân bản dữ liệu"""
        self.print_header("TEST 2: NHÂN BẢN DỮ LIỆU")
        
        # PUT dữ liệu
        print("  → PUT data vào node...")
        self.client.put("replicated_key", "replicated_value", hien_thi=False)
        
        # Đợi nhân bản
        self.wait_for_sync(3)
        
        # Kiểm tra từng node
        print("  → Kiểm tra dữ liệu trên từng node...")
        for i in range(len(NODES)):
            client_node = KVStoreClient([NODES[i]], timeout=2.0)
            stats = client_node.lay_thong_ke_node(0)
            
            if stats:
                so_key = stats.get('so_key', 0)
                print(f"    Node {i+1}: {so_key} keys")
                self.assert_true(so_key > 0, f"Node {i+1} có dữ liệu")
            else:
                print(f"    Node {i+1}: OFFLINE")
    
    def test_consistent_hashing(self):
        """Test consistent hashing - dữ liệu được phân phối đúng"""
        self.print_header("TEST 3: CONSISTENT HASHING")
        
        # PUT nhiều keys
        test_data = {
            "user:1": "Alice",
            "user:2": "Bob",
            "user:3": "Charlie",
            "product:1": "Laptop",
            "product:2": "Phone",
            "order:1": "Order#001"
        }
        
        print("  → PUT nhiều keys vào cluster...")
        for key, value in test_data.items():
            self.client.put(key, value, hien_thi=False)
        
        self.wait_for_sync(3)
        
        # Kiểm tra tất cả keys đều có thể GET được
        print("  → Kiểm tra tất cả keys có thể GET...")
        for key, expected_value in test_data.items():
            value = self.client.get(key, hien_thi=False)
            # Kiểm tra nodes sẵn sàng trước khi assert
            if not self.check_nodes_ready():
                print("\n❌ Không phải tất cả nodes đang online!")
                print("   Vui lòng khởi động lại tất cả 3 nodes")
                return
            self.assert_equal(value, expected_value, f"GET {key}")
    
    def test_data_consistency(self):
        """Test tính nhất quán dữ liệu"""
        self.print_header("TEST 4: TÍNH NHẤT QUÁN DỮ LIỆU")
        
        # PUT dữ liệu
        print("  → PUT key='consistency_test'")
        self.client.put("consistency_test", "version_1", hien_thi=False)
        self.wait_for_sync(2)
        
        # GET từ các nodes khác nhau
        print("  → GET từ các nodes khác nhau...")
        values = []
        for i in range(len(NODES)):
            client_node = KVStoreClient([NODES[i]], timeout=2.0)
            value = client_node.get("consistency_test", hien_thi=False)
            values.append(value)
            print(f"    Node {i+1}: {value}")
        
        # Kiểm tra tất cả nodes trả về cùng giá trị (hoặc None nếu không chịu trách nhiệm)
        non_none_values = [v for v in values if v is not None]
        if len(non_none_values) > 0:
            all_same = all(v == non_none_values[0] for v in non_none_values)
            self.assert_true(all_same, "Tất cả nodes có cùng giá trị")
        
        # UPDATE và kiểm tra lại
        print("  → UPDATE key='consistency_test'")
        self.client.put("consistency_test", "version_2", hien_thi=False)
        self.wait_for_sync(2)
        
        print("  → GET lại từ các nodes...")
        updated_values = []
        for i in range(len(NODES)):
            client_node = KVStoreClient([NODES[i]], timeout=2.0)
            value = client_node.get("consistency_test", hien_thi=False)
            updated_values.append(value)
            print(f"    Node {i+1}: {value}")
        
        non_none_updated = [v for v in updated_values if v is not None]
        if len(non_none_updated) > 0:
            all_updated = all(v == "version_2" for v in non_none_updated)
            self.assert_true(all_updated, "Tất cả nodes đã cập nhật giá trị mới")
    
    def test_failover(self):
        """Test failover - yêu cầu tắt thủ công một node"""
        self.print_header("TEST 5: FAILOVER (YÊU CẦU THAO TÁC THỦ CÔNG)")
        
        print("\n  📝 HƯỚNG DẪN:")
        print("  1. Test này sẽ kiểm tra khả năng chuyển đổi dự phòng")
        print("  2. Trước khi bắt đầu, hãy đảm bảo tất cả 3 nodes đang chạy")
        print("  3. Khi được yêu cầu, hãy TẮT Node 2 (Ctrl+C trên terminal của Node 2)")
        print("  4. Sau khi test xong, hãy KHỞI ĐỘNG LẠI Node 2")
        
        input("\n  ⏸  Nhấn Enter khi đã sẵn sàng để bắt đầu test...")
        
        # PUT dữ liệu trước khi tắt node
        print("\n  → PUT dữ liệu vào cluster...")
        self.client.put("failover_test", "data_before_failure", hien_thi=False)
        self.wait_for_sync(2)
        
        # Kiểm tra tất cả nodes
        print("  → Kiểm tra trạng thái tất cả nodes...")
        self.client.hien_thi_trang_thai_cluster()
        
        print("\n  🔴 NGAY BÂY GIỜ: Hãy TẮT Node 2 (port 5002)")
        input("  ⏸  Nhấn Enter sau khi đã TẮT Node 2...")
        
        # Đợi để hệ thống phát hiện node bị lỗi
        print("  ⏳ Đợi 12s để hệ thống phát hiện node lỗi...")
        time.sleep(12)
        
        # Thử PUT dữ liệu mới
        print("  → PUT dữ liệu mới sau khi node lỗi...")
        result = self.client.put("failover_test_2", "data_after_failure", hien_thi=False)
        self.assert_true(result, "PUT thành công khi có node lỗi")
        
        # Thử GET dữ liệu cũ
        print("  → GET dữ liệu cũ...")
        value = self.client.get("failover_test", hien_thi=False)
        self.assert_equal(value, "data_before_failure", "GET dữ liệu cũ thành công")
        
        # Kiểm tra cluster status
        print("\n  → Kiểm tra trạng thái cluster sau khi có node lỗi...")
        self.client.hien_thi_trang_thai_cluster()
        
        print("\n  🟢 NGAY BÂY GIỜ: Hãy KHỞI ĐỘNG LẠI Node 2")
        print("     Chạy: python node.py 5002 127.0.0.1 5001")
        input("  ⏸  Nhấn Enter sau khi đã KHỞI ĐỘNG Node 2...")
        
        # Đợi node khôi phục
        print("  ⏳ Đợi 15s để node khôi phục và đồng bộ...")
        time.sleep(15)
        
        # Kiểm tra dữ liệu đã được đồng bộ
        print("  → Kiểm tra dữ liệu trên Node 2 sau khi khôi phục...")
        client_node2 = KVStoreClient([NODES[1]], timeout=2.0)
        stats = client_node2.lay_thong_ke_node(0)
        
        if stats:
            so_key = stats.get('so_key', 0)
            print(f"    Node 2 có {so_key} keys")
            self.assert_true(so_key > 0, "Node 2 đã đồng bộ dữ liệu")
        else:
            self.assert_true(False, "Node 2 vẫn offline")
        
        # Kiểm tra cluster status cuối cùng
        print("\n  → Kiểm tra trạng thái cluster cuối cùng...")
        self.client.hien_thi_trang_thai_cluster()

    def check_nodes_ready(self):
        """Kiểm tra tất cả nodes đã sẵn sàng"""
        print("  → Kiểm tra tất cả nodes đang online...")
        online_count = 0
        for i in range(len(NODES)):
            try:
                client_node = KVStoreClient([NODES[i]], timeout=2.0)
                stats = client_node.lay_thong_ke_node(0)
                if stats:
                    online_count += 1
                    print(f"    ✓ Node {i+1} online")
                else:
                    print(f"    ✗ Node {i+1} offline")
            except Exception:
                print(f"    ✗ Node {i+1} offline")

        return online_count == len(NODES)
    
    def test_load_distribution(self):
        """Test phân phối tải"""
        self.print_header("TEST 6: PHÂN PHỐI TẢI")
        
        # PUT nhiều keys
        num_keys = 30
        print(f"  → PUT {num_keys} keys vào cluster...")
        for i in range(num_keys):
            key = f"load_test_{i}"
            value = f"value_{i}"
            self.client.put(key, value, hien_thi=False)
        
        self.wait_for_sync(3)
        
        # Kiểm tra phân phối
        print("  → Kiểm tra phân phối dữ liệu trên các nodes...")
        key_counts = []
        for i in range(len(NODES)):
            client_node = KVStoreClient([NODES[i]], timeout=2.0)
            stats = client_node.lay_thong_ke_node(0)
            if stats:
                count = stats.get('so_key', 0)
                key_counts.append(count)
                print(f"    Node {i+1}: {count} keys")
        
        # Kiểm tra không có node nào có quá nhiều hoặc quá ít dữ liệu
        if len(key_counts) > 0:
            avg = sum(key_counts) / len(key_counts)
            print(f"  → Trung bình: {avg:.1f} keys/node")
            
            # Chấp nhận độ lệch 50% (do replication)
            for i, count in enumerate(key_counts):
                deviation = abs(count - avg) / avg if avg > 0 else 0
                print(f"    Node {i+1} độ lệch: {deviation*100:.1f}%")
    
    def run_all_tests(self):
        """Chạy tất cả các test"""
        print("\n" + "=" * 70)
        print(" BẮT ĐẦU TEST TỰ ĐỘNG HỆ THỐNG KV PHÂN TÁN")
        print("=" * 70)
        print("\n⚠️  LƯU Ý:")
        print("  - Đảm bảo tất cả 3 nodes đang chạy trước khi bắt đầu")
        print("  - Test 5 (Failover) yêu cầu thao tác thủ công")
        print()
        
        input("Nhấn Enter để bắt đầu...")
        
        try:
            # Chạy từng test
            self.test_basic_operations()
            self.test_replication()
            self.test_consistent_hashing()
            self.test_data_consistency()
            
            # Test failover (yêu cầu thủ công)
            print("\n" + "=" * 70)
            chay_failover = input("Bạn có muốn chạy Test Failover (yêu cầu thao tác thủ công)? (y/n): ")
            if chay_failover.lower() == 'y':
                self.test_failover()
            else:
                print("  ⏭  Bỏ qua Test Failover")
            
            self.test_load_distribution()
            
            # In tổng kết
            self.print_summary()
            
        except KeyboardInterrupt:
            print("\n\n⚠️  Test bị gián đoạn bởi người dùng")
            self.print_summary()
        except Exception as e:
            print(f"\n\n✗ Lỗi trong quá trình test: {e}")
            import traceback
            traceback.print_exc()
            self.print_summary()


if __name__ == "__main__":
    print("""
╔══════════════════════════════════════════════════════════════════════╗
║                   TEST TỰ ĐỘNG HỆ THỐNG KV PHÂN TÁN                 ║
╚══════════════════════════════════════════════════════════════════════╝

HƯỚNG DẪN CHUẨN BỊ:
-------------------
1. Khởi động 3 nodes trong các terminal riêng biệt:
   
   Terminal 1:  python node.py 5001
   Terminal 2:  python node.py 5002 127.0.0.1 5001
   Terminal 3:  python node.py 5003 127.0.0.1 5001

2. Đợi tất cả nodes kết nối với nhau (khoảng 5 giây)

3. Chạy test này: python test_auto.py

CÁC TEST SẼ CHẠY:
-----------------
✓ Test 1: Các thao tác cơ bản (PUT, GET, DELETE)
✓ Test 2: Nhân bản dữ liệu
✓ Test 3: Consistent hashing
✓ Test 4: Tính nhất quán dữ liệu
⚠ Test 5: Failover (yêu cầu tắt/bật node thủ công)
✓ Test 6: Phân phối tải

    """)
    
    runner = TestRunner()
    runner.run_all_tests()