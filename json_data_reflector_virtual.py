import threading
import uuid
from typing import Any, Dict, List, Optional

class DataAccessor:
    def __init__(self, data: dict):
        self.data = data

    def get(self, key_path: str, default=None):
        keys = key_path.split('.')
        d = self.data
        for k in keys:
            if isinstance(d, dict) and k in d:
                d = d[k]
            else:
                return default
        return d

    def set(self, key_path: str, value: Any):
        keys = key_path.split('.')
        d = self.data
        for k in keys[:-1]:
            if k not in d or not isinstance(d[k], dict):
                d[k] = {}
            d = d[k]
        d[keys[-1]] = value

class PersistenceAccessor:
    def __init__(self, data_loader, data_saver):
        self.data_loader = data_loader
        self.data_saver = data_saver
    def load(self, keys: List[str]):
        return self.data_loader.load_batch(keys)
    def save(self, batch: Dict[str, Any]):
        self.data_saver.save_batch(batch)

class JsonDataReflector:
    def __init__(self, managed_data: dict, managed_data_store, data_sources, max_memory_size="2MB"):
        self.managed_data = managed_data  # virtual data in RAM
        self.managed_data_store = managed_data_store  # nơi lưu object json (angrodb)
        self.data_sources = data_sources  # list/các data source (collection of object)
        self.max_memory_size = self._parse_size(max_memory_size)
        self.data_accessor = DataAccessor(self.managed_data)
        self.lock = threading.Lock()
        # Tạo id nếu chưa có
        if 'id' not in self.managed_data:
            self.managed_data['id'] = str(uuid.uuid4())
            self.managed_data_store.save_object(self.managed_data['id'], self.managed_data)
        else:
            # Nếu có id, kiểm tra tồn tại trong store
            if not self.managed_data_store.exists(self.managed_data['id']):
                raise ValueError(f"Object id {self.managed_data['id']} not found in managed_data_store")
        self.managed_data_id = self.managed_data['id']

    def _parse_size(self, size_str: str) -> int:
        size_str = size_str.upper().strip()
        if size_str.endswith('MB'):
            return int(float(size_str[:-2]) * 1024 * 1024)
        elif size_str.endswith('KB'):
            return int(float(size_str[:-2]) * 1024)
        elif size_str.endswith('B'):
            return int(float(size_str[:-1]))
        else:
            raise ValueError(f"Unknown size format: {size_str}")

    def managed_metadata_loader(self):
        # Duyệt qua các data_sources, tìm object có managed_data_id == self.managed_data_id, lấy metadata
        all_keys = set()
        for ds in self.data_sources:
            for obj in ds.objects():
                if obj.get('managed_data_id') == self.managed_data_id:
                    meta = obj.get('metadata', {})
                    keys = meta.get('key', [])
                    all_keys.update(keys)
        return list(all_keys)

    def get_persistence_accessor(self, data_source):
        return PersistenceAccessor(data_source.data_loader, data_source.data_saver)

    def get(self, key):
        # Lấy từ RAM nếu có, không thì load từ data_source
        with self.lock:
            value = self.data_accessor.get(key, None)
            if value is not None:
                return value
            # Nếu không có, tìm trong các data_source
            for ds in self.data_sources:
                for obj in ds.objects():
                    if obj.get('managed_data_id') == self.managed_data_id:
                        meta = obj.get('metadata', {})
                        data = meta.get('data', {})
                        if key in data:
                            self.data_accessor.set(key, data[key])
                            return data[key]
            return None

    def set(self, key, value):
        # Set vào RAM, đồng thời lưu vào data_source (metadata)
        with self.lock:
            self.data_accessor.set(key, value)
            # Lưu vào data_source đầu tiên (hoặc chọn theo logic khác)
            ds = self.data_sources[0]
            # Tìm hoặc tạo object
            obj = None
            for o in ds.objects():
                if o.get('managed_data_id') == self.managed_data_id:
                    obj = o
                    break
            if obj is None:
                obj = {'managed_data_id': self.managed_data_id, 'metadata': {'key': [], 'data': {}}}
                ds.add_object(obj)
            meta = obj['metadata']
            if key not in meta['key']:
                meta['key'].append(key)
            meta['data'][key] = value
            ds.save_object(obj)

    def unload(self, keys: List[str]):
        # Xóa khỏi RAM, không xóa khỏi data_source
        with self.lock:
            for key in keys:
                keys = key.split('.')
                d = self.managed_data
                for k in keys[:-1]:
                    if k in d and isinstance(d[k], dict):
                        d = d[k]
                    else:
                        d = None
                        break
                if d and keys[-1] in d:
                    del d[keys[-1]]

    def load(self, keys: List[str]):
        # Load các key từ data_source vào RAM
        with self.lock:
            for key in keys:
                if self.data_accessor.get(key, None) is not None:
                    continue
                for ds in self.data_sources:
                    for obj in ds.objects():
                        if obj.get('managed_data_id') == self.managed_data_id:
                            meta = obj.get('metadata', {})
                            data = meta.get('data', {})
                            if key in data:
                                self.data_accessor.set(key, data[key])
                                break
