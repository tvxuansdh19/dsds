import json
import threading
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

class DataSource:
    def __init__(self, source_id: str, keys: Optional[List[str]] = None):
        self.source_id = source_id
        self.keys = set(keys) if keys else set()
        self.data = {}

    def add_key(self, key: str, value: Any):
        self.keys.add(key)
        self.data[key] = value

    def remove_key(self, key: str):
        self.keys.discard(key)
        self.data.pop(key, None)

    def get(self, key: str, default=None):
        return self.data.get(key, default)

    def set(self, key: str, value: Any):
        self.data[key] = value
        self.keys.add(key)

    def get_keys(self):
        return list(self.keys)

class DataLoader:
    def __init__(self, sources: Dict[str, DataSource]):
        self.sources = sources

    def load_batch(self, source_id: str, keys: List[str]):
        source = self.sources.get(source_id)
        if not source:
            return {k: None for k in keys}
        return {k: source.get(k, None) for k in keys}

class DataSaver:
    def __init__(self, sources: Dict[str, DataSource]):
        self.sources = sources

    def save_batch(self, source_id: str, batch: Dict[str, Any]):
        source = self.sources.get(source_id)
        if not source:
            return
        for k, v in batch.items():
            source.set(k, v)

class JsonDataReflectorMultiSource:
    def __init__(self, managed_data, sources: Dict[str, DataSource], max_memory_size: str = "2MB", default_source_id: Optional[str] = None):
        self.sources = sources  # {source_id: DataSource}
        self.max_memory_size = self._parse_size(max_memory_size)
        self.lock = threading.Lock()
        self.unloaded_keys = set()
        self.key_sizes = {}  # Track size of large keys
        self.large_key_threshold = self.max_memory_size // 10  # 10% of max size

        # Xác định id object
        self.object_id = managed_data.get('id', None)
        self.managed_data = None
        self.source_id = None
        if self.object_id is not None:
            # Tìm object trong các source
            found = False
            for sid, source in self.sources.items():
                if self.object_id in source.data:
                    self.managed_data = source.data[self.object_id]
                    self.source_id = sid
                    found = True
                    break
            if not found:
                raise ValueError(f"Object with id {self.object_id} not found in any source.")
        else:
            # Tạo id mới và đăng ký object mới vào source
            import uuid
            self.object_id = str(uuid.uuid4())
            managed_data['id'] = self.object_id
            # Chọn source để lưu object mới
            if default_source_id and default_source_id in self.sources:
                self.source_id = default_source_id
            else:
                self.source_id = next(iter(self.sources.keys()))
            self.managed_data = managed_data
            self.sources[self.source_id].data[self.object_id] = self.managed_data

        self.accessor = DataAccessor(self.managed_data)
        self.loader = DataLoader(self.sources)
        self.saver = DataSaver(self.sources)

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

    def _build_combined_data(self) -> dict:
        combined = {}
        for source in self.sources.values():
            combined.update(source.data)
        return combined

    def _current_memory_size(self) -> int:
        total = sum(self.key_sizes.values())
        small_keys = [k for k in self._build_combined_data() if k not in self.key_sizes and k not in self.unloaded_keys]
        if small_keys:
            small_dict = {k: self._build_combined_data()[k] for k in small_keys}
            total += len(json.dumps(small_dict).encode('utf-8'))
        return total

    def _unload_if_needed(self):
        while self._current_memory_size() > self.max_memory_size:
            candidates = [k for k in self.key_sizes if k not in self.unloaded_keys]
            if candidates:
                k = max(candidates, key=lambda x: self.key_sizes[x])
            else:
                k = None
                for key in self._build_combined_data().keys():
                    if key not in self.unloaded_keys:
                        k = key
                        break
            if k is not None:
                # Find which source owns this key
                for source in self.sources.values():
                    if k in source.data:
                        source.data[k] = "unloaded"
                        self.unloaded_keys.add(k)
                        self.key_sizes.pop(k, None)
                        break
            else:
                break

    def get(self, key_path: str, default=None):
        with self.lock:
            value = self.accessor.get(key_path, default)
            if value == "unloaded":
                key = key_path.split('.')[0]
                # Find source containing this key
                for source_id, source in self.sources.items():
                    if key in source.data:
                        loaded = self.loader.load_batch(source_id, [key])
                        source.data[key] = loaded.get(key, default)
                        self.unloaded_keys.discard(key)
                        size = len(json.dumps(source.data[key]).encode('utf-8'))
                        if size > self.large_key_threshold:
                            self.key_sizes[key] = size
                        else:
                            self.key_sizes.pop(key, None)
                        break
                value = self.accessor.get(key_path, default)
            return value

    def set(self, key_path: str, value: Any, source_id: Optional[str] = None):
        with self.lock:
            key = key_path.split('.')[0]
            # Xác định source chứa key, hoặc dùng source_id nếu có
            target_source = None
            if source_id and source_id in self.sources:
                target_source = self.sources[source_id]
            else:
                for s in self.sources.values():
                    if key in s.data:
                        target_source = s
                        break
            if not target_source:
                # Nếu không tìm thấy, mặc định lấy source đầu tiên
                target_source = next(iter(self.sources.values()))
            target_source.set(key, value)
            size = len(json.dumps(value).encode('utf-8'))
            if size > self.large_key_threshold:
                self.key_sizes[key] = size
            else:
                self.key_sizes.pop(key, None)
            self._unload_if_needed()
            self.saver.save_batch(target_source.source_id, {key: value})
            # Update accessor
            self.accessor = DataAccessor(self._build_combined_data())

# Example usage:
# ds1 = DataSource('source1', ['a', 'b'])
# ds2 = DataSource('source2', ['c', 'd'])
# ds1.set('a', 123)
# ds2.set('c', 456)
# sources = {'source1': ds1, 'source2': ds2}
# reflector = JsonDataReflectorMultiSource(sources)
# reflector.set('a', 789, source_id='source1')
# print(reflector.get('a'))
