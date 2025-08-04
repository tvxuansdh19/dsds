import sys
import json
import threading
from typing import Any, Dict, Optional

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

class DataLoader:
    def __init__(self, data: dict):
        self.data = data

    def load_batch(self, keys: list):
        # Giả lập load batch, thực tế có thể load từ file hoặc db
        return {k: self.data.get(k, None) for k in keys}

class DataSaver:
    def __init__(self, data: dict):
        self.data = data

    def save_batch(self, batch: Dict[str, Any]):
        # Giả lập lưu batch, thực tế có thể lưu ra file hoặc db
        for k, v in batch.items():
            self.data[k] = v

class JsonDataReflector:
    def __init__(self, managed_data: dict, max_memory_size: str = "2MB"):
        self.managed_data = managed_data
        self.max_memory_size = self._parse_size(max_memory_size)
        self.accessor = DataAccessor(self.managed_data)
        self.loader = DataLoader(self.managed_data)
        self.saver = DataSaver(self.managed_data)
        self.lock = threading.Lock()
        self.unloaded_keys = set()

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

    def _current_memory_size(self) -> int:
        return len(json.dumps(self.managed_data).encode('utf-8'))

    def _unload_if_needed(self):
        while self._current_memory_size() > self.max_memory_size:
            # Unload the first key (FIFO), mark as 'unloaded'
            for k in list(self.managed_data.keys()):
                if k not in self.unloaded_keys:
                    self.managed_data[k] = "unloaded"
                    self.unloaded_keys.add(k)
                    break
            else:
                break

    def get(self, key_path: str, default=None):
        with self.lock:
            value = self.accessor.get(key_path, default)
            if value == "unloaded":
                # Giả lập load lại dữ liệu
                key = key_path.split('.')[0]
                loaded = self.loader.load_batch([key])
                self.managed_data[key] = loaded.get(key, default)
                self.unloaded_keys.discard(key)
                value = self.accessor.get(key_path, default)
            return value

    def set(self, key_path: str, value: Any):
        with self.lock:
            self.accessor.set(key_path, value)
            self._unload_if_needed()
            # Giả lập lưu batch
            key = key_path.split('.')[0]
            self.saver.save_batch({key: self.managed_data[key]})

# Example usage:
# managed_data = {}
# reflector = JsonDataReflector(managed_data, max_memory_size="2MB")
# reflector.set("a.b.c", 123)
# print(reflector.get("a.b.c"))
