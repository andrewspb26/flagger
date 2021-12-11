import pickle
import queue
from core.entry import Operation
from typing import Tuple


class MainHeap:

    def __init__(self, queue_name: str, maxsize: int = 10000, is_cold_start: bool = True):
        self.pq = queue.PriorityQueue(maxsize=maxsize)
        self.queue_name = queue_name
        self.deleted = {}
        self.executing_operation = None
        if not is_cold_start:
            self.load_queue_from_disk()

    def load_queue_from_disk(self) -> None:
        try:
            with open(f"{self.queue_name}.p", "rb") as f:
                self.pq.queue = pickle.load(f)
        except (FileNotFoundError, EOFError) as e:
            print(f"Failed while loading {self.queue_name}. Starting with empty queue")

    def dump_queue_to_disk(self) -> None:
        with open(f"{self.queue_name}.p", "w") as f:
            pickle.dumps(self.pq.queue)

    def push(self, operation: Operation) -> bool:
        if not self.pq.full():
            self.pq.put(operation)
            return True
        else:
            return False

    def pop(self) -> Operation:
        for attempt in range(self.pq.qsize()):
            operation = self.pq.get()
            if operation.operation_id not in self.deleted:
                self.executing_operation = operation.operation_id
                return operation
            else:
                del self.deleted[operation.operation_id]

    def mark_operation_as_deleted(self, operation_id: str) -> bool:
        if operation_id in self.deleted:
            return False
        else:
            self.deleted[operation_id] = True
            return True
