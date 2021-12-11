from dataclasses import dataclass


@dataclass(order=True)
class Operation:
    priority: int
    timeout: int
    operation_id: str
    data: str

