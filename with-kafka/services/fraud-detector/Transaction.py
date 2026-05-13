from dataclasses import dataclass


@dataclass
class Transaction:
    transactionId: str
    senderAccountId: str
    receiverAccountId: str
    amount: int
    currency: str
    timestamp: str

    @staticmethod
    def from_dict(data: dict) -> "Transaction":
        return Transaction(**data)
