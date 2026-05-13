from confluent_kafka import Consumer
from redis import Redis
import json
from Transaction import Transaction

consumer = Consumer(
    {
        "bootstrap.servers": "localhost:29092",
        "group.id": "fraud-detector-group",
        "auto.offset.reset": "earliest",
    }
)

redis = Redis(host="localhost", port=6379, db=0)


def checkFraud(transaction: Transaction) -> None:
    if transaction.amount > 10000:
        result = "fraud"
    elif transaction.senderAccountId == transaction.receiverAccountId:
        result = "fraud"
    else:
        result = "ok"

    redis.set(f"transaction:{transaction.transactionId}", result)
    print(
        f"[FRAUD DETECTOR] Transaction {transaction.transactionId} | ${transaction.amount} | {transaction.senderAccountId} -> {transaction.receiverAccountId} | Status: {result.upper()}"
    )


def main():
    consumer.subscribe(["transactions"])

    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                print(f"Consumer error: {message.error()}")
                continue

            data = json.loads(message.value().decode("utf-8"))
            transaction = Transaction.from_dict(data)
            checkFraud(transaction)

    except KeyboardInterrupt:
        consumer.close()


if __name__ == "__main__":
    main()
