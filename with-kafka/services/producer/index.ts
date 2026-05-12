import { Kafka } from "kafkajs";
import { Transaction } from "../../types/Transaction";

const transactions: Transaction[] = [
  new Transaction("Alice", "Bob", 5000),
  new Transaction("Ahmed", "Muhammad", 4000),
  new Transaction("Charlie", "Charlie", 2000),
  new Transaction("Alice", "Bob", 8000),
  new Transaction("Alice", "Bob", 8000),
  new Transaction("Alice", "Bob", 8000),
];

// Create new kafka client
const kafkaClient: Kafka = new Kafka({
  clientId: "wireflow-producer",
  brokers: ["localhost:29092"],
});

// Create producer
const producer = kafkaClient.producer();

// Connect producer and publish transactions
const publishTransactions = async () => {
  await producer.connect();

  await producer.send({
    topic: "transactions",
    messages: transactions.map((transaction) => ({
      key: transaction.transactionId,
      value: JSON.stringify(transaction),
    })),
  });

  await producer.disconnect();

  console.log("Transactions published successfully");
};

publishTransactions().catch(console.error);
