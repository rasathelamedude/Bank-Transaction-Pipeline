import { Kafka } from "kafkajs";
import { KafkaMessage } from "./KafkaMessage";

// Connect producer
const transactions: KafkaMessage[] = [
  new KafkaMessage("Alice", "Bob", 5000),
  new KafkaMessage("Ahmed", "Muhammad", 4000),
  new KafkaMessage("Charlie", "Charlie", 2000),
  new KafkaMessage("Alice", "Bob", 8000),
  new KafkaMessage("Alice", "Bob", 8000),
  new KafkaMessage("Alice", "Bob", 8000),
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
