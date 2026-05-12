import { Kafka } from "kafkajs";
import { Transaction } from "../../types/Transaction";

const kafka = new Kafka({
  clientId: "wireflow-notifier",
  brokers: ["localhost:29092"],
});

const consumer = kafka.consumer({ groupId: "notifier-group" });

const runConsumer = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: "transactions", fromBeginning: true });

  consumer.run({
    eachMessage: async ({ partition, message, topic }) => {
      try {
        const value = message.value?.toString();

        if (!value) return;

        const transaction: Transaction = JSON.parse(value);

        console.log(`Processing transaction ${transaction.transactionId}...`);
      } catch (error) {
        console.log(`Error processing transaction: ${error}`);
      }
    },
  });
};

async function main() {
  await runConsumer();
}

main();
