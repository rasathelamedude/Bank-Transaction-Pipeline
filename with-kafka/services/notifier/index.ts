import { Kafka } from "kafkajs";
import type { Transaction } from "../../types/Transaction";

const kafka = new Kafka({
  clientId: "wireflow-notifier",
  brokers: ["localhost:29092"],
});

const consumer = kafka.consumer({ groupId: "notifier-group" });

const runConsumer = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: "transactions", fromBeginning: true });

  consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const value = message.value?.toString();

        if (!value) return;

        const transaction: Transaction = JSON.parse(value);

        console.log(
          `Transaction ${transaction.transactionId} | ${transaction.senderAccountId} -> ${transaction.receiverAccountId} | $${transaction.amount} USD | Status: Delivered`,
        );
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
