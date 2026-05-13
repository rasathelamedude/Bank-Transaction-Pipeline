import { Kafka } from "kafkajs";
import type { Transaction } from "../../types/Transaction";
import { appendAudit } from "../../utils/redisClient";

const kafka: Kafka = new Kafka({
  brokers: ["localhost:29092"],
  clientId: "wireflow-audit-logger",
});

const consumer = kafka.consumer({ groupId: "audit-logger-group" });

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
          `[AUDIT] Auditing transaction ${transaction.transactionId}...`,
        );
        await appendAudit(transaction);
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
