import { getBalance, setBalance, updateBalance } from "../../utils/redisClient";
import { Kafka } from "kafkajs";
import { Transaction } from "../../types/Transaction";

// Create Kafka client
const kafka: Kafka = new Kafka({
  clientId: "wireflow-balance-updater",
  brokers: ["localhost:29092"],
});

// Create consumer instance
const consumer = kafka.consumer({ groupId: "balance-updater-group" });

async function seedBalance(accountId: string, balance: number) {
  const existingBalance = await getBalance(accountId);

  if (!existingBalance) {
    await setBalance(accountId, balance);
    console.log(`Seeded balance for accountId: ${accountId}`);
  }
}

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

        await updateBalance(transaction.senderAccountId, -transaction.amount);
        await updateBalance(transaction.recieverAccountId, transaction.amount);

        console.log(
          `Balance updated: ${transaction.senderAccountId} -${transaction.amount}, ${transaction.recieverAccountId} +${transaction.amount}`,
        );
      } catch (error) {
        console.log(`Error processing transaction: ${error}`);
      }
    },
  });
};

async function main() {
  await seedBalance("Alice", 10000);
  await seedBalance("Bob", 5000);
  await seedBalance("Ahmed", 8000);
  await seedBalance("Muhammad", 3000);
  await seedBalance("Charlie", 2000);

  await runConsumer();
}

main();
