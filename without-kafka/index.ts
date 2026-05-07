/**
 *
 * Run this file as is by executing the following commands in your terminal:
 *  cd without-kafka
 *  bun run index.ts
 *
 * Observe the output and you can conclude that:
 *    - 7 transactions took ~31 seconds to process.
 *    - Each transaction waits for the previous one to complete before starting.
 *    - If one service crashes or is slow, the whole system will be affected.
 *    - Alice's payment to bob is blocked by charlie's transaction even though they have nothing to do with each other.
 *
 * This happens becuase are tightly coupled in one process.
 * There is no independence, no parallelism, and no scalability.
 *
 * Real banks process millions of transactions per day.
 * At this rate this is practiclly impossible.
 *
 *
 * This is one case where a Message Broker like Kafka shines.
 * The producer drops the even and moves on.
 * Each service consumes independently and in parallel.
 * A slow fraud check doesn't slow down the notifier.
 * Alice's payment to bob is not affected by charlie's transaction.
 *
 */

interface Transaction {
  transactionId: string;
  from: string;
  to: string;
  amount: number;
  currency: string;
  timestamp: Date;
}

function generateTransactionId(): string {
  return Math.random().toString(36).substr(2, 9);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// 1. Define a transaction object
async function sendTransaction(from: string, to: string, amount: number) {
  const transaction: Transaction = {
    transactionId: generateTransactionId(),
    from,
    to,
    amount,
    currency: "USD",
    timestamp: new Date(),
  };

  await checkFraud(transaction);
  await updateUserBalance(transaction);
  await notifyUser(transaction);
  logTransaction(transaction);
}

// 2. Check for fraudulent activity
async function checkFraud(transaction: Transaction): Promise<void> {
  await sleep(3000); // Simulate time taken for fraud check

  if (transaction.amount > 10000) {
    console.warn(
      `Transaction ${transaction.transactionId} flagged for review: amount exceeds threshold.`,
    );
    return;
  }

  if (transaction.from === transaction.to) {
    console.warn(
      `Transaction ${transaction.transactionId} flagged for review: sender and receiver are the same.`,
    );
    return;
  }

  console.log(`Transaction ${transaction.transactionId} passed fraud check.`);
}

// 3. Update the balance of the user
async function updateUserBalance(transaction: Transaction) {
  await sleep(1000); // Simulate time taken to update balance
  console.log(
    `Updating balance for ${transaction.from} and ${transaction.to}...`,
  );
}

// 4. Notify the user about the transaction
async function notifyUser(transaction: Transaction) {
  await sleep(500); // Simulate time taken to notify user
  console.log(`Notifying user ${transaction.from} about the transaction...`);
}

// 5. Log the transaction for auditing purposes
function logTransaction(transaction: Transaction) {
  console.log(
    `Logging transaction ${transaction.transactionId} for auditing purposes...`,
  );
}

// Example usage
async function main() {
  const start = Date.now();

  await sendTransaction("Alice", "Bob", 5000);
  await sendTransaction("Ahmed", "Muhammad", 4000);
  await sendTransaction("Charlie", "Charlie", 2000);
  await sendTransaction("Alice", "Bob", 8000);
  await sendTransaction("Alice", "Bob", 8000);
  await sendTransaction("Alice", "Bob", 8000);

  console.log(`All transactions processed in ${Date.now() - start} ms.`);
}

main();
