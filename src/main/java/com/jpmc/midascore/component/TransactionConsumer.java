package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class TransactionConsumer {
    private static final Logger logger = LoggerFactory.getLogger(TransactionConsumer.class);
    private final DatabaseConduit databaseConduit;
    private final IncentiveClient incentiveClient;

    public TransactionConsumer(DatabaseConduit databaseConduit, IncentiveClient incentiveClient) {
        this.databaseConduit = databaseConduit;
        this.incentiveClient = incentiveClient;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "${spring.kafka.consumer.group-id}")
    @Transactional
    public void consume(Transaction transaction) {
        logger.info("Received transaction: {}", transaction);

        // Validate sender exists
        UserRecord sender = databaseConduit.findUserById(transaction.getSenderId());
        if (sender == null) {
            logger.warn("Invalid senderId: {}", transaction.getSenderId());
            return;
        }

        // Validate recipient exists
        UserRecord recipient = databaseConduit.findUserById(transaction.getRecipientId());
        if (recipient == null) {
            logger.warn("Invalid recipientId: {}", transaction.getRecipientId());
            return;
        }

        // Validate sender has sufficient balance
        if (sender.getBalance() < transaction.getAmount()) {
            logger.warn("Insufficient balance for sender {}: balance={}, amount={}", 
                sender.getName(), sender.getBalance(), transaction.getAmount());
            return;
        }

        // Get incentive from API
        Incentive incentive = incentiveClient.getIncentive(transaction);
        float incentiveAmount = incentive != null ? incentive.getAmount() : 0;

        // Process the transaction
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);

        // Save updated balances
        databaseConduit.save(sender);
        databaseConduit.save(recipient);

        // Record the transaction
        TransactionRecord transactionRecord = new TransactionRecord(sender, recipient, transaction.getAmount(), incentiveAmount);
        databaseConduit.saveTransaction(transactionRecord);

        logger.info("Transaction processed successfully: {} -> {}, amount: {}, incentive: {}", 
            sender.getName(), recipient.getName(), transaction.getAmount(), incentiveAmount);
    }
}
