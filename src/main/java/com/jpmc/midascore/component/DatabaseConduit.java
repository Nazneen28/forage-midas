package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.stereotype.Component;
import org.springframework.kafka.annotation.KafkaListener;
import com.jpmc.midascore.foundation.Transaction;

@Component
public class DatabaseConduit {
    private final UserRepository userRepository;

    public DatabaseConduit(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    public void save(UserRecord userRecord) {
        userRepository.save(userRecord);
    }
    @KafkaListener(topics = "${kafka.topic}", groupId = "midas-group")
public void listen(Transaction transaction) {
     System.out.println("Transaction amount: " + transaction.getAmount());

    UserRecord userRecord = new UserRecord(
        String.valueOf(transaction.getSenderId()),
        transaction.getAmount()
    );

    userRepository.save(userRecord);
}
}
