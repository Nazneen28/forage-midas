package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.UserRepository;
import com.jpmc.midascore.foundation.Transaction;

import org.springframework.stereotype.Component;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.client.RestTemplate;

@Component
public class DatabaseConduit {

    private final UserRepository userRepository;
    private final RestTemplate restTemplate = new RestTemplate();

    public DatabaseConduit(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    public void save(UserRecord userRecord) {
        userRepository.save(userRecord);
    }

    @KafkaListener(topics = "${kafka.topic}", groupId = "midas-group")
    public void listen(Transaction transaction) {

        // 👉 Get sender & receiver
       UserRecord sender = userRepository.findById(transaction.getSenderId());
UserRecord receiver = userRepository.findById(transaction.getRecipientId());
        // 👉 Check if users exist
        if (sender == null || receiver == null) {
            return;
        }

        double amount = transaction.getAmount();

        // 👉 Check balance
        if (sender.getBalance() < amount) {
            return;
        }

        // 👉 CALL INCENTIVE API
        double incentive = 0.0;

        try {
            String url = "http://localhost:8080/incentive";
            incentive = restTemplate.postForObject(url, transaction, Double.class);
        } catch (Exception e) {
            incentive = 0.0; // fallback if API fails
        }

        // 👉 UPDATE BALANCES (IMPORTANT LOGIC)
        sender.setBalance((float)(sender.getBalance() - amount));
receiver.setBalance((float)(receiver.getBalance() + amount + incentive));
        // 👉 SAVE USERS
        userRepository.save(sender);
        userRepository.save(receiver);
    }
}