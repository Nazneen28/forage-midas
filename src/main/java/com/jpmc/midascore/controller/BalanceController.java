package com.jpmc.midascore.controller;

import com.jpmc.midascore.foundation.Balance;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class BalanceController {

    @GetMapping("/balance")
    public Balance getBalance(@RequestParam String userId) {

        Balance balance = new Balance();
        balance.setAmount(0);

        return balance;
    }
}