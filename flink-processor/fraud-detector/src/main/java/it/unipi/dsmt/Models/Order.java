package it.unipi.dsmt.Models;
import java.util.UUID;
import it.unipi.dsmt.Models.Customer.*;
import lombok.Data;
import com.fasterxml.jackson.annotation.JsonProperty;


public class Order {
    public long timestamp ;
    @JsonProperty("orderId")
        private String orderId;
        @JsonProperty("customer")
        private Customer customer = new Customer();
        @JsonProperty("productName")
        private String productName;
        @JsonProperty("amount")
        private double amount;

        // Constructors

        public Order() {
                // Default constructor
        }

        public Order(String orderId, Customer customer, String productName,
                     double amount) {
                this.orderId = orderId;
                this.customer = customer;
                this.productName = productName;
                this.amount = amount;
        }

        // Getters
        public Long getTimestamp() {return timestamp;}
        public String getOrderId() {
                return orderId;
        }

        public Customer getCustomer() {
                return customer;
        }

        public String getProductName() {
                return productName;
        }

        public double getAmount() {
                return amount;
        }

        // Setters

        public void setOrderId(String orderId) {
                this.orderId = orderId;
        }

        public void setCustomer(Customer customer) {
                this.customer = customer;
        }

        public void setProductName(String productName) {
                this.productName = productName;
        }

        public void setAmount(double amount) {
                this.amount = amount;
        }

        public void setTimestamp(Long timestamp) {this.timestamp = timestamp;}
}
