package com.roy.hdfs.group;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Order implements WritableComparable<Order> {

    private String orderId;

    private String productId;

    private Double price;

    public Order() {
    }


    public Order(String orderId, String productId, Double price) {
        this.orderId = orderId;
        this.productId = productId;
        this.price = price;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    @Override
    public int compareTo(Order o) {
        int result = this.orderId.compareTo(o.getOrderId());
        if(result == 0) {
            result = o.getPrice().compareTo(this.price);
        }
        return result;
    }

    @Override
    public void write(DataOutput input) throws IOException {
        input.writeUTF(this.orderId);
        input.writeUTF(this.productId);
        input.writeDouble(this.price);
    }

    @Override
    public void readFields(DataInput output) throws IOException {
        this.orderId = output.readUTF();
        this.productId = output.readUTF();
        this.price = output.readDouble();
    }
}
