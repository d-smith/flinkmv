package org.ds.flinkmv.pojos;

public class MarketValue {
    public String owner;
    public String symbol;
    public Double amount;
    public Double value;

    public MarketValue(){}

    public MarketValue(String owner, String symbol, Double amount, Double value) {
        this.owner = owner;
        this.symbol = symbol;
        this.amount = amount;
        this.value = value;
    }

    @Override
    public String toString() {
        return "MarketValue{" +
                "owner='" + owner + '\'' +
                ", symbol='" + symbol + '\'' +
                ", amount='" + amount + '\'' +
                ", value=" + value +
                '}';
    }
}
