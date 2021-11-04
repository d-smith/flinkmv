package org.ds.flinkmv.pojos;

public class Position {
    public String owner;
    public String symbol;
    public Double amount;

    public Position(){}

    public Position(String owner, String symbol, Double amount) {
        this.owner = owner;
        this.symbol = symbol;
        this.amount = amount;
    }

    @Override
    public String toString() {
        return "Position{" +
                "owner='" + owner + '\'' +
                ", symbol='" + symbol + '\'' +
                ", amount=" + amount +
                '}';
    }
}
