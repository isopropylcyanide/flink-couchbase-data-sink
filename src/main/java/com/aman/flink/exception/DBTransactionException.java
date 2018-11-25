package com.aman.flink.exception;

public class DBTransactionException extends RuntimeException {

	public DBTransactionException(String message) {
		super(message);
	}

}
