package dev.gokhun.fifoq;

public record Message(String id, String value) {

	@Override
	public String toString() {
		return "Message [id=" + id + ", value=" + value + "]";
	}
}
