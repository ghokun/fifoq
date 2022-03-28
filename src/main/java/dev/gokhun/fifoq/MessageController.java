package dev.gokhun.fifoq;

import java.util.Properties;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpStatus;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import reactor.core.publisher.Mono;

@RestController
public class MessageController {

	private static final Logger logger = LoggerFactory.getLogger(MessageController.class);

	private RabbitTemplate rabbitTemplate;
	private RabbitAdmin rabbitAdmin;

	@Autowired
	public MessageController(RabbitTemplate rabbitTemplate, RabbitAdmin rabbitAdmin) {
		this.rabbitTemplate = rabbitTemplate;
		this.rabbitAdmin = rabbitAdmin;
	}

	@GetMapping("/fetch")
	public Mono<Message> fetchMessage() {
		Message message = this.rabbitTemplate
			.receiveAndConvert(MessageConfig.QUEUE_NAME, new ParameterizedTypeReference<Message>() {});
		if (message == null) {
			throw new ResponseStatusException(HttpStatus.TOO_MANY_REQUESTS, "Queue is empty");
		}
		return Mono.just(message);
	}

	@Scheduled(fixedDelay = 1000)
	public void sendMessage() {
		Properties properties = this.rabbitAdmin.getQueueProperties(MessageConfig.QUEUE_NAME);
		Integer messageCount = (Integer) properties.get(RabbitAdmin.QUEUE_MESSAGE_COUNT);
		logger.info("Message count: {}", messageCount);
		if (messageCount < MessageConfig.QUEUE_SIZE) {
			Message message = new Message(UUID.randomUUID().toString(), UUID.randomUUID().toString());
			logger.info("Sending message: {}", message.toString());
			this.rabbitTemplate.convertAndSend(MessageConfig.QUEUE_NAME, message);
		}
	}
}
