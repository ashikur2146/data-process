package data.processing;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class CustomerBatchProcessor {

	private static final int BATCH_SIZE = 100000;
	private static final Pattern EMAIL_PATTERN = Pattern.compile("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}$");

	private final BlockingQueue<Customer> queue = new LinkedBlockingQueue<>();
	private final AtomicBoolean isProcessing = new AtomicBoolean(false);
	private final String validFilePath;
	private final String invalidFilePath;

	public CustomerBatchProcessor(String validFilePath, String invalidFilePath) {
		this.validFilePath = validFilePath;
		this.invalidFilePath = invalidFilePath;
	}

	public void start() {
		if (isProcessing.compareAndSet(false, true)) {
			new Thread(this::processBatch).start();
		}
	}

	public void stop() {
		if (isProcessing.compareAndSet(true, false)) {
			queue.offer(Customer.POISON_PILL);
		}
	}

	public void submit(Customer customer) {
		if (isProcessing.get()) {
			try {
				queue.put(customer);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
	}

	private void processBatch() {
		List<Customer> validBatch = new ArrayList<>();
		List<Customer> invalidBatch = new ArrayList<>();
		int batchCount = 1;
		while (isProcessing.get() || !queue.isEmpty()) {
			List<Customer> batch = new ArrayList<>(BATCH_SIZE);
			queue.drainTo(batch, BATCH_SIZE);
			batch.stream().collect(Collectors.groupingBy(this::isValidCustomer)).forEach((isValid, customers) -> {
				if (isValid) {
					validBatch.addAll(customers);
				} else {
					invalidBatch.addAll(customers);
				}
			});
			if (validBatch.size() >= BATCH_SIZE) {
				writeBatch(validBatch, validFilePath, batchCount++);
				validBatch.clear();
			}
			if (invalidBatch.size() >= BATCH_SIZE) {
				writeBatch(invalidBatch, invalidFilePath, batchCount++);
				invalidBatch.clear();
			}
		}
		// process any remaining customers
		if (!validBatch.isEmpty()) {
			writeBatch(validBatch, validFilePath, batchCount++);
		}
		if (!invalidBatch.isEmpty()) {
			writeBatch(invalidBatch, invalidFilePath, batchCount);
		}
	}

	private boolean isValidCustomer(Customer customer) {
		return isValidEmail(customer.getEmail()) && isValidPhoneNumber(customer.getPhone());
	}

	private boolean isValidEmail(String email) {
		return email != null && EMAIL_PATTERN.matcher(email).matches();
	}

	private boolean isValidPhoneNumber(String phone) {
		String digits = phone.replaceAll("\\D+", "");
		return digits.matches("^[2-9]\\d{2}[2-9]\\d{2}\\d{4}$") || digits.matches("^1?[2-9]\\d{2}[2-9]\\d{2}\\d{4}$")
				|| digits.matches("^1?[2-9]\\d{2}[2-9]\\d{2}\\d{2}[2-9]\\d{3}$");
	}

	private void writeBatch(List<Customer> customers, String filePath, int batchCount) {
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath + "_" + batchCount + ".txt"))) {
			for (Customer customer : customers) {
				writer.write(customer.toString());
				writer.newLine();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
