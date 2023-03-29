package data.processing;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CustomerBatchProcessor implements Runnable {

	private BlockingQueue<Customer> queue;
	private String validFilePath;
	private String invalidFilePath;
	private static final int BATCH_SIZE = 100000;

	public CustomerBatchProcessor(BlockingQueue<Customer> queue, String validFilePath, String invalidFilePath) {
		this.queue = queue;
		this.validFilePath = validFilePath;
		this.invalidFilePath = invalidFilePath;
	}

	@Override
	public void run() {
		try {
			List<Customer> validBatch = new ArrayList<>();
			List<Customer> invalidBatch = new ArrayList<>();
			int batchCount = 0;
			long startTime = System.currentTimeMillis();
			while (true) {
				Customer customer = queue.take();
				if (customer == null || customer == Customer.POISON_PILL) {
					break;
				}

				if (isValidCustomer(customer)) {
					validBatch.add(customer);
				} else {
					invalidBatch.add(customer);
				}

				if (validBatch.size() >= BATCH_SIZE) {
					writeBatch(validBatch, validFilePath, ++batchCount);
					validBatch.clear();
				}

				if (invalidBatch.size() >= BATCH_SIZE) {
					writeBatch(invalidBatch, invalidFilePath, batchCount);
					invalidBatch.clear();
				}
			}
			// process any remaining customers
			if (!validBatch.isEmpty()) {
				writeBatch(validBatch, validFilePath, ++batchCount);
			}
			if (!invalidBatch.isEmpty()) {
				writeBatch(invalidBatch, invalidFilePath, batchCount);
			}
			long endTime = System.currentTimeMillis();
			long duration = endTime - startTime;
			System.out.println("Batch processing completed in " + duration + " milliseconds.");
		} catch (InterruptedException | IOException e) {
			e.printStackTrace();
		}
	}

	private boolean isValidCustomer(Customer customer) {
		return isValidEmail(customer.getEmail()) && isValidPhoneNumber(customer.getPhone());
	}

	private boolean isValidEmail(String email) {
		if (email == null || email.isEmpty()) {
			return false;
		}
		String regex = "^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}$";
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(email);
		return matcher.matches();
	}

	private boolean isValidPhoneNumber(String phone) {
		String digits = phone.replaceAll("\\D+", "");
		if (digits.matches("^[2-9]\\d{2}[2-9]\\d{2}\\d{4}$")) {
			return true;
		} else if (digits.matches("^1?[2-9]\\d{2}[2-9]\\d{2}\\d{4}$")) {
			return true;
		} else if (digits.matches("^1?[2-9]\\d{2}[2-9]\\d{2}\\d{2}[2-9]\\d{3}$")) {
			return true;
		} else {
			return false;
		}
	}

	private void writeBatch(List<Customer> customers, String filePath, int batchCount) throws IOException {
		BufferedWriter writer = new BufferedWriter(new FileWriter(filePath + "_" + batchCount + ".txt"));
		for (Customer customer : customers) {
			writer.write(customer.toString() + "\n");
		}
		writer.close();
	}
}
