package data.processing;

public class Customer {
	private String name;
	private String address;
	private String phone;
	private String email;
	private String ipAddress;
	public static final Customer POISON_PILL = new Customer();

	public Customer() {
		super();
	}

	public Customer(String name, String address, String phone, String email, String ipAddress) {
		this.name = name;
		this.address = address;
		this.phone = phone;
		this.email = email;
		this.ipAddress = ipAddress;
	}

	public String getName() {
		return name;
	}

	public String getAddress() {
		return address;
	}

	public String getPhone() {
		return phone;
	}

	public String getEmail() {
		return email;
	}

	public String getIpAddress() {
		return ipAddress;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (!(obj instanceof Customer)) {
			return false;
		}
		Customer other = (Customer) obj;
		return email.equals(other.email);
	}

	@Override
	public String toString() {
		return "Customer [name=" + name + ", address=" + address + ", phone=" + phone + ", email=" + email
				+ ", ipAddress=" + ipAddress + "]";
	}
}
