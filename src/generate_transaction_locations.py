import csv
import hashlib
import os


customer_path = "data/raw/customers.csv"
account_path = "data/raw/accounts.csv"
transaction_path = "data/raw/transactions.csv"

output_dir = "data/synthetic"
output_path = f"{output_dir}/transaction_locations.csv"

os.makedirs(output_dir, exist_ok=True)


# ambil kota masing-masing customer
customer_city = {}

with open(customer_path, newline="", encoding="utf-8") as file:
    reader = csv.DictReader(file)

    for row in reader:
        if row["city"]:
            customer_city[row["customer_id"]] = row["city"]


# mapping account ke customer
account_customer = {}

with open(account_path, newline="", encoding="utf-8") as file:
    reader = csv.DictReader(file)

    for row in reader:
        account_customer[row["account_id"]] = row["customer_id"]


# daftar kota diambil dari data customer yang tersedia
city_list = sorted(set(customer_city.values()))


def get_merchant_city(txn_id, home_city):

    # hash dipakai supaya hasil tetap sama setiap kali script dijalankan
    hash_value = int(
        hashlib.md5(txn_id.encode("utf-8")).hexdigest()[:8],
        16
    )

    # sebagian besar transaksi diasumsikan terjadi di kota customer
    if hash_value % 100 < 95:
        return home_city

    # sebagian kecil dibuat terjadi di kota lain untuk kebutuhan test fraud
    other_cities = [
        city
        for city in city_list
        if city != home_city
    ]

    city_index = (hash_value // 100) % len(other_cities)

    return other_cities[city_index]


generated_count = 0

with open(transaction_path, newline="", encoding="utf-8") as source_file, \
     open(output_path, "w", newline="", encoding="utf-8") as output_file:

    reader = csv.DictReader(source_file)

    writer = csv.DictWriter(
        output_file,
        fieldnames=[
            "txn_id",
            "merchant_city",
            "location_source"
        ]
    )

    writer.writeheader()

    for row in reader:

        # hanya transaksi yang punya merchant category
        if not row["merchant_category"]:
            continue

        customer_id = account_customer.get(
            row["account_id"]
        )

        home_city = customer_city.get(
            customer_id
        )

        if not home_city:
            continue

        merchant_city = get_merchant_city(
            row["txn_id"],
            home_city
        )

        writer.writerow({
            "txn_id": row["txn_id"],
            "merchant_city": merchant_city,
            "location_source": "SYNTHETIC_TEST"
        })

        generated_count += 1


print(f"synthetic location selesai: {generated_count}")
print(f"output: {output_path}")