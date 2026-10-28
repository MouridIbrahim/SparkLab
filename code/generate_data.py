import csv
import random
random.seed(2026)
categories = ["Electronics", "Books", "Clothing", "Food", "Sports"]
cities = ["Beijing", "Shanghai", "Guangzhou", "Shenzhen", "Chengdu"]
with open("orders.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["order_id", "user_id", "category", "price", "quantity", "city"])
    for i in range(1, 1001):
        writer.writerow([i, random.randint(1, 200), random.choice(categories),
                            round(random.uniform(10, 1000), 2),
                            random.randint(1, 5), random.choice(cities)])
print("Dataset generated successfully!")