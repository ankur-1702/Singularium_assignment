"""Regenerate the deterministic 200-row interview dataset."""
import csv
from pathlib import Path

items = [
    ("Amul Butter 500g", "Pack of 2, salted table butter"), ("Amul Taaza Toned Milk 1L", "UHT dairy milk carton"),
    ("India Gate Basmati Rice 5kg", "Classic long grain rice"), ("Aashirvaad Whole Wheat Atta 5kg", "Chakki fresh flour"),
    ("Britannia Good Day Butter Cookies 200g", "Family pack biscuits"), ("Parle-G Glucose Biscuits 800g", "Value pack snack"),
    ("Tata Salt 1kg", "Iodised vacuum evaporated salt"), ("Fortune Sunflower Oil 1L", "Refined cooking oil pouch"),
    ("Mother Dairy Paneer 200g", "Fresh cottage cheese pack"), ("Maggi 2-Minute Noodles  pack of 4", "Masala instant noodles"),
    ("Coca-Cola Original Taste 750ml", "Sparkling soft drink bottle"), ("Tropicana Mixed Fruit Juice 1L", "Fruit beverage carton"),
    ("Tata Tea Gold 500g", "Black tea leaves pouch"), ("Nescafe Classic Coffee 100g", "Instant coffee jar"),
    ("Bisleri Mineral Water 1L", "Packaged drinking water bottle"), ("Paper Boat Aam Panna 600ml", "Mango beverage bottle"),
    ("Dove Daily Shine Shampoo 340ml", "Nourishing hair care bottle"), ("Colgate Strong Teeth Toothpaste 200g", "Fluoride toothpaste tube"),
    ("Pears Pure and Gentle Soap  pack of 3", "Transparent bathing bars"), ("Nivea Soft Moisturizer 200ml", "Face and body cream"),
    ("Dettol Liquid Handwash  refill 750ml", "Original antibacterial hand wash"), ("Whisper Ultra Clean Pads pack of 15", "Sanitary care"),
    ("Surf Excel Easy Wash Detergent 2kg", "Laundry washing powder"), ("Vim Lemon Dishwash Gel 750ml", "Lemon dish cleaning liquid"),
    ("Harpic Power Plus 500ml", "Bathroom toilet cleaner"), ("Scotch-Brite Scrub Pad pack of 3", "Kitchen cleaning sponge"),
    ("Philips LED Bulb 9W", "Cool daylight energy saving bulb"), ("boAt BassHeads 100 Earphones", "Wired in-ear headphones with mic"),
    ("Portronics USB C Cable 1m", "Fast charging braided cable"), ("Logitech M235 Wireless Mouse", "Compact USB receiver mouse"),
    ("Samsung 25W USB C Charger", "Fast wall charger adapter"), ("Realme Buds Wireless Neckband", "Bluetooth audio headset"),
    ("Men's Cotton Crew Neck T Shirt", "Regular fit navy blue size M"), ("Women's Straight Fit Jeans", "Mid rise denim blue size 30"),
    ("Kids Cotton Hoodie", "Fleece pullover sweatshirt size 8 years"), ("Unisex Sports Socks pack of 3", "Cotton ankle socks size L"),
    ("Prestige Omega Deluxe Fry Pan 24cm", "Non-stick cookware with glass lid"), ("Cello Stainless Steel Water Bottle 1L", "Insulated kitchen flask"),
    ("Milton Thermosteel Flask 750ml", "Vacuum insulated hot and cold bottle"), ("Solimo Microfiber Bath Towel", "Quick dry cotton blue towel"),
    ("Pigeon Stainless Steel Pressure Cooker 3L", "Induction base kitchen cookware"), ("IKEA Storage Box 10L", "Clear plastic home organizer"),
    ("Haldiram's Aloo Bhujia 200g", "Spicy Indian namkeen snack"), ("Kellogg's Corn Flakes 475g", "Breakfast cereal family pack"),
    ("Dabur Honey 500g", "Natural honey squeeze bottle"), ("Patanjali Cow Ghee 1L", "Clarified butter jar"),
    ("Lays India's Magic Masala Chips  pack of 3", "Potato crisps snack"), ("Himalaya Neem Face Wash 150ml", "Purifying skincare cleanser"),
    ("Godrej Aer Room Freshener  room spray", "Fresh fragrance household spray"), ("Prestige Electric Kettle 1.5L", "Stainless steel kitchen appliance"),
]
out = Path(__file__).with_name("sample_listings.csv")
with out.open("w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["sku", "raw_title", "raw_description"])
    for i in range(200):
        title, description = items[i % len(items)]
        # Repeated base listings have distinct SKUs so the cache is easy to observe.
        writer.writerow([f"CIQ-{i+1:04d}", title, description])
print(f"Wrote 200 listings to {out}")
