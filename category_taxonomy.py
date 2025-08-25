# category_taxonomy.py
# Canonical taxonomy (Income + Expense) with sub-categories

INCOME_MAIN = [
    "Salary & Wages",
    "Business & Freelance",
    "Investments",
    "Rental & Property",
    "Government & Benefits",
    "Gifts & Grants",
    "Refunds & Adjustments",
    "Other Income",
]

INCOME_SUB = {
    "Salary & Wages": ["Base Salary", "Bonuses", "Overtime", "Commissions", "Tips"],
    "Business & Freelance": ["Client Payments", "Project Income", "Consulting Fees", "Sales Revenue"],
    "Investments": ["Dividends", "Interest Income", "Capital Gains", "Bond Income"],
    "Rental & Property": ["Residential Rent", "Commercial Rent", "Short-Term Rental (e.g., Airbnb)"],
    "Government & Benefits": ["Pension", "Unemployment Benefits", "Child Support", "Social Security"],
    "Gifts & Grants": ["Monetary Gifts", "Scholarships", "Grants", "Inheritance"],
    "Refunds & Adjustments": ["Tax Refunds", "Purchase Refunds", "Cashback"],
    "Other Income": ["Lottery/Prize", "Insurance Payouts", "Cryptocurrency Gains"],
}

EXPENSE_MAIN = [
    "Housing & Utilities",
    "Food & Groceries",
    "Transportation",
    "Debts & Liabilities",
    "Insurance",
    "Healthcare & Wellness",
    "Education & Learning",
    "Shopping & Personal Care",
    "Entertainment & Leisure",
    "Travel & Holidays",
    "Gifts & Donations",
    "Taxes & Fees",
    "Business Expenses",
    "Savings & Investments",
    "Miscellaneous",
]

EXPENSE_SUB = {
    "Housing & Utilities": ["Rent/Mortgage", "Property Tax", "Water", "Electricity", "Gas", "Internet", "Phone"],
    "Food & Groceries": ["Groceries", "Dining Out", "Coffee/Tea", "Snacks"],
    "Transportation": ["Fuel", "Public Transport", "Ride-Hailing", "Vehicle Maintenance", "Tolls & Parking"],
    "Debts & Liabilities": ["Credit Card Payment", "Loan Payment", "Mortgage Payment", "Overdraft Fees"],
    "Insurance": ["Health", "Life", "Vehicle", "Property", "Travel"],
    "Healthcare & Wellness": ["Doctor Visits", "Medicines", "Dental Care", "Therapy", "Fitness & Gym"],
    "Education & Learning": ["Tuition", "Courses", "Books", "Supplies"],
    "Shopping & Personal Care": ["Clothing", "Accessories", "Electronics", "Personal Care", "Cosmetics"],
    "Entertainment & Leisure": ["Subscriptions (Netflix, Spotify)", "Movies", "Events", "Gaming"],
    "Travel & Holidays": ["Flights", "Accommodation", "Local Transport", "Activities"],
    "Gifts & Donations": ["Charitable Donations", "Monetary Gifts", "Non-Cash Gifts"],
    "Taxes & Fees": ["Income Tax", "Property Tax", "Service Charges", "Penalties"],
    "Business Expenses": ["Office Rent", "Software", "Marketing", "Equipment", "Professional Fees"],
    "Savings & Investments": ["Savings Deposit", "Retirement Fund", "Stock Purchase", "Crypto Purchase"],
    "Miscellaneous": ["Unplanned Purchases", "Pet Care", "Home Maintenance"],
}

ALL_MAIN = INCOME_MAIN + EXPENSE_MAIN

# Minimal MCC → (main, sub) mapping (extend as needed)
MCC_MAP = {
    "5411": ("Food & Groceries", "Groceries"),
    "5541": ("Transportation", "Fuel"),
    "5812": ("Food & Groceries", "Dining Out"),
    "5814": ("Food & Groceries", "Dining Out"),
    "4111": ("Transportation", "Public Transport"),
    "4899": ("Housing & Utilities", "Internet"),
    "4814": ("Housing & Utilities", "Phone"),
    "5732": ("Shopping & Personal Care", "Electronics"),
}

# Keyword aliases (TR+EN) → (main, sub). Longest-substring wins.
KEYWORD_MAP = {
    # Food & Groceries
    "migros": ("Food & Groceries", "Groceries"),
    "carrefour": ("Food & Groceries", "Groceries"),
    "a101": ("Food & Groceries", "Groceries"),
    "bim": ("Food & Groceries", "Groceries"),
    "şok": ("Food & Groceries", "Groceries"),
    "sok": ("Food & Groceries", "Groceries"),
    "getir": ("Food & Groceries", "Groceries"),
    "banabi": ("Food & Groceries", "Groceries"),
    "yemeksepeti": ("Food & Groceries", "Dining Out"),
    "trendyol yemek": ("Food & Groceries", "Dining Out"),
    "kahve": ("Food & Groceries", "Coffee/Tea"),
    "starbucks": ("Food & Groceries", "Coffee/Tea"),
    "cafe": ("Food & Groceries", "Dining Out"),
    "restaurant": ("Food & Groceries", "Dining Out"),

    # Transportation
    "uber": ("Transportation", "Ride-Hailing"),
    "bitaksi": ("Transportation", "Ride-Hailing"),
    "iett": ("Transportation", "Public Transport"),
    "metro ist": ("Transportation", "Public Transport"),
    "havabus": ("Transportation", "Public Transport"),
    "taksi": ("Transportation", "Ride-Hailing"),
    "opet": ("Transportation", "Fuel"),
    "shell": ("Transportation", "Fuel"),
    "bp": ("Transportation", "Fuel"),
    "total": ("Transportation", "Fuel"),
    "aytemiz": ("Transportation", "Fuel"),
    "petrol ofisi": ("Transportation", "Fuel"),

    # Shopping & Personal Care
    "hepsiburada": ("Shopping & Personal Care", "Electronics"),
    "trendyol": ("Shopping & Personal Care", "Clothing"),
    "n11": ("Shopping & Personal Care", "Electronics"),
    "amazon": ("Shopping & Personal Care", "Electronics"),
    "boyner": ("Shopping & Personal Care", "Clothing"),
    "decathlon": ("Shopping & Personal Care", "Accessories"),
    "flo": ("Shopping & Personal Care", "Clothing"),
    "koton": ("Shopping & Personal Care", "Clothing"),
    "zara": ("Shopping & Personal Care", "Clothing"),
    "hm ": ("Shopping & Personal Care", "Clothing"),

    # Housing & Utilities
    "elektrik": ("Housing & Utilities", "Electricity"),
    "doğalgaz": ("Housing & Utilities", "Gas"),
    "dogalgaz": ("Housing & Utilities", "Gas"),
    "su faturası": ("Housing & Utilities", "Water"),
    "su fatura": ("Housing & Utilities", "Water"),
    "internet": ("Housing & Utilities", "Internet"),
    "turkcell": ("Housing & Utilities", "Phone"),
    "vodafone": ("Housing & Utilities", "Phone"),
    "turktelekom": ("Housing & Utilities", "Phone"),
    "ttnet": ("Housing & Utilities", "Internet"),
    "kira": ("Housing & Utilities", "Rent/Mortgage"),
    "ev kirası": ("Housing & Utilities", "Rent/Mortgage"),
    "aidat": ("Housing & Utilities", "Home Maintenance"),

    # Healthcare & Wellness
    "eczane": ("Healthcare & Wellness", "Medicines"),
    "hastane": ("Healthcare & Wellness", "Doctor Visits"),
    "hospital": ("Healthcare & Wellness", "Doctor Visits"),

    # Insurance
    "sigorta": ("Insurance", "Vehicle"),

    # Education & Learning
    "okul": ("Education & Learning", "Tuition"),
    "üniversite": ("Education & Learning", "Tuition"),
    "kurs": ("Education & Learning", "Courses"),

    # Entertainment & Leisure / Subscriptions
    "sinema": ("Entertainment & Leisure", "Movies"),
    "cinema": ("Entertainment & Leisure", "Movies"),
    "spotify": ("Entertainment & Leisure", "Subscriptions (Netflix, Spotify)"),
    "netflix": ("Entertainment & Leisure", "Subscriptions (Netflix, Spotify)"),
    "youtube": ("Entertainment & Leisure", "Subscriptions (Netflix, Spotify)"),
    "blutv": ("Entertainment & Leisure", "Subscriptions (Netflix, Spotify)"),
    "exxen": ("Entertainment & Leisure", "Subscriptions (Netflix, Spotify)"),

    # Travel & Holidays
    "pegasus": ("Travel & Holidays", "Flights"),
    "thy": ("Travel & Holidays", "Flights"),
    "turkish airlines": ("Travel & Holidays", "Flights"),
    "booking": ("Travel & Holidays", "Accommodation"),
    "airbnb": ("Travel & Holidays", "Accommodation"),

    # Banking / Transfers / Debts
    "eft": ("Debts & Liabilities", "Credit Card Payment"),
    "havale": ("Debts & Liabilities", "Loan Payment"),
    "fast ": ("Debts & Liabilities", "Credit Card Payment"),
    "iban": ("Debts & Liabilities", "Loan Payment"),
    "para transfer": ("Debts & Liabilities", "Loan Payment"),
    "virman": ("Debts & Liabilities", "Loan Payment"),
    "nakit çekim": ("Transportation", "Tolls & Parking"),  # or Cash handling; adjust if needed
    "atm": ("Transportation", "Tolls & Parking"),

    # Income signals
    "maas": ("Salary & Wages", "Base Salary"),
    "maaş": ("Salary & Wages", "Base Salary"),
    "salary": ("Salary & Wages", "Base Salary"),
    "ucret": ("Salary & Wages", "Base Salary"),
    "ücret": ("Salary & Wages", "Base Salary"),

    # Taxes & Fees
    "komisyon": ("Taxes & Fees", "Service Charges"),
    "hesap işletim": ("Taxes & Fees", "Service Charges"),
    "isletim ücreti": ("Taxes & Fees", "Service Charges"),
    "bsmv": ("Taxes & Fees", "Income Tax"),
    "kkdf": ("Taxes & Fees", "Income Tax"),
    "vergi": ("Taxes & Fees", "Income Tax"),
}
