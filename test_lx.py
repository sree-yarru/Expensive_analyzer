import langextract as lx
import os

api_key = os.environ.get("GEMINI_API_KEY", "")

# We simulate OCR text from Gemini
ocr_text = """
Walmart Store #3412
123 Main St
Tel: 555-0192
Date: 2023-11-04 14:32

1 Apple            $1.50
2 Bananas          $2.00
3 Milk 1gal        $4.50

Total: $8.00
"""

examples = [
    lx.ExampleData(
        text="Target Seattle\nDate: 2023-10-01\n1 Orange $1.00\nTotal: $1.00",
        extractions=[
            lx.Extraction("shop_details", "Target Seattle"),
            lx.Extraction("date", "2023-10-01"),
            lx.Extraction("item", "Orange", attributes={"quantity": "1", "price": "1.00"}),
            lx.Extraction("total", "1.00"),
        ]
    )
]

print("Starting extraction...")
try:
    results = lx.extract(
        text_or_documents=ocr_text,
        prompt_description="Extract receipt details including shop details, date, items (with quantity and price), and total cost.",
        examples=examples,
        api_key=api_key,
        model_id="gemini-2.5-flash"
    )

    print("Extraction successful.")
    if hasattr(results, "extractions"):
        for ext in results.extractions:
            print(f"[{ext.extraction_class}] {ext.extraction_text} - {ext.attributes}")
except Exception as e:
    print(f"Error: {e}")
