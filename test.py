import os
import json
import time

def extract_text_from_pdf(pdf_name):
    sample_data = {
        "sample1.pdf": "This is a sample extracted text from Sample1 PDF.",
        "sample2.pdf": "Here is another example of extracted text from Sample2 PDF.",
        "sample3.pdf": "Testing OCR extraction simulation with Sample3 PDF."
    }
    return sample_data.get(pdf_name, "No text found.")

def process_pdfs():
    sample_pdfs = ["sample1.pdf", "sample2.pdf", "sample3.pdf"]
    output_folder = "./output_json"
    os.makedirs(output_folder, exist_ok=True)
    
    extracted_data = []
    for pdf_name in sample_pdfs:
        print(f"Processing {pdf_name}...")
        time.sleep(1) 
        text = extract_text_from_pdf(pdf_name)
        result = {"filename": pdf_name, "text": text}
        extracted_data.append(result)
        
        with open(os.path.join(output_folder, f"{pdf_name}.json"), "w") as f:
            json.dump(result, f, indent=4)
        
        print(f"Saved extracted text for {pdf_name} in output_json/{pdf_name}.json")
    
    return extracted_data

if __name__ == "__main__":
    print("Starting PDF text extraction test...")
    process_pdfs()
    print("Processing complete!")
