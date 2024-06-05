import pickle
import pandas as pd

#chat gpt link https://chatgpt.com/share/5bbc164a-4a83-4419-b07b-282b3678e6b6

class DataExtractor:
    def __init__(self, invoice_file, expired_ids_file):
        self.invoice_file = invoice_file
        self.expired_ids_file = expired_ids_file

    def load_data(self):
        with open(self.invoice_file, 'rb') as f:
            self.invoices_data = pickle.load(f)
            print("Invoices data loaded successfully.")
            print("Sample invoices data:")
            for invoice in self.invoices_data[:5]:
                print(invoice)
        
        with open(self.expired_ids_file, 'r') as f:
            expired_ids_string = f.read().strip()
            self.expired_ids = [int(id_str) for id_str in expired_ids_string.split(',')]
            print("\nExpired invoice IDs loaded successfully.")
            print("Expired invoice IDs:")
            print(self.expired_ids)

    def transform_data(self):
        records = []
        type_conversion = {0: 'Material', 1: 'Equipment', 2: 'Service', 3: 'Other'}
        
        for invoice in self.invoices_data:
            invoice_id = invoice.get('id')
            created_on_str = invoice.get('created_on')
            try:
                created_on = pd.to_datetime(created_on_str)
            except ValueError:
                print(f"Skipping invalid date: {created_on_str}")
                continue

            invoice_items = invoice.get('items', [])
            invoice_total = sum(int(item.get('unit_price', 0)) * int(item.get('quantity', 0)) for item in invoice_items if isinstance(item.get('unit_price'), str) and isinstance(item.get('quantity'), str))
            is_expired = invoice_id in self.expired_ids
            
            for item in invoice_items:
                unit_price_str = item.get('unit_price', '0')
                quantity_str = item.get('quantity', '0')
                if not (isinstance(unit_price_str, str) and isinstance(quantity_str, str)):
                    continue
                if not (unit_price_str.isdigit() and quantity_str.isdigit()):
                    continue
                unit_price = int(unit_price_str)
                quantity = int(quantity_str)
                total_price = unit_price * quantity
                percentage_in_invoice = total_price / invoice_total if invoice_total else 0


                record = {
                    'invoice_id': invoice_id,
                    'created_on': created_on,
                    'invoiceitem_id': item.get('id'),
                    'invoiceitem_name': item.get('name', 'Unknown'),
                    'type': type_conversion.get(item.get('type', 3), 'Other'),
                    'unit_price': unit_price,
                    'total_price': total_price,
                    'percentage_in_invoice': percentage_in_invoice,
                    'is_expired': is_expired
                }
                records.append(record)
        self.df = pd.DataFrame(records)
        columns_to_convert = ['invoice_id', 'created_on', 'invoiceitem_id', 'invoiceitem_name', 'type', 'unit_price', 'total_price', 'percentage_in_invoice', 'is_expired']
        columns_missing = [col for col in columns_to_convert if col not in self.df.columns]
        if columns_missing:
            print("Columns missing in DataFrame:")
            print(columns_missing)
        else:
            self.df = self.df.astype({
                'invoice_id': 'int64',
                'created_on': 'datetime64[ns]',
                'invoiceitem_id': 'int64',
                'invoiceitem_name': 'str',
                'type': 'str',
                'unit_price': 'int64',
                'total_price': 'int64',
                'percentage_in_invoice': 'float64',
                'is_expired': 'bool'
            })
            self.df = self.df.sort_values(by=['invoice_id', 'invoiceitem_id']).reset_index(drop=True)
            print("Data transformation completed.")

    def save_to_csv(self, output_file):
        self.df.to_csv(output_file, index=False)
        print(f"DataFrame saved to '{output_file}'.")

if __name__ == "__main__":
    extractor = DataExtractor('invoices_new.pkl', 'expired_invoices.txt')
    extractor.load_data()
    extractor.transform_data()
    extractor.save_to_csv('/Users/user/Desktop/data/invoices_flat.csv')
    print("Data extraction and transformation complete.")


