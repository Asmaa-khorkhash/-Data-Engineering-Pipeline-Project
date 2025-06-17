import re
import logging
import os
import sqlalchemy 
import pandas as pd
import numpy as np
from dotenv import load_dotenv

load_dotenv()

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(
    filename=os.getenv("filename"),
    filemode='w',  # write mode
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)

logger = logging.getLogger()

class DataQualityChecker:

    def __init__(self, dataframes, tables_config):
        self.dataframes = dataframes
        self.tables_config = tables_config
        self.error_directory = "data_quality_errors"
        self.error_rows = []  
        self.emoji_report = {}
        self.dtype_mappings = {}  
        if not os.path.exists(self.error_directory):
            os.makedirs(self.error_directory)

    def infer_sqlalchemy_types(self):
        """
       Automatically map pandas dtypes to SQLAlchemy types for each dataframe.
        """
        for table_name, df in self.dataframes.items():
          table_dtype_mapping = {}
          schema = self.tables_config.get(table_name, {}).get("columns", {})
 
          for col in df.columns:
            dtype = df[col].dtype
            expected_type = schema.get(col, "").lower()
            
            if dtype == 'float64' and 'int' in expected_type:
                table_dtype_mapping[col] = sqlalchemy.types.INTEGER()
            elif dtype == 'float64':
                table_dtype_mapping[col] = sqlalchemy.types.FLOAT()
            elif dtype == 'datetime64[ns]':
                    table_dtype_mapping[col] = sqlalchemy.types.DATETIME()
            elif dtype == 'object':
                    table_dtype_mapping[col] = sqlalchemy.types.VARCHAR(length=255)
            elif dtype == 'object' and (str(col.values()).strip.lower in ("true", "1") or  str(col.values()).strip.lower in ("false", "0")): 
                    table_dtype_mapping[col] = sqlalchemy.types.Boolean
        self.dtype_mappings[table_name] = table_dtype_mapping

    
 
 
    # ---------------- Schema Based Checks ----------------
    def save_error_to_csv(self, table_name, issue, error_data):
        """
        Save each error to a CSV based on table name and issue type.
        """
        issue_filename = f"{table_name}_{issue.replace(' ', '_').replace(',', '')}.csv"
        issue_filepath = os.path.join(self.error_directory, issue_filename)
       
        # Convert the error data to a DataFrame and save it to CSV
        error_df = pd.DataFrame([error_data])
        error_df.to_csv(issue_filepath, index=False, mode='a', header=not os.path.exists(issue_filepath))

    def extract_primary_keys_from_schema(self, table_schema):
        primary_keys = []
        for column in table_schema["schema"]:
            if getattr(column, 'primary_key', False):
                primary_keys.append(column.name)
        return primary_keys
 
    def extract_foreign_keys_from_schema(self, table_schema):
        foreign_keys = {}
        for column in table_schema["schema"]:
            for constraint in column.foreign_keys:
                fk_column = column.name
                ref_table = constraint.target_fullname.split('.')[0]  # referenced table
                foreign_keys[fk_column] = ref_table
        return foreign_keys
    def extract_column_data_types_from_schema(self, table_schema):
        column_data_types = {}
        for column in table_schema["schema"]:
            column_data_types[column.name] = str(column.type)
        return column_data_types
   
    def validate_primary_keys(self, table, table_schema, table_name):
        pk_cols = self.extract_primary_keys_from_schema(table_schema)
        if not pk_cols:
            logger.warning(f"[PK Violation] No primary keys defined for '{table_name}'")
            return table
 
        if len(pk_cols) > 1:  # Composite Primary Key
            null_rows_mask = table[pk_cols].isnull().any(axis=1)
            null_rows_count = null_rows_mask.sum()
            if null_rows_count > 0:
                logger.warning(f"[PK Violation] {null_rows_count} rows have NULLs in composite primary key columns {pk_cols}.")
            table = table[~null_rows_mask]
 
            duplicate_rows_mask = table[pk_cols].duplicated()
            duplicate_rows_count = duplicate_rows_mask.sum()
            if duplicate_rows_count > 0:
                logger.warning(f"[PK Violation] {duplicate_rows_count} rows have duplicated composite primary key in columns {pk_cols}.")
            table = table[~duplicate_rows_mask]
 
        else:  # Single Primary Key
            pk_col = pk_cols[0]
 
            null_rows_mask = table[pk_col].isnull()
            null_rows_count = null_rows_mask.sum()
            if null_rows_count > 0:
                logger.warning(f"[PK Violation] {null_rows_count} NULL values found in primary key column '{pk_col}'.")
 
            duplicate_rows_mask = table[pk_col].duplicated() & ~null_rows_mask
            duplicate_rows_count = duplicate_rows_mask.sum()
            if duplicate_rows_count > 0:
                logger.warning(f"[PK Violation] {duplicate_rows_count} duplicated values found in primary key column '{pk_col}'.")
 
            table = table[~null_rows_mask & ~duplicate_rows_mask]
 
        return table
 
    def validate_foreign_keys(self, table, table_schema, table_name):
        """
        Validate foreign key integrity using extracted foreign key relationships.
        """
        foreign_keys = self.extract_foreign_keys_from_schema(table_schema)
 
        for fk_col, ref_table_name in foreign_keys.items():
            ref_table = self.dataframes.get(ref_table_name)
            if ref_table is not None:
                parent_pk_col = ref_table.columns[0]  
 
                # Check NULLs in FK column
                null_mask = table[fk_col].isnull()
                null_fk_rows = table[null_mask]
                if not null_fk_rows.empty:
                    logger.info(
                        f"{len(null_fk_rows)} rows in '{table_name}' have NULL values in foreign key '{fk_col}'."
                    )
 
                # Check unmatched FK values
                unmatched_mask = ~table[fk_col].isin(ref_table[parent_pk_col]) & ~table[fk_col].isnull()
                unmatched_fk_rows = table[unmatched_mask]
                if not unmatched_fk_rows.empty:
                    logger.warning(
                        f"[FK Violation] {len(unmatched_fk_rows)} rows in '{table_name}' have values in '{fk_col}' "
                        f"that do not match any '{parent_pk_col}' in '{ref_table_name}'."
                    )
 
                # Clean the table
                table = table[~unmatched_mask]
            else:
                logger.warning(f"[FK Violation] Reference table '{ref_table_name}' not found in loaded dataframes.")
 
        return table
 
    def validate_column_data_types(self, table, table_schema, table_name):
        column_data_types = self.extract_column_data_types_from_schema(table_schema)
 
        if table_name not in self.dtype_mappings:
            logger.warning(f"No dtype mapping found for table '{table_name}'. Skipping data type validation.")
            return table
 
        actual_dtypes = self.dtype_mappings[table_name]
 
        for column_name, expected_type in column_data_types.items():
            if column_name in table.columns:
                actual_type_obj = actual_dtypes.get(column_name)
                if actual_type_obj is None:
                    continue
 
                actual_type = str(actual_type_obj.__class__.__name__)
                expected_type_lower = expected_type.lower()
 
            # FIX: Allow FLOAT to match INTEGER if schema says INTEGER
                if expected_type_lower == "integer" and actual_type == "FLOAT":
                    continue
 
                if actual_type not in expected_type:
                    logger.warning(f"[Data Type Mismatch] Column '{column_name}' in table '{table_name}' has type '{actual_type}' but expected '{expected_type}'.")
            else:
                logger.warning(f"[Column Missing] Column '{column_name}' is missing in table '{table_name}'.")
 
        return table
 
 
   
    def detect_emoji_columns(self, table, table_name):
        """
        Detect columns containing emojis and log them.
        Also return a list of columns with emojis.
        """
        emoji_pattern = re.compile(
            "["
            "\U0001F600-\U0001F64F"  # Emoticons
            "\U0001F300-\U0001F5FF"  # Symbols & Pictographs
            "\U0001F680-\U0001F6FF"  # Transport & Map Symbols
            "\U0001F1E0-\U0001F1FF"  # Flags
            "\U00002700-\U000027BF"  # Dingbats
            "\U000024C2-\U0001F251"  # Enclosed characters
            "]+", flags=re.UNICODE)
 
        emoji_columns = []
 
        for column in table.columns:
            try:
                non_null_text = table[column].dropna().astype(str)
                if non_null_text.apply(lambda x: bool(emoji_pattern.search(x))).any():
                    logger.warning(f"[Emoji Detected] Column '{column}' in table '{table_name}' contains emojis.")
                    emoji_columns.append(column)
            except Exception as e:
                logger.error(f"[Error] Checking column '{column}' in table '{table_name}': {str(e)}")
 
        return emoji_columns
   
 
    def check_nulls(self, table, table_name):
        null_summary = table.isnull().sum()
        null_columns = null_summary[null_summary > 0]
 
        for col, null_count in null_columns.items():
            self.error_rows.append({
                "Table": table_name,
                "Column": col,
                "Issue": f"{null_count} NULL values",
                "Record_Index": "Multiple"
            })
            logging.info(f"[Null Check] Table: {table_name}, Column: {col}, NULL Count: {null_count}")
       
        return table
 
 
    # ---------------- Business Rules ---------------- #
 
    def detect_missing_product_dimensions(self, table, table_name):
        if table_name != 'products':
            return table
 
        required_cols = [
            "product_weight_g", "product_length_cm",
            "product_height_cm", "product_width_cm",
            "product_category_name","product_name_lenght"
        ]
 
        missing = table[table[required_cols].isnull().any(axis=1)]
 
        if not missing.empty:
            for idx, row in missing.iterrows():
                missing_cols = [col for col in required_cols if pd.isna(row[col])]
                error_data = {
                    "Table": table_name,
                    "Column": str(missing_cols),
                    "Issue": "Missing Product Dimension",
                    "Value": str(row[missing_cols].to_dict()),
                    "Record_Index": idx
                }
                self.save_error_to_csv(table_name, "Missing Product Dimension", error_data)
 
        return table
 
    def check_review_dates(self, table, table_name):
        if table_name != 'reviews':
            return table
 
        table['review_creation_date'] = pd.to_datetime(table['review_creation_date'], errors='coerce')
        table['review_answer_timestamp'] = pd.to_datetime(table['review_answer_timestamp'], errors='coerce')
 
        invalid = table[
            (table['review_creation_date'].notna()) &
            (table['review_answer_timestamp'].notna()) &
            (table['review_creation_date'] > table['review_answer_timestamp'])
        ]
 
        if not invalid.empty:
            for idx in invalid.index:
                error_data = {
                    "Table": table_name,
                    "Column": "review_creation_date, review_answer_timestamp",
                    "Issue": "Invalid Review Dates",
                    "Record_Index": idx
                }
                self.save_error_to_csv(table_name, "Invalid Review Dates", error_data)
 
        return table
 
    def check_chronological_order(self, table, table_name):
        if table_name != 'orders':
            return table
 
        timestamps = [
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date"
        ]
       
        for col in timestamps:
            if col in table.columns:
                table[col] = pd.to_datetime(table[col], errors='coerce')
 
        for idx, row in table.iterrows():
            ordered_dates = row[timestamps].dropna().values
            timestamp_columns = row[timestamps].dropna().index
 
            issues = []
            for i in range(1, len(ordered_dates)):
                if ordered_dates[i] < ordered_dates[i-1]:
                    issues.append(f"{timestamp_columns[i]} comes before {timestamp_columns[i-1]}")
 
            if issues:
                issue_description = "; ".join(issues)
                error_data = {
                    "Table": table_name,
                    "Issue": f"{issue_description}",
                    "Record_Index": idx
                }
                self.save_error_to_csv(table_name, "Non-Chronological Order", error_data)
 
        return table
 
    def detect_negative_values(self, table, table_name):
        for col in table.select_dtypes(include=[np.number]).columns:
            if col in ["longitude", "longitude"]:
                continue
 
            negatives = table[table[col] < 0]
 
            if not negatives.empty:
                for idx in negatives.index:
                    error_data = {
                        "Table": table_name,
                        "Column": col,
                        "Issue": "Negative Value",
                        "Record_Index": idx
                    }
                    self.save_error_to_csv(table_name, "Negative Value", error_data)
 
        return table
   
    # ---------------- Master Run Methods ---------------- #
 
    def run_all_checks(self):
    
      # First, infer all SQLAlchemy types for the dataframes
      self.infer_sqlalchemy_types()
 
      for table_name, table in self.dataframes.items():
          if table_name not in self.tables_config:
              logger.warning(f"No config for table: {table_name}")
              continue
          cleaned_data_dict = {}
          schema = self.tables_config[table_name]
 
          table = self.validate_primary_keys(table, schema, table_name)
          table = self.validate_foreign_keys(table, schema, table_name)
          table = self.validate_column_data_types(table, schema, table_name)
          table = self.check_nulls(table, table_name)
          emoji_cols = self.detect_emoji_columns(table, table_name)
          if emoji_cols:
              self.emoji_report[table_name] = emoji_cols
 
          self.dataframes[table_name] = table
          cleaned_data_dict[table_name] = table
 
    def run_business_rules(self):
        """
        Returns the error table.
        """
        if 'orders' in self.dataframes:
            self.dataframes['orders'] = self.check_chronological_order(self.dataframes['orders'], 'orders')
 
        if 'reviews' in self.dataframes:
            self.dataframes['reviews'] = self.check_review_dates(self.dataframes['reviews'], 'reviews')
 
        if 'products' in self.dataframes:
            self.dataframes['products'] = self.detect_missing_product_dimensions(self.dataframes['products'], 'products')
 
        for table_name, table in self.dataframes.items():
            self.dataframes[table_name] = self.detect_negative_values(table, table_name)
 
        logger.info("Business rule validations completed.")
 
    def get_cleaned_data_dict(self):
        """
        Returns a dictionary of cleaned DataFrames without saving them as CSV files.
        """
        cleaned_data_dict = {}
 
        # Iterate over the dataframes to clean them
        for table_name, table in self.dataframes.items():
            # Perform all checks on each table (like primary keys, foreign keys, nulls, etc.)
            table = self.validate_primary_keys(table, self.tables_config.get(table_name, {}), table_name)
            table = self.validate_foreign_keys(table, self.tables_config.get(table_name, {}), table_name)
            table = self.validate_column_data_types(table, self.tables_config.get(table_name, {}), table_name)
            table = self.check_nulls(table, table_name)
            emoji_cols = self.detect_emoji_columns(table, table_name)
            if emoji_cols:
                self.emoji_report[table_name] = emoji_cols
 
            cleaned_data_dict[table_name] = table
 
        return cleaned_data_dict  

    
