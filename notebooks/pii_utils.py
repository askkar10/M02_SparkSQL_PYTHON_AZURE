from pyspark.sql.functions import aes_encrypt, aes_decrypt, lit, col

class PIIProcessor:
    def __init__(self, encryption_key):
        self.encryption_key = encryption_key

    def encrypt_columns(self, df, columns_to_encrypt):
        for column in columns_to_encrypt:
            df = df.withColumn(column, aes_encrypt(col(column).cast("string").cast("binary"), lit(self.encryption_key)))
        return df

    def decrypt_columns(self, df, columns_to_decrypt):
        for column in columns_to_decrypt:
            df = df.withColumn(column, aes_decrypt(col(column), lit(self.encryption_key)).cast("string"))
        return df