import pandas as pd
import openpyxl

file_path = r"C:\Users\asvr4\Desktop\work\Files\csv\EMP.csv"
df = pd.read_csv(file_path)
df.to_excel("emp.xlsx", index=False)
print("File saved as emp.xlsx")