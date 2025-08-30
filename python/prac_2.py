import pandas as pd 

data = [{"id": 1,"name": "siva", "age": 34},
        {"id": 2,"name": "reddy", "age": 35},
        {"id": 3,"name": "meera", "age": 33}
        ]

df = pd.DataFrame(data)
print(df)