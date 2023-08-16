# from fastapi import FastAPI
# from pydantic import BaseModel
# from typing import List

# app = FastAPI()

# class Item(BaseModel):
#     name: str
#     price: float

# class Items(BaseModel):
#     items: List[Item]

# @app.post("/items/")
# async def create_items(items: Items):
#     return {"items": items.items}

from fastapi import FastAPI
from pydantic import BaseModel
from typing import List

app = FastAPI()

class Item(BaseModel):
    sku: str
    name: str

@app.post("/items/")
async def create_items(items: List[Item]):
    # Here, you can process each item in the list of items
    for item in items:
        # Do something with the item, such as adding it to a database
        print(f"Added item {item.sku}: {item.name}")

    # Return a response indicating success
    return {"message": "Items created successfully"}

