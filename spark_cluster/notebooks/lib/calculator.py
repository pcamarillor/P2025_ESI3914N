class Calculator:
    #def __init__(self):


    def add(self, a, b):
        return a + b
    
    def subtract(self, a, b):
        return a - b

    def multiply(self, a, b):
        return a * b
    
    def divide(self, a, b):
        if b != 0:
            return a / b
        else:
            raise ValueError("Cannot divide by zero")
    
    execute_operation = {
        "add": add,
        "subtract": subtract,
        "multiply": multiply,
        "divide": divide
    }