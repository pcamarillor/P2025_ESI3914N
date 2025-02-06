class Calculator:
    def __init__(self):
        self.operations = {
            "add": self.add,
            "subtract": self.subtract,
            "multiply": self.multiply,
            "divide": self.divide
        }

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
        
    def execute_operation(self, operation, a, b):
        if operation in self.operations:
            return self.operations[operation](a, b)
        raise ValueError("Operation not supported")