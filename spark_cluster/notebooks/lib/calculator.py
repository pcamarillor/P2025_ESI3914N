class Calculator:
    def __init__(self):
        # Dictionary storing operations as functions
        self.operations = {
            "add": self.add,
            "substract": self.substract,
            "multiply": self.multiply,
            "divide": self.divide
        }
    
    def add(self, a, b):
        return a + b
    
    def substract(self, a, b):
        return a - b
    
    def multiply(self, a, b):
        return a * b
    
    def divide(self, a, b):
        if b == 0:
            return "Error: Division by zero"
        return a / b
    
    def execute_operation(self, operation, a, b):
        if operation in self.operations:
            return self.operations[operation](a, b)
        else:
            return f"Error: '{operation}' is not a valid operation"