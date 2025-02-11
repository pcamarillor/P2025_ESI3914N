class Calculator:
    def add(self, x , y):
        return x + y

    def substract(self, x , y):
        return x - y

    def multiply(self, x , y):
        return x * y

    def divide(self, dividend, divider):
        if divider == 0:
            raise ValueError("No se puede divir entre 0")
        return dividend // divider
