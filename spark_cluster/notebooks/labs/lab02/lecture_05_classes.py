class Calculator:
    def __init__(self):
        
        self.op_dict = {
            'add': self.__add,
            'substract': self.__substract,
            'multiply': self.__multiply,
            'divide': self.__divide
        }
    
    def __add(self,var1, var2):
        return var1 + var2
    
    def __substract(self,var1, var2):
        return var1 - var2
    
    def __multiply(self,var1, var2):
        return var1 * var2
    
    def __divide(self,var1, var2):
        if var2 != 0:
            return var1 / var2
        else:
            return "ERROR: can't divide by 0."
        
    def exec_operations(self, key, var1, var2):
        return self.op_dict[key](var1, var2)