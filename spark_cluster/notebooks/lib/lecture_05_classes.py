import math

class Book:
  def __init__(self, title, author):
      self.title = title
      self.author = author

  def __str__(self):
      return f"{self.title} by {self.author}"

  def __repr__(self):
      return f"Book('{self.title}', '{self.author}')"
  
class MathUtils:
  @staticmethod
  def add(x, y):
      return x + y

class Dog:
  species = "Canis familiaris"

  @classmethod
  def get_species(cls):
      return cls.species

# Base class
class Figure:
    def __init__(self, name):
        self.name = name
    
    def area(self):
        raise NotImplementedError("Subclasses must implement this method")
    
    def perimeter(self):
        raise NotImplementedError("Subclasses must implement this method")
    
    def display(self):
        print(f"Figure: {self.name}")
        print(f"Area: {self.area()}")
        print(f"Perimeter: {self.perimeter()}")

# Inherited class for Circle
class Circle(Figure):
    def __init__(self, radius):
        super().__init__("Circle")
        self._radius = radius
    
    @property
    def radius(self):
        return self._radius
    
    @radius.setter
    def radius(self, value):
        if value < 0:
            raise ValueError("Radius cannot be negative")
        self._radius = value

    def __repr__(self):
        return f"Circle(Radius:'{self._radius}')"
    

    def area(self):
        return math.pi * self._radius ** 2
    
    def perimeter(self):
        return 2 * math.pi * self._radius

# Inherited class for Triangle
class Triangle(Figure):
    def __init__(self, side_a, side_b, side_c):
        super().__init__("Triangle")
        self.side_a = side_a
        self.side_b = side_b
        self.side_c = side_c
    
    def area(self):
        # Using Heron's formula to calculate the area
        s = (self.side_a + self.side_b + self.side_c) / 2
        return math.sqrt(s * (s - self.side_a) * (s - self.side_b) * (s - self.side_c))
    
    def perimeter(self):
        return self.side_a + self.side_b + self.side_c

# Inherited class for Rectangle
class Rectangle(Figure):
    def __init__(self, width, height):
        super().__init__("Rectangle")
        self.width = width
        self.height = height
    
    def area(self):
        return self.width * self.height
    
    def perimeter(self):
        return 2 * (self.width + self.height)
    
class Person:
  def __init__(self, name, age):
      self.name = name
      self.age = age

  def introduce(self):
      print(f"Hi, I'm {self.name} and I'm {self.age} years old.")