class VariableExpression:
    def __init__(self, name, value):
        self.name = name
        self.value = value

    def __repr__(self):
        return f"{self.value}"

    def evaluate(self, context):
        return self.value
