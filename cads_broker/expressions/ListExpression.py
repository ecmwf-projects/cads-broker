class ListExpression:
    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return repr(self.value)

    def evaluate(self, context):
        return [item.evaluate(context) for item in self.value]
