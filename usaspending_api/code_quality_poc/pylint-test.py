import random


def too_many_args(one, two, three, four, five, six, seven, eight, nine, ten):
    return sum([one, two, three, four, five, six, seven, eight, nine, ten])

class TooManyAttributesAndMethods:

    def __init__(self):
        self.one = 1
        self.two = 2
        self.three = 3
        self.four = 4
        self.five = 5
        self.six = 6
        self.seven = 7
        self.eight = 8
        self.nine = 9
        self.ten = 10
        self.eleven = 11
        self.twelve = 12
        self.thirteen = 13
        self.fourteen = 14
        self.fifteen = 15
        self.sixteen = 16
        self.seventeen = 17
        self.eighteen = 18
        self.nineteen = 19
        self.twenty = 20
        self.twentyone = 30

    def one(self):
        return self.one
    def two(self):
        return self.two
    def three(self):
        return self.three
    def four(self):
        return self.four
    def five(self):
        return self.five
    def six(self):
        return self.six
    def seven(self):
        return self.seven
    def eight(self):
        return self.eight
    def nine(self):
        return self.nine
    def ten(self):
        return self.ten
    def eleven(self):
        return self.eleven
    def twelve(self):
        return self.twelve
    def thirteen(self):
        return self.thirteen
    def fourteen(self):
        return self.fourteen
    def fifteen(self):
        return self.fifteen
    def sixteen(self):
        return self.sixteen
    def seventeen(self):
        return self.seventeen
    def eighteen(self):
        return self.eighteen
    def nineteen(self):
        return self.nineteen
    def twenty(self):
        return self.twenty
    def twentyone(self):
        return self.twentyone

    def bad_method(self):
        if self.one == 1:
            if self.two == 2:
                if self.three == 3:
                    if self.four == 4:
                        if self.five == 5:
                            return "foo"


    def too_many_returns(self):
        num = random.randint(1, 21)
        if self.one == 1:
            return self.one
        if self.two == 2:
            return self.two
        if self.three == 3:
            return self.three
        if self.four == 4:
            return self.four
        else:
            return self.five
