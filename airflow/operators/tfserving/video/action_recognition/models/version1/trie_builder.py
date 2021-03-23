"""Builder class : Provides an utility function to build Trie
using Action recognition dataset -> 700 classes. Returns a hierarchy tree for all
action classes available in the dataset Action recognition dataset -> 700.
"""


class TrieBuilder:
    """class to make a trie for action classes hierarchy"""

    def __init__(self, class_name):
        self.class_name = class_name
        self.isLeaf = False
        self.children = []

    def insert(self, array):
        node = None
        for i in self.children:
            if i.class_name == array[0]:
                node = i
                break
        else:
            node = TrieBuilder(array[0])
            self.children.append(node)
        if len(array) == 1:
            node.isLeaf = True
        else:
            node.insert(array[1:])

    def get_dict(self):
        d = {"children": dict(), "children_leaf": []}
        for i in self.children:
            if i.isLeaf:
                d["children_leaf"].append(i.class_name)
            else:
                d["children"][i.class_name] = i.get_dict()
        return d if self.class_name != "" else d['children']

    def get_probability(self, probability_dict, level):
        if self.isLeaf:
            try:
                return {self.class_name: probability_dict[self.class_name]}
            except KeyError:
                return dict()
        d = dict()
        for i in self.children:
            res = i.get_probability(probability_dict, level - 1)
            for j in res:
                d[(self.class_name + ', ' if self.class_name != "" else "") + j] = res[j]
        if level <= 0:
            d2 = {self.class_name: 0}
            for i in d:
                d2[self.class_name] += d[i]
            return d2
        return d

    def get_heirarchical_classes(self, probability_dict):
        l1 = self.get_probability(probability_dict, 1)
        l2 = self.get_probability(probability_dict, 2)
        l3 = self.get_probability(probability_dict, 3)
        return l1, l2, l3
