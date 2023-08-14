import unittest
from main import flatten_json


class Exercise4Test(unittest.TestCase):
    def test_flatten_not_nested(self):
        expected = {'a': 1, 'b': 2}
        result = flatten_json({'a': 1, 'b': 2})
        self.assertEqual(result, expected)

    def test_flatten_nested_dict(self):
        result = flatten_json({'a': 1, 'b': 2, 'c': {'a': 'ab', 'b': 'xy'}})
        expected = {'a': 1, 'b': 2, 'c_a': 'ab', 'c_b': 'xy'}
        self.assertEqual(result, expected)

    def test_flatten_nested_list(self):
        # result = flatten_json({'a': 1, 'b': 2, 'c': ['a', 'ab', 'b', 'xy']})
        # expected = {'a': 1, 'b': 2, 'c_0': 'a', 'c_1': 'ab', 'c_2': 'b', 'c_3': 'xy'}
        result = flatten_json({"reclat": "16.883330", "reclong": "-99.900000", "geolocation": {"type": "Point",
                                                                                               "coordinates": [-99.9,
                                                                                                               16.88333]}})
        expected = {"reclat": "16.883330", "reclong": "-99.900000", "geolocation_type": "Point",
                    "geolocation_coordinates_0": -99.9, "geolocation_coordinates_1": 16.88333}

        self.assertEqual(result, expected)

        if __name__ == '__main__':
            unittest.main()
