import pandas as pd

import main
from main import run
from unittest import TestCase


class TestBlackBox(TestCase):
    def test_black_box(self):
        input = 'io/input.csv'
        expected_output = pd.read_csv('io/output.csv')
        run(input)
        output = pd.read_csv('../output/results-00000-of-00001')
        pd.testing.assert_frame_equal(output, expected_output)


class TestUnit(TestCase):
    def test_filter_results(self):
        input = 'io/filter_input.csv'
        input_df = pd.read_csv(input)
        output = main.filter_results(input_df)
        expected_output = pd.read_csv('io/filter_output.csv')
        self.assertEqual(len(output), len(expected_output))