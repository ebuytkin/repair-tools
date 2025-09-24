import unittest
from unittest.mock import patch, MagicMock
import os
import shutil
from pathlib import Path

import repair_tools.move_reingest as move_reingest

class TestMoveScript(unittest.TestCase):

    def setUp(self):
        # create temp root dir
        self.test_root = Path("./temp_test_area")
        self.test_root.mkdir(exist_ok=True)

        # define temp source & dest paths
        self.source_path = self.test_root / "source"
        self.destination_path = self.test_root / "destination"
        self.source_path.mkdir()
        self.destination_path.mkdir()

        # create fake dir structure to search within
        # temp nested path: source/level1/level2/
        nested_path = self.source_path / "level1" / "level2"
        nested_path.mkdir(parents=True)

        # create exptected dirs
        (nested_path / "123456").mkdir() # 1 unique directory
        (self.source_path / "789012").mkdir() # dupl at top lovel
        (nested_path / "789012").mkdir() # dupl in nested level

        # create dir that already exists to test conflicts
        (self.destination_path / "654321").mkdir()
        (self.source_path / "654321").mkdir()


    def tearDown(self):
        shutil.rmtree(self.test_root)

    @patch('move_reingest.shutil.move')
    @patch('move_reingest.DESTINATION_PATH')
    @patch('move_reingest.SOURCE_PATH')
    def test_find_and_move_single_directory(self, mock_source, mock_dest, mock_move):
        # point constants to temp paths
        mock_source.return_value = self.source_path
        mock_dest.return_value = self.destination_path

        # override list of dirs to find
        move_reingest.DIRS_TO_FIND = {"123456"}

        move_reingest.main()

        # shutil.move called only once?
        mock_move.assert_called_once()
        
        # called w/ correct source and dest paths?
        expected_source = self.source_path / "level1" / "level2" / "123456"
        expected_destination = self.destination_path / "123456"
        mock_move.assert_called_with(expected_source, expected_destination)

    @patch('move_reingest.shutil.move')
    @patch('move_reingest.DESTINATION_PATH')
    @patch('move_reingest.SOURCE_PATH')
    def test_handles_duplicates_and_conflicts(self, mock_source, mock_dest, mock_move):

        mock_source.return_value = self.source_path
        mock_dest.return_value = self.destination_path

        # search for dupl (789012) & conflict (654321)
        move_reingest.DIRS_TO_FIND = {"789012", "654321"}

        move_reingest.main()

        # shutil.move called twice? (for 789012)
        # NOT for 654321 because == conflict.
        self.assertEqual(mock_move.call_count, 2)

        # check args of calls
        calls = mock_move.call_args_list
        
        expected_source1 = self.source_path / "789012"
        expected_source2 = self.source_path / "level1" / "level2" / "789012"
        expected_destination = self.destination_path / "789012"
        
        # move calls w/ the correct dest?
        self.assertIn(((expected_source1, expected_destination),), calls)
        self.assertIn(((expected_source2, expected_destination),), calls)


if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)
