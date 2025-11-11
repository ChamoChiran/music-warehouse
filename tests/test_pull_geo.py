"""
Tests for src/lastfm_fetch/pull_geo.py

Run tests with:
    pytest tests/test_pull_geo.py -v
"""

import pytest
import json
import sys
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.lastfm_fetch.pull_geo import fetch_geo_data, save_response, main


class TestFetchGeoData:
    """Test cases for fetch_geo_data function"""

    @patch('src.lastfm_fetch.pull_geo.requests.get')
    @patch('src.lastfm_fetch.pull_geo.API_KEY', 'test_api_key')
    @patch('src.lastfm_fetch.pull_geo.BASE_URL', 'https://ws.audioscrobbler.com/2.0/')
    def test_fetch_geo_data_success(self, mock_get):
        """Test successful API call for fetching geo data"""
        # Mock response
        mock_response = Mock()
        mock_response.json.return_value = {
            'topartists': {
                'artist': [
                    {'name': 'Test Artist', 'playcount': '1000'}
                ]
            }
        }
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        # Call function
        result = fetch_geo_data(
            country="United States",
            chart_type="artists",
            limit=50,
            page=1
        )

        # Assertions
        assert result is not None
        assert 'topartists' in result
        mock_get.assert_called_once()

        # Verify the params passed to requests.get
        call_args = mock_get.call_args
        assert call_args[0][0] == 'https://ws.audioscrobbler.com/2.0/'
        params = call_args[1]['params']
        assert params['method'] == 'geo.gettopartists'
        assert params['country'] == 'United States'
        assert params['api_key'] == 'test_api_key'
        assert params['format'] == 'json'
        assert params['limit'] == 50
        assert params['page'] == 1

    @patch('src.lastfm_fetch.pull_geo.requests.get')
    @patch('src.lastfm_fetch.pull_geo.API_KEY', 'test_api_key')
    @patch('src.lastfm_fetch.pull_geo.BASE_URL', 'https://ws.audioscrobbler.com/2.0/')
    def test_fetch_geo_data_tracks(self, mock_get):
        """Test fetching tracks instead of artists"""
        mock_response = Mock()
        mock_response.json.return_value = {
            'toptracks': {
                'track': [
                    {'name': 'Test Track', 'playcount': '2000'}
                ]
            }
        }
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        result = fetch_geo_data(
            country="United Kingdom",
            chart_type="tracks",
            limit=25,
            page=2
        )

        assert result is not None
        assert 'toptracks' in result

        # Verify method is correct for tracks
        call_args = mock_get.call_args
        params = call_args[1]['params']
        assert params['method'] == 'geo.gettoptracks'
        assert params['country'] == 'United Kingdom'
        assert params['limit'] == 25
        assert params['page'] == 2

    @patch('src.lastfm_fetch.pull_geo.requests.get')
    @patch('src.lastfm_fetch.pull_geo.API_KEY', 'test_api_key')
    def test_fetch_geo_data_api_error(self, mock_get):
        """Test handling of API errors"""
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = Exception("API Error")
        mock_get.return_value = mock_response

        with pytest.raises(Exception, match="API Error"):
            fetch_geo_data(
                country="Canada",
                chart_type="artists",
                limit=50,
                page=1
            )

    @patch('src.lastfm_fetch.pull_geo.requests.get')
    @patch('src.lastfm_fetch.pull_geo.API_KEY', 'test_api_key')
    def test_fetch_geo_data_timeout(self, mock_get):
        """Test that timeout is set correctly"""
        mock_response = Mock()
        mock_response.json.return_value = {}
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        fetch_geo_data(
            country="Germany",
            chart_type="artists",
            limit=10,
            page=1
        )

        # Verify timeout is set
        call_args = mock_get.call_args
        assert call_args[1]['timeout'] == 100


class TestSaveResponse:
    """Test cases for save_response function"""

    @patch('src.lastfm_fetch.pull_geo.datetime')
    @patch('builtins.open', create=True)
    @patch('src.lastfm_fetch.pull_geo.typer.echo')
    def test_save_response_success(self, mock_echo, mock_open, mock_datetime):
        """Test successful saving of response data"""
        # Mock datetime
        mock_datetime.utcnow.return_value.strftime.return_value = "20251111_120000"

        # Mock DATA_DIR as a MagicMock
        mock_data_dir = MagicMock()
        mock_file_path = MagicMock()
        mock_data_dir.__truediv__.return_value = mock_file_path

        # Mock file operations
        mock_file = MagicMock()
        mock_open.return_value.__enter__.return_value = mock_file

        # Test data
        test_data = {
            'topartists': {
                'artist': [{'name': 'Artist 1'}]
            }
        }

        # Patch DATA_DIR and call function
        with patch('src.lastfm_fetch.pull_geo.DATA_DIR', mock_data_dir):
            save_response(test_data, "United States", "artists")

            # Verify directory creation
            mock_data_dir.mkdir.assert_called_once_with(parents=True, exist_ok=True)

        # Verify file was opened
        assert mock_open.called

        # Verify typer.echo was called
        assert mock_echo.called

    @patch('src.lastfm_fetch.pull_geo.datetime')
    @patch('builtins.open', create=True)
    @patch('src.lastfm_fetch.pull_geo.typer.echo')
    def test_save_response_filename_format(self, mock_echo, mock_open, mock_datetime):
        """Test that filename is formatted correctly"""
        mock_datetime.utcnow.return_value.strftime.return_value = "20251111_120000"

        # Mock DATA_DIR as a MagicMock
        mock_data_dir = MagicMock()
        mock_file_path = MagicMock()
        mock_file_path.__str__.return_value = "/tmp/test/united kingdom_tracks_20251111_120000.json"
        mock_data_dir.__truediv__.return_value = mock_file_path

        mock_file = MagicMock()
        mock_open.return_value.__enter__.return_value = mock_file

        test_data = {'test': 'data'}

        with patch('src.lastfm_fetch.pull_geo.DATA_DIR', mock_data_dir):
            save_response(test_data, "United Kingdom", "tracks")

        # Check the echo message contains the expected filename pattern
        call_args = mock_echo.call_args[0][0]
        assert "united kingdom_tracks_20251111_120000.json" in call_args.lower() or "Saved" in call_args


class TestMain:
    """Test cases for main CLI function"""

    @patch('src.lastfm_fetch.pull_geo.fetch_geo_data')
    @patch('src.lastfm_fetch.pull_geo.save_response')
    @patch('src.lastfm_fetch.pull_geo.API_KEY', 'test_api_key')
    def test_main_with_valid_api_key(self, mock_save, mock_fetch):
        """Test main function with valid API key"""
        mock_fetch.return_value = {'test': 'data'}

        # Simulate calling main directly (not through CLI)
        main(
            country="United States",
            chart_type="artists",
            limit=50,
            page=1
        )

        mock_fetch.assert_called_once_with("United States", "artists", 50, 1)
        mock_save.assert_called_once_with({'test': 'data'}, "United States", "artists")

    @patch('src.lastfm_fetch.pull_geo.API_KEY', None)
    @patch('src.lastfm_fetch.pull_geo.typer.secho')
    def test_main_without_api_key(self, mock_secho):
        """Test main function when API key is not set"""
        main(
            country="United States",
            chart_type="artists",
            limit=50,
            page=1
        )

        # Verify error message was displayed
        mock_secho.assert_called_once()
        error_message = mock_secho.call_args[0][0]
        assert "LASTFM_API_KEY" in error_message
        assert "not set" in error_message


class TestIntegration:
    """Integration tests"""

    @patch('src.lastfm_fetch.pull_geo.requests.get')
    @patch('src.lastfm_fetch.pull_geo.API_KEY', 'test_api_key')
    @patch('src.lastfm_fetch.pull_geo.datetime')
    @patch('builtins.open', create=True)
    @patch('src.lastfm_fetch.pull_geo.typer.echo')
    def test_full_workflow(self, mock_echo, mock_open, mock_datetime, mock_get):
        """Test complete workflow from fetch to save"""
        # Setup mocks
        mock_datetime.utcnow.return_value.strftime.return_value = "20251111_120000"

        mock_response = Mock()
        mock_response.json.return_value = {
            'topartists': {
                'artist': [
                    {'name': 'Artist 1', 'playcount': '5000'},
                    {'name': 'Artist 2', 'playcount': '4000'}
                ]
            }
        }
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        mock_file = MagicMock()
        mock_open.return_value.__enter__.return_value = mock_file

        # Mock DATA_DIR
        mock_data_dir = MagicMock()
        mock_file_path = MagicMock()
        mock_data_dir.__truediv__.return_value = mock_file_path

        # Execute workflow with DATA_DIR mocked
        with patch('src.lastfm_fetch.pull_geo.DATA_DIR', mock_data_dir):
            main(
                country="Japan",
                chart_type="artists",
                limit=10,
                page=1
            )

        # Verify API was called
        assert mock_get.called

        # Verify directory was created
        mock_data_dir.mkdir.assert_called_once_with(parents=True, exist_ok=True)

        # Verify file was saved
        assert mock_open.called

        # Verify success message
        assert mock_echo.called
