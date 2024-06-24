from unittest.mock import MagicMock

import pytest

from auto_sizing.export_json import build_target_key_from_recipe, parse_recipe_from_slug


@pytest.fixture
def manifest_toml():
    fixture = MagicMock()
    r3 = '{"locale": "(\'EN-US\')", "release_channel": "release", "country": "all", "user_type": "new"}'  # noqa: E501
    r6 = '{"locale": "(\'EN-US\', \'EN-CA\', \'EN-GB\')", "release_channel": "release", "country": "US", "user_type": "new"}'  # noqa: E501
    target_manifest = {
        "argo_target_0": {
            "app_id": "firefox_desktop",
            "target_recipe": r3,
        },
        "argo_target_1": {
            "app_id": "firefox_desktop",
            "target_recipe": r6,
        },
    }
    fixture.return_value = target_manifest
    return fixture


def test_parse_recipe_from_slug(monkeypatch, manifest_toml):
    # test some static targets that match what we expect the manifest to contain
    monkeypatch.setattr("toml.load", manifest_toml)

    target_slug = "argo_target_0"

    recipe_info = parse_recipe_from_slug(target_slug)
    assert recipe_info.get("app_id") == "firefox_desktop"
    assert recipe_info.get("locale") == "('EN-US')"
    assert recipe_info.get("channel") == "release"
    assert recipe_info.get("country") == "all"

    target_slug = "argo_target_1"

    recipe_info = parse_recipe_from_slug(target_slug)
    assert recipe_info.get("app_id") == "firefox_desktop"
    assert recipe_info.get("locale") == "('EN-US', 'EN-CA', 'EN-GB')"
    assert recipe_info.get("channel") == "release"
    assert recipe_info.get("country") == "US"


def test_parse_recipe_from_slug_manifest():
    # grab target 0 from actual manifest
    target_slug = "argo_target_0"

    recipe_info = parse_recipe_from_slug(target_slug)
    assert recipe_info.get("app_id") == "firefox_desktop"
    assert recipe_info.get("locale") == "('EN-US')"
    assert recipe_info.get("channel") == "release"
    assert recipe_info.get("country") == "US"


def test_build_target_key_from_recipe(monkeypatch, manifest_toml):
    # test some static targets that match what we expect the manifest to contain
    monkeypatch.setattr("toml.load", manifest_toml)
    target_slug = "argo_target_0"
    recipe_info = parse_recipe_from_slug(target_slug)

    target_key = build_target_key_from_recipe(recipe_info)

    expected_target_key = "firefox_desktop:release:['EN-US']:all"

    assert target_key == expected_target_key

    target_slug = "argo_target_1"
    recipe_info = parse_recipe_from_slug(target_slug)

    target_key = build_target_key_from_recipe(recipe_info)

    expected_target_key = "firefox_desktop:release:['EN-CA','EN-GB','EN-US']:US"

    assert target_key == expected_target_key


def test_build_target_key_from_recipe_manifest():
    # grab target 0 from actual manifest
    target_slug = "argo_target_0"
    recipe_info = parse_recipe_from_slug(target_slug)

    target_key = build_target_key_from_recipe(recipe_info)

    expected_target_key = "firefox_desktop:release:['EN-US']:US"

    assert target_key == expected_target_key
