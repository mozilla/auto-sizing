from auto_sizing.export_json import build_target_key_from_recipe, parse_recipe_from_slug


def test_parse_recipe_from_slug():
    target_slug = "argo_target_0"
    # [argo_target_0]
    # app_id = "firefox_desktop"
    # target_recipe = "{
    #     \"locale\": \"('EN-US')\",
    #     \"release_channel\": \"('RELEASE')\",
    #     \"country\": \"('US')\",
    #     \"user_type\": \"new\"
    # }"

    recipe_info = parse_recipe_from_slug(target_slug)
    assert recipe_info.get("app_id") == "firefox_desktop"
    assert recipe_info.get("locale") == "EN-US"
    assert recipe_info.get("channel") == "release"
    assert recipe_info.get("country") == "US"


def test_build_target_key_from_recipe():
    target_slug = "argo_target_0"
    recipe_info = parse_recipe_from_slug(target_slug)

    target_key = build_target_key_from_recipe(recipe_info)

    expected_target_key = "firefox_desktop:release:['EN-US']:US"

    assert target_key == expected_target_key
