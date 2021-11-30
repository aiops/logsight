import requests


def make_request(error_message):
    print("Searching for " + error_message)
    resp = requests.get(
        "https://api.stackexchange.com/" + "2.2/search?order=desc&sort=activity&intitle={}&site=stackoverflow".format(
            error_message))
    return resp.json()


def get_urls(json_dict):
    for i in json_dict['items']:
        if i["is_answered"]:
            return i["link"]
    return "null"


def get_stackoverflow_link(error_message):
    result_json = make_request(error_message)
    return get_urls(result_json)
