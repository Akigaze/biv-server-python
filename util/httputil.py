import requests


def retrieve_request_file(request, name):
    if request and name:
        return request.files.get(name)


def form_request(url, form=None):
    if form is None:
        return False
    headers = {
        "Content-Type": "multipart/form-data"
    }
    form_data = dict()
    for key, value in form.items():
        form_data[key] = (None, value)
    return requests.post(url, headers=headers, files=form_data)


def get_request(url, params=None):
    return requests.get(url, params=params)
