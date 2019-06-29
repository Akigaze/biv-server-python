def retrieve_request_file(request, name):
    if request and name:
        return request.files.get(name)
