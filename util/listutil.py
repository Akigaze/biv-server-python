def get_sub_list(array, length=-1):
    if length == -1:
        return array
    if length < len(array):
        return array[:length]
    result = []
    for i in range(length):
        item = array[i] if i < len(array) else None
        result.append(item)
    return result

