from django.core.urlresolvers import resolve
from django.utils.six.moves.urllib.parse import urlparse


def format_path(path):
    # Do some cleanup on the path
    # Add a starting slash if we don't have it
    if path[0] != "/":
        path = "/{}".format(path)

    # Add a trailing slash if we don't have get parameters and there is no trailing slash
    if '?' not in path and path[-1] != "/":
        path = "{}/".format(path)

    # Prepend with /api/v1 if it's not there
    if path[:7] != "/api/v1":
        path = "/api/v1{}".format(path)

    return path


def create_filename_from_options(path, checksum):
    split_path = [x for x in path.split("/") if len(x) > 0 and x != "api"]
    split_path.append(checksum)

    filename = "{}.csv".format("_".join(split_path))

    return filename


def resolve_path_to_view(request_path):
    '''
    Returns a viewset if the path resolves to a view and if that view supports
    the get queryset function. In any other case, it returns None
    '''
    # Resolve the path to a view
    view, args, kwargs = resolve(urlparse(request_path)[2])

    if not view:
        return None

    # Instantiate the view and pass the request in
    view = view.cls()

    if not hasattr(view, "get_queryset"):
        return None

    return view
