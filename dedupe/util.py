'''
Utilities for working with CKAN packages.
'''


def get_package_extra(package, key, default=None):
    '''
    Returns the value of the named key from the extras list.
    '''

    try:
        return next(extra['value'] for extra in package['extras'] if extra['key'] == key)
    except StopIteration:
        return default


def set_package_extra(package, key, value=None):
    '''
    Sets an extra property on the package. If value is None, remove the property.
    This does not call the update API.
    '''
    # Get the list of extras, without the existing property (if it exists)
    extras = [extra for extra in package['extras'] if extra['key'] != key]

    if value:
        extras.append(dict(key=key, value=value))

    package['extras'] = extras
