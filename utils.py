def update_dict(target, updater):
    """ Updates a dictionary, keeping the same structure

    Args:
        target (:obj:`dict`): dictionary to update
        updater (:obj:`dict`): dictionary with the new information
    """
    target_keys = list(target.keys())
    for key in list(updater.keys()):
        if key in target_keys:
            if isinstance(target[key], dict):
                update_dict(target[key], updater[key])
            else:
                target[key] = updater[key]