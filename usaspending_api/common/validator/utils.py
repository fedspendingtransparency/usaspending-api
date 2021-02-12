def get_model_by_name(models, name):
    """
    Little helper function to return a TinyShield model from a list of models
    given the model's name.  Returns None if the model was not found.
    """
    for model in models:
        if model.get("name") == name:
            return model
    return None


def update_model_in_list(model_list: list, model_name: str, new_dict: dict, replace: bool = False) -> list:
    """
    This is a generic helper utility function which can update or fully
      replace a TinyShield model in a list of models. Provide the list of
      models, the model name, and the dict of items to update
    """
    original_model, index = next((model, i) for i, model in enumerate(model_list) if model["name"] == model_name)

    if replace:
        original_model = new_dict
    else:
        original_model.update(new_dict)

    del model_list[index]
    model_list.append(original_model)

    return model_list
