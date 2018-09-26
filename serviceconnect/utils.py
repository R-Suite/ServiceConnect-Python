import copy


def mergedicts(target, source):
    output = dict(target)
    for k in source:
        if type(source[k]) is dict:
            if k not in target:
                output[k] = source[k]
            else:
                output[k] = mergedicts(target[k], source[k])
        else:
            output[k] = source[k]
    return output
