def impure(cls):
    cls.__pcc_impure__ = True
    return cls