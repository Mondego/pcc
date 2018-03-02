import os
import hashlib
import cbor
import shutil

class FileStateRecorder(object):
    def __init__(self, base_folder, typename):
        self.foldername = os.path.join(base_folder, typename)
        if not os.path.exists(self.foldername):
            os.makedirs(self.foldername)

        self.obj_to_filestate = dict()

    def check_oid_exists(self, oid):
        if not self.has_obj(oid):
            raise RuntimeError("Object %r not found" % oid)

    def add_next_change(self, oid, version, changes):
        self.check_oid_exists(oid)
        self.obj_to_filestate[oid].add_next_change(version, changes)

    def add_transformation(self, oid, version, transform):
        self.check_oid_exists(oid)
        self.obj_to_filestate[oid].add_transformation(version, transform)

    def get_dim_changes_since(self, oid, prev_version):
        self.check_oid_exists(oid)
        return self.obj_to_filestate[oid].get_dim_changes_since(prev_version)

    def lastkey(self, oid):
        self.check_oid_exists(oid)
        return self.obj_to_filestate[oid].lastkey()

    def delete_obj(self, oid):
        self.check_oid_exists(oid)
        self.obj_to_filestate[oid].delete_obj()
        del self.obj_to_filestate[oid]

    def add_obj(self, oid, version, full_changes):
        if self.has_obj(oid):
            raise RuntimeError("Adding object that is already present %r" % oid)
        self.obj_to_filestate[oid] = FileState(
            self.foldername, oid, version, full_changes)

    def has_obj(self, oid):
        return oid in self.obj_to_filestate

    def get_full_obj(self, oid):
        self.check_oid_exists(oid)
        return self.obj_to_filestate[oid].get_full_obj()

class FileState(object):
    def __init__(self, base_foldername, oid, version, full_changes):
        self.foldername = os.path.join(base_foldername, str(hash(oid)))
        if os.path.exists(self.foldername):
            raise RuntimeError("Folder already exists, cannot create object.")
        self.tail = version
        self.head = version
        self.head_change = {
            "version": version,
            "changes": full_changes,
            "prev_version": None,
            "next_version": None
        }
        os.makedirs(self.foldername)
        filename = os.path.join(self.foldername, str(version))
        cbor.dump(self.head_change, open(filename, "wb"))
        self.full_version = full_changes

    def get_full_obj(self):
        yield self.full_version

    def delete_obj(self):
        shutil.rmtree(self.foldername)

    def lastkey(self):
        return self.head

    def get_dim_changes_since(self, version):
        if version is None:
            yield self.full_version

        filename = os.path.join(self.foldername, str(version))
        if os.path.exists(filename):
            point = cbor.load(open(filename, "rb"))
            while point["next_version"] is not None:
                p_filename = os.path.join(
                    self.foldername, str(point["next_version"]))
                point = cbor.load(open(p_filename, "rb"))
                yield point["changes"]
        else:
            transform_filename = os.path.join(
                self.foldername, "TFORM_{0}".format(version))
            if os.path.exists(transform_filename):
                transforms = cbor.load(open(transform_filename), "rb")
                next_tp = transforms["next_timestamp"]
                yield transforms["transform"]
                for next_change in self.get_dim_changes_since(next_tp):
                    yield next_change

    def add_transformation(self, version, transform):
        transform_filename = os.path.join(
            self.foldername, "TFORM_{0}".format(version))
        cbor.dump(transform, open(transform_filename, "wb"))

    def add_next_change(self, version, change):
        prev_head = self.head
        prev_change = self.head_change
        prev_change["next_version"] = version
        prev_filename = os.path.join(self.foldername, str(prev_head))
        filename = os.path.join(self.foldername, str(version))
        cbor.dump(prev_change, open(prev_filename, "wb"))
        self.head_change = {
            "version": version,
            "changes": change,
            "prev_version": prev_head,
            "next_version": None
        }

        self.head = version
        self.full_version = FileState.merge(self.full_version, change)
        cbor.dump(self.head_change, open(filename, "wb"))

    @staticmethod
    def merge(full_version, change):
        full_version["dims"].update(change.setdefault("dims", dict()))
        return full_version
