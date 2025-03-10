import uuid

class primary_key(str):
    @classmethod
    def generate(cls):
        return cls(str(uuid.uuid4()))
    def is_valid(self):
        try:
            uuid.UUID(self)
            return True
        except ValueError:
            return False