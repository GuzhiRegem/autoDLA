import pytest
from autodla import Object, primary_key
from autodla.dbs import MemoryDB
from autodla.utils import DataGenerator

class User(Object):
    id: primary_key = primary_key.auto_increment()
    name: str
    age: int

class Team(Object):
    id: primary_key = primary_key.auto_increment()
    participants: list[User]
    group_name: str

@pytest.fixture
def db(monkeypatch):
    from autodla.engine import object as engine_object
    from autodla.engine.lambda_conversion import lambda_to_sql
    original_init = engine_object.Table.__init__
    original_update = engine_object.Table.update

    def init(self, table_name: str, schema: dict, db: MemoryDB | None = None):
        self.table_name = table_name
        self.schema = schema
        if db:
            engine_object.Table.set_db(self, db)

    def tbl_update(self, l_func, data):
        alias = "".join(self.table_name.split("."))
        where_st = lambda_to_sql(self.schema, l_func, self.db.data_transformer, alias=alias)
        update_data = {f'{key}': value for key, value in data.items()}
        qry = self.db.query.update(self.table_name, where=where_st, values=update_data)
        return self.db.execute(qry)

    monkeypatch.setattr(engine_object.Table, "__init__", init)
    monkeypatch.setattr(engine_object.Table, "update", tbl_update)
    db = MemoryDB()
    db.attach([User, Team])
    yield db
    User.delete_all()
    Team.delete_all()
    monkeypatch.setattr(engine_object.Table, "__init__", original_init)
    monkeypatch.setattr(engine_object.Table, "update", original_update)


def test_create_and_retrieve_user(db):
    user = User.new(name="Alice", age=25)
    assert isinstance(user.id, primary_key)
    users = User.all(limit=None)
    assert len(users) == 1
    assert users[0] is user


def test_filter_users(db):
    u1 = User.new(name="A", age=20)
    u2 = User.new(name="B", age=30)
    res = User.filter(lambda x: x.age >= 25, limit=None)
    assert res == [u2]


def test_update_and_delete_user(db):
    user = User.new(name="John", age=20)
    user.update(age=21)
    assert User.get_by_id(user.id).age == 21
    user.delete()
    assert User.all(limit=None) == []


def test_group_relationship(db):
    u1 = User.new(name=DataGenerator.name(), age=DataGenerator.age())
    grp = Team.new(participants=[u1], group_name="Group1")
    groups = Team.all(limit=None)
    assert groups[0].participants[0] is u1
