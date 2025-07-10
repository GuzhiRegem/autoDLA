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

class Item(Object):
    id: primary_key = primary_key.auto_increment()
    name: str
    tags: list[str]

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
    db.attach([User, Team, Item])
    yield db
    User.delete_all()
    Team.delete_all()
    Item.delete_all()
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


def test_history_tracks_updates(db):
    user = User.new(name="Hist", age=20)
    user.update(age=21)
    history = user.history()
    assert len(history["self"]) == 2
    operations = [row["DLA_operation"] for row in history["self"]]
    assert operations[0] == "INSERT" and operations[1] == "UPDATE"


def test_get_table_res_only_current(db):
    user = User.new(name="Table", age=22)
    user.update(age=23)
    current = User.get_table_res(limit=None)
    full = User.get_table_res(limit=None, only_current=False, only_active=False)
    assert len(current.to_dicts()) == 1
    assert len(full.to_dicts()) == 2


def test_list_value_dependency(db):
    item = Item.new(name="Item1", tags=["a", "b"])
    item.update(tags=["a", "c"])
    assert item.tags == ["a", "c"]
    hist = item.history()
    assert len(hist["self"]) == 2
    assert len(hist["dependencies"]["tags"]) == 4


def test_delete_preserves_history(db):
    user = User.new(name="Del", age=40)
    user.delete()
    assert User.all(limit=None) == []
    hist = user.history()
    assert len(hist["self"]) == 2
    assert hist["self"][1]["DLA_operation"] == "DELETE"
    assert hist["self"][1]["DLA_is_active"] == 0


def test_complex_filtering(db):
    User.new(name="Alice", age=32)
    User.new(name="Bob", age=26)
    User.new(name="Charlie", age=40)
    res = User.filter(lambda x: (x.age >= 30 or x.age <= 20) and (x.age != 26), limit=None)
    assert len(res) == 2
    names = sorted([u.name for u in res])
    assert names == ["Alice", "Charlie"]


def test_object_identity_consistency(db):
    user = User.new(name="Ident", age=50)
    from_all = User.all(limit=None)[0]
    by_id = User.get_by_id(user.id)
    assert from_all is user
    assert by_id is user


def test_team_update_participants_history(db):
    u1 = User.new(name="U1", age=20)
    u2 = User.new(name="U2", age=22)
    team = Team.new(participants=[u1], group_name="G1")
    team.update(participants=[u1, u2])
    assert team.participants == [u1, u2]
    hist = team.history()
    assert len(hist["self"]) == 2
    assert len(hist["dependencies"]["participants"]) == 3
