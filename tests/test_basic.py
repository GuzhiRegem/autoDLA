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


db = MemoryDB()
db.attach([User, Team, Item])


def clean_db(func):
    def wrapper(*args, **kwargs):
        db.clean_db(DO_NOT_ASK=True)
        return func(*args, **kwargs)
    return wrapper


@clean_db
def test_create_and_retrieve_user():
    user = User.new(name="Alice", age=25)
    assert isinstance(user, User)
    assert isinstance(user.id, primary_key)
    users = User.all(limit=None)
    assert len(users) == 1
    assert users[0] is user


@clean_db
def test_filter_users():
    u1 = User.new(name="A", age=20)
    u2 = User.new(name="B", age=30)
    assert User.filter(lambda x: x.age <= 25, limit=None) == [u1]
    assert User.filter(lambda x: x.age >= 25, limit=None) == [u2]


@clean_db
def test_update_and_delete_user():
    user = User.new(name="John", age=20)
    user.update(age=21)
    assert user.age == 21
    user.delete()
    assert User.all(limit=None) == []


@clean_db
def test_group_relationship():
    u1 = User.new(name=DataGenerator.name(), age=DataGenerator.age())
    grp = Team.new(participants=[u1], group_name="Group1")
    assert grp.participants[0] is u1


@clean_db
def test_history_tracks_updates():
    user = User.new(name="Hist", age=20)
    user.update(age=21)
    history = user.history()
    assert len(history["self"]) == 2
    operations = [row["dla_operation"] for row in history["self"]]
    assert operations[0] == "INSERT" and operations[1] == "UPDATE"


@clean_db
def test_get_table_res_only_current():
    user = User.new(name="Table", age=22)
    user.update(age=23)
    current = User.get_table_res(limit=None)
    full = User.get_table_res(limit=None, only_current=False, only_active=False)
    assert len(current.to_dicts()) == 1
    assert len(full.to_dicts()) == 2


@clean_db
def test_list_value_dependency():
    item = Item.new(name="Item1", tags=["a", "b"])
    item.update(tags=["a", "c"])
    assert item.tags == ["a", "c"]
    hist = item.history()
    assert len(hist["self"]) == 2
    assert len(hist["tags"]) == 4


@clean_db
def test_delete_preserves_history():
    user = User.new(name="Del", age=40)
    user.delete()
    assert User.all(limit=None) == []
    hist = user.history()
    assert len(hist["self"]) == 2
    assert hist["self"][1]["dla_operation"] == "DELETE"
    assert hist["self"][1]["dla_is_active"] == 0


@clean_db
def test_complex_filtering():
    User.new(name="Alice", age=32)
    User.new(name="Bob", age=26)
    User.new(name="Charlie", age=40)
    res = User.filter(lambda x: (x.age >= 30 or x.age <= 20) and (x.age != 26), limit=None)
    assert len(res) == 2
    names = sorted([u.name for u in res])
    assert names == ["Alice", "Charlie"]


@clean_db
def test_object_identity_consistency():
    user = User.new(name="Ident", age=50)
    from_all = User.all(limit=None)[0]
    by_id = User.get_by_id(user.id)
    assert from_all is user
    assert by_id is user


@clean_db
def test_team_update_participants_history():
    u1 = User.new(name="U1", age=20)
    u2 = User.new(name="U2", age=22)
    team = Team.new(participants=[u1], group_name="G1")
    team.update(participants=[u1, u2])
    assert team.participants == [u1, u2]
    hist = team.history()
    assert len(hist["self"]) == 2
    assert len(hist["participants"]) == 3
