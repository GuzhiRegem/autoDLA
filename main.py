from dataclasses import dataclass, fields
from typing import List
from engine.Object import Object
from engine.Engine import Engine

@dataclass(kw_only=True)
class User(Object):
    name : str
    age : int

@dataclass(kw_only=True)
class Group(Object):
    name : str
    users : List[User]
    numbers : List[int]

engine = Engine(classes=[User, Group])
res = User.all()
print(res)

# engine.attach(User)
# engine.attach(Group)

# #Ensure that a user named jhon(18) is in the database and that a group named "jhon_family" has jhon as a member
# jhon = User.get_by(name = "jhon", age = 18, can_be_none = True)
# if jhon is None:
#     jhon = User(name = "jhon", age = 18)
# grp = Group.get_by(name = "jhon_family", can_be_none = True)
# if grp is None:
#     grp = Group(name = "jhon_family", users = [jhon])
# grp.edit(name="jhon_family_2")

# h = grp.get_history()
# print(h)


