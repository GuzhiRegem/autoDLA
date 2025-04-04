## `primary_key (UUID)`
### Class Methods
- > #### **generate()** -> `primary_key`
> Creates a new instance of primary_key with a generated random value
### Static Methods
- > #### **auto_increment(`**kwargs: dict`)** -> `field`
> Defines a new field with the `default_factory` set as `primary_key.generate()`