# Installation

### Using PyPi
```bash
pip install autodla
```

### From source
```bash
git clone https://github.com/GuzhiRegem/autoDLA.git
cd autoDLA/
pip install .
```

---

### Features

To keep AutoDLA as lightweight as possible, you need to install separatelly the features you need to use, that includes the DataBase connection you are going to use, to install a feature, you can run the installation command as follows:
```bash
pip install autodla[<package_name>]
```
For example, PostgreSQL connection:
```bash
pip install autodla[db-postgres]
```
The complete list of features can be found [here](reference/dependencies_list.md)