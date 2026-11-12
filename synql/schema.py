"""Schema description: relations, unique keys and foreign keys with deletion semantics."""
from dataclasses import dataclass


@dataclass(frozen=True)
class ForeignKey:
    field: str
    ref: str                     # referenced relation
    on_delete: str = "RESTRICT"  # RESTRICT -> add-win (abort), CASCADE -> remove-win


@dataclass(frozen=True)
class Relation:
    name: str
    fields: tuple
    unique: tuple = ()           # fields forming the (single) unique key
    foreign_keys: tuple = ()

    def ddl(self):
        fks = {fk.field: fk for fk in self.foreign_keys}
        cols = ["id INTEGER PRIMARY KEY AUTOINCREMENT"]   # local, non-replicated auto id
        for f in self.fields:
            if f in fks:
                fk = fks[f]
                cols.append(f'"{f}" INTEGER NOT NULL REFERENCES "{fk.ref}"(id) ON DELETE {fk.on_delete}')
            else:
                cols.append(f'"{f}" TEXT NOT NULL')
        if self.unique:
            cols.append("UNIQUE(" + ",".join(f'"{f}"' for f in self.unique) + ")")
        return f'CREATE TABLE IF NOT EXISTS "{self.name}" ({", ".join(cols)})'


class Schema:
    def __init__(self, *relations):
        self.relations = {r.name: r for r in relations}

    def __getitem__(self, name):
        return self.relations[name]

    def ordered(self):
        """Relations sorted parents-first (cyclic FKs are not supported)."""
        out, seen = [], set()

        def visit(r):
            if r.name in seen:
                return
            seen.add(r.name)
            for fk in r.foreign_keys:
                visit(self.relations[fk.ref])
            out.append(r)

        for r in self.relations.values():
            visit(r)
        return out


def contest_schema():
    """The running example of the paper (Figures 4-22)."""
    return Schema(
        Relation("player", ("name",), unique=("name",)),
        Relation("contest", ("name",), unique=("name",)),
        Relation("game", ("contest",),
                 foreign_keys=(ForeignKey("contest", "contest", "CASCADE"),)),
        Relation("enrolled", ("player", "contest"),
                 foreign_keys=(ForeignKey("player", "player", "RESTRICT"),
                               ForeignKey("contest", "contest", "RESTRICT"))),
    )
