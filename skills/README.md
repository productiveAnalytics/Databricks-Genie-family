# Skills

Reusable user skills for Genie Code, version-controlled in this repo. Each skill lives in its own folder and is defined by a `SKILL.md` file: YAML frontmatter (`name`, `description`) plus a markdown body of instructions.

## Skill reference

| Skill | What it does | Trigger |
| --- | --- | --- |
| [medallion-from-raw](medallion-from-raw/SKILL.md) | Builds and runs a bronze → silver → gold medallion Lakeflow Spark Declarative Pipeline from raw source tables: preflight checks, one Python file per dataset as batch materialized views, dry-run then full update, and row-count verification. | User provides a source schema, a target publish schema, and a short description of the gold tables they want. |

## Adding a new skill

1. Create `skills/<skill-name>/SKILL.md` here, with `name` (kebab-case) and a specific, action-oriented `description` (this is what Genie Code matches on).
2. Add a row to the table above.
3. Copy the folder to the live skills location `/Workspace/Users/lalitstar@gmail.com/.assistant/skills/` so Genie Code picks it up.

## Sync with workspace

The live copies Genie Code reads are at `/Users/lalitstar@gmail.com/.assistant/skills/`. This repo is the source of truth — after editing a skill here, copy it to the live location (and vice versa) to keep them in sync.
