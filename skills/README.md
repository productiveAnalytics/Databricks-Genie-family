# Skills

Reusable user skills for Genie Code, version-controlled in this repo. Each skill lives in its own folder and is defined by a `SKILL.md` file: YAML frontmatter (`name`, `description`) plus a markdown body of instructions.

## Skills v/s Instructions
### Filter the list by User or Workspace
- User skills live in `/Users/<you>/.assistant/skills/`. Only you can use them.
- Workspace skills live in `/Workspace/.assistant/skills/`. Every user in the workspace can use them.
### For rules that should apply to every task, use Instructions instead
- User instructions apply to every conversation you start. Click Add instructions file to create `/Users/<you>/.assistant_instructions.md`, then write your preferences in it.
- Workspace instructions apply to every user in the workspace. Only workspace admins can edit that file, `/Workspace/.assistant_workspace_instructions.md`. When the two files conflict, the workspace file wins

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
