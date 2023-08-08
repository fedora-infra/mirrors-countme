# Maintenance

## Changing things

### Commits

Changes should be split into meaningful “topical” commits, with informative commit logs (see
[here](https://cbea.ms/git-commit/#seven-rules) for an outline), and signed off.

Tests should be added or updated with the commits that introduce or change code.

## Roll a Release

### Bump the Version

The version of `mirrors-countme` is maintained in `pyproject.toml`, either adjust it there or use
`poetry version [patch/minor/major]` to bump it (according to the nature of changes).

Commit this change using a commit log message of e.g. “Version 0.1.2”. Don’t push upstream yet!

### Build Installable Files

Use `poetry build` to roll a tarball and Python wheel file for the new version. They will be placed
in the `dist/` directory.

### Test the New Version

Do any tests which require an installable archive file, e.g. smoke tests like “are all expected
files contained in the archive”.

### Tag the New Version

If the previous tests are successful, create a GPG-signed tag for the previously created commit,
using the version number as the tag name:

```
git tag --sign 0.1.2
```

### Publish the New Version

#### Push Changes Upstream

Push the new version and tag upstream:

```
git push origin main 0.1.2
```

#### Create an Upstream Release on GitHub

1. Open the list of tags on GitHub: https://github.com/fedora-infra/mirrors-countme/tags
2. In the `…` menu to the right of the tag in question, click on “Create release”.
3. Use e.g. “Version 0.1.2” as the title of the release.
4. If applicable, describe important changes in the description field.
5. Attach the tarball and Python wheel file for the release by clicking the region on the page
   labelled “Attach binaries by dropping them here or selecting them.” and selecting them, or
   dragging and dropping them there from a file manager.

#### Build the Release as an RPM Package in Fedora

Build the [`python-mirrors-countme` RPM
package](https://src.fedoraproject.org/rpms/python-mirrors-countme) for the new version for Fedora
and EPEL, and submit updates. Refer to the [Fedora Package Maintenance
Guide](https://docs.fedoraproject.org/en-US/package-maintainers/Package_Maintenance_Guide/) for
details.
